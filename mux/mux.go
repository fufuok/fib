// Package mux serves several protocols on one port. Its Handler looks at the
// first bytes a peer sends, picks the route whose Matcher recognises them,
// and hands the connection to that route's handler, which then serves it as
// if it had accepted it itself:
//
//	tlsHandler := fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(web))
//	config := fib.DefaultConfig()
//	config.Addr = ":8000"
//	engine, err := fib.Bind(config, mux.New(
//		mux.Route{Match: mux.TLS(), Handler: tlsHandler},
//		mux.Route{Match: mux.HTTP(), Handler: fibhttp.NewHandler(web)}, // HTTP/1.x and h2c
//		mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{})},
//		mux.Route{Match: mux.Prefix("PING"), Handler: pinger},
//	))
//
// The handoff goes through fib.Connection.SetHandler, so once a connection's
// protocol is known the mux is out of its way: the engine calls the chosen
// handler directly, with no dispatch in between, and asks that handler, not
// the mux, whether it takes OnHangup or OnShutdown. The chosen handler's
// OnOpen runs at the handoff, on the worker that read the bytes, followed by
// an OnData with every byte read so far.
//
// Only a protocol whose client speaks first can be told apart this way,
// which TLS, HTTP and most RPC protocols do. A peer that waits for the server
// to speak first, as SMTP, FTP and MySQL clients do, sends nothing to sniff,
// and is closed when Config.Timeout passes; give such a protocol a port of its
// own, with a fib.Binding.
package mux

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
)

// DefaultTimeout is how long a connection may stay silent before the mux
// closes it, when Config.Timeout is zero.
const DefaultTimeout = 10 * time.Second

// DefaultMaxPeek is how many bytes the mux reads at most before it decides,
// when Config.MaxPeek is zero. It is well beyond what any built-in Matcher
// needs: the HTTP/2 preface, the longest of them, is 24 bytes.
const DefaultMaxPeek = 64

// ErrNoMatch is what OnClose is given for a connection that no route
// recognised and that Config.Fallback was not there to take.
var ErrNoMatch = errors.New("mux: no route matches the connection's first bytes")

// Route is a protocol the mux serves: the connections whose first bytes
// Match recognises go to Handler.
type Route struct {
	Match   Matcher
	Handler fib.Handler
}

// Config is how a Handler decides.
type Config struct {
	// Fallback serves the connections that no route recognises. Nil closes
	// them, with ErrNoMatch.
	Fallback fib.Handler
	// MaxPeek is how many bytes the mux reads at most before it decides. A
	// route still waiting for more once that many have arrived counts as not
	// matching. Zero means DefaultMaxPeek.
	MaxPeek int
	// Timeout closes a connection that has not let the mux decide this long
	// after it opened, which bounds a peer that connects and says nothing; the
	// connection's OnClose, which the mux runs, is given
	// os.ErrDeadlineExceeded. The chosen handler starts with no deadline, and
	// sets its own. Zero means DefaultTimeout, and a negative value none.
	Timeout time.Duration
}

// Handler is a fib.Handler that serves a connection only until it knows the
// protocol, and then hands it to the route that serves that protocol.
type Handler struct {
	routes   []Route
	fallback fib.Handler
	maxPeek  int
	timeout  time.Duration
}

var (
	_ fib.Handler         = (*Handler)(nil)
	_ fib.ShutdownHandler = (*Handler)(nil)
)

// New returns a Handler that tries routes in order, with the default Config.
func New(routes ...Route) *Handler { return NewWithConfig(Config{}, routes...) }

// NewWithConfig returns a Handler that tries routes in order. The first one
// that matches wins, but only once every route before it has ruled itself
// out, so that a route listed first keeps its priority over a later one that
// recognises a shorter prefix of the same bytes.
func NewWithConfig(config Config, routes ...Route) *Handler {
	h := &Handler{routes: append([]Route(nil), routes...), fallback: config.Fallback,
		maxPeek: config.MaxPeek, timeout: config.Timeout}
	if h.maxPeek <= 0 {
		h.maxPeek = DefaultMaxPeek
	}
	if h.timeout == 0 {
		h.timeout = DefaultTimeout
	}
	for i, r := range h.routes {
		if r.Match == nil || r.Handler == nil {
			panic("mux: route " + strconv.Itoa(i) + " needs both a Matcher and a Handler")
		}
	}
	return h
}

// sniff is a connection's state while the mux decides, kept as its
// attachment until the handoff gives the attachment to the chosen handler.
type sniff struct {
	// peeked holds what arrived before the decision, when a first read was
	// too short to decide on.
	peeked []byte
	// decided is set once, by whichever comes first: the handoff, or a
	// shutdown closing the connection undecided.
	decided atomic.Bool
}

// OnOpen starts the sniff, and the timeout that bounds it.
func (h *Handler) OnOpen(c *fib.Connection) {
	c.SetAttachment(&sniff{})
	if h.timeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(h.timeout))
	}
}

// OnData decides with what has arrived, or keeps it for the next read.
func (h *Handler) OnData(c *fib.Connection, data []byte) {
	s, ok := c.Attachment().(*sniff)
	if !ok {
		// Not opened by the mux, which a Handler only is by being one.
		return
	}
	input := data
	if len(s.peeked) > 0 {
		s.peeked = append(s.peeked, data...)
		input = s.peeked
	}
	route, decided := h.decide(input)
	if !decided {
		if len(s.peeked) == 0 {
			// data is the engine's only for this call.
			s.peeked = append(make([]byte, 0, h.maxPeek), data...)
		}
		return
	}
	if route == nil {
		if !s.decided.CompareAndSwap(false, true) {
			return
		}
		c.CloseWithError(ErrNoMatch)
		return
	}
	h.handoff(c, s, route, input)
}

// decide returns the handler for input, or reports that more is needed.
func (h *Handler) decide(input []byte) (fib.Handler, bool) {
	final := len(input) >= h.maxPeek
	for _, r := range h.routes {
		switch r.Match(input) {
		case Match:
			return r.Handler, true
		case NeedMore:
			if !final {
				return nil, false
			}
		}
	}
	return h.fallback, true
}

// handoff gives the connection to handler: the attachment and the deadline
// are left to it, it is opened, and it reads what the mux read.
func (h *Handler) handoff(c *fib.Connection, s *sniff, handler fib.Handler, input []byte) {
	if !s.decided.CompareAndSwap(false, true) {
		// A shutdown closed the connection first.
		return
	}
	if h.timeout > 0 {
		_ = c.SetReadDeadline(time.Time{})
	}
	c.SetAttachment(nil)
	c.SetHandler(handler)
	handler.OnOpen(c)
	handler.OnData(c, input)
}

// OnPriorityData drops out-of-band bytes that arrive before the decision:
// there is no protocol yet to give them to.
func (h *Handler) OnPriorityData(*fib.Connection, []byte) {}

// OnShutdown closes a connection that is still undecided, which has no
// exchange in flight to finish. One already handed off is its handler's to
// end, and the engine asks that handler instead.
func (h *Handler) OnShutdown(c *fib.Connection) {
	if s, ok := c.Attachment().(*sniff); ok && s.decided.CompareAndSwap(false, true) {
		c.Close()
	}
}

// OnClose ends the sniff of a connection closed before it was handed off.
func (h *Handler) OnClose(c *fib.Connection, _ error) {
	if _, ok := c.Attachment().(*sniff); ok {
		c.SetAttachment(nil)
	}
}
