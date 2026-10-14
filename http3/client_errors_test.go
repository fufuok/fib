//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// scriptServer is an HTTP/3 server written frame by frame that answers each
// request with the script registered for its path, for the responses and
// streams a well-behaved server never sends. One server, and one client
// connection to it, serves a whole table of cases, so that a test makes as
// few handshakes as it can.
type scriptServer struct {
	mu sync.Mutex
	// scripts answer a request on s; n counts the requests for the path, from
	// one, so that a script can answer a request sent again differently.
	scripts  map[string]func(s *quic.Stream, n int)
	counts   map[string]int
	conns    map[string]*quic.Conn
	controls map[*quic.Conn]*quic.Stream
	closes   map[*quic.Conn]chan error
	// arrived has the path of every request as it arrives.
	arrived chan string
}

type scriptBuffer struct{ data []byte }

func (sv *scriptServer) handle(path string, script func(s *quic.Stream, n int)) {
	sv.mu.Lock()
	sv.scripts[path] = script
	sv.mu.Unlock()
}

func (sv *scriptServer) OnHandshake(qc *quic.Conn) {
	control, err := qc.OpenUniStream()
	if err != nil {
		return
	}
	sv.mu.Lock()
	sv.controls[qc] = control
	sv.mu.Unlock()
	_ = control.Write(appendSettings([]byte{streamControl}), false)
}

func (sv *scriptServer) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	if !s.Bidirectional() {
		return
	}
	buf, _ := s.Context.(*scriptBuffer)
	if buf == nil {
		buf = &scriptBuffer{}
		s.Context = buf
	}
	buf.data = append(buf.data, data...)
	if !fin {
		return
	}
	// The request's HEADERS frame names the script.
	path := ""
	p := frameParser{maxFrame: 1 << 20}
	_ = p.feed(buf.data, func([]byte) error { return nil }, func(typ uint64, payload []byte) error {
		if typ == frameHeaders && path == "" {
			_ = qpack.Decode(payload, 0, func(f qpack.HeaderField) error {
				if f.Name == ":path" {
					path = f.Value
				}
				return nil
			})
		}
		return nil
	})
	sv.mu.Lock()
	sv.counts[path]++
	n := sv.counts[path]
	sv.conns[path] = s.Conn()
	script := sv.scripts[path]
	sv.mu.Unlock()
	select {
	case sv.arrived <- path:
	default:
	}
	if script == nil {
		script = func(s *quic.Stream, _ int) { _ = s.Write(responseFrames("200"), true) }
	}
	script(s, n)
}

func (sv *scriptServer) OnStreamReset(*quic.Stream, uint64) {}
func (sv *scriptServer) OnStopSending(*quic.Stream, uint64) {}
func (sv *scriptServer) OnStreamsAvailable(*quic.Conn)      {}
func (sv *scriptServer) OnClose(qc *quic.Conn, err error)   { sv.closeChan(qc) <- err }

func (sv *scriptServer) closeChan(qc *quic.Conn) chan error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	ch := sv.closes[qc]
	if ch == nil {
		ch = make(chan error, 1)
		sv.closes[qc] = ch
	}
	return ch
}

// control is the control stream of the connection the last request for path
// came on.
func (sv *scriptServer) control(path string) *quic.Stream {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.controls[sv.conns[path]]
}

// expectClose waits for the connection the last request for path came on
// to be closed by the client with code.
func (sv *scriptServer) expectClose(t *testing.T, path string, code ErrorCode) {
	t.Helper()
	sv.mu.Lock()
	qc := sv.conns[path]
	sv.mu.Unlock()
	if qc == nil {
		t.Fatalf("%s: no request arrived", path)
	}
	select {
	case err := <-sv.closeChan(qc):
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || ErrorCode(appErr.Code) != code {
			t.Fatalf("%s: the client closed with %v, want %v", path, err, code)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: the client did not close the connection with %v", path, code)
	}
}

// awaitArrival waits for a request for path to reach the server.
func (sv *scriptServer) awaitArrival(t *testing.T, path string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case got := <-sv.arrived:
			if got == path {
				return
			}
		case <-deadline:
			t.Fatalf("%s did not arrive", path)
		}
	}
}

// scriptServerHandler feeds the engine's datagrams to the QUIC connections
// of a scriptServer, one per client address.
type scriptServerHandler struct {
	server *scriptServer
	config quic.Config
}

func (h *scriptServerHandler) OnOpen(*fib.Connection)                 {}
func (h *scriptServerHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *scriptServerHandler) OnData(c *fib.Connection, data []byte) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.HandleDatagram(data)
		return
	}
	if !quic.IsInitial(data) {
		return
	}
	if qc := quic.Accept(c, c.RemoteAddr(), h.config, h.server, data); qc != nil {
		c.SetAttachment(qc)
	}
}

func (h *scriptServerHandler) OnClose(c *fib.Connection, err error) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.Abort(err)
	}
	c.SetAttachment(nil)
}

// startScriptServer serves a scriptServer, its QUIC connections configured
// by configure if it is not nil, and returns it and its URL.
func startScriptServer(t *testing.T, configure func(*quic.Config)) (*scriptServer, string) {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	server := &scriptServer{
		scripts:  map[string]func(*quic.Stream, int){},
		counts:   map[string]int{},
		conns:    map[string]*quic.Conn{},
		controls: map[*quic.Conn]*quic.Stream{},
		closes:   map[*quic.Conn]chan error{},
		arrived:  make(chan string, 64),
	}
	qconfig := quic.Config{TLSConfig: ConfigureTLS(serverTLS)}
	if configure != nil {
		configure(&qconfig)
	}
	config := fib.DefaultConfig()
	config.Network = "udp"
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, &scriptServerHandler{server: server, config: qconfig})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return server, fmt.Sprintf("https://localhost:%d", addr.Port)
}

// responseFrames is a HEADERS frame with status and the given name, value
// pairs.
func responseFrames(status string, pairs ...string) []byte {
	fields := []qpack.HeaderField{{Name: ":status", Value: status}}
	for i := 0; i+1 < len(pairs); i += 2 {
		fields = append(fields, qpack.HeaderField{Name: pairs[i], Value: pairs[i+1]})
	}
	return headersFrame(fields...)
}

// writeFrames writes frames joined, in one write, ending the stream.
func writeFrames(s *quic.Stream, frames ...[]byte) {
	var out []byte
	for _, f := range frames {
		out = append(out, f...)
	}
	_ = s.Write(out, true)
}

// A response the client cannot take fails its request, and only that: the
// connection goes on carrying the next one. A request the server rejected
// unprocessed goes again, once.
func TestClientRefusesMalformedResponses(t *testing.T) {
	server, base := startScriptServer(t, nil)
	manyFields := make([]string, 0, 400)
	for i := 0; i < 200; i++ {
		manyFields = append(manyFields, "x-a", "b")
	}
	scripts := map[string]func(s *quic.Stream, n int){
		"/malformed-status": func(s *quic.Stream, _ int) { writeFrames(s, responseFrames("2xx")) },
		"/malformed-trailers": func(s *quic.Stream, _ int) {
			writeFrames(s, responseFrames("200"), headersFrame(qpack.HeaderField{Name: ":path", Value: "/"}))
		},
		"/length-mismatch": func(s *quic.Stream, _ int) {
			writeFrames(s, responseFrames("200", "content-length", "10"), dataFrame([]byte("short")))
		},
		"/header-too-large": func(s *quic.Stream, _ int) { writeFrames(s, responseFrames("200", manyFields...)) },
		"/body-too-large": func(s *quic.Stream, _ int) {
			writeFrames(s, responseFrames("200"), dataFrame(make([]byte, 2000)))
		},
		"/reset": func(s *quic.Stream, _ int) { s.Reset(uint64(ErrCodeInternalError)) },
		// Whatever follows a response that failed is passed over.
		"/after-failure": func(s *quic.Stream, _ int) {
			writeFrames(s, responseFrames("2xx"), dataFrame([]byte("x")), responseFrames("200"))
		},
		"/rejected": func(s *quic.Stream, _ int) { s.Reset(uint64(ErrCodeRequestRejected)) },
	}
	for path, script := range scripts {
		server.handle(path, script)
	}
	// Sent again after the first refusal, and served.
	server.handle("/rejected-once", func(s *quic.Stream, n int) {
		if n == 1 {
			s.Reset(uint64(ErrCodeRequestRejected))
			return
		}
		writeFrames(s, responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})
	// A frame of a type nothing knows is skipped (RFC 9114 section 9).
	server.handle("/unknown-frame", func(s *quic.Stream, _ int) {
		writeFrames(s, controlFrame(0x21, []byte("abc")), responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})

	client := newClient(t, func(config *ClientConfig) {
		config.MaxResponseHeaderBytes = 4096
		config.MaxResponseBodyBytes = 1024
	})
	get := func(path string) (*stdhttp.Response, error) {
		return client.Go(mustRequest(t, stdhttp.MethodGet, base+path, nil)).Wait()
	}
	failures := map[string]func(error) bool{
		"/malformed-status":   func(err error) bool { return strings.Contains(fmt.Sprint(err), "malformed response") },
		"/malformed-trailers": func(err error) bool { return strings.Contains(fmt.Sprint(err), "malformed trailers") },
		"/length-mismatch":    func(err error) bool { return strings.Contains(fmt.Sprint(err), "does not match") },
		"/header-too-large":   func(err error) bool { return errors.Is(err, errHeaderTooLarge) },
		"/body-too-large":     func(err error) bool { return errors.Is(err, errBodyTooLarge) },
		"/after-failure":      func(err error) bool { return strings.Contains(fmt.Sprint(err), "malformed response") },
		"/reset": func(err error) bool {
			var se *StreamError
			return errors.As(err, &se) && se.Remote && se.Code == ErrCodeInternalError
		},
		"/rejected": func(err error) bool {
			var se *StreamError
			return errors.As(err, &se) && se.Code == ErrCodeRequestRejected
		},
	}
	for path, check := range failures {
		if resp, err := get(path); !check(err) {
			t.Errorf("%s: %v, %v", path, resp, err)
		}
	}
	for _, path := range []string{"/rejected-once", "/unknown-frame"} {
		resp, err := get(path)
		if err != nil || readBody(t, resp) != "ok" {
			t.Fatalf("%s: %v, %v", path, resp, err)
		}
	}
	server.mu.Lock()
	rejected, once := server.counts["/rejected"], server.counts["/rejected-once"]
	server.mu.Unlock()
	if rejected != 2 || once != 2 {
		t.Fatalf("a rejected request was sent %d times, and one served the second time %d, want 2 each", rejected, once)
	}
	// All of that on the one connection.
	server.mu.Lock()
	conns := len(server.controls)
	server.mu.Unlock()
	if conns != 1 {
		t.Fatalf("%d connections, want 1", conns)
	}
}

// What a server may not send on a request stream, or on the streams it
// opens, is a connection error (RFC 9114 sections 6, 7 and 8.1).
func TestClientClosesOnProtocolViolations(t *testing.T) {
	server, base := startScriptServer(t, nil)
	cases := map[string]struct {
		script func(s *quic.Stream)
		code   ErrorCode
		// answered is set where the response may already have been handed
		// over, at its HEADERS, before what breaks the rules arrives.
		answered bool
	}{
		"/push-promise": {func(s *quic.Stream) {
			writeFrames(s, controlFrame(framePushPromise, append(quic.AppendVarint(nil, 0), 0, 0)))
		}, ErrCodeIDError, false},
		"/settings": {func(s *quic.Stream) { writeFrames(s, appendSettings(nil)) }, ErrCodeFrameUnexpected, false},
		"/http2-frame": {func(s *quic.Stream) {
			writeFrames(s, controlFrame(0x06, make([]byte, 8)))
		}, ErrCodeFrameUnexpected, false},
		"/headers-after-trailers": {func(s *quic.Stream) {
			trailer := headersFrame(qpack.HeaderField{Name: "x-sum", Value: "1"})
			writeFrames(s, responseFrames("200"), trailer, trailer)
		}, ErrCodeFrameUnexpected, true},
		"/data-first": {func(s *quic.Stream) { writeFrames(s, dataFrame([]byte("x"))) }, ErrCodeFrameUnexpected, false},
		"/mid-frame": {func(s *quic.Stream) {
			writeFrames(s, responseFrames("200"), appendFrameHeader(nil, frameData, 10), []byte("abc"))
		}, ErrCodeFrameError, true},
		"/bidirectional": {func(s *quic.Stream) {
			if bs, err := s.Conn().OpenStream(); err == nil {
				_ = bs.Write([]byte{0}, false)
			}
		}, ErrCodeStreamCreationError, false},
		"/push-stream": {func(s *quic.Stream) {
			if us, err := s.Conn().OpenUniStream(); err == nil {
				_ = us.Write([]byte{streamPush, 0}, false)
			}
		}, ErrCodeIDError, false},
		// The SETTINGS frame opening the control stream goes out with the
		// flush that ends this call; reset before that, it would be
		// discarded, and the client would see a reset stream of no type.
		"/reset-control": {func(s *quic.Stream) {
			control := server.control("/reset-control")
			time.AfterFunc(50*time.Millisecond, func() { control.Reset(uint64(ErrCodeNoError)) })
		}, ErrCodeClosedCriticalStream, false},
		"/end-control": {func(s *quic.Stream) {
			_ = server.control("/end-control").Write(nil, true)
		}, ErrCodeClosedCriticalStream, false},
	}
	for path, c := range cases {
		script := c.script
		server.handle(path, func(s *quic.Stream, _ int) { script(s) })
	}
	client := newClient(t, nil)
	for path, c := range cases {
		if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+path, nil)).Wait(); err == nil && !c.answered {
			t.Fatalf("%s: the request succeeded", path)
		}
		server.expectClose(t, path, c.code)
	}
}

// A unidirectional stream of a type the client does not know is left
// unread (RFC 9114 section 6.2), and the connection goes on.
func TestClientIgnoresUnknownStreamType(t *testing.T) {
	server, base := startScriptServer(t, nil)
	server.handle("/", func(s *quic.Stream, _ int) {
		if us, err := s.Conn().OpenUniStream(); err == nil {
			_ = us.Write([]byte{0x21, 'x', 'y'}, false)
		}
		writeFrames(s, responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})
	client := newClient(t, nil)
	for i := 0; i < 2; i++ {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)).Wait()
		if err != nil || readBody(t, resp) != "ok" {
			t.Fatalf("request %d: %v, %v", i, resp, err)
		}
	}
}

// A streamed body is held to MaxStreamedBodyBytes by its declared length,
// before any of it arrives, and to its declared length once it ends.
func TestClientStreamedBodyLimits(t *testing.T) {
	server, base := startScriptServer(t, nil)
	server.handle("/declared-too-large", func(s *quic.Stream, _ int) {
		writeFrames(s, responseFrames("200", "content-length", "5000"), dataFrame(make([]byte, 100)))
	})
	server.handle("/short", func(s *quic.Stream, _ int) {
		writeFrames(s, responseFrames("200", "content-length", "100"), dataFrame(make([]byte, 50)))
	})
	client := newClient(t, func(config *ClientConfig) {
		streaming(0)(config)
		config.MaxStreamedBodyBytes = 1000
	})
	var tooLarge streamRun
	tooLarge.run(t, client, mustRequest(t, stdhttp.MethodGet, base+"/declared-too-large", nil))
	if !errors.Is(tooLarge.callErr, errBodyTooLarge) {
		t.Fatalf("declared too large: %v", tooLarge.callErr)
	}
	var short streamRun
	short.run(t, client, mustRequest(t, stdhttp.MethodGet, base+"/short", nil))
	if short.err == nil || !strings.Contains(short.err.Error(), "does not match") {
		t.Fatalf("short body: %v after %d bytes", short.err, len(short.data))
	}
}

// startHoldServer is a script server that takes one request at a time on
// a connection, and holds the one to /hold until release is called.
func startHoldServer(t *testing.T) (server *scriptServer, base string, release func()) {
	t.Helper()
	server, base = startScriptServer(t, func(config *quic.Config) { config.MaxIncomingStreams = 1 })
	held := make(chan *quic.Stream, 1)
	server.handle("/hold", func(s *quic.Stream, _ int) { held <- s })
	server.handle("/", func(s *quic.Stream, _ int) {
		writeFrames(s, responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})
	release = func() {
		select {
		case s := <-held:
			writeFrames(s, responseFrames("200", "content-length", "4"), dataFrame([]byte("held")))
		case <-time.After(10 * time.Second):
			t.Fatal("the held request never arrived")
		}
	}
	return server, base, release
}

// checkOK waits for f and checks it got body.
func checkOK(t *testing.T, f *Future, body string) {
	t.Helper()
	select {
	case <-f.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("no response")
	}
	resp, err := f.Wait()
	if err != nil || readBody(t, resp) != body {
		t.Fatalf("%v, %v, want %q", resp, err, body)
	}
}

// Requests beyond the server's stream limit wait for a stream; one that
// is given up meanwhile is passed over when a stream frees up, and the next
// one takes it.
func TestClientWaitingRequestGivenUp(t *testing.T) {
	server, base, release := startHoldServer(t)
	client := newClient(t, nil)
	held := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/hold", nil))
	server.awaitArrival(t, "/hold")

	ctx, cancel := context.WithCancel(context.Background())
	givenUp := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil).WithContext(ctx))
	next := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))
	cancel()
	if _, err := givenUp.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("given up: %v", err)
	}
	release()
	checkOK(t, held, "held")
	checkOK(t, next, "ok")
	server.mu.Lock()
	n := server.counts["/"]
	server.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d requests for / arrived, want the one not given up", n)
	}
}

// A request still waiting for a stream when the server closes the
// connection was never sent, so it goes on a new connection.
func TestClientWaitingRequestOutlivesItsConnection(t *testing.T) {
	server, base, _ := startHoldServer(t)
	client := newClient(t, nil)
	held := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/hold", nil))
	server.awaitArrival(t, "/hold")
	waiting := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))
	server.mu.Lock()
	qc := server.conns["/hold"]
	server.mu.Unlock()
	qc.Close(uint64(ErrCodeInternalError), "going")
	if _, err := held.Wait(); err == nil {
		t.Fatal("the request in flight survived its connection")
	}
	checkOK(t, waiting, "ok")
}

// A GOAWAY sends what waits for a stream to a new connection, while the
// requests below the identifier it names finish where they are.
func TestClientGoAwayMovesWaitingRequests(t *testing.T) {
	server, base, release := startHoldServer(t)
	client := newClient(t, nil)
	held := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/hold", nil))
	server.awaitArrival(t, "/hold")
	waiting := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))
	_ = server.control("/hold").Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 4)), false)
	checkOK(t, waiting, "ok")
	release()
	checkOK(t, held, "held")
	server.mu.Lock()
	conns := len(server.controls)
	server.mu.Unlock()
	if conns != 2 {
		t.Fatalf("%d connections, want 2", conns)
	}
}

// A GOAWAY naming a request the server has not begun has it sent again on
// a new connection (RFC 9114 section 5.2).
func TestClientGoAwayRetriesUnservedRequest(t *testing.T) {
	server, base := startScriptServer(t, nil)
	server.handle("/", func(s *quic.Stream, n int) {
		if n == 1 {
			_ = server.control("/").Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 0)), false)
			return
		}
		writeFrames(s, responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})
	client := newClient(t, nil)
	checkOK(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)), "ok")
}

// Closing the client fails what waits for a stream, and lets what was sent
// finish.
func TestClientCloseFailsWaitingRequests(t *testing.T) {
	server, base, release := startHoldServer(t)
	client := newClient(t, nil)
	held := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/hold", nil))
	server.awaitArrival(t, "/hold")
	waiting := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))
	client.Close()
	if _, err := waiting.Wait(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("waiting: %v", err)
	}
	release()
	checkOK(t, held, "held")
}

// A connection that has carried no request for IdleConnTimeout is closed,
// gracefully.
func TestClientIdleConnTimeout(t *testing.T) {
	server, base := startScriptServer(t, nil)
	client := newClient(t, func(config *ClientConfig) { config.IdleConnTimeout = 50 * time.Millisecond })
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	server.expectClose(t, "/", ErrCodeNoError)
}
