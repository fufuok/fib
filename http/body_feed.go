//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	"io"
	"math"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
)

// BodyFeed is the producing side of a request body that streams to its
// handler on a multiplexed protocol: HTTP/2 here, and HTTP/3 in package
// http3. The protocol writes the body into it as it arrives, and the handler
// reads it from Request.Body, a *BodyStream, or takes it through
// Context.OnBody, as it would an HTTP/1 body.
//
// A multiplexed connection cannot stop reading for the sake of one request,
// so a streamed body is paced by the protocol's flow control instead: the
// protocol gives the peer room for more of it only as BodyFeedConfig.Credit
// is told the handler has consumed what it has.
type BodyFeed struct {
	s        *BodyStream
	declared int64
}

// BodyFeedConfig says what a BodyFeed needs of the protocol it serves.
type BodyFeedConfig struct {
	// Declared is the body's length when its request declared one, or -1.
	// A body that turns out longer or shorter fails with ErrMalformed.
	Declared int64
	// Limit bounds the body, as Config.MaxStreamedBodyBytes does; zero
	// leaves it unbounded.
	Limit int64
	// Continue, when set, sends the 100 Continue the client is waiting for
	// before it sends the body. It is called once, when the handler first
	// asks for the body, so that a handler which refuses the request answers
	// it before the upload starts.
	Continue func()
	// MaxHold is Config.MaxBodyHold for the body: zero is DefaultMaxBodyHold,
	// and below zero no limit.
	MaxHold time.Duration
	// Credit is told of every byte of the body the handler has consumed, by
	// reading it or through OnBody, and of every byte thrown away unread, for
	// the protocol to give the peer room for as much again. It is called
	// from whichever goroutine consumed them, and never with the feed's own
	// lock held.
	Credit func(n int)
}

// StreamContext returns the Context through which a handler answers Request,
// which arrived on conn and is answered through stream, as Context does, for
// a request whose body is still arriving: the returned BodyFeed is where the
// protocol writes it, and it is the request's Body, with ContentLength set to
// config.Declared.
func (r *StreamRequest) StreamContext(conn *fib.Connection, stream Stream, config BodyFeedConfig) (*Context, *BodyFeed) {
	c := r.bind(conn, nil)
	c.external = stream
	return c, r.streamBody(c, config)
}

// streamBody makes the request's body a stream fed through the returned
// BodyFeed.
func (r *StreamRequest) streamBody(c *Context, config BodyFeedConfig) *BodyFeed {
	s := &BodyStream{
		request:      &r.Request,
		remaining:    -1,
		limit:        config.Limit,
		highWater:    math.MaxInt,
		wantContinue: config.Continue != nil,
		sendContinue: config.Continue,
		credit:       config.Credit,
		maxHold:      maxBodyHold(config.MaxHold),
		offload:      true,
		owner:        c.Conn,
	}
	r.Request.Body, r.Request.ContentLength = s, config.Declared
	c.streamed = true
	return &BodyFeed{s: s, declared: config.Declared}
}

// ErrBodyOverrun is what a body that goes past its declared length fails
// with, and ErrBodyShort what one that ends short of it fails with. Both are
// ErrMalformed.
var ErrBodyOverrun = fmt.Errorf("%w: body longer than its declared length", ErrMalformed)

// Overruns reports whether n more bytes would take a body still arriving past
// its declared length, which Write would fail it for. A protocol that resets
// the request for it resets it first, and then writes the bytes, which fails
// the body if the reset has not and credits them: that way nothing the
// handler sends on hearing of the failure leaves ahead of the reset. Only the
// writer calls it, so what it reports holds for the Write that follows.
func (f *BodyFeed) Overruns(n int) bool {
	s := f.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return f.declared >= 0 && !s.abandoned && s.err == nil && !s.ended && s.total+int64(n) > f.declared
}

var ErrBodyShort = fmt.Errorf("%w: body shorter than its declared length", ErrMalformed)

// EndsShort reports whether a body still arriving that ended now would be
// short of its declared length, which End would fail it for; see Overruns.
func (f *BodyFeed) EndsShort() bool {
	s := f.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return f.declared >= 0 && s.total != f.declared && s.err == nil
}

// Write hands the handler the next piece of the body, which Write copies, so
// the caller keeps data. It never calls the handler's callback itself: it is
// called on the goroutine reading the connection, which serves every stream
// on it, so a callback taking the body is handed what arrived on the engine's
// handler pool. It fails, and fails the body with the same error, when the
// body goes past its declared length (ErrMalformed) or past the limit
// (ErrBodyTooLarge). What is written once the body has failed, has been
// given up by the handler or has ended is dropped, and credited.
func (f *BodyFeed) Write(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	s := f.s
	s.mu.Lock()
	if s.abandoned || s.err != nil || s.ended {
		s.mu.Unlock()
		s.credited(len(data))
		return nil
	}
	s.total += int64(len(data))
	var err error
	switch {
	case f.declared >= 0 && s.total > f.declared:
		err = ErrBodyOverrun
	case s.limit > 0 && s.total > s.limit:
		err = ErrBodyTooLarge
	}
	s.mu.Unlock()
	if err != nil {
		s.credited(len(data))
		s.fail(err, true)
		return err
	}
	s.push(data)
	return nil
}

// End ends the body, with the trailer that followed it, if any, which becomes
// the request's Trailer. It fails, and fails the body, when the body is
// shorter than its declared length.
func (f *BodyFeed) End(trailer stdhttp.Header) error {
	s := f.s
	s.mu.Lock()
	short := f.declared >= 0 && s.total != f.declared && s.err == nil
	s.mu.Unlock()
	if short {
		s.fail(ErrBodyShort, true)
		return ErrBodyShort
	}
	s.finish(trailer)
	return nil
}

// Fail ends the body short: the request was reset, its connection went, or
// its response was written first. The handler hears err through Read, or
// through OnBody once it has what arrived before it. Fail never calls the
// handler's callback itself, so it may be called under the protocol's own
// locks and on the connection's reader: when the callback is owed the news it is
// handed it on the engine's handler pool, unless a call to it is under way
// already, which then hands it on.
func (f *BodyFeed) Fail(err error) {
	s := f.s
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	if s.err == nil && !s.ended {
		s.err = err
	}
	s.releaseLocked()
	if s.sink == nil || s.sinkBusy || !s.owedLocked() {
		s.mu.Unlock()
		return
	}
	s.sinkBusy = true
	s.mu.Unlock()
	runOnStreams(s.owner, func() { s.drain(false) })
}
