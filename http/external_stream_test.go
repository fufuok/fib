//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// A Stream of a protocol served outside this package, as HTTP/3 is by package
// http3, answers a request through the same Context as HTTP/1 and HTTP/2. The
// tests here drive a Context over hand-made streams, without a connection, to
// pin down what the Context hands such a stream and what it keeps to itself.

// errExtStream is what an extStream reports when told to fail.
var errExtStream = errors.New("ext stream failed")

// extStream is a Stream that records what it is handed. fail makes every
// method that can fail report errExtStream.
type extStream struct {
	mu        sync.Mutex
	responses []Response
	interims  []int
	pushes    []string
	fail      bool
}

func (s *extStream) WriteResponse(_ *stdhttp.Request, response Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errExtStream
	}
	response.Body = append([]byte(nil), response.Body...)
	s.responses = append(s.responses, response)
	return nil
}

func (s *extStream) WriteInterim(status int, _ stdhttp.Header) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.interims = append(s.interims, status)
	return nil
}

func (s *extStream) Push(_ *stdhttp.Request, target string, _ *stdhttp.PushOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushes = append(s.pushes, target)
	return nil
}

func (s *extStream) response(t *testing.T) Response {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) != 1 {
		t.Fatalf("the stream was handed %d responses, want 1", len(s.responses))
	}
	return s.responses[0]
}

// extDeadlineStream is an extStream that takes deadlines.
type extDeadlineStream struct {
	extStream
	read, write time.Time
}

func (s *extDeadlineStream) SetReadDeadline(t time.Time) error  { s.read = t; return nil }
func (s *extDeadlineStream) SetWriteDeadline(t time.Time) error { s.write = t; return nil }

// extStreamingStream is an extStream that sends a response as it is written.
// failBegin and failEnd make BeginResponse and EndResponse fail.
type extStreamingStream struct {
	extStream
	status             int
	body               []byte
	trailer            stdhttp.Header
	ended, complete    bool
	failBegin, failEnd bool
}

func (s *extStreamingStream) BeginResponse(_ *stdhttp.Request, status int, _ stdhttp.Header) error {
	if s.failBegin {
		return errExtStream
	}
	s.status = status
	return nil
}

func (s *extStreamingStream) WriteBody(p []byte, _ bool) error {
	s.body = append(s.body, p...)
	return nil
}

func (s *extStreamingStream) EndResponse(trailer stdhttp.Header, complete bool) error {
	if s.failEnd {
		return errExtStream
	}
	s.trailer, s.ended, s.complete = trailer, true, complete
	return nil
}

// extUpgradeStream is an extStream whose requests can become tunnels, which
// it runs through writer. refuse makes Upgrade report err instead.
type extUpgradeStream struct {
	extStream
	writer   *extTunnelWriter
	refuse   error
	protocol string
	header   stdhttp.Header
}

func (s *extUpgradeStream) Upgrade(_ *stdhttp.Request, protocol string, header stdhttp.Header) (*Tunnel, error) {
	if s.refuse != nil {
		return nil, s.refuse
	}
	s.protocol, s.header = protocol, header
	return NewTunnelFeed(nil, s.writer, 0).Tunnel(), nil
}

// extTunnelWriter is a TunnelWriter that records what goes through it.
type extTunnelWriter struct {
	mu                  sync.Mutex
	sent                []string
	closedAfter, closed bool
}

func (w *extTunnelWriter) SendParts(first, second []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, "parts:"+string(first)+string(second))
	return nil
}

func (w *extTunnelWriter) SendOwned(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, "owned:"+string(data))
	return nil
}

func (w *extTunnelWriter) CloseAfterSend() { w.closedAfter = true }
func (w *extTunnelWriter) Close() error    { w.closed = true; return nil }

// extSharingTunnelWriter is an extTunnelWriter that queues shared arrays.
type extSharingTunnelWriter struct{ extTunnelWriter }

func (w *extSharingTunnelWriter) SendShared(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, "shared:"+string(data))
	return nil
}

// newExtRequest is a request of method for path that arrived over stream.
func newExtRequest(method, path string, stream Stream) (*StreamRequest, *Context) {
	r := new(StreamRequest)
	r.URL.Path = path
	r.Request.URL = &r.URL
	r.Request.Method = method
	r.Request.Proto, r.Request.ProtoMajor = "HTTP/3.0", 3
	r.Request.Header = stdhttp.Header{}
	return r, r.Context(nil, stream, nil)
}

// TestExternalStreamServe serves a request through NewStreamContext and Serve,
// as a protocol outside this package does, and checks that what the handler
// answers, what it sends ahead of it and what it pushes all reach the stream.
func TestExternalStreamServe(t *testing.T) {
	stream := new(extStream)
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "/served", nil)
	c := NewStreamContext(nil, req, stream)
	Serve(HandlerFunc(func(c *Context) {
		if err := c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"</a.css>"}}); err != nil {
			t.Errorf("WriteInterim: %v", err)
		}
		if err := c.Push("/a.css", nil); err != nil {
			t.Errorf("Push: %v", err)
		}
		// A status left zero is a 200.
		if err := c.WriteResponse(Response{Body: []byte("served")}); err != nil {
			t.Errorf("WriteResponse: %v", err)
		}
		if err := c.WriteResponse(Response{Body: []byte("again")}); !errors.Is(err, ErrResponseWritten) {
			t.Errorf("a second WriteResponse reported %v, want ErrResponseWritten", err)
		}
	}), c)
	if got := stream.response(t); got.StatusCode != stdhttp.StatusOK || string(got.Body) != "served" {
		t.Fatalf("the stream got %d %q", got.StatusCode, got.Body)
	}
	if len(stream.interims) != 1 || stream.interims[0] != stdhttp.StatusEarlyHints {
		t.Fatalf("interim responses %v, want one 103", stream.interims)
	}
	if len(stream.pushes) != 1 || stream.pushes[0] != "/a.css" {
		t.Fatalf("pushes %v, want /a.css", stream.pushes)
	}
}

// TestExternalStreamResponseAfterBegin checks that a handler that began its
// response through the ResponseWriter methods cannot then answer it whole
// with Respond or WriteResponse, hooked or not, and that a status outside
// what WriteHeader takes is a programming error, as it is in net/http.
func TestExternalStreamResponseAfterBegin(t *testing.T) {
	for _, hooked := range []bool{false, true} {
		stream := new(extStream)
		_, c := newExtRequest(stdhttp.MethodGet, "/begun", stream)
		Serve(HandlerFunc(func(c *Context) {
			if hooked {
				c.OnFinish(func(int, stdhttp.Header, int64) {})
			}
			c.WriteHeader(stdhttp.StatusAccepted)
			// A second status is ignored, as net/http ignores it.
			c.WriteHeader(stdhttp.StatusTeapot)
			if err := c.Respond(stdhttp.StatusOK, "text/plain", []byte("x")); !errors.Is(err, ErrResponseWritten) {
				t.Errorf("hooked=%v: Respond after WriteHeader reported %v", hooked, err)
			}
			if err := c.WriteResponse(Response{StatusCode: stdhttp.StatusOK}); !errors.Is(err, ErrResponseWritten) {
				t.Errorf("hooked=%v: WriteResponse after WriteHeader reported %v", hooked, err)
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("hooked=%v: WriteHeader(99) did not panic", hooked)
					}
				}()
				c.WriteHeader(99)
			}()
		}), c)
		if got := stream.response(t); got.StatusCode != stdhttp.StatusAccepted {
			t.Fatalf("hooked=%v: the stream got status %d, want the first one set", hooked, got.StatusCode)
		}
	}
}

// TestExternalStreamDeadlines checks that SetReadDeadline and
// SetWriteDeadline reach a stream that takes deadlines, report
// http.ErrNotSupported on one that does not, and ErrResponseWritten once the
// response is finished.
func TestExternalStreamDeadlines(t *testing.T) {
	at := time.Now().Add(time.Hour)
	deadlines := new(extDeadlineStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/deadline", deadlines)
	if err := c.SetReadDeadline(at); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if err := c.SetWriteDeadline(at); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if !deadlines.read.Equal(at) || !deadlines.write.Equal(at) {
		t.Fatalf("the stream got read %v and write %v, want %v", deadlines.read, deadlines.write, at)
	}

	_, c = newExtRequest(stdhttp.MethodGet, "/plain", new(extStream))
	if err := c.SetReadDeadline(at); !errors.Is(err, stdhttp.ErrNotSupported) {
		t.Fatalf("SetReadDeadline on a stream without deadlines reported %v", err)
	}
	if err := c.SetWriteDeadline(at); !errors.Is(err, stdhttp.ErrNotSupported) {
		t.Fatalf("SetWriteDeadline on a stream without deadlines reported %v", err)
	}
	if err := c.WriteResponse(Response{}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteDeadline(at); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("SetWriteDeadline after the response reported %v", err)
	}
	if err := c.FlushError(); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("FlushError after the response reported %v", err)
	}
	if _, err := c.ReadFrom(strings.NewReader("late")); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("ReadFrom after the response reported %v", err)
	}
	if _, err := c.Write([]byte("late")); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("Write after the response reported %v", err)
	}
}

// TestExternalStreamHeldWhole checks that a response written through the
// ResponseWriter methods on a stream that cannot send it as it is written is
// held, Flush or not, and handed over whole once the handler returns.
func TestExternalStreamHeldWhole(t *testing.T) {
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/whole", stream)
	long := strings.Repeat("w", 3*writerBufferSize)
	Serve(HandlerFunc(func(c *Context) {
		c.Header().Set("Content-Type", "text/plain")
		_, _ = c.WriteString(long[:writerBufferSize])
		c.Flush()
		_, _ = c.WriteString(long[writerBufferSize:])
	}), c)
	if got := stream.response(t); string(got.Body) != long {
		t.Fatalf("the stream got %d bytes, want %d whole", len(got.Body), len(long))
	}
}

// TestExternalStreamStreamed checks that a response too long to hold back
// streams through a ResponseStreamer, and that a response to HEAD, which
// carries no body, is ended as complete with OnFinish told it sent none.
func TestExternalStreamStreamed(t *testing.T) {
	stream := new(extStreamingStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/streamed", stream)
	long := strings.Repeat("s", 3*writerBufferSize)
	Serve(HandlerFunc(func(c *Context) {
		c.Header().Set("Trailer", "X-Sum")
		_, _ = c.WriteString(long)
		c.Header().Set("X-Sum", "42")
	}), c)
	if stream.status != stdhttp.StatusOK || string(stream.body) != long || !stream.ended || !stream.complete {
		t.Fatalf("streamed %d, %d bytes, ended %v complete %v", stream.status, len(stream.body), stream.ended, stream.complete)
	}
	if stream.trailer.Get("X-Sum") != "42" {
		t.Fatalf("trailer %v, want X-Sum: 42", stream.trailer)
	}

	head := new(extStreamingStream)
	_, c = newExtRequest(stdhttp.MethodHead, "/head", head)
	size := int64(-1)
	Serve(HandlerFunc(func(c *Context) {
		c.OnFinish(func(_ int, _ stdhttp.Header, n int64) { size = n })
		c.WriteHeader(stdhttp.StatusOK)
		c.Flush()
	}), c)
	if !head.ended || !head.complete || size != 0 {
		t.Fatalf("HEAD ended %v complete %v, OnFinish saw %d bytes", head.ended, head.complete, size)
	}
}

// TestExternalStreamFailures checks what a handler hears when the stream
// fails under it: a header the stream will not begin fails the flush that
// sends it, and once ending the response has failed, nothing more is
// written to it.
func TestExternalStreamFailures(t *testing.T) {
	begin := &extStreamingStream{failBegin: true}
	_, c := newExtRequest(stdhttp.MethodGet, "/begin", begin)
	c.WriteHeader(stdhttp.StatusOK)
	if err := c.FlushError(); !errors.Is(err, errExtStream) {
		t.Fatalf("FlushError on a stream that would not begin reported %v", err)
	}

	end := &extStreamingStream{failEnd: true}
	_, c = newExtRequest(stdhttp.MethodGet, "/end", end)
	c.WriteHeader(stdhttp.StatusOK)
	c.Flush()
	if err := c.Finish(); !errors.Is(err, errExtStream) {
		t.Fatalf("Finish on a stream that would not end reported %v", err)
	}
	if _, err := c.Write([]byte("more")); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("Write after a failed Finish reported %v", err)
	}
}

// TestExternalStreamHookedHead checks that OnFinish is told a response to
// HEAD carried no body, whatever body the handler gave it.
func TestExternalStreamHookedHead(t *testing.T) {
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodHead, "/head", stream)
	size := int64(-1)
	Serve(HandlerFunc(func(c *Context) {
		c.OnFinish(func(_ int, _ stdhttp.Header, n int64) { size = n })
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: []byte("not sent")})
	}), c)
	if size != 0 {
		t.Fatalf("OnFinish saw %d bytes for HEAD, want 0", size)
	}
}

// extTunnelRecorder is a TunnelHandler that records its calls.
type extTunnelRecorder struct {
	mu     sync.Mutex
	calls  []string
	closed chan error
}

func newExtTunnelRecorder() *extTunnelRecorder {
	return &extTunnelRecorder{closed: make(chan error, 1)}
}

func (r *extTunnelRecorder) OnTunnelOpen(*Tunnel) { r.add("open") }
func (r *extTunnelRecorder) OnTunnelData(_ *Tunnel, data []byte) {
	r.add("data:" + string(data))
}
func (r *extTunnelRecorder) OnTunnelClose(_ *Tunnel, err error) {
	r.add("close")
	r.closed <- err
}

func (r *extTunnelRecorder) add(call string) {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
}

func (r *extTunnelRecorder) got() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.calls, " ")
}

// TestExternalStreamUpgrade switches a request on an external stream to a
// tunnel: the stream is asked to upgrade with the header the OnHeader hooks
// left, OnFinish hears of the 200 that accepted it, and the tunnel the
// handler gets sends through the stream's writer.
func TestExternalStreamUpgrade(t *testing.T) {
	writer := new(extSharingTunnelWriter)
	stream := &extUpgradeStream{writer: &writer.extTunnelWriter}
	_, c := newExtRequest(stdhttp.MethodConnect, "/chat", stream)
	recorder := newExtTunnelRecorder()
	var finished int
	var tunnel *Tunnel
	Serve(HandlerFunc(func(c *Context) {
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hooked", "yes") })
		c.OnFinish(func(status int, _ stdhttp.Header, _ int64) { finished = status })
		var err error
		if tunnel, err = c.Upgrade("websocket", nil, recorder); err != nil {
			t.Errorf("Upgrade: %v", err)
		}
	}), c)
	if tunnel == nil {
		t.FailNow()
	}
	if stream.protocol != "websocket" || stream.header.Get("X-Hooked") != "yes" || finished != stdhttp.StatusOK {
		t.Fatalf("upgraded to %q with %v, OnFinish saw %d", stream.protocol, stream.header, finished)
	}
	if tunnel.Conn() != nil {
		t.Fatal("a tunnel without a connection reported one")
	}
	_ = tunnel.Send([]byte("a"))
	_ = tunnel.SendParts([]byte("b"), []byte("c"))
	_ = tunnel.SendOwned([]byte("d"))
	_ = tunnel.SendShared([]byte("e"))
	tunnel.CloseAfterSend()
	_ = tunnel.Close()
	// The stream's writer does not queue shared arrays, so SendShared hands
	// it the data as SendParts would.
	if got := strings.Join(writer.sent, " "); got != "parts:a parts:bc owned:d parts:e" {
		t.Fatalf("sent %q", got)
	}
	if !writer.closedAfter || !writer.closed {
		t.Fatalf("CloseAfterSend reached the writer: %v, Close: %v", writer.closedAfter, writer.closed)
	}
	if got := recorder.got(); got != "open" {
		t.Fatalf("handler calls %q, want open", got)
	}

	// A writer that queues shared arrays itself is handed them as they are.
	feed := NewTunnelFeed(nil, writer, 0)
	_ = feed.Tunnel().SendShared([]byte("f"))
	if last := writer.sent[len(writer.sent)-1]; last != "shared:f" {
		t.Fatalf("SendShared on a sharing writer sent %q", last)
	}
}

// TestExternalStreamUpgradeRefused checks the requests Upgrade turns down:
// a malformed call, a response already begun, a handler that has returned,
// and a stream that cannot upgrade or refuses to. A refusal leaves the
// header hooks to run for the response the handler answers with instead.
func TestExternalStreamUpgradeRefused(t *testing.T) {
	recorder := newExtTunnelRecorder()
	_, c := newExtRequest(stdhttp.MethodConnect, "/plain", new(extStream))
	Serve(HandlerFunc(func(c *Context) {
		if _, err := c.Upgrade("", nil, recorder); err == nil || errors.Is(err, ErrNotUpgradable) {
			t.Errorf("Upgrade to no protocol reported %v", err)
		}
		if _, err := c.Upgrade("websocket", nil, nil); err == nil || errors.Is(err, ErrNotUpgradable) {
			t.Errorf("Upgrade without a handler reported %v", err)
		}
		if _, err := c.Upgrade("websocket", nil, recorder); !errors.Is(err, ErrNotUpgradable) {
			t.Errorf("Upgrade on a stream without tunnels reported %v", err)
		}
		c.WriteHeader(stdhttp.StatusOK)
		if _, err := c.Upgrade("websocket", nil, recorder); !errors.Is(err, ErrResponseWritten) {
			t.Errorf("Upgrade after WriteHeader reported %v", err)
		}
	}), c)

	// Away from its handler, a request cannot switch.
	_, c = newExtRequest(stdhttp.MethodConnect, "/late", &extUpgradeStream{writer: new(extTunnelWriter)})
	if _, err := c.Upgrade("websocket", nil, recorder); !errors.Is(err, ErrNotUpgradable) {
		t.Fatalf("Upgrade outside the handler reported %v", err)
	}

	stream := &extUpgradeStream{refuse: ErrNotUpgradable}
	_, c = newExtRequest(stdhttp.MethodConnect, "/refused", stream)
	hooked := 0
	Serve(HandlerFunc(func(c *Context) {
		c.OnHeader(func(int, stdhttp.Header) { hooked++ })
		if _, err := c.Upgrade("websocket", stdhttp.Header{"X-Offer": {"1"}}, recorder); !errors.Is(err, ErrNotUpgradable) {
			t.Errorf("Upgrade the stream refused reported %v", err)
		}
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte("no"))
	}), c)
	if hooked != 2 || stream.response(t).StatusCode != stdhttp.StatusBadRequest {
		t.Fatalf("the header hooks ran %d times, want once for the refused switch and once for the answer", hooked)
	}
	if recorder.got() != "" {
		t.Fatalf("a refused tunnel's handler was called: %q", recorder.got())
	}
}

// TestTunnelFeedBacklogAndEnd checks a feed's limits: what arrives before
// the tunnel opens is held up to the limit and refused past it, what arrives
// once it has ended is dropped, and ending it twice tells the handler once.
func TestTunnelFeedBacklogAndEnd(t *testing.T) {
	feed := NewTunnelFeed(nil, new(extTunnelWriter), 4)
	if err := feed.Write(nil); err != nil {
		t.Fatalf("an empty write reported %v", err)
	}
	if err := feed.Write([]byte("abc")); err != nil {
		t.Fatalf("a write within the backlog reported %v", err)
	}
	if err := feed.Write([]byte("de")); !errors.Is(err, ErrTunnelBacklog) {
		t.Fatalf("a write past the backlog reported %v", err)
	}
	recorder := newExtTunnelRecorder()
	feed.Tunnel().open(recorder)
	feed.End(nil)
	feed.End(errExtStream)
	if err := <-recorder.closed; err != nil {
		t.Fatalf("the tunnel closed with %v, want the first end's nil", err)
	}
	if err := feed.Write([]byte("late")); err != nil {
		t.Fatalf("a write after the end reported %v", err)
	}
	if got := recorder.got(); got != "open data:abc close" {
		t.Fatalf("handler calls %q", got)
	}
}

// extAbandonStream is an extStream that fails every response and records
// that it was given up on.
type extAbandonStream struct {
	extStream
	abandoned bool
}

func (s *extAbandonStream) Abandon() { s.abandoned = true }

// TestExternalStreamAbandonedOnFailedResponse checks that a response the
// stream cannot send, found once the handler is done, has the stream given
// up on, as an HTTP/1 connection is closed then, so that the client is not
// left waiting for an answer that will not come.
func TestExternalStreamAbandonedOnFailedResponse(t *testing.T) {
	stream := &extAbandonStream{extStream: extStream{fail: true}}
	_, c := newExtRequest(stdhttp.MethodGet, "/fails", stream)
	Serve(HandlerFunc(func(c *Context) { _, _ = c.WriteString("never sent") }), c)
	if !stream.abandoned {
		t.Fatal("the stream was not abandoned")
	}
}

// TestExternalStreamContextAfterEnd checks that a request's context asked
// for only once the request has ended is already cancelled.
func TestExternalStreamContextAfterEnd(t *testing.T) {
	_, c := newExtRequest(stdhttp.MethodGet, "/ended", new(extStream))
	Serve(HandlerFunc(func(c *Context) { _ = c.Respond(stdhttp.StatusOK, "", nil) }), c)
	ctx := c.Context()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("the context of an ended request is not done")
	}
}
