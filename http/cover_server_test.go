//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// cvSrvStream is a Stream of a protocol served outside this package, which
// records what the Context hands it.
type cvSrvStream struct {
	mu        sync.Mutex
	responses []Response
	interims  []int
	pushErr   error
}

func (s *cvSrvStream) WriteResponse(_ *stdhttp.Request, response Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The header is the caller's only for the call, as Respond's is.
	response.Header = response.Header.Clone()
	s.responses = append(s.responses, response)
	return nil
}

func (s *cvSrvStream) WriteInterim(status int, _ stdhttp.Header) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.interims = append(s.interims, status)
	return nil
}

func (s *cvSrvStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return s.pushErr }

// cvSrvUpgrader is a cvSrvStream whose extended CONNECT switches to feed's
// tunnel.
type cvSrvUpgrader struct {
	cvSrvStream
	feed   *TunnelFeed
	header stdhttp.Header
}

func (s *cvSrvUpgrader) Upgrade(_ *stdhttp.Request, _ string, header stdhttp.Header) (*Tunnel, error) {
	s.header = header
	return s.feed.Tunnel(), nil
}

// cvSrvWriter is a TunnelWriter that frames what it sends, as an HTTP/2
// stream does, and so has no SendShared of its own.
type cvSrvWriter struct {
	sent            bytes.Buffer
	closedAfterSend bool
	closed          bool
}

func (w *cvSrvWriter) SendParts(first, second []byte) error {
	w.sent.Write(first)
	w.sent.Write(second)
	return nil
}
func (w *cvSrvWriter) SendOwned(data []byte) error { w.sent.Write(data); return nil }
func (w *cvSrvWriter) CloseAfterSend()             { w.closedAfterSend = true }
func (w *cvSrvWriter) Close() error                { w.closed = true; return nil }

// cvSrvTunnel records the calls a tunnel makes to its handler.
type cvSrvTunnel struct {
	mu       sync.Mutex
	calls    []string
	shutdown chan struct{}
}

func (h *cvSrvTunnel) record(call string) {
	h.mu.Lock()
	h.calls = append(h.calls, call)
	h.mu.Unlock()
}
func (h *cvSrvTunnel) OnTunnelOpen(*Tunnel)                { h.record("open") }
func (h *cvSrvTunnel) OnTunnelData(_ *Tunnel, data []byte) { h.record("data " + string(data)) }
func (h *cvSrvTunnel) OnTunnelClose(_ *Tunnel, err error)  { h.record(fmt.Sprint("close ", err)) }
func (h *cvSrvTunnel) callList() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.calls, "|")
}
func (h *cvSrvShutdownTunnel) OnTunnelShutdown(t *Tunnel) { h.record("shutdown"); h.onShutdown(t) }
func (h *cvSrvShutdownTunnel) OnTunnelClose(_ *Tunnel, e error) {
	h.record(fmt.Sprint("close ", e))
	h.closed <- e
}

// cvSrvShutdownTunnel is a cvSrvTunnel that hears of the server's shutdown.
type cvSrvShutdownTunnel struct {
	cvSrvTunnel
	onShutdown func(t *Tunnel)
	closed     chan error
}

func cvSrvRequest() *stdhttp.Request {
	return &stdhttp.Request{Method: stdhttp.MethodGet, URL: &url.URL{Path: "/"}, Proto: "HTTP/3",
		ProtoMajor: 3, Header: make(stdhttp.Header)}
}

// TestCoverServerExternalStream checks that a Context made for a protocol
// served outside this package hands its response, interim responses and
// pushes to its Stream, and that Serve writes the handler's response.
func TestCoverServerExternalStream(t *testing.T) {
	stream := &cvSrvStream{pushErr: errors.New("no push")}
	ctx := NewStreamContext(nil, cvSrvRequest(), stream)
	var pushErr, interimErr error
	Serve(HandlerFunc(func(c *Context) {
		pushErr = c.Push("/style.css", nil)
		interimErr = c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"</a>"}})
		_ = c.Respond(stdhttp.StatusCreated, "text/plain", []byte("made"))
	}), ctx)
	if pushErr != stream.pushErr || interimErr != nil {
		t.Fatalf("Push %v, WriteInterim %v", pushErr, interimErr)
	}
	if len(stream.interims) != 1 || stream.interims[0] != stdhttp.StatusEarlyHints {
		t.Fatalf("interims %v", stream.interims)
	}
	if len(stream.responses) != 1 || stream.responses[0].StatusCode != stdhttp.StatusCreated ||
		string(stream.responses[0].Body) != "made" {
		t.Fatalf("responses %+v", stream.responses)
	}
}

// TestCoverServerServeRecycled checks that Serve does not run the handler of
// a request whose StreamRequest has been recycled, and that Cancel does
// nothing for one that was never handed a Context.
func TestCoverServerServeRecycled(t *testing.T) {
	(&StreamRequest{}).Cancel(io.EOF)

	r := new(StreamRequest)
	r.Request = *cvSrvRequest()
	ctx := r.Context(nil, &cvSrvStream{}, nil)
	r.Recycle()
	ran := false
	Serve(HandlerFunc(func(*Context) { ran = true }), ctx)
	if ran {
		t.Fatal("the handler of a recycled request ran")
	}
}

// TestCoverServerSmallUnits pins down the small helpers: an unparsable
// request target, a refusal's error text, an empty header name, and a server
// without a handler answering 404.
func TestCoverServerSmallUnits(t *testing.T) {
	var u url.URL
	if err := ParseRequestTarget(&u, ""); err == nil {
		t.Error("an empty request target parsed")
	}
	if got := requestError(stdhttp.StatusBadRequest).Error(); got != "http: Bad Request" {
		t.Errorf("requestError = %q", got)
	}
	if _, err := appendHeaderLines(nil, stdhttp.Header{"": {"v"}}, nil); err == nil {
		t.Error("an empty header name was written")
	}
	// HTTP/1's marshalResponse, for a chunked response and an invalid status.
	req := &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 1, ProtoMinor: 1}
	out, err := marshalResponse(req, Response{StatusCode: 200, Body: []byte("x"), Trailer: stdhttp.Header{"X-T": {"1"}}}, false)
	if err != nil || !bytes.Contains(out, []byte("Transfer-Encoding: chunked")) || !bytes.HasSuffix(out, []byte("X-T: 1\r\n\r\n")) {
		t.Errorf("chunked marshal %q %v", out, err)
	}
	if _, err := marshalResponse(req, Response{StatusCode: 1000}, false); err == nil {
		t.Error("status 1000 marshalled")
	}
	h := NewHandlerWithConfig(DefaultConfig(), nil)
	h.OnPriorityData(nil, nil)
	conn := dialRaw(t, serve(t, h))
	conn.send("GET /x HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp, body := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusNotFound || body != "404 page not found\n" {
		t.Fatalf("no handler: %d %q", resp.StatusCode, body)
	}
}

// TestCoverServerHTTP1WriteErrors checks the ways an HTTP/1 handler can try
// to write a response twice, or one that cannot be written, and that each is
// refused without disturbing the response that does go out.
func TestCoverServerHTTP1WriteErrors(t *testing.T) {
	results := make(chan string, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var errs []string
		note := func(err error) { errs = append(errs, fmt.Sprint(err)) }
		switch c.Request.URL.Path {
		case "/respond-after-header":
			// Respond with a content type, no hooks, after WriteHeader.
			c.WriteHeader(stdhttp.StatusAccepted)
			note(c.Respond(stdhttp.StatusOK, "text/plain", nil))
			note(c.WriteResponse(Response{}))
			fmt.Fprint(c, strings.Join(errs, ","))
			return
		case "/hooked-respond-after-header":
			c.OnHeader(func(int, stdhttp.Header) {})
			c.WriteHeader(stdhttp.StatusAccepted)
			note(c.Respond(stdhttp.StatusOK, "text/plain", nil))
			fmt.Fprint(c, strings.Join(errs, ","))
			return
		case "/twice-typed":
			// A zero status is 200, and a second write is refused.
			note(c.Respond(0, "text/plain", []byte("one")))
			note(c.Respond(stdhttp.StatusOK, "text/plain", []byte("two")))
		case "/twice-whole":
			note(c.WriteResponse(Response{Body: []byte("one")}))
			note(c.Respond(stdhttp.StatusOK, "", []byte("two")))
		case "/bad-interim":
			note(c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"a\nb"}}))
			note(c.WriteResponse(Response{Header: stdhttp.Header{"": {"v"}}}))
			note(c.WriteResponse(Response{Body: []byte("x"), Trailer: stdhttp.Header{"X-T": {"a\nb"}}}))
			note(c.WriteResponse(Response{StatusCode: 1000, Body: []byte("x"), Trailer: stdhttp.Header{"X-T": {"1"}}}))
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strings.Join(errs, ",")))
		}
		results <- strings.Join(errs, ",")
	})))
	conn := dialRaw(t, addr)
	check := func(path string, status int, want string) {
		t.Helper()
		conn.send("GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n")
		resp, body := conn.response(stdhttp.MethodGet)
		if resp.StatusCode != status || body != want {
			t.Errorf("%s: %d %q, want %d %q", path, resp.StatusCode, body, status, want)
		}
	}
	written := ErrResponseWritten.Error()
	check("/respond-after-header", stdhttp.StatusAccepted, written+","+written)
	check("/hooked-respond-after-header", stdhttp.StatusAccepted, written)
	for _, path := range []string{"/twice-typed", "/twice-whole"} {
		check(path, stdhttp.StatusOK, "one")
		if errs := <-results; errs != "<nil>,"+written {
			t.Errorf("%s: %s", path, errs)
		}
	}
	check("/bad-interim", stdhttp.StatusOK, "http: invalid response header value,http: invalid response header name,"+
		"http: invalid response header value,http: invalid status code 1000")
	<-results
}

// TestCoverServerHooks checks the hook paths a plain response takes: an
// OnHeader hook given a response without a header gets one to change, an
// OnResponse hook that zeroes the status leaves 200, and OnFinish reports no
// body for a HEAD.
func TestCoverServerHooks(t *testing.T) {
	sizes := make(chan int64, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		switch c.Request.URL.Path {
		case "/header":
			c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
			_ = c.Respond(stdhttp.StatusOK, "", []byte("body"))
		case "/zero":
			c.OnResponse(func(r *Response) { r.StatusCode = 0 })
			_ = c.Respond(stdhttp.StatusTeapot, "", []byte("body"))
		case "/head":
			c.OnFinish(func(_ int, _ stdhttp.Header, size int64) { sizes <- size })
			_ = c.WriteResponse(Response{Body: []byte("body")})
		}
	})))
	conn := dialRaw(t, addr)
	conn.send("GET /header HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp, body := conn.response(stdhttp.MethodGet); resp.Header.Get("X-Hook") != "1" || body != "body" {
		t.Errorf("/header: %v %q", resp.Header, body)
	}
	conn.send("GET /zero HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK {
		t.Errorf("/zero: %d, want 200", resp.StatusCode)
	}
	conn.send("HEAD /head HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp, _ := conn.response(stdhttp.MethodHead); resp.ContentLength != 4 {
		t.Errorf("/head: Content-Length %d", resp.ContentLength)
	}
	if size := <-sizes; size != 0 {
		t.Errorf("OnFinish for HEAD reported %d bytes", size)
	}
}

// TestCoverServerHeaderTooLarge checks that a header past MaxHeaderBytes is
// refused with 431.
func TestCoverServerHeaderTooLarge(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 64
	conn := dialRaw(t, serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {}))))
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nX-Long: " + strings.Repeat("a", 128) + "\r\n\r\n")
	if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status %d, want 431", resp.StatusCode)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
}

// TestCoverServerHeaderTimeoutFallsBackToReadTimeout checks that a header
// that stops arriving is bounded by ReadTimeout when ReadHeaderTimeout is not
// set.
func TestCoverServerHeaderTimeoutFallsBackToReadTimeout(t *testing.T) {
	config := DefaultConfig()
	// DefaultConfig sets one; this is the server without it.
	config.ReadHeaderTimeout = 0
	config.ReadTimeout = 100 * time.Millisecond
	h := NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {}))
	if h.headerTimeout() != config.ReadTimeout {
		t.Fatalf("headerTimeout %v", h.headerTimeout())
	}
	conn := dialRaw(t, serve(t, h))
	conn.send("GET / HTTP/1.1\r\nHost: x\r\n")
	if !conn.closed() {
		t.Fatal("a stalled header outlived ReadTimeout")
	}
}

// TestCoverServerRetainedClose checks that a retained request asking to
// close is answered from another goroutine and its connection then closed.
func TestCoverServerRetainedClose(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		go func() {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("later"))
			c.Release()
		}()
	})))
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if resp, body := conn.response(stdhttp.MethodGet); body != "later" || !resp.Close {
		t.Fatalf("%q, Close %v", body, resp.Close)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
}

// TestCoverServerStreamedBodyAnsweredInRoundCloses checks that a streamed
// body whose callback answers a Connection: close request from inside the
// round that brought its last byte closes the connection behind the answer.
func TestCoverServerStreamedBodyAnsweredInRoundCloses(t *testing.T) {
	entered := make(chan struct{}, 1)
	addr := serve(t, NewHandlerWithConfig(immediateStreamingConfig(), HandlerFunc(func(c *Context) {
		var got []byte
		c.OnBody(func(data []byte, fin bool, err error) {
			got = append(got, data...)
			if fin || err != nil {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", got)
			}
		})
		entered <- struct{}{}
	})))
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 6\r\n\r\nabc")
	<-entered
	conn.send("def")
	if resp, body := conn.response(stdhttp.MethodPost); body != "abcdef" || !resp.Close {
		t.Fatalf("%q, Close %v", body, resp.Close)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
}

// TestCoverServerStreamedBodyMalformedBeforeHandler checks that a streamed
// body misframed in the bytes that came with its header fails the handler's
// read and ends the connection once the handler has answered.
func TestCoverServerStreamedBodyMalformedBeforeHandler(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(immediateStreamingConfig(), HandlerFunc(func(c *Context) {
		_, err := io.ReadAll(c.Request.Body)
		if !errors.Is(err, ErrMalformed) {
			err = fmt.Errorf("read %v, want ErrMalformed", err)
		}
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
	})))
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
	if resp, body := conn.response(stdhttp.MethodPost); resp.StatusCode != stdhttp.StatusBadRequest ||
		!strings.Contains(body, "invalid chunk size") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
}

// TestCoverServerHandlerWithoutOpen checks the entry points a wrapping
// fib.Handler may call out of the usual order: OnData on a connection whose
// OnOpen it never passed on, OnHangup on one with no request in flight, and
// OnShutdown on one whose request is being served, which leaves it be.
func TestCoverServerHandlerWithoutOpen(t *testing.T) {
	var h *ServerHandler
	h = NewHandler(HandlerFunc(func(c *Context) {
		h.OnShutdown(c.Conn)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(c.Request.URL.Path))
	}))
	for _, wrapped := range []fib.HandlerFuncs{
		{Data: h.OnData, Close: h.OnClose},
		{Open: func(c *fib.Connection) { h.OnOpen(c); h.OnHangup(c) }, Data: h.OnData, Close: h.OnClose},
	} {
		conn := dialRaw(t, serve(t, wrapped))
		conn.send("GET /p HTTP/1.1\r\nHost: x\r\n\r\n")
		if _, body := conn.response(stdhttp.MethodGet); body != "/p" {
			t.Fatalf("body %q", body)
		}
	}
}

// TestCoverServerUnixSocketRemoteAddr checks that a request over a Unix
// socket, which has no IP address and port, still carries the peer's
// address as RemoteAddr reports it.
func TestCoverServerUnixSocketRemoteAddr(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no Unix sockets for the engine here")
	}
	dir, err := os.MkdirTemp("/tmp", "cvsrv")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	config := fib.DefaultConfig()
	server, err := fib.New(config, fib.Binding{Network: "unix", Addr: path, Handler: NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("remote="+c.Request.RemoteAddr))
	}))})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	conn, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.HasPrefix(string(body), "remote=") {
		t.Fatalf("body %q", body)
	}
}

// cvSrvSendTunnel answers what arrives through an HTTP/1 tunnel by sending it
// back through whichever of the tunnel's methods the data names.
type cvSrvSendTunnel struct{ closed chan error }

func (h *cvSrvSendTunnel) OnTunnelOpen(*Tunnel) {}
func (h *cvSrvSendTunnel) OnTunnelData(t *Tunnel, data []byte) {
	if t.Conn() == nil {
		_ = t.Send([]byte("no conn;"))
	}
	switch string(data) {
	case "parts":
		_ = t.SendParts([]byte("pa"), []byte("rts;"))
	case "owned":
		_ = t.SendOwned([]byte("owned;"))
	case "shared":
		_ = t.SendShared([]byte("shared;"))
	case "bye":
		_ = t.Send([]byte("bye;"))
		t.CloseAfterSend()
	case "close":
		_ = t.Close()
	}
}
func (h *cvSrvSendTunnel) OnTunnelClose(_ *Tunnel, err error) { h.closed <- err }

// TestCoverServerHTTP1TunnelSends checks the ways a handler sends through an
// HTTP/1 tunnel, and both ways it ends one.
func TestCoverServerHTTP1TunnelSends(t *testing.T) {
	handler := &cvSrvSendTunnel{closed: make(chan error, 2)}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if _, err := c.Upgrade("cv", nil, handler); err != nil {
			_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
		}
	})))
	upgrade := func() *rawConn {
		conn := dialRaw(t, addr)
		conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: cv\r\n\r\n")
		if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
			t.Fatalf("status %d", resp.StatusCode)
		}
		return conn
	}
	conn := upgrade()
	var want string
	for _, word := range []string{"parts", "owned", "shared", "bye"} {
		conn.send(word)
		want += word + ";"
		got := make([]byte, len(word)+1)
		if _, err := io.ReadFull(conn.r, got); err != nil || string(got) != word+";" {
			t.Fatalf("sent %q, read %q %v", word, got, err)
		}
	}
	if !conn.closed() {
		t.Fatal("CloseAfterSend left the tunnel open")
	}
	<-handler.closed
	conn = upgrade()
	conn.send("close")
	if !conn.closed() {
		t.Fatal("Close left the tunnel open")
	}
	<-handler.closed
}

// TestCoverServerHTTP1UpgradeRefusals checks the requests and arguments
// Upgrade refuses over HTTP/1, each answered by the handler instead; an
// OnHeader hook still runs for that answer.
func TestCoverServerHTTP1UpgradeRefusals(t *testing.T) {
	tunnel := &cvSrvTunnel{}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var err error
		switch c.Request.URL.Path {
		case "/nil":
			_, err = c.Upgrade("cv", nil, nil)
		case "/empty":
			_, err = c.Upgrade("", nil, tunnel)
		case "/ctl":
			_, err = c.Upgrade("c\nv", nil, tunnel)
		case "/begun":
			c.WriteHeader(stdhttp.StatusOK)
			_, err = c.Upgrade("cv", nil, tunnel)
			fmt.Fprint(c, err)
			return
		case "/hooked":
			c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
			_, err = c.Upgrade("cv", nil, tunnel)
		case "/bad-header":
			_, err = c.Upgrade("cv", stdhttp.Header{"X": {"a\nb"}}, tunnel)
		case "/closed":
			_ = c.Conn.Close()
			// The connection has gone, so nothing can answer; the test
			// only needs the call to fail rather than upgrade.
			_, _ = c.Upgrade("cv", nil, tunnel)
			return
		default:
			_, err = c.Upgrade("cv", nil, tunnel)
		}
		_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(fmt.Sprint(err)))
	})))
	conn := dialRaw(t, addr)
	check := func(path, upgrade string, status int, want string) *stdhttp.Response {
		t.Helper()
		conn.send("GET " + path + " HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: " + upgrade + "\r\n\r\n")
		resp, body := conn.response(stdhttp.MethodGet)
		if resp.StatusCode != status || body != want {
			t.Errorf("%s: %d %q, want %d %q", path, resp.StatusCode, body, status, want)
		}
		return resp
	}
	check("/nil", "cv", stdhttp.StatusUpgradeRequired, "http: invalid upgrade")
	check("/empty", "cv", stdhttp.StatusUpgradeRequired, "http: invalid upgrade")
	check("/ctl", "cv", stdhttp.StatusUpgradeRequired, "http: invalid upgrade")
	check("/begun", "cv", stdhttp.StatusOK, ErrResponseWritten.Error())
	check("/other", "websocket, h2c", stdhttp.StatusUpgradeRequired, ErrNotUpgradable.Error())
	if resp := check("/hooked", "other", stdhttp.StatusUpgradeRequired, ErrNotUpgradable.Error()); resp.Header.Get("X-Hook") != "1" {
		t.Errorf("/hooked: the refusal's answer lost its OnHeader hook: %v", resp.Header)
	}
	check("/bad-header", "cv", stdhttp.StatusUpgradeRequired, "http: invalid response header value")
	conn.send("GET /closed HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: cv\r\n\r\n")
	if !conn.closed() {
		t.Error("/closed: the connection stayed open")
	}
	if calls := tunnel.callList(); calls != "" {
		t.Errorf("a refused upgrade reached its handler: %s", calls)
	}
}

// TestCoverServerHTTP1UpgradeHooks checks that a successful upgrade runs the
// OnHeader hooks on the 101 it sends, given no header of the handler's, and
// the OnFinish hooks after it.
func TestCoverServerHTTP1UpgradeHooks(t *testing.T) {
	finished := make(chan int, 1)
	echo := &echoTunnel{closed: make(chan error, 1)}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
		c.OnFinish(func(status int, _ stdhttp.Header, _ int64) { finished <- status })
		if _, err := c.Upgrade("echo", nil, echo); err != nil {
			_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
		}
	})))
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusSwitchingProtocols ||
		resp.Header.Get("X-Hook") != "1" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	if status := <-finished; status != stdhttp.StatusSwitchingProtocols {
		t.Errorf("OnFinish status %d", status)
	}
	conn.Close()
	<-echo.closed
}

// TestCoverServerUpgradeAfterStreamedBody checks that a connection whose
// earlier request streamed its body can still be upgraded, the body's read
// deadline bookkeeping going with it.
func TestCoverServerUpgradeAfterStreamedBody(t *testing.T) {
	echo := &echoTunnel{closed: make(chan error, 1)}
	addr := serve(t, NewHandlerWithConfig(immediateStreamingConfig(), HandlerFunc(func(c *Context) {
		if c.Request.Method == stdhttp.MethodPost {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("posted"))
			return
		}
		if _, err := c.Upgrade("echo", nil, echo); err != nil {
			_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
		}
	})))
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello")
	if _, body := conn.response(stdhttp.MethodPost); body != "posted" {
		t.Fatalf("body %q", body)
	}
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\nping")
	if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(conn.r, got); err != nil || string(got) != "ping" {
		t.Fatalf("read %q %v", got, err)
	}
	conn.Close()
	<-echo.closed
}

// TestCoverServerShutdownTunnel checks that Shutdown tells an HTTP/1
// tunnel's TunnelShutdownHandler, once, and returns when the protocol it
// carries ends it.
func TestCoverServerShutdownTunnel(t *testing.T) {
	handler := &cvSrvShutdownTunnel{closed: make(chan error, 1)}
	handler.onShutdown = func(t *Tunnel) {
		_ = t.Send([]byte("going away"))
		t.CloseAfterSend()
	}
	opened := make(chan struct{}, 1)
	engine, addr, done := shutdownServer(t, NewHandler(HandlerFunc(func(c *Context) {
		if _, err := c.Upgrade("cv", nil, handler); err != nil {
			_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
			return
		}
		opened <- struct{}{}
	})))
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: cv\r\n\r\n")
	if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("status %d", resp.StatusCode)
	}
	<-opened
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	got := make([]byte, len("going away"))
	if _, err := io.ReadFull(conn.r, got); err != nil || string(got) != "going away" {
		t.Fatalf("read %q %v", got, err)
	}
	if !conn.closed() {
		t.Fatal("the tunnel stayed open")
	}
	if err := <-shut; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	<-handler.closed
	if calls := handler.callList(); calls != "open|shutdown|close <nil>" && calls != "open|shutdown|close EOF" {
		t.Errorf("calls %s", calls)
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestCoverServerTunnelFeed checks a TunnelFeed as a protocol served outside
// this package drives it: what arrives before the tunnel opens is held up to
// its limit, a shutdown before it opens is told as it opens and only once,
// SendShared falls back to SendParts on a writer that frames, and nothing is
// delivered or told once the tunnel has ended.
func TestCoverServerTunnelFeed(t *testing.T) {
	w := &cvSrvWriter{}
	feed := NewTunnelFeed(nil, w, 4)
	if err := feed.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := feed.Write([]byte("de")); err != ErrTunnelBacklog {
		t.Fatalf("Write past the limit: %v", err)
	}
	feed.Shutdown()

	handler := &cvSrvShutdownTunnel{closed: make(chan error, 1), onShutdown: func(*Tunnel) {}}
	upgrader := &cvSrvUpgrader{feed: feed}
	var tunnel *Tunnel
	var upgradeErr error
	finished := 0
	Serve(HandlerFunc(func(c *Context) {
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
		c.OnFinish(func(int, stdhttp.Header, int64) { finished++ })
		tunnel, upgradeErr = c.Upgrade("cv", nil, handler)
	}), NewStreamContext(nil, cvSrvRequest(), upgrader))
	if upgradeErr != nil || tunnel != feed.Tunnel() {
		t.Fatalf("Upgrade %v", upgradeErr)
	}
	if upgrader.header.Get("X-Hook") != "1" || finished != 1 {
		t.Errorf("hooks: header %v, %d finishes", upgrader.header, finished)
	}
	feed.Shutdown()
	_ = tunnel.SendShared([]byte("shared"))
	tunnel.CloseAfterSend()
	_ = tunnel.Close()
	if w.sent.String() != "shared" || !w.closedAfterSend || !w.closed || tunnel.Conn() != nil {
		t.Errorf("writer %q %v %v", w.sent.String(), w.closedAfterSend, w.closed)
	}
	feed.End(io.EOF)
	feed.End(nil)
	feed.Shutdown()
	_ = feed.Write([]byte("late"))
	<-handler.closed
	if calls := handler.callList(); calls != "open|shutdown|data abc|close EOF" {
		t.Errorf("calls %s", calls)
	}

	// A shutdown reaches an open tunnel at once; a handler that does not
	// take it is not told.
	plain := &cvSrvTunnel{}
	open := NewTunnelFeed(nil, w, 0)
	open.tunnel.open(plain)
	open.Shutdown()
	open.End(nil)
	if calls := plain.callList(); calls != "open|close <nil>" {
		t.Errorf("calls %s", calls)
	}
}

// TestCoverServerExternalUpgradeRefusals checks Upgrade on a Context of an
// external stream: refused when the stream has no tunnels, and when the
// handler is not running.
func TestCoverServerExternalUpgradeRefusals(t *testing.T) {
	tunnel := &cvSrvTunnel{}
	idle := NewStreamContext(nil, cvSrvRequest(), &cvSrvStream{})
	if _, err := idle.Upgrade("cv", nil, tunnel); err != ErrNotUpgradable {
		t.Errorf("Upgrade outside the handler: %v", err)
	}
	stream := &cvSrvStream{}
	var err error
	Serve(HandlerFunc(func(c *Context) {
		_, err = c.Upgrade("cv", nil, tunnel)
	}), NewStreamContext(nil, cvSrvRequest(), stream))
	if err != ErrNotUpgradable {
		t.Errorf("Upgrade on a stream without tunnels: %v", err)
	}
}

// TestCoverServerExternalUpgradeRefusalKeepsHooks is a repro: an Upgrade
// refused because the external stream has no tunnels has already run the
// OnHeader hooks, and does not reset them as the other refusals do, so the
// response the handler answers with instead goes out without them.
func TestCoverServerExternalUpgradeRefusalKeepsHooks(t *testing.T) {
	stream := &cvSrvStream{}
	Serve(HandlerFunc(func(c *Context) {
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
		_, _ = c.Upgrade("cv", nil, &cvSrvTunnel{})
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	}), NewStreamContext(nil, cvSrvRequest(), stream))
	if len(stream.responses) != 1 || stream.responses[0].Header.Get("X-Hook") != "1" {
		t.Fatalf("responses %+v, want one with X-Hook", stream.responses)
	}
}

// cvSrvSignalHandler is a ServerHandler that says when it has taken a round
// of data and when it has heard a round of the engine's shutdown.
type cvSrvSignalHandler struct {
	*ServerHandler
	read, shut chan struct{}
}

func (h *cvSrvSignalHandler) OnData(c *fib.Connection, data []byte) {
	h.ServerHandler.OnData(c, data)
	select {
	case h.read <- struct{}{}:
	default:
	}
}

func (h *cvSrvSignalHandler) OnShutdown(c *fib.Connection) {
	h.ServerHandler.OnShutdown(c)
	select {
	case h.shut <- struct{}{}:
	default:
	}
}

// TestCoverServerShutdownMidHeader checks that Shutdown leaves open a
// connection whose request header is still arriving, and that the request,
// once in, is answered with Connection: close.
func TestCoverServerShutdownMidHeader(t *testing.T) {
	h := &cvSrvSignalHandler{ServerHandler: NewHandler(slowHandler(nil, nil)),
		read: make(chan struct{}, 1), shut: make(chan struct{}, 1)}
	engine, addr, done := shutdownServer(t, h)
	conn := dialRaw(t, addr)
	conn.send("GET /mid HTTP/1.1\r\n")
	<-h.read
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	<-h.shut
	conn.send("Host: x\r\n\r\n")
	if resp, body := conn.response(stdhttp.MethodGet); body != "ok" || !resp.Close {
		t.Fatalf("%q, Close %v", body, resp.Close)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
	if err := <-shut; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestCoverServerFailedWriteResponseKeepsHooks checks that a WriteResponse
// that sent nothing, its header being invalid, leaves the OnHeader hooks to
// run for the response the handler answers with instead.
func TestCoverServerFailedWriteResponseKeepsHooks(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "1") })
		bad := Response{StatusCode: stdhttp.StatusOK, Header: stdhttp.Header{"X-Bad": {"a\nb"}}}
		if err := c.WriteResponse(bad); err == nil {
			t.Error("a header value with a newline was sent")
		}
		c.WriteHeader(stdhttp.StatusInternalServerError)
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	resp, err := stdhttp.ReadResponse(conn.r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusInternalServerError || resp.Header.Get("X-Hook") != "1" {
		t.Fatalf("got %d with X-Hook %q, want 500 with X-Hook 1", resp.StatusCode, resp.Header.Get("X-Hook"))
	}
}
