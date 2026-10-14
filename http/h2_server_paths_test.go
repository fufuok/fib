//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdtls "crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// h2covWaitErr waits for an error a handler reports.
func h2covWaitErr(t *testing.T, errs chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the handler reported nothing")
		return nil
	}
}

// TestH2ServerRefusesInvalidResponses checks that a response whose status or
// fields HTTP/2 cannot carry is refused before anything is encoded, so the
// handler can still answer, and that the statuses the server spells out
// without formatting come out as they should.
func TestH2ServerRefusesInvalidResponses(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var errs []string
		try := func(err error) {
			if err == nil {
				errs = append(errs, "<nil>")
			} else {
				errs = append(errs, "error")
			}
		}
		switch path := c.Request.URL.Path; path {
		case "/invalid":
			try(c.WriteResponse(Response{StatusCode: 1000}))
			try(c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"Bad Name": {"x"}}}))
			try(c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"X-Bad": {"a\nb"}}}))
			try(c.WriteInterim(103, stdhttp.Header{"Bad Name": {"x"}}))
			_ = c.Respond(200, "text/plain", []byte(strings.Join(errs, " ")))
		case "/big-header":
			_ = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"X-Big": {strings.Repeat("~", 70000)}}})
		default:
			status, _ := strconv.Atoi(path[1:])
			_ = c.Respond(status, "text/plain", nil)
		}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/invalid")...)
	if _, body := tc.response(1); string(body) != "error error error error" {
		t.Fatalf("invalid responses: %q", body)
	}
	id := uint32(3)
	for _, status := range []string{"301", "302", "400"} {
		tc.headers(id, true, get("/"+status)...)
		if header, _ := tc.response(id); header[":status"] != status {
			t.Fatalf("status %s, want %s", header[":status"], status)
		}
		id += 2
	}
	// A header block larger than the connection keeps for the next one.
	for i := 0; i < 2; i++ {
		tc.headers(id, true, get("/big-header")...)
		if header, _ := tc.response(id); len(header["x-big"]) != 70000 {
			t.Fatalf("x-big of %d bytes", len(header["x-big"]))
		}
		id += 2
	}
}

// TestH2ServerStreamingResponseRefusesInvalidHeader checks that a response
// that streams is refused, as one sent whole is, when its header has a field
// HTTP/2 cannot carry or a status HTTP/2 has no use for (101 is neither an
// interim nor a final response there), and that nothing of it reaches the
// client.
func TestH2ServerStreamingResponseRefusesInvalidHeader(t *testing.T) {
	errs := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/ok" {
			_ = c.Respond(200, "text/plain", []byte("ok"))
			return
		}
		if c.Request.URL.Path == "/101" {
			// 101 is no interim response on HTTP/2, and no final one.
			c.WriteHeader(stdhttp.StatusSwitchingProtocols)
		} else {
			c.Header().Set("Bad Name", "x")
		}
		_, _ = c.Write([]byte("x"))
		errs <- c.FlushError()
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/bad")...)
	if err := h2covWaitErr(t, errs); err == nil {
		t.Fatal("a header with an invalid name was sent")
	}
	// The handler is left with a response it cannot send, so the stream is
	// reset rather than left open.
	if code := h2covReset(t, tc, 1); code != H2InternalError {
		t.Fatalf("reset %v, want %v", code, H2InternalError)
	}
	tc.headers(3, true, get("/101")...)
	if err := h2covWaitErr(t, errs); err == nil {
		t.Fatal("a 101 was sent as a final response")
	}
	if code := h2covReset(t, tc, 3); code != H2InternalError {
		t.Fatalf("reset %v, want %v", code, H2InternalError)
	}
	tc.headers(5, true, get("/ok")...)
	if _, body := tc.response(5); string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
	for _, id := range []uint32{1, 3} {
		if st := tc.stream(id); st.header != nil {
			t.Fatalf("the refused response on stream %d was sent: %v", id, st.header)
		}
	}
}

// TestH2ServerStreamingResponseDrainsOnWindowUpdate checks that a response
// held back by the client's window goes out when the window opens, and ends
// cleanly once its handler finishes after it has all gone.
func TestH2ServerStreamingResponseDrainsOnWindowUpdate(t *testing.T) {
	finish := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_, _ = c.Write([]byte(strings.Repeat("a", 100)))
		c.Flush()
		select {
		case <-finish:
		case <-time.After(10 * time.Second):
		}
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 10})
	tc.headers(1, true, get("/")...)
	st := tc.stream(1)
	for len(st.body) < 10 {
		tc.read()
	}
	tc.write(h2AppendWindowUpdate(nil, 1, 1000))
	for len(st.body) < 100 {
		tc.read()
	}
	close(finish)
	if _, body := tc.response(1); len(body) != 100 {
		t.Fatalf("body of %d bytes", len(body))
	}
}

// TestH2ServerIncompleteResponseWhileBodyStreams checks that a response that
// ends short of its declared length, while the request's body is still
// arriving, resets the stream and abandons the body.
func TestH2ServerIncompleteResponseWhileBodyStreams(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		c.Header().Set("Content-Length", "10")
		_, _ = c.Write([]byte("short"))
		c.Flush()
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-length", "100")
	tc.data(1, false, []byte("some"))
	if code := h2covReset(t, tc, 1); code != H2InternalError {
		t.Fatalf("reset %v, want %v", code, H2InternalError)
	}
}

// TestH2ServerStreamedBodyErrors checks the stream errors a body that
// streams to its handler can end in: more than its content-length, less
// than it, and more than the stream's window while the handler takes none
// of it.
func TestH2ServerStreamedBodyErrors(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	release := make(chan struct{})
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/idle" {
			// Takes none of the body until the test is over.
			c.Retain()
			go func() {
				<-release
				c.Release()
			}()
			return
		}
		c.OnBody(func(data []byte, fin bool, err error) {})
	})))
	t.Cleanup(func() { close(release) })
	tc := dialH2(t, addr)
	post := func(id uint32, path string, length int) {
		tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", path,
			"content-length", strconv.Itoa(length))
	}

	post(1, "/", 5)
	tc.data(1, false, []byte("too long"))
	if code := h2covReset(t, tc, 1); code != H2ProtocolError {
		t.Fatalf("long body: reset %v, want %v", code, H2ProtocolError)
	}

	post(3, "/", 10)
	tc.data(3, true, []byte("short"))
	if code := h2covReset(t, tc, 3); code != H2ProtocolError {
		t.Fatalf("short body: reset %v, want %v", code, H2ProtocolError)
	}

	post(5, "/idle", 4<<20)
	chunk := make([]byte, h2DefaultMaxFrameSize)
	for sent := 0; sent <= h2StreamWindow; sent += len(chunk) {
		tc.data(5, false, chunk)
	}
	if code := h2covReset(t, tc, 5); code != H2FlowControlError {
		t.Fatalf("body past the window: reset %v, want %v", code, H2FlowControlError)
	}
}

// TestH2ServerReadDeadlineStoppedWhenBodyEnds checks that a read deadline
// on a body that then arrives in full is stopped, leaving the request to be
// answered as usual.
func TestH2ServerReadDeadlineStoppedWhenBodyEnds(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	set := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		var got []byte
		c.OnBody(func(data []byte, fin bool, err error) {
			got = append(got, data...)
			if fin || err != nil {
				_ = c.Respond(200, "text/plain", fmt.Appendf(nil, "%s %v", got, err))
			}
		})
		set <- c.SetReadDeadline(time.Now().Add(time.Minute))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-length", "5")
	// The body is sent once the deadline is set on it.
	if err := h2covWaitErr(t, set); err != nil {
		t.Fatal(err)
	}
	tc.data(1, true, []byte("hello"))
	if _, body := tc.response(1); string(body) != "hello <nil>" {
		t.Fatalf("body %q", body)
	}
}

// TestH2ServerStreamCallsAfterReset checks what a handler hears from the
// calls it makes on a stream its client has reset: each reports the stream
// closed rather than sending on it.
func TestH2ServerStreamCallsAfterReset(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	results := make(chan []error, 1)
	entered := make(chan struct{}, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		cancelled := make(chan struct{})
		c.OnCancel(func(error) { close(cancelled) })
		entered <- struct{}{}
		select {
		case <-cancelled:
		case <-time.After(10 * time.Second):
		}
		results <- []error{
			c.SetWriteDeadline(time.Now().Add(time.Minute)),
			c.SetReadDeadline(time.Now().Add(time.Minute)),
			c.WriteInterim(103, nil),
			c.Push("/pushed", nil),
		}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-length", "100")
	<-entered
	tc.write(h2AppendRSTStream(nil, 1, H2Cancel))
	select {
	case errs := <-results:
		for i, err := range errs {
			if !errors.Is(err, errH2StreamClosed) {
				t.Errorf("call %d after the reset: %v, want %v", i, err, errH2StreamClosed)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never told of the reset")
	}
}

// TestH2ServerPushRules checks what a promise may name and carry: GET or
// HEAD only, an absolute path or a URL of the connection's own scheme, a
// parsable target, and no fields the connection itself manages; a valid
// HEAD promise of a URL with a header too large for one frame goes out in
// CONTINUATION frames; and a promise after the response has gone is refused.
func TestH2ServerPushRules(t *testing.T) {
	results := make(chan []error, 1)
	// Huffman would shrink a run of one letter below a frame; "~" grows.
	big := strings.Repeat("~", 20000)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path != "/" {
			_ = c.Respond(200, "text/plain", []byte(c.Request.Method+" "+c.Request.Host+c.Request.URL.Path))
			return
		}
		errs := []error{
			c.Push("/x", &stdhttp.PushOptions{Method: "POST"}),
			c.Push("ftp://test/x", nil),
			c.Push("http://", nil),
			c.Push("/%zz", nil),
			c.Push("/x", &stdhttp.PushOptions{Header: stdhttp.Header{"Content-Length": {"1"}}}),
			c.Push("/x", &stdhttp.PushOptions{Header: stdhttp.Header{"Bad Name": {"1"}}}),
			c.Push("http://other/abs", &stdhttp.PushOptions{Method: "HEAD", Header: stdhttp.Header{"X-Big": {big}}}),
		}
		_ = c.Respond(200, "text/plain", []byte("parent"))
		errs = append(errs, c.Push("/late", nil))
		results <- errs
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	if _, body := tc.response(1); string(body) != "parent" {
		t.Fatalf("parent %q", body)
	}
	errs := <-results
	for i, err := range errs[:6] {
		if err == nil {
			t.Errorf("invalid push %d was promised", i)
		}
	}
	if errs[6] != nil {
		t.Fatalf("valid push: %v", errs[6])
	}
	if !errors.Is(errs[7], errPushAfterResponse) {
		t.Fatalf("push after the response: %v, want %v", errs[7], errPushAfterResponse)
	}
	parent := tc.stream(1)
	if len(parent.promises) != 1 {
		t.Fatalf("promises %v", parent.promises)
	}
	promised := tc.stream(parent.promises[0])
	if promised.promised[":method"] != "HEAD" || promised.promised[":authority"] != "other" ||
		promised.promised["x-big"] != big {
		t.Fatalf("promise %v", promised.promised[":method"])
	}
	if header, _ := tc.response(parent.promises[0]); header[":status"] != "200" {
		t.Fatalf("pushed status %s", header[":status"])
	}
}

// TestH2ServerPushOverTLSUsesHTTPS checks that a promise made on a TLS
// connection names the https scheme, and so takes https URLs as targets.
func TestH2ServerPushOverTLSUsesHTTPS(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 1)
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/" {
			errs <- c.Push("https://test/pushed", nil)
		}
		_ = c.Respond(200, "text/plain", []byte(c.Request.URL.Path))
	}))))
	clientConfig.NextProtos = []string{"h2"}
	conn, err := stdtls.Dial("tcp", addr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	tc := newH2TestConn(t, conn)
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	tc.headers(1, true, get("/")...)
	if _, body := tc.response(1); string(body) != "/" {
		t.Fatalf("body %q", body)
	}
	if err := h2covWaitErr(t, errs); err != nil {
		t.Fatal(err)
	}
	promises := tc.stream(1).promises
	if len(promises) != 1 || tc.stream(promises[0]).promised[":scheme"] != "https" {
		t.Fatalf("promises %v", promises)
	}
}

// TestH2CUpgradeFallsBackToHTTP1 checks that an h2c Upgrade whose
// HTTP2-Settings cannot be decoded, or carries settings the server cannot
// accept, is answered over HTTP/1.1 rather than switched.
func TestH2CUpgradeFallsBackToHTTP1(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	for _, settings := range []string{
		"!!!",
		base64.RawURLEncoding.EncodeToString(h2AppendSettings(nil, [2]uint32{uint32(h2SettingEnablePush), 2})[h2FrameHeaderLen:]),
	} {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		_, err = io.WriteString(c, "GET /plain HTTP/1.1\r\nHost: test\r\nConnection: Upgrade, HTTP2-Settings\r\n"+
			"Upgrade: h2c\r\nHTTP2-Settings: "+settings+"\r\n\r\n")
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4096)
		n, _ := io.ReadAtLeast(c, buf, 12)
		c.Close()
		if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 200") {
			t.Fatalf("settings %q: response %q", settings, buf[:n])
		}
	}
}

// TestH2CUpgradeWithPrefaceInSameWrite checks that the preface and frames a
// client sends right behind its h2c Upgrade request, in the same write, are
// taken as HTTP/2.
func TestH2CUpgradeWithPrefaceInSameWrite(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	tc := newH2TestConn(t, c)
	request := "GET /upgrade HTTP/1.1\r\nHost: test\r\nConnection: Upgrade, HTTP2-Settings\r\n" +
		"Upgrade: h2c\r\nHTTP2-Settings: \r\n\r\n"
	block := tc.enc.Begin(nil)
	for _, kv := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":authority", "t"}, {":path", "/next"}} {
		block = tc.enc.AppendField(block, kv[0], kv[1], false)
	}
	out := append([]byte(request), h2Preface...)
	out = h2AppendSettings(out)
	out = h2AppendHeaderBlock(out, 3, block, true, h2DefaultMaxFrameSize)
	tc.write(out)
	head := make([]byte, len(h2cSwitchingProtocols))
	if _, err = io.ReadFull(c, head); err != nil || string(head) != h2cSwitchingProtocols {
		t.Fatalf("got %q, %v", head, err)
	}
	if _, body := tc.response(1); string(body) != "GET /upgrade " {
		t.Fatalf("stream 1: %q", body)
	}
	if _, body := tc.response(3); string(body) != "GET /next " {
		t.Fatalf("stream 3: %q", body)
	}
}
