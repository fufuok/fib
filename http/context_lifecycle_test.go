//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"io"
	stdhttp "net/http"
	"os"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// extLiveConn is a live connection for a Context served over an external
// stream, as an HTTP/3 request's is served over its QUIC connection: what is
// released away from the handler flushes the connection it arrived on. It is
// an HTTP/1 connection whose one request has been answered, which stays open
// for the rest of the test.
func extLiveConn(t *testing.T) *fib.Connection {
	t.Helper()
	conns := make(chan *fib.Connection, 1)
	addr := serveHTTP1(t, func(c *Context) {
		conns <- c.Conn
		_ = c.Respond(stdhttp.StatusNoContent, "", nil)
	})
	raw := dialRaw(t, addr)
	raw.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	return <-conns
}

// waitLifecycle waits for a value a callback on the handler pool sends.
func waitLifecycle[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never happened", what)
		panic("unreachable")
	}
}

// TestContextRetained checks what Retained reports through a handler's life:
// not while the handler's own hold is the only one, while a Retain holds the
// response open, and not once the last hold has gone.
func TestContextRetained(t *testing.T) {
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/retained", stream)
	c.Conn = extLiveConn(t)
	var before, during bool
	Serve(HandlerFunc(func(c *Context) {
		before = c.Retained()
		c.Retain()
		during = c.Retained()
	}), c)
	if before || !during {
		t.Fatalf("Retained before a Retain %v, after %v", before, during)
	}
	if !c.Retained() {
		t.Fatal("a retained request whose handler returned reported not retained")
	}
	c.Release()
	if c.Retained() {
		t.Fatal("a released request reported retained")
	}
	if got := stream.response(t); got.StatusCode != stdhttp.StatusOK {
		t.Fatalf("the released response was %d", got.StatusCode)
	}
}

// TestContextTooManyRetains checks that the count of holds on one request
// cannot wrap around: the Retain past the last one panics rather than
// finishing the response under the holds it would lose.
func TestContextTooManyRetains(t *testing.T) {
	c := new(Context)
	for range ctxHolds {
		c.Retain()
	}
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "too many Retains") {
			t.Fatalf("the Retain past the limit recovered %v", recovered)
		}
	}()
	c.Retain()
}

// TestContextReleaseNeverServed checks that a Release on a Context that was
// never served, which holds nothing, does nothing.
func TestContextReleaseNeverServed(t *testing.T) {
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/unserved", stream)
	c.Release()
	if ctxState(c.word.Load()) != ctxOpen || len(stream.responses) != 0 {
		t.Fatal("a Release on an unserved Context finished it")
	}
}

// TestContextQueryWithoutRequest checks that Query on a Context with no
// request, as a recycled one has, finds nothing rather than panicking.
func TestContextQueryWithoutRequest(t *testing.T) {
	if got := new(Context).Query("q"); got != "" {
		t.Fatalf("Query = %q", got)
	}
}

// TestContextCancelledBeforeServed cancels a request before its handler
// runs, as a stream reset while the request waits for a worker does. The
// handler is not run, a callback registered afterwards hears the reason at
// once, OnBody's on the handler pool, and one registered after the response
// was written hears nothing.
func TestContextCancelledBeforeServed(t *testing.T) {
	new(StreamRequest).Cancel(os.ErrDeadlineExceeded) // no Context yet: nothing to cancel

	r, c := newExtRequest(stdhttp.MethodGet, "/cancelled", new(extStream))
	r.Cancel(os.ErrDeadlineExceeded)
	deadline := time.Now().Add(10 * time.Second)
	for c.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the cancellation never reached the request")
		}
		time.Sleep(time.Millisecond)
	}
	ran := false
	Serve(HandlerFunc(func(*Context) { ran = true }), c)
	if ran {
		t.Fatal("the handler of a cancelled request ran")
	}
	c.OnCancel(nil)
	var heard error
	c.OnCancel(func(err error) { heard = err })
	if !errors.Is(heard, os.ErrDeadlineExceeded) {
		t.Fatalf("OnCancel on a cancelled request heard %v", heard)
	}
	c.OnBody(nil)
	bodies := make(chan error, 1)
	c.OnBody(func(data []byte, fin bool, err error) {
		if len(data) != 0 || !fin {
			err = errors.New("the failed body was delivered with data or without fin")
		}
		bodies <- err
	})
	if err := waitLifecycle(t, bodies, "the failed body's callback"); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnBody on a cancelled request heard %v", err)
	}

	_, c = newExtRequest(stdhttp.MethodGet, "/answered", new(extStream))
	Serve(HandlerFunc(func(c *Context) { _ = c.Respond(stdhttp.StatusOK, "text/plain", nil) }), c)
	c.OnCancel(func(error) { t.Error("OnCancel on an answered request ran") })
	c.OnBody(func([]byte, bool, error) { t.Error("OnBody on an answered request ran") })
}

// TestContextOnBodyTwice checks that a second OnBody while the handler runs
// replaces the first: the body is handed over once, to the second.
func TestContextOnBodyTwice(t *testing.T) {
	r := new(StreamRequest)
	r.URL.Path = "/twice"
	r.Request.URL, r.Request.Method, r.Request.Header = &r.URL, stdhttp.MethodPost, stdhttp.Header{}
	stream := new(extStream)
	c := r.Context(nil, stream, []byte("payload"))
	var got []string
	Serve(HandlerFunc(func(c *Context) {
		c.OnBody(func(data []byte, _ bool, _ error) { got = append(got, "first:"+string(data)) })
		c.OnBody(func(data []byte, fin bool, _ error) {
			got = append(got, "second:"+string(data))
			if fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
			}
		})
	}), c)
	if strings.Join(got, " ") != "second:payload" {
		t.Fatalf("callbacks got %q", got)
	}
	if resp := stream.response(t); string(resp.Body) != "ok" {
		t.Fatalf("response %q", resp.Body)
	}
}

// TestContextBodyDeadlineSet checks what a handler's SetReadDeadline does on
// an HTTP/1 body that streams, at the two points where it has nothing left
// to bound: a request that has been cancelled, which reports
// ErrResponseWritten, and one whose body is no longer arriving, for which
// the deadline is recorded and nothing applied.
func TestContextBodyDeadlineSet(t *testing.T) {
	b := new(bodyDeadline)
	cancelled := new(Context)
	cancelled.word.Store(withState(0, ctxCancelled))
	if err := b.set(cancelled, time.Now()); !errors.Is(err, ErrResponseWritten) {
		t.Fatalf("set on a cancelled request reported %v", err)
	}
	open := new(Context)
	at := time.Now().Add(time.Minute)
	if err := b.set(open, at); err != nil || b.owner != open || !b.at.Equal(at) || b.applied {
		t.Fatalf("set on a body no longer arriving: %v, owner %v, at %v, applied %v", err, b.owner == open, b.at, b.applied)
	}
}

// newFeedRequest is a request whose body streams through a BodyFeed, as an
// HTTP/3 request's does.
func newFeedRequest(conn *fib.Connection, path string, config BodyFeedConfig) (*StreamRequest, *Context, *BodyFeed, *extStream) {
	r := new(StreamRequest)
	r.URL.Path = path
	r.Request.URL, r.Request.Method, r.Request.Header = &r.URL, stdhttp.MethodPost, stdhttp.Header{}
	stream := new(extStream)
	c, feed := r.StreamContext(conn, stream, config)
	return r, c, feed, stream
}

// TestBodyFeedFraming checks a fed body held to its framing: one longer than
// its declared length, or than the limit, fails, and so does one that ends
// short of its declared length; what is written after the body has failed
// is dropped, and all of it is credited back to the peer.
func TestBodyFeedFraming(t *testing.T) {
	credited := 0
	_, c, feed, _ := newFeedRequest(nil, "/long", BodyFeedConfig{Declared: 4, Credit: func(n int) { credited += n }})
	if c.RequestBody() == nil || c.Request.ContentLength != 4 {
		t.Fatalf("the request's body is %T, length %d", c.Request.Body, c.Request.ContentLength)
	}
	if err := feed.Write(nil); err != nil {
		t.Fatalf("an empty write reported %v", err)
	}
	if err := feed.Write([]byte("abcde")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a body past its declared length reported %v", err)
	}
	if err := feed.Write([]byte("fg")); err != nil {
		t.Fatalf("a write after the body failed reported %v", err)
	}
	if credited != 7 {
		t.Fatalf("credited %d bytes, want all 7 dropped ones", credited)
	}
	if _, err := c.Request.Body.Read(make([]byte, 8)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read of the failed body reported %v", err)
	}

	_, _, feed, _ = newFeedRequest(nil, "/limit", BodyFeedConfig{Declared: -1, Limit: 3})
	if err := feed.Write([]byte("abcd")); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("a body past the limit reported %v", err)
	}

	_, _, feed, _ = newFeedRequest(nil, "/short", BodyFeedConfig{Declared: 4})
	_ = feed.Write([]byte("ab"))
	if err := feed.End(nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a body short of its declared length reported %v", err)
	}
}

// TestBodyFeedReads checks the reads of a fed body that are not of its
// bytes: an empty read, the trailer once the body has ended, a read after
// the handler closed the body, and the reason a body failed with nothing
// more said, which is an unexpected EOF.
func TestBodyFeedReads(t *testing.T) {
	_, c, feed, _ := newFeedRequest(nil, "/trailer", BodyFeedConfig{Declared: -1})
	stream := c.RequestBody()
	if n, err := stream.Read(nil); n != 0 || err != nil {
		t.Fatalf("an empty Read = %d, %v", n, err)
	}
	_ = feed.Write([]byte("body"))
	if err := feed.End(stdhttp.Header{"X-Sum": {"9"}}); err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(stream); err != nil || string(body) != "body" {
		t.Fatalf("ReadAll = %q, %v", body, err)
	}
	if stream.Trailer().Get("X-Sum") != "9" {
		t.Fatalf("Trailer = %v", stream.Trailer())
	}

	_, c, _, _ = newFeedRequest(nil, "/closed", BodyFeedConfig{Declared: -1})
	_ = c.Request.Body.Close()
	if _, err := c.Request.Body.Read(make([]byte, 4)); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("Read after Close reported %v", err)
	}

	_, c, feed, _ = newFeedRequest(nil, "/failed", BodyFeedConfig{Declared: -1})
	feed.Fail(nil)
	if _, err := c.Request.Body.Read(make([]byte, 4)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Read of a body failed for no reason reported %v", err)
	}
}

// TestBodyFeedOnBodyClosed checks that a handler that took its body through
// OnBody and then closes it, from another goroutine, has its callback told
// the body ended with ErrBodyAbandoned, on the handler pool.
func TestBodyFeedOnBodyClosed(t *testing.T) {
	_, c, _, stream := newFeedRequest(extLiveConn(t), "/abandoned", BodyFeedConfig{Declared: -1})
	ended := make(chan error, 1)
	Serve(HandlerFunc(func(c *Context) {
		c.OnBody(func(_ []byte, fin bool, err error) {
			if fin || err != nil {
				_ = c.Respond(stdhttp.StatusNoContent, "", nil)
				ended <- err
			}
		})
	}), c)
	_ = c.Request.Body.Close()
	if err := waitLifecycle(t, ended, "the abandoned body's last call"); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("the callback heard %v", err)
	}
	if got := stream.response(t); got.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("response %d", got.StatusCode)
	}
}

// TestBodyFeedCancelledUnderOnBody cancels a request whose handler took its
// body through OnBody and is waiting for it: the callback hears the body
// ended short, as an unexpected EOF when the peer simply went away.
func TestBodyFeedCancelledUnderOnBody(t *testing.T) {
	r, c, _, _ := newFeedRequest(extLiveConn(t), "/gone", BodyFeedConfig{Declared: -1})
	ended := make(chan error, 1)
	Serve(HandlerFunc(func(c *Context) {
		c.OnBody(func(_ []byte, fin bool, err error) {
			if fin || err != nil {
				ended <- err
			}
		})
	}), c)
	r.Cancel(io.EOF)
	if err := waitLifecycle(t, ended, "the cancelled body's last call"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("the callback heard %v", err)
	}
}

// TestBodyFeedFailureReported checks that OnBody registered on a request
// cancelled after its body failed reports the body's own failure, which may
// be why the request ended, rather than the cancellation.
func TestBodyFeedFailureReported(t *testing.T) {
	r, c, feed, _ := newFeedRequest(nil, "/toolarge", BodyFeedConfig{Declared: -1, Limit: 2})
	_ = feed.Write([]byte("abc"))
	r.Cancel(os.ErrDeadlineExceeded)
	deadline := time.Now().Add(10 * time.Second)
	for c.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the cancellation never reached the request")
		}
		time.Sleep(time.Millisecond)
	}
	ended := make(chan error, 1)
	c.OnBody(func(_ []byte, _ bool, err error) { ended <- err })
	if err := waitLifecycle(t, ended, "the late callback"); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("the late callback heard %v, want the body's own failure", err)
	}
}

// TestWholeBodyAfterRelease checks a body read whole once its buffer has
// gone back: WriteTo reports ErrBodyReleased and Len nothing, and before
// that, WriteTo of a body read to its end writes nothing.
func TestWholeBodyAfterRelease(t *testing.T) {
	b := &wholeBody{data: []byte("abc")}
	if b.Len() != 3 {
		t.Fatalf("Len = %d", b.Len())
	}
	var out strings.Builder
	if n, err := b.WriteTo(&out); n != 3 || err != nil || out.String() != "abc" {
		t.Fatalf("WriteTo = %d, %v, %q", n, err, out.String())
	}
	if n, err := b.WriteTo(&out); n != 0 || err != nil {
		t.Fatalf("WriteTo at the end = %d, %v", n, err)
	}
	b.release()
	if n, err := b.WriteTo(&out); n != 0 || !errors.Is(err, ErrBodyReleased) {
		t.Fatalf("WriteTo after release = %d, %v", n, err)
	}
	if b.Len() != 0 {
		t.Fatalf("Len after release = %d", b.Len())
	}
}

// TestRetainReleasedUnansweredGetsEmpty200 releases the last hold on a
// retained request without having answered it. The response is finished then,
// as it is at the return of a handler that retained nothing, so it is the
// same empty 200: on HTTP/1 leaving it unwritten would hand the client the
// response to the request pipelined behind it as this one's.
func TestRetainReleasedUnansweredGetsEmpty200(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		if c.Request.URL.Path == "/released" {
			c.Retain()
			go func() {
				time.Sleep(20 * time.Millisecond)
				c.Release()
			}()
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("next"))
	})
	conn := dialRaw(t, addr)
	conn.send("GET /released HTTP/1.1\r\nHost: test\r\n\r\nGET /next HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, want := range []string{"", "next"} {
		if resp, body := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK || body != want {
			t.Fatalf("got %d %q, want 200 %q", resp.StatusCode, body, want)
		}
	}
}
