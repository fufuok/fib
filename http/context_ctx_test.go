//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"context"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"runtime"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// waitDone waits for ctx to be cancelled, as something on the handler pool
// cancels it.
func waitDone(t *testing.T, ctx context.Context, what string) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the context was never cancelled", what)
	}
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// ctxStream is a Stream that takes what it is handed and drops it, for a
// Context served as a protocol outside this package serves one.
type ctxStream struct{}

func (ctxStream) WriteResponse(*stdhttp.Request, Response) error            { return nil }
func (ctxStream) WriteInterim(int, stdhttp.Header) error                    { return nil }
func (ctxStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return nil }

// newCtxRequest is a request served over a ctxStream, on a live connection,
// which a release away from the handler flushes: an HTTP/1 connection whose
// one request has been answered, open for the rest of the test.
func newCtxRequest(t *testing.T, path string) (*StreamRequest, *Context) {
	t.Helper()
	conns := make(chan *fib.Connection, 1)
	raw := dialRaw(t, serveHTTP1(t, func(c *Context) {
		conns <- c.Conn
		_ = c.Respond(stdhttp.StatusNoContent, "", nil)
	}))
	raw.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	r := new(StreamRequest)
	r.URL.Path = path
	r.Request.URL = &r.URL
	r.Request.Method = stdhttp.MethodGet
	r.Request.Proto, r.Request.ProtoMajor = "HTTP/3.0", 3
	r.Request.Header = stdhttp.Header{}
	return r, r.Context(<-conns, ctxStream{}, nil)
}

// TestContextCancelledOnHangup covers a handler that waits in place on
// something that takes a context, as a database query or an upstream request
// does: a client that goes away cancels the context while it waits, with the
// reason OnCancel is given as its cause.
func TestContextCancelledOnHangup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows arms a connection's read only between its rounds; see fib.HangupHandler")
	}
	started := make(chan struct{}, 1)
	finished := make(chan error, 1)
	addr := serveHTTP1(t, func(c *Context) {
		ctx := c.Context()
		if c.Context() != ctx {
			t.Error("Context made a second context for the same request")
		}
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		finished <- context.Cause(ctx)
	})
	raw := dialRaw(t, addr)
	raw.send("GET /wait HTTP/1.1\r\nHost: test\r\n\r\n")
	<-started
	start := time.Now()
	if err := raw.Conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case cause := <-finished:
		if cause != io.EOF {
			t.Fatalf("the context's cause was %v, want io.EOF", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was never told")
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("the context was cancelled %v after the hangup", waited)
	}
}

// TestContextCancelledWhenTheRequestEnds checks that a request answered in the
// ordinary way cancels its context too, as net/http's does once ServeHTTP
// returns: not while the handler is still running, even past its answer, and
// for a retained request not before its last Release.
func TestContextCancelledWhenTheRequestEnds(t *testing.T) {
	contexts := make(chan context.Context, 2)
	release := make(chan struct{})
	addr := serveHTTP1(t, func(c *Context) {
		ctx := c.Context()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		if isDone(ctx) {
			t.Error("the context was cancelled while the handler still ran")
		}
		if c.Request.URL.Path == "/retained" {
			c.Retain()
			go func() {
				<-release
				c.Release()
			}()
		}
		contexts <- ctx
	})
	raw := dialRaw(t, addr)
	raw.send("GET /plain HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	ctx := <-contexts
	waitDone(t, ctx, "plain")
	if ctx.Err() != context.Canceled || context.Cause(ctx) != context.Canceled {
		t.Fatalf("an ended request's context reports %v, cause %v", ctx.Err(), context.Cause(ctx))
	}

	raw.send("GET /retained HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	ctx = <-contexts
	time.Sleep(50 * time.Millisecond)
	if isDone(ctx) {
		t.Fatal("a retained request's context was cancelled before its Release")
	}
	close(release)
	waitDone(t, ctx, "retained")
}

// TestContextOutlivesItsRecycledContext checks that the context a request
// handed out holds nothing of the Context it came from: once the Reuse options
// have that Context serving the next request on the connection, the first
// context still reports that its request ended, and the second request gets
// a context of its own.
func TestContextOutlivesItsRecycledContext(t *testing.T) {
	config := DefaultConfig()
	setReuseAll(&config)
	got := make(chan context.Context, 2)
	addr := serveStreamingServer(t, config, func(c *Context) {
		got <- c.Context()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(c.Request.URL.Path))
	})
	raw := dialRaw(t, addr)
	raw.send("GET /one HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	first := <-got
	waitDone(t, first, "first")
	raw.send("GET /two HTTP/1.1\r\nHost: test\r\n\r\n")
	raw.response(stdhttp.MethodGet)
	second := <-got
	if second == first {
		t.Fatal("the second request was handed the first one's context")
	}
	if first.Err() != context.Canceled {
		t.Fatalf("the first context reports %v once its Context served another", first.Err())
	}
	waitDone(t, second, "second")
}

type ctxKey struct{}

// TestContextOverAnExternalStream covers a Context served over a protocol
// outside this package, as HTTP/3 is: the context carries the request's
// values, a cancelled stream cancels it with the stream's reason, and asked
// for after the cancellation it is cancelled already.
func TestContextOverAnExternalStream(t *testing.T) {
	req, c := newCtxRequest(t, "/ext")
	c.Request = c.Request.WithContext(context.WithValue(context.Background(), ctxKey{}, "v"))
	var ctx context.Context
	Serve(HandlerFunc(func(c *Context) {
		ctx = c.Context()
		c.Retain()
	}), c)
	if got := ctx.Value(ctxKey{}); got != "v" {
		t.Fatalf("the context's value is %v, want the request's", got)
	}
	if isDone(ctx) {
		t.Fatal("a retained request's context was cancelled when its handler returned")
	}
	reason := errors.New("stream reset")
	req.Cancel(reason)
	waitDone(t, ctx, "cancelled")
	if cause := context.Cause(ctx); cause != reason {
		t.Fatalf("the context's cause is %v, want %v", cause, reason)
	}
	c.Release()
}

// TestContextAskedForAfterACancellation checks that a handler that asks for
// its context only once its request has been cancelled finds it cancelled
// already, with the same cause.
func TestContextAskedForAfterACancellation(t *testing.T) {
	req, c := newCtxRequest(t, "/late")
	reason := errors.New("gone")
	asked := make(chan error, 1)
	Serve(HandlerFunc(func(c *Context) {
		c.Retain()
		c.OnCancel(func(error) {
			asked <- context.Cause(c.Context())
			c.Release()
		})
	}), c)
	req.Cancel(reason)
	var cause error
	select {
	case cause = <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel never ran")
	}
	if cause != reason {
		t.Fatalf("the context's cause is %v, want %v", cause, reason)
	}
}

// TestH2ContextCancelledOnReset checks that a stream the client resets
// cancels the context of a handler waiting on it.
func TestH2ContextCancelledOnReset(t *testing.T) {
	entered := make(chan struct{}, 1)
	causes := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		ctx := c.Context()
		entered <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		causes <- context.Cause(ctx)
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/slow")...)
	<-entered
	tc.write(h2AppendRSTStream(nil, 1, H2Cancel))
	var cause error
	select {
	case cause = <-causes:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never returned")
	}
	if cause == nil || cause == context.Canceled {
		t.Fatalf("the reset stream's context has cause %v, want the reset", cause)
	}
}
