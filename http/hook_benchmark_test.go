//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdhttp "net/http"
	"net/url"
	"testing"
)

// benchmarkStreamRequests serves b.N requests through one StreamRequest,
// recycled after each, as a protocol that reuses its streams serves them:
// what is left is the Context's own cost for what handler does.
func benchmarkStreamRequests(b *testing.B, handler HandlerFunc) {
	var r StreamRequest
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Request = stdhttp.Request{Method: stdhttp.MethodGet, URL: &r.URL, Proto: "HTTP/2.0", ProtoMajor: 2}
		r.URL = url.URL{Path: "/"}
		serveRequest(handler, r.Context(nil, discardStream{}, nil))
		r.Recycle()
	}
}

// BenchmarkStreamRespond is a response written whole, with nothing hooked.
func BenchmarkStreamRespond(b *testing.B) {
	benchmarkStreamRequests(b, func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
	})
}

// BenchmarkStreamAccessLog is what an access log does on every request: it
// keeps its entry with SetValue and registers an OnFinish hook, which reads
// it back.
func BenchmarkStreamAccessLog(b *testing.B) {
	var entry struct{ status int }
	finish := func(c *Context) func(int, stdhttp.Header, int64) {
		return func(status int, _ stdhttp.Header, _ int64) {
			c.Value().(*struct{ status int }).status = status
		}
	}
	benchmarkStreamRequests(b, func(c *Context) {
		c.SetValue(&entry)
		c.OnFinish(finish(c))
		_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
	})
}
