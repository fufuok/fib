//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package requestid_test

import (
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/mwtest"
	"github.com/lesismal/fib/middleware/requestid"
)

// TestRequestIDConfig checks a custom header and generator, an id with a
// character that could not be logged as it is being replaced, a response
// that sets the header itself keeping its own, a request Next skips, and a
// request whose header map an earlier middleware dropped.
func TestRequestIDConfig(t *testing.T) {
	handler := middleware.Chain(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		h := stdhttp.Header{"Content-Type": {"text/plain"}}
		if c.Request.URL.Path == "/own" {
			h.Set("Trace-Id", "the-handler's")
		}
		_ = c.WriteResponse(fibhttp.Response{Header: h, Body: []byte(c.Request.Header.Get("Trace-Id"))})
	}), func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			if c.Request.URL.Path == "/nil-header" {
				c.Request.Header = nil
			}
			next.ServeHTTP(c)
		})
	}, requestid.New(requestid.Config{
		Header:    "trace-id",
		Generator: func() string { return "generated" },
		Next:      func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	}))
	url := mwtest.Serve(t, handler)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for _, tc := range []struct {
			path, sent, wantHeader, wantSeen string
		}{
			{"/", "ok-id", "ok-id", "ok-id"},
			{"/", "has space", "generated", "generated"},
			{"/", "café", "generated", "generated"},
			{"/nil-header", "", "generated", "generated"},
			{"/own", "", "the-handler's", "generated"},
			{"/skip", "", "", ""},
		} {
			header := stdhttp.Header{}
			if tc.sent != "" {
				header.Set("Trace-Id", tc.sent)
			}
			resp, body := c.Do(t, "GET", url+tc.path, header, "")
			if got := resp.Header.Get("Trace-Id"); got != tc.wantHeader || body != tc.wantSeen {
				t.Errorf("%s with %q: header %q, handler saw %q; want %q, %q", tc.path, tc.sent, got, body, tc.wantHeader, tc.wantSeen)
			}
		}
	})
}
