//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package pprof

import (
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestServeRecorder checks how serve answers with what a handler wrote to
// the recorder: 200 when it wrote nothing, the first status it set when it
// set two, and a Content-Type sniffed from the body when it set none.
func TestServeRecorder(t *testing.T) {
	handlers := map[string]stdhttp.HandlerFunc{
		"/nothing": func(stdhttp.ResponseWriter, *stdhttp.Request) {},
		"/twice": func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
			w.WriteHeader(stdhttp.StatusAccepted)
			w.WriteHeader(stdhttp.StatusTeapot)
			_, _ = w.Write([]byte("{}"))
		},
		"/sniffed": func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
			_, _ = w.Write([]byte("<html><body>hi</body></html>"))
			_, _ = w.Write([]byte("<p>more</p>"))
		},
	}
	url := mwtest.Serve(t, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		serve(c, handlers[c.Request.URL.Path])
	}))
	c := mwtest.Clients(t)[0]
	for path, want := range map[string]struct {
		status      int
		contentType string
	}{
		"/nothing": {stdhttp.StatusOK, ""},
		"/twice":   {stdhttp.StatusAccepted, "text/plain; charset=utf-8"},
		"/sniffed": {stdhttp.StatusOK, "text/html; charset=utf-8"},
	} {
		resp, _ := c.Do(t, "GET", url+path, nil, "")
		if resp.StatusCode != want.status || resp.Header.Get("Content-Type") != want.contentType {
			t.Errorf("%s: %d %q, want %d %q", path, resp.StatusCode, resp.Header.Get("Content-Type"), want.status, want.contentType)
		}
	}
}
