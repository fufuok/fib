//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package responsetime_test

import (
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/mwtest"
	"github.com/lesismal/fib/middleware/responsetime"
)

// TestResponseTimeConfig checks a custom header and format, and that a
// request Next skips gets no header.
func TestResponseTimeConfig(t *testing.T) {
	url := mwtest.Serve(t, responsetime.New(responsetime.Config{
		Header: "x-elapsed",
		Format: func(time.Duration) string { return "fast" },
		Next:   func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})))
	c := mwtest.Clients(t)[0]
	if resp, _ := c.Do(t, "GET", url, nil, ""); resp.Header.Get("X-Elapsed") != "fast" {
		t.Errorf("X-Elapsed = %q, want fast", resp.Header.Get("X-Elapsed"))
	}
	if resp, _ := c.Do(t, "GET", url+"/skip", nil, ""); resp.Header.Get("X-Elapsed") != "" {
		t.Errorf("skipped request: X-Elapsed = %q", resp.Header.Get("X-Elapsed"))
	}
}
