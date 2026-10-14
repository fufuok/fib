//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package limiter_test

import (
	stdhttp "net/http"
	"strconv"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/limiter"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestLimiterDefaults checks the defaults a bare New gets: DefaultMax
// requests per client address, and that a request Next skips is neither
// counted nor given the X-RateLimit-* fields.
func TestLimiterDefaults(t *testing.T) {
	url := mwtest.Serve(t, limiter.New(limiter.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(handler)))
	c := mwtest.Clients(t)[0]
	resp, _ := c.Do(t, "GET", url+"/skip", nil, "")
	if resp.StatusCode != stdhttp.StatusOK || resp.Header.Get("X-Ratelimit-Limit") != "" {
		t.Errorf("skipped request: %d, header %v", resp.StatusCode, resp.Header)
	}
	resp, _ = c.Do(t, "GET", url, nil, "")
	if got, want := resp.Header.Get("X-Ratelimit-Limit"), strconv.Itoa(limiter.DefaultMax); got != want {
		t.Errorf("X-RateLimit-Limit %q, want %q", got, want)
	}
	if got, want := resp.Header.Get("X-Ratelimit-Remaining"), strconv.Itoa(limiter.DefaultMax-1); got != want {
		t.Errorf("X-RateLimit-Remaining %q, want %q: the skipped request was counted", got, want)
	}
	if reset, _ := strconv.Atoi(resp.Header.Get("X-Ratelimit-Reset")); reset <= 0 || reset > 60 {
		t.Errorf("X-RateLimit-Reset %q, want within DefaultExpiration", resp.Header.Get("X-Ratelimit-Reset"))
	}
}
