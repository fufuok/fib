//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package cors_test

import (
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/cors"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestCORSPreflightOptions checks the preflight fields the configuration
// sets: the fixed AllowHeaders rather than the ones asked for, private
// network access when asked, a negative MaxAge as "0", and an origin allowed
// by AllowOriginsFunc alone.
func TestCORSPreflightOptions(t *testing.T) {
	url := mwtest.Serve(t, cors.New(cors.Config{
		AllowOriginsFunc:    func(origin string) bool { return strings.HasSuffix(origin, ".func.example") },
		AllowHeaders:        []string{"X-One", "X-Two"},
		AllowPrivateNetwork: true,
		MaxAge:              -1,
	})(ok))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, _ := c.Do(t, "OPTIONS", url, stdhttp.Header{
			"Origin":                                 {"https://a.func.example"},
			"Access-Control-Request-Method":          {"PUT"},
			"Access-Control-Request-Headers":         {"X-Other"},
			"Access-Control-Request-Private-Network": {"true"},
		}, "")
		for key, want := range map[string]string{
			"Access-Control-Allow-Origin":          "https://a.func.example",
			"Access-Control-Allow-Headers":         "X-One, X-Two",
			"Access-Control-Allow-Private-Network": "true",
			"Access-Control-Max-Age":               "0",
		} {
			if got := resp.Header.Get(key); got != want {
				t.Errorf("preflight %s = %q, want %q", key, got, want)
			}
		}
		// An origin neither the list nor the func allows gets no answer.
		resp, _ = c.Do(t, "OPTIONS", url, stdhttp.Header{
			"Origin": {"https://evil.example"}, "Access-Control-Request-Method": {"PUT"},
		}, "")
		if got := resp.Header.Get("Access-Control-Allow-Origin"); resp.StatusCode != stdhttp.StatusNoContent || got != "" {
			t.Errorf("disallowed preflight: %d, Access-Control-Allow-Origin %q", resp.StatusCode, got)
		}
	})
}

// TestCORSNextAndNoOrigin checks that a request Next skips gets no CORS
// fields, and that with any origin allowed a request without Origin passes
// through untouched, without a Vary it has no need of.
func TestCORSNextAndNoOrigin(t *testing.T) {
	url := mwtest.Serve(t, cors.New(cors.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(ok))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url+"/skip", stdhttp.Header{"Origin": {"https://a.example"}}, "")
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" || body != "ok" {
			t.Errorf("skipped request: Access-Control-Allow-Origin %q, body %q", got, body)
		}
		resp, body = c.Do(t, "GET", url, nil, "")
		if resp.Header.Get("Access-Control-Allow-Origin") != "" || resp.Header.Get("Vary") != "" || body != "ok" {
			t.Errorf("no origin: header %v, body %q", resp.Header, body)
		}
	})
}
