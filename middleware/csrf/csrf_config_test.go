//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package csrf_test

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/csrf"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestCSRFQueryTrustedOrigins checks a token sent as a query parameter, an
// unsafe request from a trusted origin, a request Next skips, a cookie that
// is not a token the middleware could have issued being replaced, and a
// custom ErrorHandler being given the reason for a refusal.
func TestCSRFQueryTrustedOrigins(t *testing.T) {
	var (
		mu     sync.Mutex
		errs   []error
		cookie = csrf.DefaultCookieName
	)
	url := mwtest.Serve(t, csrf.New(csrf.Config{
		Lookup:            "query:token",
		TrustedOrigins:    []string{"https://trusted.example"},
		CookieSessionOnly: true,
		Next:              func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
		ErrorHandler: func(c *fibhttp.Context, err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
			_ = c.Respond(stdhttp.StatusTeapot, "text/plain", []byte(err.Error()))
		},
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(csrf.Token(c.Request)))
	})))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, token := c.Do(t, "GET", url, nil, "")
		if len(resp.Cookies()) != 1 || resp.Cookies()[0].MaxAge != 0 {
			t.Fatalf("GET: cookies %v, want a session cookie", resp.Cookies())
		}
		withCookie := stdhttp.Header{"Cookie": {cookie + "=" + token}, "Origin": {"https://trusted.example"}}
		if resp, body := c.Do(t, "POST", url+"?token="+token, withCookie, ""); resp.StatusCode != stdhttp.StatusOK || body != token {
			t.Errorf("POST from a trusted origin: %d %q", resp.StatusCode, body)
		}
		if resp, body := c.Do(t, "POST", url+"/skip", nil, ""); resp.StatusCode != stdhttp.StatusOK || body != "" {
			t.Errorf("skipped POST: %d %q", resp.StatusCode, body)
		}
		// A malformed cookie, of the right length or not, counts as none: a
		// GET is given a new one, and a POST that matches it is refused.
		for _, bad := range []string{"short", strings.Repeat("!", len(token))} {
			resp, body := c.Do(t, "GET", url, stdhttp.Header{"Cookie": {cookie + "=" + bad}}, "")
			if len(resp.Cookies()) != 1 || body == bad || body == "" {
				t.Errorf("GET with cookie %q: token %q, cookies %v", bad, body, resp.Cookies())
			}
			resp, _ = c.Do(t, "POST", url+"?token="+bad, stdhttp.Header{"Cookie": {cookie + "=" + bad}}, "")
			if resp.StatusCode != stdhttp.StatusTeapot {
				t.Errorf("POST with cookie %q: %d", bad, resp.StatusCode)
			}
		}
	})
	mu.Lock()
	defer mu.Unlock()
	for _, err := range errs {
		if !errors.Is(err, csrf.ErrTokenMissing) {
			t.Errorf("ErrorHandler given %v, want ErrTokenMissing", err)
		}
	}
	if len(errs) != 4 {
		t.Errorf("ErrorHandler called %d times, want 4", len(errs))
	}
}

// TestCSRFBadConfig checks that New refuses, by panicking, a Lookup it cannot
// read a token from and a trusted origin that is not one.
func TestCSRFBadConfig(t *testing.T) {
	for _, cfg := range []csrf.Config{
		{Lookup: "header"},
		{Lookup: "header: "},
		{Lookup: "cookie:x"},
		{TrustedOrigins: []string{"not an origin"}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%+v) did not panic", cfg)
				}
			}()
			csrf.New(cfg)
		}()
	}
}
