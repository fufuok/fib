//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package etag_test

import (
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/etag"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestETagConfig checks a weak ETag added to a response written with no
// header map at all, an empty body left without one, and a request Next
// skips.
func TestETagConfig(t *testing.T) {
	url := mwtest.Serve(t, etag.New(etag.Config{
		Weak: true,
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/empty" {
			_ = c.WriteResponse(fibhttp.Response{})
			return
		}
		_ = c.WriteResponse(fibhttp.Response{Body: []byte("hello")})
	})))
	want := etag.Generate([]byte("hello"), true)
	if !strings.HasPrefix(want, `W/"`) {
		t.Fatalf("Generate weak = %q", want)
	}
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url, nil, "")
		if got := resp.Header.Get("Etag"); got != want || body != "hello" {
			t.Errorf("ETag %q, body %q; want %q", got, body, want)
		}
		resp, _ = c.Do(t, "GET", url, stdhttp.Header{"If-None-Match": {want}}, "")
		if resp.StatusCode != stdhttp.StatusNotModified {
			t.Errorf("a weak ETag sent back answered %d", resp.StatusCode)
		}
		for _, path := range []string{"/empty", "/skip"} {
			resp, _ = c.Do(t, "GET", url+path, nil, "")
			if got := resp.Header.Get("Etag"); got != "" {
				t.Errorf("%s: ETag %q, want none", path, got)
			}
		}
	})
}

// TestMatchesTrailingJunk checks that a list ending in a member that is not
// an entity tag matches nothing, rather than running off its end.
func TestMatchesTrailingJunk(t *testing.T) {
	if etag.Matches(`"b", junk`, `"a"`) {
		t.Error("matched a list of other tags and junk")
	}
}
