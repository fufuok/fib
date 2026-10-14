//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package proxy

import (
	stdhttp "net/http"
	"testing"
)

// ParseRoute says what is wrong with a route it cannot use.
func TestParseRouteRejects(t *testing.T) {
	for _, spec := range []string{
		"no-equals",
		"api=http://127.0.0.1:1",
		"/api=http://127.0.0.1:1/%zz",
		"/api=ftp://127.0.0.1:1",
		"/api=http://",
	} {
		if _, err := ParseRoute(spec); err == nil {
			t.Errorf("%q: accepted", spec)
		}
	}
	route, err := ParseRoute("/=http://127.0.0.1:1")
	if err != nil || route.Prefix != "/" {
		t.Fatalf("root route: %+v, %v", route, err)
	}
}

// A request for the prefix itself goes to the target's own path.
func TestUpstreamPathOfThePrefixItself(t *testing.T) {
	route, err := ParseRoute("/api=http://127.0.0.1:1/v1/")
	if err != nil {
		t.Fatal(err)
	}
	if got := route.upstreamPath("/api"); got != "/v1/" {
		t.Fatalf("upstreamPath(/api) = %q", got)
	}
}

func TestNewNeedsARoute(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("made a gateway with nowhere to forward")
	}
}

// A request that has already passed a proxy keeps the addresses it was
// forwarded for, with this hop's added.
func TestAddForwardedAppends(t *testing.T) {
	h := stdhttp.Header{"X-Forwarded-For": {"10.0.0.1"}}
	addForwarded(h, &stdhttp.Request{RemoteAddr: "127.0.0.1:5000"})
	if got := h.Get("X-Forwarded-For"); got != "10.0.0.1, 127.0.0.1" {
		t.Fatalf("X-Forwarded-For %q", got)
	}
}
