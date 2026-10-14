//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package backend

import (
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/tlstest"
)

// The routes fall back to something sensible on a query they cannot use, and
// answer 404 off them.
func TestHandlerDefaults(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	engines, err := NewEngines(serverTLS, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range engines {
		stop := example.Start(engine)
		defer func() { _ = stop() }()
	}
	base := "http://" + example.Addr(engines[2])
	client := &stdhttp.Client{Timeout: 30 * time.Second}
	for _, c := range []struct {
		path string
		code int
		body string
	}{
		{"/status?code=418", 418, "I'm a teapot\n"},
		{"/status?code=nonsense", 400, "Bad Request\n"},
		{"/download?size=-1", 400, "size?\n"},
		{"/stream?n=0&delay=1ms", 200, "line 1 of 5\nline 2 of 5\nline 3 of 5\nline 4 of 5\nline 5 of 5\n"},
		{"/stream?n=1&delay=nonsense", 200, "line 1 of 1\n"},
		{"/nowhere", 404, "not found\n"},
	} {
		resp, err := client.Get(base + c.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != c.code || string(body) != c.body {
			t.Errorf("%s: %d %q", c.path, resp.StatusCode, body)
		}
	}
}

// NewEnginesFor closes what it bound when a later listener fails.
func TestNewEnginesForBadPlainAddress(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngines(serverTLS, "127.0.0.1:0", "not an address"); err == nil || !strings.Contains(err.Error(), "address") {
		t.Fatalf("err %v", err)
	}
	if _, err := NewEngines(serverTLS, "not an address", ""); err == nil {
		t.Fatal("bound a bad address")
	}
}
