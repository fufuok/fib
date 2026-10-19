//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// TestResponseHeaderTimeout checks, over HTTP/1.1 and h2c, that a response
// whose header takes longer than ResponseHeaderTimeout fails with
// ErrResponseHeaderTimeout, that an interim response does not end the wait,
// and that a body slower than it, once the header is in, does not fail; and
// that Timeout, when it is the shorter, still fails the request as before.
func TestResponseHeaderTimeout(t *testing.T) {
	const wait = 150 * time.Millisecond
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/head":
			time.Sleep(3 * wait)
		case "/interim":
			w.WriteHeader(stdhttp.StatusEarlyHints)
			time.Sleep(3 * wait)
		case "/body":
			w.WriteHeader(stdhttp.StatusOK)
			w.(stdhttp.Flusher).Flush()
			time.Sleep(3 * wait)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	ts.Config.Protocols = new(stdhttp.Protocols)
	ts.Config.Protocols.SetHTTP1(true)
	ts.Config.Protocols.SetUnencryptedHTTP2(true)
	ts.Start()
	t.Cleanup(ts.Close)
	for _, h2c := range []bool{false, true} {
		for _, timeout := range []time.Duration{10 * time.Second, wait} {
			config := DefaultClientConfig()
			config.UnencryptedHTTP2 = h2c
			config.Timeout = timeout
			config.ResponseHeaderTimeout = wait
			if timeout == wait {
				config.ResponseHeaderTimeout = 10 * time.Second
			}
			client := newTestClient(t, config)
			for path, want := range map[string]error{"/": nil, "/head": ErrResponseHeaderTimeout, "/interim": ErrResponseHeaderTimeout, "/body": nil} {
				if timeout == wait && want != nil {
					want = errRequestTimeout
				} else if timeout == wait && path == "/body" {
					want = errRequestTimeout
				}
				start := time.Now()
				resp, err := client.Go(mustRequest(t, "GET", ts.URL+path, nil)).Wait()
				if err == nil {
					if got := readBody(t, resp); got != "ok" {
						t.Errorf("h2c=%v timeout=%v %s: body %q", h2c, timeout, path, got)
					}
				}
				if !errors.Is(err, want) || want == nil && err != nil {
					t.Errorf("h2c=%v timeout=%v %s: err = %v, want %v", h2c, timeout, path, err, want)
				}
				if want != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Errorf("h2c=%v timeout=%v %s: %v is not os.ErrDeadlineExceeded", h2c, timeout, path, err)
				}
				if want != nil && time.Since(start) > 2*wait+time.Second {
					t.Errorf("h2c=%v timeout=%v %s: failed after %v", h2c, timeout, path, time.Since(start))
				}
			}
		}
	}
}
