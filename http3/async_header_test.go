package http3

import (
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestOnHeaderAsync checks over HTTP/3 that a response held by an
// OnHeaderAsync hook, sent whole or streamed past what is held, goes once
// the hook resumes after the handler has returned, with what it set.
func TestOnHeaderAsync(t *testing.T) {
	long := strings.Repeat("y", 200*1000)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.OnHeaderAsync(func(status int, h stdhttp.Header, resume func(error)) {
			go func() {
				time.Sleep(20 * time.Millisecond)
				h.Set("X-Async", "set")
				resume(nil)
			}()
		})
		if c.Request.URL.Path == "/long" {
			for i := 0; i < len(long); i += 1000 {
				_, _ = io.WriteString(c, long[i:i+1000])
			}
			return
		}
		_ = c.Respond(stdhttp.StatusAccepted, "text/plain", []byte("whole"))
	})
	client := newClient(t, nil)
	for path, want := range map[string]string{"/whole": "whole", "/long": long} {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+path, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != want || resp.Header.Get("X-Async") != "set" {
			t.Fatalf("%s: %d bytes, %v", path, len(got), resp.Header)
		}
	}
}
