//go:build linux || darwin || windows

package http3

import (
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestStreamedTrailerWhileHandlerReadsTrailer has a handler read the
// request's Trailer as it starts, while the body and its trailer arrive on
// the connection's goroutine: the trailer becomes the request's Trailer only
// with OnBody's last call, so the two never touch the Trailer at once (the
// race detector sees it if they do), and the handler then reads its value.
func TestStreamedTrailerWhileHandlerReadsTrailer(t *testing.T) {
	url := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		deadline := time.Now().Add(5 * time.Millisecond)
		for time.Now().Before(deadline) {
			for name := range c.Request.Trailer {
				_ = name
			}
		}
		c.OnBody(func(_ []byte, fin bool, err error) {
			if fin && err == nil {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("sum="+c.Request.Trailer.Get("X-Sum")))
			}
		})
	})
	client := uploadClient(t)
	for range 10 {
		body := fibhttp.NewBodyWriter(0)
		req := mustRequest(t, stdhttp.MethodPost, url+"/", nil)
		req.Body = body
		req.Trailer = stdhttp.Header{"X-Sum": nil}
		future := client.Go(req)
		_, _ = body.Write([]byte("hello"))
		if err := body.CloseWithTrailer(stdhttp.Header{"X-Sum": {"42"}}); err != nil {
			t.Fatal(err)
		}
		resp, err := future.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "sum=42" {
			t.Fatalf("%q", got)
		}
	}
}
