//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"testing"
	"time"
)

// TestH2StreamedTrailerWhileHandlerReadsTrailer has a handler read the
// request's Trailer as it starts, as net/http lets it before it reads the
// body, while the body and its trailer arrive at once on the goroutine that
// reads the connection: the trailer becomes the request's Trailer only as
// the body's end is handed over, through OnBody's last call or Read's
// io.EOF, so the two never touch the Trailer at once (the race detector
// sees it if they do), and the handler then reads the trailer's values.
func TestH2StreamedTrailerWhileHandlerReadsTrailer(t *testing.T) {
	for _, how := range []string{"onbody", "read"} {
		t.Run(how, func(t *testing.T) {
			addr := serveStreamingServer(t, immediateStreamingConfig(), func(c *Context) {
				// The body ends meanwhile, on the connection's goroutine.
				deadline := time.Now().Add(5 * time.Millisecond)
				for time.Now().Before(deadline) {
					for name := range c.Request.Trailer {
						_ = name
					}
				}
				answer := func(c *Context) {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "sum=%s", c.Request.Trailer.Get("X-Sum")))
				}
				if how == "onbody" {
					c.OnBody(func(_ []byte, fin bool, err error) {
						if fin && err == nil {
							answer(c)
						}
					})
					return
				}
				buf := make([]byte, 64)
				for {
					_, err := c.Request.Body.Read(buf)
					if errors.Is(err, io.EOF) {
						break
					}
					if errors.Is(err, ErrWouldBlock) {
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil {
						t.Errorf("reading the body: %v", err)
						return
					}
				}
				answer(c)
			})
			tc := dialH2(t, addr)
			for i := uint32(1); i <= 21; i += 2 {
				tc.headers(i, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/")
				tc.data(i, false, []byte("hello"))
				tc.headers(i, true, "x-sum", "42")
				if header, body := tc.response(i); header[":status"] != "200" || string(body) != "sum=42" {
					t.Fatalf("stream %d: %v %q", i, header, body)
				}
			}
		})
	}
}
