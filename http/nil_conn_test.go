//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"fmt"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// TestReleaseWithoutConnection checks that a Context NewStreamContext made
// with no connection, whose handler retains the request and answers it from
// another goroutine, with WriteResponse or with Write, sends the response
// through its stream at the last Release, on a stream that streams and on
// one that takes responses whole.
func TestReleaseWithoutConnection(t *testing.T) {
	long := strings.Repeat("w", writerBufferSize+1)
	for _, streams := range []bool{false, true} {
		for _, how := range []string{"WriteResponse", "Write", "Write short", "nothing"} {
			t.Run(fmt.Sprintf("streams=%v/%s", streams, how), func(t *testing.T) {
				whole, streamer := new(cvWrStream), new(cvWrStreamer)
				var stream Stream = whole
				if streams {
					stream = streamer
				}
				req, _ := stdhttp.NewRequest("GET", "/", nil)
				c := NewStreamContext(nil, req, stream)
				done := make(chan error, 1)
				Serve(HandlerFunc(func(c *Context) {
					c.Retain()
					go func() {
						var err error
						switch how {
						case "WriteResponse":
							err = c.WriteResponse(Response{Body: []byte("x")})
						case "Write":
							_, err = c.WriteString(long)
						case "Write short":
							_, err = c.WriteString("x")
						}
						c.Release()
						done <- err
					}()
				}), c)
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the response was never released")
				}
				want := map[string]string{"WriteResponse": "x", "Write": long, "Write short": "x", "nothing": ""}[how]
				var got string
				var status int
				switch {
				case streams && streamer.status != 0:
					got, status = string(streamer.body), streamer.status
					if !streamer.ended || !streamer.complete {
						t.Error("the streamed response was not ended")
					}
				case streams && streamer.whole != nil:
					got, status = string(streamer.whole.Body), streamer.whole.StatusCode
				case !streams && whole.whole != nil:
					got, status = string(whole.whole.Body), whole.whole.StatusCode
				default:
					t.Fatal("nothing was sent")
				}
				if status != stdhttp.StatusOK || got != want {
					t.Fatalf("sent %d with %d bytes, want 200 with %d", status, len(got), len(want))
				}
			})
		}
	}
}
