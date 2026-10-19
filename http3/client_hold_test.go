//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestClientHoldBody checks that over HTTP/3 a body held with HoldBody after
// each piece, and resumed a moment later from elsewhere, is delivered whole
// and in order, never to a function that holds it.
func TestClientHoldBody(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = c.Write(body)
	})
	client := newClient(t, func(c *ClientConfig) { c.StreamResponseBody = true })
	var mu sync.Mutex
	var data []byte
	var held bool
	var whileHeld int
	done := make(chan error, 1)
	client.Do(mustRequest(t, "GET", base+"/", nil), func(cr *fibhttp.ClientResponse, err error) {
		if err != nil {
			done <- err
			return
		}
		cr.OnBody(func(p []byte, fin bool, err error) {
			mu.Lock()
			if held {
				whileHeld++
			}
			data = append(data, p...)
			if fin {
				mu.Unlock()
				done <- err
				return
			}
			held = true
			mu.Unlock()
			cr.HoldBody(true)
			time.AfterFunc(time.Millisecond, func() {
				mu.Lock()
				held = false
				mu.Unlock()
				cr.HoldBody(false)
			})
		})
	})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the held body never finished")
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(data, body) {
		t.Fatalf("got %d of %d bytes", len(data), len(body))
	}
	if whileHeld != 0 {
		t.Fatalf("%d calls came while the body was held", whileHeld)
	}
}
