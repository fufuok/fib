//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"strings"
	"sync"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestStreamUnknownLengthAtHeader: with StreamUnknownLengthAtHeader, a body
// of no declared length is handed on from its header, before the threshold
// of it has arrived; without, it is not; and a small body with a length is
// whole at its callback either way.
func TestStreamUnknownLengthAtHeader(t *testing.T) {
	for _, atHeader := range []bool{true, false} {
		release := make(chan struct{})
		url := startServer(t, Config{}, func(c *fibhttp.Context) {
			if c.Request.URL.Query().Get("length") != "" {
				c.Header().Set("Content-Length", "5")
				_, _ = c.Write([]byte("whole"))
				return
			}
			_, _ = c.Write([]byte("first"))
			c.Flush()
			select {
			case <-release:
			case <-time.After(10 * time.Second):
			}
			_, _ = c.Write([]byte("second"))
		})
		client := newClient(t, func(config *ClientConfig) {
			streaming(64 << 10)(config)
			config.StreamUnknownLengthAtHeader = atHeader
		})

		var mu sync.Mutex
		var data strings.Builder
		called := make(chan bool, 1)
		gotFirst := make(chan struct{})
		done := make(chan struct{})
		var once sync.Once
		client.Do(mustRequest(t, "GET", url+"/", nil), func(cr *fibhttp.ClientResponse, err error) {
			if err != nil {
				t.Error(err)
				close(done)
				return
			}
			called <- cr.BodyComplete()
			cr.OnBody(func(b []byte, fin bool, err error) {
				mu.Lock()
				data.Write(b)
				first := strings.HasPrefix(data.String(), "first")
				mu.Unlock()
				if first {
					once.Do(func() { close(gotFirst) })
				}
				if fin {
					close(done)
				}
			})
		})
		wait := 5 * time.Second
		if !atHeader {
			wait = 300 * time.Millisecond
		}
		select {
		case complete := <-called:
			if !atHeader || complete {
				t.Fatalf("at header %v: the callback ran early, complete %v", atHeader, complete)
			}
			select {
			case <-gotFirst:
			case <-time.After(5 * time.Second):
				t.Fatal("OnBody did not get the first piece before the rest was sent")
			}
		case <-time.After(wait):
			if atHeader {
				t.Fatal("the callback did not run at the header")
			}
		}
		close(release)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("the body did not end")
		}
		mu.Lock()
		got := data.String()
		mu.Unlock()
		if got != "firstsecond" {
			t.Fatalf("body %q", got)
		}

		var whole streamRun
		whole.run(t, client, mustRequest(t, "GET", url+"/?length=1", nil))
		if !whole.complete || string(whole.data) != "whole" {
			t.Fatalf("a small body with a length: complete %v, %q", whole.complete, whole.data)
		}
	}
}
