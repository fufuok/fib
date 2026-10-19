//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// holdRun takes a body with OnBody, holding it after every call and having
// it resumed a moment later from another goroutine, as a proxy waiting for
// its client's room does. It records the calls that came while it was held.
type holdRun struct {
	mu        sync.Mutex
	data      []byte
	held      bool
	whileHeld int
	err       error
	done      chan struct{}
}

func (h *holdRun) take(cr *ClientResponse) {
	cr.OnBody(func(data []byte, fin bool, err error) {
		h.mu.Lock()
		if h.held {
			h.whileHeld++
		}
		h.data = append(h.data, data...)
		if fin {
			h.err = err
			h.mu.Unlock()
			close(h.done)
			return
		}
		h.held = true
		h.mu.Unlock()
		cr.HoldBody(true)
		time.AfterFunc(time.Millisecond, func() {
			h.mu.Lock()
			h.held = false
			h.mu.Unlock()
			cr.HoldBody(false)
		})
	})
}

// TestClientHoldBody checks, over HTTP/1 and HTTP/2, that a body held with
// HoldBody after each piece is delivered whole and in order, never to a
// function that holds it, and over HTTP/2 that a body held for good holds
// up no other stream on the connection.
func TestClientHoldBody(t *testing.T) {
	const size = 4 << 20
	body := pattern(size)
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(body)
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, counter, tlsConfig := newH2TestServer(t, handler)
	for _, proto := range []string{"HTTP/1.1", "HTTP/2.0"} {
		t.Run(proto, func(t *testing.T) {
			config := streamConfig(0)
			config.StreamResponseBodyBuffer = 64 << 10
			url := h1.URL
			if proto == "HTTP/2.0" {
				config.TLSConfig, url = tlsConfig, h2.URL
			}
			client := newTestClient(t, config)
			if proto == "HTTP/2.0" {
				// One body held for good, beside the others.
				stuck := make(chan *ClientResponse, 1)
				client.Do(mustRequest(t, "GET", url, nil), func(cr *ClientResponse, err error) {
					if err == nil {
						cr.HoldBody(true)
						cr.OnBody(func([]byte, bool, error) {})
						stuck <- cr
					}
				})
				defer (<-stuck).Close()
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					h := &holdRun{done: make(chan struct{})}
					client.Do(mustRequest(t, "GET", url, nil), func(cr *ClientResponse, err error) {
						if err != nil {
							h.err = err
							close(h.done)
							return
						}
						h.take(cr)
					})
					select {
					case <-h.done:
					case <-time.After(20 * time.Second):
						t.Error("the held body never finished")
						return
					}
					h.mu.Lock()
					defer h.mu.Unlock()
					if h.err != nil || !bytes.Equal(h.data, body) {
						t.Errorf("got %d of %d bytes, err %v", len(h.data), size, h.err)
					}
					if h.whileHeld != 0 {
						t.Errorf("%d calls came while the body was held", h.whileHeld)
					}
				}()
			}
			wg.Wait()
			if proto == "HTTP/2.0" {
				if n := counter.n.Load(); n != 1 {
					t.Errorf("%d connections, want 1", n)
				}
			}
		})
	}
}

// TestClientHoldWholeBody checks that a response that arrived whole, held
// from its callback, reaches OnBody's function only once it is resumed.
func TestClientHoldWholeBody(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write([]byte("whole"))
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	got := make(chan string, 1)
	held := make(chan *ClientResponse, 1)
	client.Do(mustRequest(t, "GET", server.URL, nil), func(cr *ClientResponse, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		cr.HoldBody(true)
		cr.OnBody(func(data []byte, fin bool, err error) { got <- fmt.Sprintf("%s %v %v", data, fin, err) })
		held <- cr
	})
	cr := <-held
	select {
	case s := <-got:
		t.Fatalf("a held body was delivered: %s", s)
	case <-time.After(100 * time.Millisecond):
	}
	cr.HoldBody(false)
	select {
	case s := <-got:
		if s != "whole true <nil>" {
			t.Fatalf("delivered %q", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the resumed body was never delivered")
	}
}

// TestClientHoldBodyError checks that a body that fails while held tells
// OnBody's function at once.
func TestClientHoldBodyError(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "1000000")
		_, _ = w.Write(pattern(1000))
		w.(stdhttp.Flusher).Flush()
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	config := streamConfig(0)
	config.Timeout = 300 * time.Millisecond
	client := newTestClient(t, config)
	got := make(chan error, 1)
	client.Do(mustRequest(t, "GET", server.URL, nil), func(cr *ClientResponse, err error) {
		if err != nil {
			got <- err
			return
		}
		cr.HoldBody(true)
		cr.OnBody(func(_ []byte, fin bool, err error) {
			if fin {
				got <- err
			}
		})
	})
	select {
	case err := <-got:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the held body ended with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a held body that failed was never told")
	}
}
