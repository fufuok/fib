//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// trickle is a handler that sends "first", then waits for release before
// it sends "second": a body of no declared length that trickles, as
// server-sent events do. A request with ?length=1 gets a small body with a
// Content-Length instead.
func trickle(release <-chan struct{}) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Query().Get("length") != "" {
			w.Header().Set("Content-Length", "5")
			_, _ = w.Write([]byte("whole"))
			return
		}
		_, _ = w.Write([]byte("first"))
		w.(stdhttp.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("second"))
	}
}

// unknownLengthRun sends a request for a trickling body and reports whether
// its callback ran, and "first" reached OnBody, before the server was
// released, and what the whole body was.
type unknownLengthRun struct {
	mu       sync.Mutex
	data     strings.Builder
	called   chan bool // whether the body was complete at the callback
	gotFirst chan struct{}
	done     chan struct{}
	first    sync.Once
}

func startUnknownLength(client *Client, req *stdhttp.Request) *unknownLengthRun {
	u := &unknownLengthRun{called: make(chan bool, 1), gotFirst: make(chan struct{}), done: make(chan struct{})}
	client.Do(req, func(cr *ClientResponse, err error) {
		if err != nil {
			u.called <- false
			close(u.done)
			return
		}
		u.called <- cr.BodyComplete()
		cr.OnBody(func(data []byte, fin bool, err error) {
			u.mu.Lock()
			u.data.Write(data)
			has := strings.HasPrefix(u.data.String(), "first")
			u.mu.Unlock()
			if has {
				u.first.Do(func() { close(u.gotFirst) })
			}
			if fin {
				close(u.done)
			}
		})
	})
	return u
}

func (u *unknownLengthRun) body() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.data.String()
}

// checkUnknownLength runs the trickling request on client, whose config sets
// StreamUnknownLengthAtHeader (atHeader) or not, and checks when the body is
// handed on.
func checkUnknownLength(t *testing.T, client *Client, url string, atHeader bool, release chan struct{}) {
	t.Helper()
	u := startUnknownLength(client, mustRequest(t, "GET", url, nil))
	wait := 3 * time.Second
	if !atHeader {
		wait = 300 * time.Millisecond
	}
	select {
	case complete := <-u.called:
		if !atHeader {
			t.Fatal("the callback ran before the threshold of the body had arrived")
		}
		if complete {
			t.Fatal("the body was complete at the callback")
		}
	case <-time.After(wait):
		if atHeader {
			t.Fatal("the callback did not run at the header")
		}
	}
	if atHeader {
		select {
		case <-u.gotFirst:
		case <-time.After(3 * time.Second):
			t.Fatalf("OnBody did not get the first piece before the rest was sent; got %q", u.body())
		}
	}
	close(release)
	select {
	case <-u.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the body did not end")
	}
	if got := u.body(); got != "firstsecond" {
		t.Fatalf("body %q", got)
	}

	// A small body with a length is still whole at its callback.
	w := startUnknownLength(client, mustRequest(t, "GET", url+"?length=1", nil))
	select {
	case complete := <-w.called:
		if !complete {
			t.Fatal("a small body with a length streamed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no callback for a small body with a length")
	}
	<-w.done
	if got := w.body(); got != "whole" {
		t.Fatalf("small body %q", got)
	}
}

func TestClientStreamUnknownLengthAtHeader(t *testing.T) {
	for _, atHeader := range []bool{true, false} {
		release := make(chan struct{})
		server := httptest.NewServer(trickle(release))
		config := streamConfig(64 << 10)
		config.StreamUnknownLengthAtHeader = atHeader
		checkUnknownLength(t, newTestClient(t, config), server.URL+"/", atHeader, release)
		server.Close()
	}
}

func TestClientHTTP2StreamUnknownLengthAtHeader(t *testing.T) {
	for _, atHeader := range []bool{true, false} {
		release := make(chan struct{})
		ts, _, tlsConfig := newH2TestServer(t, trickle(release))
		config := streamConfig(64 << 10)
		config.TLSConfig = tlsConfig
		config.StreamUnknownLengthAtHeader = atHeader
		checkUnknownLength(t, newTestClient(t, config), ts.URL+"/", atHeader, release)
	}
}
