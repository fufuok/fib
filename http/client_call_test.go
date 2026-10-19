//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClientStartCancel checks, over HTTP/1.1 and h2c, that Call.Cancel
// abandons a request whose context cannot be cancelled: one still waiting for
// its header fails with the error given, or context.Canceled for none, and a
// body that is streaming fails with it; that the connection serves the next
// request; and that cancelling a settled request, or the zero Call, does
// nothing.
func TestClientStartCancel(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/head":
			select {
			case <-release:
			case <-r.Context().Done():
			}
		case "/body":
			w.WriteHeader(stdhttp.StatusOK)
			_, _ = w.Write([]byte("part"))
			w.(stdhttp.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		_, _ = w.Write([]byte("ok"))
	}))
	ts.Config.Protocols = new(stdhttp.Protocols)
	ts.Config.Protocols.SetHTTP1(true)
	ts.Config.Protocols.SetUnencryptedHTTP2(true)
	ts.Start()
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(release) })
	errGiven := errors.New("given up")
	for _, h2c := range []bool{false, true} {
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = h2c
		config.StreamResponseBody = true
		config.StreamUnknownLengthAtHeader = true
		client := newTestClient(t, config)

		for _, given := range []error{nil, errGiven} {
			want := given
			if want == nil {
				want = context.Canceled
			}
			outcome := make(chan error, 1)
			call := client.Start(mustRequest(t, "GET", ts.URL+"/head", nil), func(_ *ClientResponse, err error) {
				outcome <- err
			})
			time.AfterFunc(50*time.Millisecond, func() { call.Cancel(given) })
			if err := <-outcome; !errors.Is(err, want) {
				t.Errorf("h2c=%v head: err = %v, want %v", h2c, err, want)
			}
			call.Cancel(errGiven)
		}

		bodyErr := make(chan error, 1)
		var call Call
		started := make(chan struct{})
		call = client.Start(mustRequest(t, "GET", ts.URL+"/body", nil), func(resp *ClientResponse, err error) {
			<-started
			if err != nil {
				bodyErr <- err
				return
			}
			resp.OnBody(func(_ []byte, fin bool, err error) {
				if fin || err != nil {
					bodyErr <- err
				}
			})
			call.Cancel(errGiven)
		})
		close(started)
		if err := <-bodyErr; !errors.Is(err, errGiven) {
			t.Errorf("h2c=%v body: err = %v, want %v", h2c, err, errGiven)
		}

		resp, err := client.Go(mustRequest(t, "GET", ts.URL+"/", nil)).Wait()
		if err != nil {
			t.Fatalf("h2c=%v after the cancels: %v", h2c, err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Errorf("h2c=%v after the cancels: body %q", h2c, got)
		}
		Call{}.Cancel(nil)
	}
}

// TestClientStartAllocs checks that Start, with a request whose context cannot
// be cancelled, allocates less than Do with one that can, which pays for the
// context.AfterFunc that watches it.
func TestClientStartAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	ts := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(ts.Close)
	client := newTestClient(t, DefaultClientConfig())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{}, 1)
	callback := func(*ClientResponse, error) { done <- struct{}{} }
	plain := mustRequest(t, "GET", ts.URL+"/", nil)
	withCtx := plain.WithContext(ctx)
	viaDo := testing.AllocsPerRun(200, func() {
		client.Do(withCtx, callback)
		<-done
	})
	viaStart := testing.AllocsPerRun(200, func() {
		client.Start(plain, callback)
		<-done
	})
	if viaStart >= viaDo {
		t.Errorf("Start allocates %.1f a request, Do with a context that can be cancelled %.1f", viaStart, viaDo)
	}
	t.Logf("allocations a request: Do with a cancellable context %.1f, Start %.1f", viaDo, viaStart)
}
