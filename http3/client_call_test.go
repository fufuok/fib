//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"context"
	"errors"
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestStartCancel checks that Call.Cancel abandons a request whose context
// cannot be cancelled, with the error given or context.Canceled for none,
// that the connection serves the next request, and that cancelling a settled
// request, or the zero Call, does nothing.
func TestStartCancel(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		if c.Request.URL.Path != "/slow" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
			return
		}
		c.Retain()
		go func() {
			<-release
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
			c.Release()
		}()
	})
	client := newClient(t, nil)
	errGiven := errors.New("given up")
	for _, given := range []error{nil, errGiven} {
		want := given
		if want == nil {
			want = context.Canceled
		}
		outcome := make(chan error, 1)
		call := client.Start(mustRequest(t, stdhttp.MethodGet, url+"/slow", nil), func(_ *fibhttp.ClientResponse, err error) {
			outcome <- err
		})
		time.AfterFunc(50*time.Millisecond, func() { call.Cancel(given) })
		if err := <-outcome; !errors.Is(err, want) {
			t.Errorf("err = %v, want %v", err, want)
		}
		call.Cancel(errGiven)
	}
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait()
	if err != nil || readBody(t, resp) != "ok" {
		t.Fatalf("after the cancels: %v", err)
	}
	Call{}.Cancel(nil)
}
