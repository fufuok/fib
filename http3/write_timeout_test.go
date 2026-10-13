//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	stdhttp "net/http"
	"os"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// WriteTimeout ends a response whose client stops taking it: the stream is
// reset once it has passed, the handler waiting for the client is let go
// with os.ErrDeadlineExceeded, and the connection goes on serving its other
// requests, before and after.
func TestWriteTimeoutResetsStalledStream(t *testing.T) {
	done := make(chan error, 1)
	base := startServer(t, Config{WriteTimeout: 300 * time.Millisecond}, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/quick" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("quick"))
			return
		}
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				done <- err
				return
			}
		}
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// The client never gives the stream room beyond its first window.
	s.HoldCredit()
	head, _ := h3Request{path: "/stall"}.frames()
	start := time.Now()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	w.await(s.ID(), "the header", func(st *h3WireStream) bool { return st.status == 200 })
	if status, body := w.response(w.send(t, h3Request{path: "/quick"}, false)); status != 200 || string(body) != "quick" {
		t.Fatalf("a request beside the stalled one got %d %q", status, body)
	}
	st := w.await(s.ID(), "reset", func(st *h3WireStream) bool { return st.done })
	w.mu.Lock()
	reset := st.reset
	w.mu.Unlock()
	if !reset {
		t.Fatal("the stalled stream ended without a reset")
	}
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Fatalf("the stream was reset after %v, before WriteTimeout", waited)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the waiting Write reported %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler still waits past WriteTimeout")
	}
	if status, body := w.response(w.send(t, h3Request{path: "/quick"}, false)); status != 200 || string(body) != "quick" {
		t.Fatalf("a request after the reset got %d %q", status, body)
	}
}

// A Write made after WriteTimeout has passed fails with
// os.ErrDeadlineExceeded, even to a client that is keeping up, and the
// handler hears of the reset through OnCancel; so does one that has not
// written anything by then.
func TestWriteTimeoutRefusesLateWrites(t *testing.T) {
	done := make(chan error, 2)
	base := startServer(t, Config{WriteTimeout: 100 * time.Millisecond}, func(c *fibhttp.Context) {
		cancelled := make(chan error, 1)
		c.OnCancel(func(err error) { cancelled <- err })
		if c.Request.URL.Path == "/streaming" {
			if _, err := c.Write(make([]byte, 32<<10)); err != nil {
				done <- err
				return
			}
			c.Flush()
		}
		select {
		case err := <-cancelled:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Errorf("cancelled with %v, want %v", err, os.ErrDeadlineExceeded)
			}
		case <-time.After(5 * time.Second):
			t.Error("the handler was not told of the reset")
		}
		_, err := c.Write(make([]byte, 32<<10))
		if err == nil {
			// A short write is held back; the response's end sends it.
			err = c.Finish()
		}
		done <- err
	})
	w := dialH3Wire(t, nil, base)
	for _, path := range []string{"/streaming", "/idle"} {
		id := w.send(t, h3Request{path: path}, false)
		st := w.await(id, "reset", func(st *h3WireStream) bool { return st.done })
		w.mu.Lock()
		reset := st.reset
		w.mu.Unlock()
		if !reset {
			t.Fatalf("%s: the stream ended without a reset", path)
		}
		select {
		case err := <-done:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("%s: Write after WriteTimeout: %v, want %v", path, err, os.ErrDeadlineExceeded)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the handler never returned", path)
		}
	}
}

// Responses that finish within WriteTimeout are left alone, whether sent
// whole or streamed, on recycled streams, long after the first of them: the
// timers of streams that finished stop, and never reach the requests their
// streams go on to serve.
func TestWriteTimeoutLeavesFinishedStreamsAlone(t *testing.T) {
	base := startServer(t, Config{WriteTimeout: 300 * time.Millisecond}, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/stream" {
			for range 8 {
				if _, err := c.Write(make([]byte, 16<<10)); err != nil {
					return
				}
			}
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("whole"))
	})
	w := dialH3Wire(t, nil, base)
	for i := range 3 {
		if i > 0 {
			time.Sleep(350 * time.Millisecond)
		}
		if status, body := w.response(w.send(t, h3Request{path: "/whole"}, false)); status != 200 || string(body) != "whole" {
			t.Fatalf("round %d: %d %q", i, status, body)
		}
		if status, body := w.response(w.send(t, h3Request{path: "/stream"}, false)); status != 200 || len(body) != 8*16<<10 {
			t.Fatalf("round %d: %d, %d bytes", i, status, len(body))
		}
	}
}
