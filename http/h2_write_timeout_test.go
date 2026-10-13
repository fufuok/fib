//go:build linux || darwin || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"os"
	"testing"
	"time"
)

// WriteTimeout ends an HTTP/2 response whose client stops taking it, as
// net/http's does: the stream is reset with INTERNAL_ERROR once it has
// passed, the handler waiting for the window is let go with
// os.ErrDeadlineExceeded, and the connection goes on serving its other
// streams, before and after.
func TestH2WriteTimeoutResetsStalledStream(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 300 * time.Millisecond
	done := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
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
	})))
	// Every stream's window is 1000 bytes, which the stalled stream fills
	// and the client never opens again.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 1000})
	start := time.Now()
	tc.headers(1, true, get("/stall")...)
	tc.headers(3, true, get("/quick")...)
	if _, body := tc.response(3); string(body) != "quick" {
		t.Fatalf("a stream beside the stalled one got %q", body)
	}
	st := tc.stream(1)
	for !st.done {
		if f := tc.read(); f.typ == h2FrameGoAway {
			t.Fatal("the connection went away")
		}
	}
	if !st.resetSet || st.reset != H2InternalError {
		t.Fatalf("the stalled stream ended with reset %v (%v), want %v", st.resetSet, st.reset, H2InternalError)
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
	tc.headers(5, true, get("/quick")...)
	if _, body := tc.response(5); string(body) != "quick" {
		t.Fatalf("a stream after the reset got %q", body)
	}
}

// A Write made after WriteTimeout has passed fails with
// os.ErrDeadlineExceeded, as on HTTP/1, even to a client that is keeping up,
// and the handler hears of the reset through OnCancel; so does one that has
// not written anything by then.
func TestH2WriteTimeoutRefusesLateWrites(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 100 * time.Millisecond
	done := make(chan error, 2)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
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
	})))
	tc := dialH2(t, addr)
	for i, path := range []string{"/streaming", "/idle"} {
		id := uint32(1 + 2*i)
		tc.headers(id, true, get(path)...)
		st := tc.stream(id)
		for !st.done {
			if f := tc.read(); f.typ == h2FrameData && len(f.payload) > 0 {
				tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))))
				tc.write(h2AppendWindowUpdate(nil, f.streamID, uint32(len(f.payload))))
			}
		}
		if !st.resetSet || st.reset != H2InternalError {
			t.Fatalf("%s: the stream ended with reset %v (%v), want %v", path, st.resetSet, st.reset, H2InternalError)
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
func TestH2WriteTimeoutLeavesFinishedStreamsAlone(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 300 * time.Millisecond
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/stream" {
			for range 8 {
				if _, err := c.Write(make([]byte, 16<<10)); err != nil {
					return
				}
			}
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("whole"))
	})))
	tc := dialH2(t, addr)
	id := uint32(1)
	for i := range 3 {
		if i > 0 {
			time.Sleep(350 * time.Millisecond)
		}
		tc.headers(id, true, get("/whole")...)
		if _, body := tc.response(id); string(body) != "whole" {
			t.Fatalf("round %d: %q", i, body)
		}
		tc.headers(id+2, true, get("/stream")...)
		if st := tc.readAcking(id + 2); len(st.body) != 8*16<<10 || st.resetSet {
			t.Fatalf("round %d: %d bytes, reset %v", i, len(st.body), st.reset)
		}
		id += 4
	}
}
