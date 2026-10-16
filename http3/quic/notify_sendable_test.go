package quic

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func waitRoom(t *testing.T, room <-chan error) error {
	t.Helper()
	select {
	case err := <-room:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the callback never ran")
		return nil
	}
}

// stuckStream opens a stream on a pair whose server never gives credit back,
// and writes more to it than the window lets go.
func stuckStream(t *testing.T) (*testPair, *Stream) {
	t.Helper()
	const window = 4 << 10
	p := newTestPair(t, 0, func(_, server *Config) {
		server.StreamReceiveWindow, server.ConnReceiveWindow = window, 2*window
	})
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { s.HoldCredit() }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(bytes.Repeat([]byte("x"), 64<<10), false); err != nil {
		t.Fatal(err)
	}
	return p, s
}

// The callback runs once the stream has sent all but limit of what it was
// written, and only then; under the limit nothing is armed.
func TestNotifySendableWhenSent(t *testing.T) {
	const window = 4 << 10
	p := newTestPair(t, 0, func(_, server *Config) {
		server.StreamReceiveWindow, server.ConnReceiveWindow = window, 2*window
	})
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("paced"), 64<<10/5)
	if err := s.Write(payload, false); err != nil {
		t.Fatal(err)
	}
	room := make(chan error, 1)
	if !s.NotifySendable(0, func(err error) { room <- err }) {
		t.Fatalf("nothing armed with %d bytes unsent", s.Unsent())
	}
	if err := waitRoom(t, room); err != nil {
		t.Fatal(err)
	}
	if n := s.Unsent(); n != 0 {
		t.Fatalf("called with %d bytes unsent", n)
	}
	if s.NotifySendable(0, func(error) { t.Error("called with nothing unsent") }) {
		t.Fatal("armed with nothing unsent")
	}
}

// A stream reset, or a connection closed, runs the callback with ErrClosed,
// and a second callback replaces the first, which never runs.
func TestNotifySendableWhenClosed(t *testing.T) {
	t.Run("reset", func(t *testing.T) {
		_, s := stuckStream(t)
		room := make(chan error, 2)
		if !s.NotifySendable(0, func(error) { room <- errors.New("replaced callback ran") }) {
			t.Fatal("nothing armed")
		}
		if !s.NotifySendable(0, func(err error) { room <- err }) {
			t.Fatal("nothing armed")
		}
		s.Reset(1)
		if err := waitRoom(t, room); err != ErrClosed {
			t.Fatalf("callback with %v, want ErrClosed", err)
		}
		select {
		case err := <-room:
			t.Fatalf("a second call: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		// A reset stream is armed and called at once.
		if !s.NotifySendable(0, func(err error) { room <- err }) {
			t.Fatal("nothing armed on a reset stream")
		}
		if err := waitRoom(t, room); err != ErrClosed {
			t.Fatalf("callback with %v, want ErrClosed", err)
		}
	})
	t.Run("close", func(t *testing.T) {
		p, s := stuckStream(t)
		room := make(chan error, 1)
		if !s.NotifySendable(0, func(err error) { room <- err }) {
			t.Fatal("nothing armed")
		}
		p.client.Close(0, "bye")
		if err := waitRoom(t, room); err != ErrClosed {
			t.Fatalf("callback with %v, want ErrClosed", err)
		}
	})
}
