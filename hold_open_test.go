//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestHoldReadsFromOpenThenHangup holds a connection's reads from its OnOpen,
// as a relay does while it dials the other side, while the peer sends and
// ends its side; releasing the hold later hands over what the peer sent and
// then the end. With release, the hold is lifted from another goroutine
// after a pause, or before (none) the peer has sent anything.
func TestHoldReadsFromOpenThenHangup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release time.Duration
		closeIn time.Duration
	}{
		{"closed during the hold", 300 * time.Millisecond, 0},
		{"closed after the hold", 50 * time.Millisecond, 300 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var got []byte
			closed := make(chan struct{})
			server, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
				Open: func(c *Connection) {
					c.HoldReads(true)
					time.AfterFunc(tc.release, func() { c.HoldReads(false) })
				},
				Data: func(_ *Connection, data []byte) {
					mu.Lock()
					got = append(got, data...)
					mu.Unlock()
				},
				Close: func(*Connection, error) { close(closed) },
			}})
			if err != nil {
				t.Fatal(err)
			}
			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Run() }()
			t.Cleanup(func() {
				server.Stop()
				<-done
				_ = server.Close()
			})
			peer, err := net.Dial("tcp", addr.String())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Write([]byte("last words")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(tc.closeIn)
			peer.Close()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("OnClose did not run once the hold was lifted")
			}
			mu.Lock()
			defer mu.Unlock()
			if string(got) != "last words" {
				t.Fatalf("OnData got %q", got)
			}
		})
	}
}
