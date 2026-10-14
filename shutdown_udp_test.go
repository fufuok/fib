//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"context"
	"net"
	"testing"
	"time"
)

// Shutdown waits for a UDP listener's peers as for accepted connections, and
// tells their handler, which may close them to let the shutdown finish.
func TestShutdownTellsUDPPeers(t *testing.T) {
	handler := &shutdownHandler{HandlerFuncs: HandlerFuncs{Data: func(c *Connection, b []byte) { _ = c.Send(b) }}, close: true}
	server, addr := startUDPServer(t, DefaultConfig(), handler)
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if handler.told.Load() != 1 {
		t.Fatalf("OnShutdown ran %d times for one peer", handler.told.Load())
	}
}

// An engine already closed has nothing left open, and Shutdown says so at once.
func TestShutdownAfterClose(t *testing.T) {
	server := newOfflineServer(t)
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after Close = %v", err)
	}
}
