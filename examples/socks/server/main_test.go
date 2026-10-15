//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is what run prints to, read while it runs.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The proxy logs a client in and relays it to an echo server.
func TestRunRelays(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	var out syncBuffer
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-user", "me", "-pass", "pw"}, &out,
			func(addr string) { started <- addr })
	}()
	var addr string
	select {
	case addr = <-started:
	case err := <-done:
		t.Fatalf("did not start: %v", err)
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	ip := echo.Addr().(*net.TCPAddr)
	req := []byte{5, 1, 2, 1, 2, 'm', 'e', 2, 'p', 'w', 5, 1, 0, 1}
	req = append(append(req, ip.IP.To4()...), byte(ip.Port>>8), byte(ip.Port))
	if _, err := conn.Write(append(req, "ping"...)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 2+2+10+4)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if got[1] != 2 || got[3] != 0 || got[5] != 0 || string(got[14:]) != "ping" {
		t.Fatalf("got %x", got)
	}
	// The same port is an HTTP proxy.
	httpConn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer httpConn.Close()
	_ = httpConn.SetDeadline(time.Now().Add(10 * time.Second))
	target := echo.Addr().String()
	login := base64.StdEncoding.EncodeToString([]byte("me:pw"))
	_, _ = httpConn.Write([]byte("CONNECT " + target + " HTTP/1.1\r\nHost: " + target +
		"\r\nProxy-Authorization: Basic " + login + "\r\n\r\npong"))
	head := "HTTP/1.1 200 OK\r\n\r\npong"
	reply := make([]byte, len(head))
	if _, err := io.ReadFull(httpConn, reply); err != nil || string(reply) != head {
		t.Fatalf("HTTP proxy: %q (%v)", reply, err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "SOCKS5 connect "+echo.Addr().String()) ||
		!strings.Contains(out.String(), "HTTP/1.1 CONNECT "+echo.Addr().String()) {
		t.Fatalf("output %q", out.String())
	}
}

func TestRunRejectsBadFlags(t *testing.T) {
	if err := run(context.Background(), []string{"-nope"}, io.Discard, nil); err == nil {
		t.Fatal("an unknown flag ran")
	}
	if err := run(context.Background(), []string{"-addr", "nowhere"}, io.Discard, nil); err == nil {
		t.Fatal("an address without a port ran")
	}
}
