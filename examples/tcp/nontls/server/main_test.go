//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The server echoes what a client writes, with writev on and off, and stops
// when its context ends.
func TestRunEchoes(t *testing.T) {
	for _, writev := range []string{"true", "false"} {
		t.Run("writev="+writev, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			var out bytes.Buffer
			addrs := make(chan string, 1)
			done := make(chan error, 1)
			go func() {
				done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-writev=" + writev}, &out, func(addr string) { addrs <- addr })
			}()
			addr := <-addrs
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			msg := []byte("hello echo")
			if _, err := conn.Write(msg); err != nil {
				t.Fatal(err)
			}
			echo := make([]byte, len(msg))
			if _, err := io.ReadFull(conn, echo); err != nil || !bytes.Equal(echo, msg) {
				t.Fatalf("echo %q, %v", echo, err)
			}
			_ = conn.Close()
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "TCP echo server listening on "+addr) {
				t.Fatalf("output %q", out.String())
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-addr", "not an address"}, io.Discard, nil); err == nil {
		t.Error("started on a bad address")
	}
}

func TestMainReportsAStartupError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"server", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
