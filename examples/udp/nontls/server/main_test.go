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

// syncBuffer is an output the engine's goroutines and the test share.
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

// The server echoes each datagram, and reports a peer when its first datagram
// arrives and again when it has been idle for -idle.
func TestRunEchoesAndClosesIdlePeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-idle", "100ms"}, out, func(addr string) { addrs <- addr })
	}()
	addr := <-addrs
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte("datagram")); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, 64)
	n, err := conn.Read(echo)
	if err != nil || string(echo[:n]) != "datagram" {
		t.Fatalf("echo %q, %v", echo[:n], err)
	}
	peer := conn.LocalAddr().String()
	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(out.String(), "peer "+peer+": closed") {
		if time.Now().After(deadline) {
			t.Fatalf("the idle peer was not closed: %q", out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "UDP echo server listening on "+addr) || !strings.Contains(text, "peer "+peer+": open") {
		t.Fatalf("output %q", text)
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
