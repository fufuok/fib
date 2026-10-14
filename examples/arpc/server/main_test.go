//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
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

// The server answers the echo, the typed service and the stream, and reports
// a stream its client abandoned.
func TestRunServes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0"}, out, func(addr string) { addrs <- addr }) }()
	addr := <-addrs

	engine, stop := example.ClientEngine()
	defer stop()
	client, err := arpc.Dial(engine, "tcp", addr, 5*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	var echo string
	if err := client.Call("/echo", "hi", &echo, 10*time.Second); err != nil || echo != "hi" {
		t.Fatalf("echo %q, %v", echo, err)
	}
	var sum SumRsp
	if err := client.Call("Math.Sum", &SumReq{A: 2, B: 3}, &sum, 10*time.Second); err != nil || sum.Sum != 5 {
		t.Fatalf("sum %d, %v", sum.Sum, err)
	}

	stream := client.NewStream("/upper")
	if err := stream.Send("abc"); err != nil {
		t.Fatal(err)
	}
	var word string
	if err := stream.Recv(&word); err != nil || word != "ABC" {
		t.Fatalf("stream %q, %v", word, err)
	}
	stream.CloseSend()
	if err := stream.Recv(&word); err != io.EOF {
		t.Fatalf("after CloseSend: %v", err)
	}

	// A client that goes away with its stream open ends the stream with an
	// error, not EOF.
	leaving, err := arpc.Dial(engine, "tcp", addr, 5*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	open := leaving.NewStream("/upper")
	if err := open.Send("x"); err != nil {
		t.Fatal(err)
	}
	if err := open.Recv(&word); err != nil {
		t.Fatal(err)
	}
	leaving.Stop()
	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(out.String(), "stream: ") {
		if time.Now().After(deadline) {
			t.Fatalf("output %q", out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "arpc server listening on "+addr) || !strings.Contains(text, "connected: 127.0.0.1:") {
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
