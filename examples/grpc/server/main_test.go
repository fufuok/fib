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

	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

// The server greets, refuses a request without a name, and streams three
// greetings.
func TestRunGreets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0"}, &out, func(addr string) { addrs <- addr }) }()
	addr := <-addrs

	engine, stop := example.ClientEngine()
	defer stop()
	conn, err := grpc.NewClient(addr, grpc.WithEngine(engine))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := greeter.NewGreeterClient(conn)
	callCtx, callCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer callCancel()

	reply, err := client.SayHello(callCtx, &greeter.HelloRequest{Name: "test"})
	if err != nil || reply.Message != "hello, test" {
		t.Fatalf("SayHello: %v, %v", reply, err)
	}
	if _, err := client.SayHello(callCtx, &greeter.HelloRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no name: %v", err)
	}
	stream, err := client.SayHellos(callCtx, &greeter.HelloRequest{Name: "s"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, reply.Message)
	}
	if strings.Join(got, "|") != "hello #1, s|hello #2, s|hello #3, s" {
		t.Fatalf("stream %q", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "gRPC server listening on "+addr) {
		t.Fatalf("output %q", out.String())
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
