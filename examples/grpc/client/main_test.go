//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

// greeterServer greets like the example server; failHello and failStream
// make it fail the first call and the stream after one greeting.
type greeterServer struct{ failHello, failStream bool }

func (s greeterServer) SayHello(_ context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if s.failHello {
		return nil, errors.New("not today")
	}
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &greeter.HelloReply{Message: "hello, " + in.Name}, nil
}

func (s greeterServer) SayHellos(in *greeter.HelloRequest, stream grpc.ServerStreamingServer[greeter.HelloReply]) error {
	if err := stream.Send(&greeter.HelloReply{Message: "hello #1, " + in.Name}); err != nil {
		return err
	}
	if s.failStream {
		return status.Error(codes.Unavailable, "cut short")
	}
	return nil
}

// serveGreeter runs srv until the test ends.
func serveGreeter(t *testing.T, srv greeterServer) string {
	t.Helper()
	s := grpc.NewServer()
	greeter.RegisterGreeterServer(s, srv)
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: s})
	if err != nil {
		t.Fatal(err)
	}
	stop := example.Start(engine)
	t.Cleanup(func() { _ = stop() })
	return example.Addr(engine)
}

func TestRunMakesEachCall(t *testing.T) {
	addr := serveGreeter(t, greeterServer{})
	var out bytes.Buffer
	if err := run([]string{"-addr", addr}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	want := "hello, fib\nrefused: InvalidArgument: name is required\nhello #1, stream\n"
	if out.String() != want {
		t.Fatalf("output %q", out.String())
	}
}

// A failed call, a stream cut short, and a server that is not there are
// each reported.
func TestRunFailures(t *testing.T) {
	if err := run([]string{"-addr", serveGreeter(t, greeterServer{failHello: true})}, io.Discard); err == nil {
		t.Error("no error from a failed call")
	}
	if err := run([]string{"-addr", serveGreeter(t, greeterServer{failStream: true})}, io.Discard); status.Code(err) != codes.Unavailable {
		t.Errorf("stream cut short: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-addr", refused}, io.Discard); err == nil {
		t.Error("no error from an address nothing listens on")
	}
	if err := run([]string{"-addr", ""}, io.Discard); err == nil {
		t.Error("no error for an empty target")
	}
	if err := run([]string{"-nope"}, io.Discard); err == nil {
		t.Error("accepted an unknown flag")
	}
}

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"client", "-nope"}
	main()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
