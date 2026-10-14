//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a gRPC greeter.
//
//	go run ./examples/grpc/server
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

type server struct{}

func (server) SayHello(ctx context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &greeter.HelloReply{Message: "hello, " + in.Name}, nil
}

func (server) SayHellos(in *greeter.HelloRequest, stream grpc.ServerStreamingServer[greeter.HelloReply]) error {
	for i := 1; i <= 3; i++ {
		if err := stream.Send(&greeter.HelloReply{Message: fmt.Sprintf("hello #%d, %s", i, in.Name)}); err != nil {
			return err
		}
	}
	return nil
}

// fatal reports a startup error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, nil); err != nil {
		fatal(err)
	}
}

// run serves until ctx ends. ready, if not nil, is told the address once the
// server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:50051", "listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}

	// Every call runs on the engine's handler pool; grpc.TaskPool(pool)
	// would run them on a pool of the program's own instead.
	s := grpc.NewServer()
	greeter.RegisterGreeterServer(s, server{})

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, s)
	if err != nil {
		return err
	}
	return example.Serve(ctx, out, ready, "gRPC server listening on %s", engine)
}
