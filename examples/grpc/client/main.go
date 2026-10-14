//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client calls the gRPC greeter: a unary call, a call the server
// refuses, and a server-streaming call.
//
//	go run ./examples/grpc/client
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/status"
)

// fatal reports an error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:50051", "server address")
	if err := flags.Parse(args); err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()
	conn, err := grpc.NewClient(*addr, grpc.WithEngine(engine))
	if err != nil {
		return err
	}
	// The connection closes before its engine does.
	defer conn.Close()
	client := greeter.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "fib"})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, reply.Message)

	_, err = client.SayHello(ctx, &greeter.HelloRequest{})
	fmt.Fprintf(out, "refused: %s: %s\n", status.Code(err), status.Convert(err).Message())

	stream, err := client.SayHellos(ctx, &greeter.HelloRequest{Name: "stream"})
	if err != nil {
		return err
	}
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Fprintln(out, reply.Message)
	}
	return nil
}
