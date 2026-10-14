//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client calls the arpc server: a call, an asynchronous call, a call
// of a typed service, and a stream.
//
//	go run ./examples/arpc/client
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
)

type SumReq struct{ A, B int }
type SumRsp struct{ Sum int }

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
	addr := flags.String("addr", "127.0.0.1:8888", "server address")
	count := flags.Int("n", 3, "calls to make")
	if err := flags.Parse(args); err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()
	client, err := arpc.Dial(engine, "tcp", *addr, 3*time.Second, nil)
	if err != nil {
		return err
	}
	// The client stops before its engine closes.
	defer client.Stop()

	for i := 1; i <= *count; i++ {
		var echo string
		if err := client.Call("/echo", example.Message(i), &echo, 5*time.Second); err != nil {
			return err
		}
		fmt.Fprintf(out, "echo: %s\n", echo)
	}

	done := make(chan struct{})
	err = client.CallAsync("/echo", "async", func(ctx *arpc.Context, err error) {
		defer close(done)
		var echo string
		if err == nil {
			err = ctx.Bind(&echo)
		}
		fmt.Fprintf(out, "async echo: %q, %v\n", echo, err)
	}, 5*time.Second)
	if err != nil {
		return err
	}
	<-done

	var sum SumRsp
	if err := client.Call("Math.Sum", &SumReq{A: 2, B: 3}, &sum, 5*time.Second); err != nil {
		return err
	}
	fmt.Fprintf(out, "Math.Sum(2, 3) = %d\n", sum.Sum)

	stream := client.NewStream("/upper")
	for _, word := range []string{"fib", "arpc"} {
		if err := stream.Send(word); err != nil {
			return err
		}
	}
	stream.CloseSend()
	for {
		var word string
		if err := stream.Recv(&word); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		fmt.Fprintf(out, "stream: %s\n", word)
	}
	return nil
}
