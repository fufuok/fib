//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client sends messages to the TCP echo server and prints the echoes.
//
//	go run ./examples/tcp/nontls/client
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
)

// fatal reports an error and exits; a test replaces it.
var fatal = example.Fatal

// echoTimeout bounds the wait for each echo; a test shortens it.
var echoTimeout = 5 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:9000", "server address")
	count := flags.Int("n", 5, "messages to send")
	if err := flags.Parse(args); err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()

	chunks := make(chan []byte, 64)
	closed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		// data is only valid during the call, so it is copied before it
		// leaves the handler.
		Data:  func(_ *fib.Connection, data []byte) { chunks <- append([]byte(nil), data...) },
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}
	dialed := make(chan error, 1)
	var conn *fib.Connection
	err := engine.DialWithHandler("tcp", *addr, 3*time.Second, handler, func(c *fib.Connection, err error) {
		conn = c
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Fprintf(out, "connected to %s\n", conn.RemoteAddr())

	for i := 1; i <= *count; i++ {
		msg := example.Message(i)
		if err := conn.Send([]byte(msg)); err != nil {
			return err
		}
		// A stream may deliver the echo in several pieces.
		var echo []byte
		for len(echo) < len(msg) {
			select {
			case chunk := <-chunks:
				echo = append(echo, chunk...)
			case err := <-closed:
				return fmt.Errorf("connection closed: %v", err)
			case <-time.After(echoTimeout):
				return errors.New("timed out waiting for the echo")
			}
		}
		fmt.Fprintf(out, "echo: %s\n", echo)
	}
	return nil
}
