//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client sends datagrams to the UDP echo server and prints the
// echoes.
//
//	go run ./examples/udp/nontls/client
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
var echoTimeout = 3 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:9001", "server address")
	count := flags.Int("n", 5, "datagrams to send")
	if err := flags.Parse(args); err != nil {
		return err
	}

	engine, stop := example.ClientEngine()
	defer stop()

	datagrams := make(chan []byte, 64)
	closed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		Data:  func(_ *fib.Connection, data []byte) { datagrams <- append([]byte(nil), data...) },
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}
	dialed := make(chan error, 1)
	var conn *fib.Connection
	// A UDP dial connects the socket to the server, so the connection
	// exchanges datagrams with it alone.
	err := engine.DialWithHandler("udp", *addr, 3*time.Second, handler, func(c *fib.Connection, err error) {
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

	for i := 1; i <= *count; i++ {
		if err := conn.Send([]byte(example.Message(i))); err != nil {
			return err
		}
		// UDP keeps message boundaries, so one datagram is one whole echo.
		select {
		case echo := <-datagrams:
			fmt.Fprintf(out, "echo: %s\n", echo)
		case err := <-closed:
			// Nothing listening at the server's port shows up here as a
			// refusal.
			return fmt.Errorf("connection closed: %v", err)
		case <-time.After(echoTimeout):
			return errors.New("no echo: UDP does not retransmit, is the server running?")
		}
	}
	return nil
}
