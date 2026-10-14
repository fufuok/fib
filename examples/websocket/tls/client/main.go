//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client sends messages to the WebSocket echo server over TLS
// (wss://) and prints the echoes.
//
//	go run ./examples/websocket/tls/client
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/websocket"
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
	url := flags.String("url", "wss://127.0.0.1:8444/ws", "server URL")
	count := flags.Int("n", 5, "messages to send")
	compress := flags.Bool("compress", false, "offer permessage-deflate compression")
	certFlags := certs.ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	engine, stop := example.ClientEngine()
	defer stop()
	config := websocket.DefaultDialerConfig()
	config.EnableCompression = *compress
	// Used for wss:// URLs; the server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	dialer := websocket.NewDialer(engine, config)

	messages := make(chan string, 64)
	closed := make(chan error, 1)
	handler := websocket.HandlerFuncs{
		Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) { messages <- string(data) },
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			closed <- fmt.Errorf("code=%d reason=%q error=%v", code, reason, err)
		},
	}
	conn, resp, err := dialer.Go(*url, nil, handler).Wait()
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "connected: %s\n", resp.Status)

	for i := 1; i <= *count; i++ {
		if err := conn.WriteText(example.Message(i)); err != nil {
			return err
		}
		select {
		case echo := <-messages:
			fmt.Fprintf(out, "echo: %s\n", echo)
		case err := <-closed:
			return fmt.Errorf("connection closed: %v", err)
		case <-time.After(echoTimeout):
			return errors.New("timed out waiting for the echo")
		}
	}
	_ = conn.Close(websocket.CloseNormal, "done")
	select {
	case err := <-closed:
		fmt.Fprintln(out, "closed:", err)
	case <-time.After(3 * time.Second):
	}
	return nil
}
