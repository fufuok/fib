//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command client speaks every protocol examples/multiproto/server serves,
// all to its one port, and prints what each answered: HTTP/1.1, h2c, HTTPS,
// WebSocket, gRPC, an HTTP/1.1 request through a SOCKS5 tunnel, an HTTPS one
// through an HTTP CONNECT tunnel, arpc and the raw TCP echo. Both tunnels
// lead back to the same port, where the request inside is served in turn.
//
//	go run ./examples/multiproto/server &
//	go run ./examples/multiproto/client
//
// Every protocol but the CONNECT tunnel uses fib's own client; package http's
// has no HTTP proxy, so that one goes through net/http's.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"os"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
	"github.com/lesismal/fib/websocket"
)

// timeout bounds each protocol's exchange.
const timeout = 10 * time.Second

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
	addr := flags.String("addr", "127.0.0.1:9000", "the server's one address")
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
	steps := []struct {
		name string
		do   func() (string, error)
	}{
		{"HTTP/1.1", func() (string, error) { return get(engine, fibhttp.DefaultClientConfig(), "http://"+*addr+"/http1") }},
		{"h2c", func() (string, error) {
			config := fibhttp.DefaultClientConfig()
			config.UnencryptedHTTP2 = true
			return get(engine, config, "http://"+*addr+"/h2c")
		}},
		{"HTTPS", func() (string, error) {
			config := fibhttp.DefaultClientConfig()
			config.TLSConfig = tlsConfig
			return get(engine, config, "https://"+*addr+"/https")
		}},
		{"WebSocket", func() (string, error) { return wsEcho(engine, "ws://"+*addr+"/ws", example.Message(1)) }},
		{"gRPC", func() (string, error) { return sayHello(engine, *addr) }},
		{"SOCKS", func() (string, error) {
			proxy, err := socks.ParseURL("socks5h://" + *addr)
			if err != nil {
				return "", err
			}
			config := fibhttp.DefaultClientConfig()
			config.Dial = proxy.Dial
			return get(engine, config, "http://"+*addr+"/via-socks")
		}},
		{"HTTP CONNECT", func() (string, error) { return getThroughConnect(*addr, tlsConfig, "https://"+*addr+"/via-connect") }},
		{"arpc", func() (string, error) { return arpcEcho(engine, *addr, example.Message(1)) }},
		{"TCP echo", func() (string, error) { return tcpEcho(engine, *addr, example.Message(1)) }},
	}
	for _, step := range steps {
		reply, err := step.do()
		if err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Fprintf(out, "%-13s %s\n", step.name, reply)
	}
	return nil
}

// get fetches rawURL with a fib client of config and returns the body.
func get(engine *fib.Engine, config fibhttp.ClientConfig, rawURL string) (string, error) {
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	config.Timeout = timeout
	client := fibhttp.NewClient(engine, config)
	defer client.Close()
	resp, err := client.Go(req).Wait()
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), err
}

// getThroughConnect fetches rawURL with net/http through the HTTP proxy at
// proxyAddr, which tunnels an https:// request with CONNECT.
func getThroughConnect(proxyAddr string, tlsConfig *tls.Config, rawURL string) (string, error) {
	transport := &stdhttp.Transport{
		Proxy:             stdhttp.ProxyURL(&url.URL{Scheme: "http", Host: proxyAddr}),
		TLSClientConfig:   tlsConfig,
		ForceAttemptHTTP2: true,
	}
	defer transport.CloseIdleConnections()
	resp, err := (&stdhttp.Client{Transport: transport, Timeout: timeout}).Get(rawURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), err
}

// wsEcho sends msg over a WebSocket and returns the echo.
func wsEcho(engine *fib.Engine, rawURL, msg string) (string, error) {
	messages := make(chan string, 1)
	conn, _, err := websocket.NewDialer(engine, websocket.DefaultDialerConfig()).Go(rawURL, nil,
		websocket.HandlerFuncs{Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
			messages <- string(data)
		}}).Wait()
	if err != nil {
		return "", err
	}
	defer conn.Close(websocket.CloseNormal, "done")
	if err := conn.WriteText(msg); err != nil {
		return "", err
	}
	select {
	case echo := <-messages:
		return echo, nil
	case <-time.After(timeout):
		return "", errors.New("timed out waiting for the echo")
	}
}

// sayHello calls the greeter's SayHello.
func sayHello(engine *fib.Engine, addr string) (string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithEngine(engine))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	reply, err := greeter.NewGreeterClient(conn).SayHello(ctx, &greeter.HelloRequest{Name: "fib"})
	if err != nil {
		return "", err
	}
	return reply.Message, nil
}

// arpcEcho calls arpc's /echo with msg.
func arpcEcho(engine *fib.Engine, addr, msg string) (string, error) {
	client, err := arpc.Dial(engine, "tcp", addr, timeout, nil)
	if err != nil {
		return "", err
	}
	defer client.Stop()
	var echo string
	err = client.Call("/echo", msg, &echo, timeout)
	return echo, err
}

// tcpEcho sends msg on a raw TCP connection, which no protocol above
// recognises, and returns what comes back once it is as long as msg.
func tcpEcho(engine *fib.Engine, addr, msg string) (string, error) {
	chunks := make(chan []byte, 64)
	dialed := make(chan error, 1)
	var conn *fib.Connection
	handler := fib.HandlerFuncs{
		// data is only valid during the call, so it is copied before it
		// leaves the handler.
		Data: func(_ *fib.Connection, data []byte) { chunks <- append([]byte(nil), data...) },
	}
	err := engine.DialWithHandler("tcp", addr, timeout, handler, func(c *fib.Connection, err error) {
		conn = c
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.Send([]byte(msg)); err != nil {
		return "", err
	}
	var echo []byte
	deadline := time.After(timeout)
	for len(echo) < len(msg) {
		select {
		case chunk := <-chunks:
			echo = append(echo, chunk...)
		case <-deadline:
			return "", errors.New("timed out waiting for the echo")
		}
	}
	return string(echo), nil
}
