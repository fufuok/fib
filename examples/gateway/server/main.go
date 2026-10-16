//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command server is a reverse proxy and gateway. It accepts HTTP/1.1, HTTP/2,
// HTTP/3 and WebSocket requests and forwards them to upstream servers by path
// prefix, without blocking an engine worker: request bodies, upstream dials,
// upstream responses and WebSocket messages are all handled in callbacks.
//
//	go run ./examples/gateway/upstream
//	go run ./examples/gateway/server
//	go run ./examples/gateway/client
//
// It listens on one address for both TLS transports, as the HTTP/3 example
// does: TCP carries HTTPS, which is HTTP/2 for clients that choose it through
// ALPN and HTTP/1.1 for the rest, and UDP carries HTTP/3. The responses on TCP
// carry Alt-Svc, which is how a browser learns that it can switch. With
// -plain-addr it also serves cleartext HTTP/1.1 and h2c, and ws://.
//
// A route is "prefix=target", where target is http:// for an HTTP/1.1
// upstream, https:// for one that speaks HTTP/2 if it can, or h3:// for an
// HTTP/3 upstream. By default it forwards to the upstream example, with the
// same request reaching it over each protocol:
//
//	/h1 -> http://127.0.0.1:9000     HTTP/1.1
//	/h2 -> https://127.0.0.1:9443    HTTP/2
//	/h3 -> h3://127.0.0.1:9443       HTTP/3
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the gateway's client looks for it. -ca names the certificate
// of the upstreams, which is the upstream example's.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/server/proxy"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
)

// routeFlags collects repeated -route flags.
type routeFlags []proxy.Route

func (r *routeFlags) String() string { return fmt.Sprint(len(*r), " routes") }

func (r *routeFlags) Set(spec string) error {
	route, err := proxy.ParseRoute(spec)
	if err != nil {
		return err
	}
	*r = append(*r, route)
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

// run serves until ctx ends. ready, if not nil, is told the TLS address once
// the gateway is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:8446", "listen address: TCP for HTTPS (HTTP/2 and HTTP/1.1), UDP for HTTP/3")
	plainAddr := flags.String("plain-addr", "127.0.0.1:8081", "listen address for cleartext HTTP/1.1, h2c and ws://; empty to disable")
	timeout := flags.Duration("timeout", 5*time.Minute, "longest an upstream request may take, body included; 0 for no limit")
	subprotocols := flags.String("subprotocols", "chat,superchat", "WebSocket subprotocols to accept from clients and ask the upstream for")
	maxBody := flags.Int64("max-request-body", 64<<20, "largest request body to forward")
	readHeaderTimeout := flags.Duration("read-header-timeout", 10*time.Second, "longest a request header may take to arrive; 0 for no limit")
	readBodyTimeout := flags.Duration("read-body-timeout", 0, "longest a request body may take to arrive; 0 for no limit")
	idleTimeout := flags.Duration("idle-timeout", 2*time.Minute, "longest a kept-alive connection may sit idle; 0 for no limit")
	maxBodyHold := flags.Duration("max-body-hold", 30*time.Second, "longest an upload may wait for an upstream that stopped taking it")
	var routes routeFlags
	flags.Var(&routes, "route", "prefix=target, repeatable (default: /h1, /h2 and /h3 to the upstream example)")
	serverCerts := certs.ServerFlagsWithOut(flags, certs.GatewayCertFile)
	upstreamCerts := certs.ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	if len(routes) == 0 {
		for _, spec := range []string{
			"/h1=http://127.0.0.1:9000",
			"/h2=https://127.0.0.1:9443",
			"/h3=h3://127.0.0.1:9443",
		} {
			route, _ := proxy.ParseRoute(spec)
			routes = append(routes, route)
		}
	}
	tlsConfig, err := serverCerts.Config()
	if err != nil {
		return err
	}
	upstreamTLS, err := upstreamCerts.Config()
	if err != nil {
		return err
	}

	gateway, err := proxy.New(proxy.Config{
		Routes:         routes,
		TLSConfig:      upstreamTLS,
		Timeout:        *timeout,
		MaxRequestBody: *maxBody,
		Subprotocols:   strings.FieldsFunc(*subprotocols, func(r rune) bool { return r == ',' || r == ' ' }),
		Logf:           log.Printf,

		ReadHeaderTimeout: *readHeaderTimeout,
		ReadBodyTimeout:   *readBodyTimeout,
		IdleTimeout:       *idleTimeout,
		MaxBodyHold:       *maxBodyHold,
	})
	if err != nil {
		return err
	}
	engines, err := newEngines(gateway, tlsConfig, *addr, *plainAddr)
	if err != nil {
		return err
	}
	// HTTP/3 is on the port the HTTPS engine got, which is what Alt-Svc
	// announces.
	tlsAddr, _ := engines[0].LocalAddr()
	gateway.SetAltSvc(http3.AltSvc(tlsAddr.Port))
	// The upstream clients dial through the first engine, the HTTPS one.
	gateway.Attach(engines[0])
	plain := ""
	if len(engines) > 2 {
		plain = example.Addr(engines[2])
	}
	return serve(ctx, gateway, engines, out, ready,
		fmt.Sprintf("gateway listening on https://%s (HTTP/2, HTTP/1.1; HTTP/3 on UDP)%s", tlsAddr, plainBanner(plain)))
}

func plainBanner(plainAddr string) string {
	if plainAddr == "" {
		return ""
	}
	return fmt.Sprintf(", http://%s (HTTP/1.1, h2c, ws://)", plainAddr)
}

// newEngines binds the gateway's listeners: the HTTPS and HTTP/3 pair on addr,
// and the cleartext one on plainAddr if it is not empty. They all serve the
// gateway's one handler.
func newEngines(gateway *proxy.Gateway, tlsConfig *tls.Config, addr, plainAddr string) ([]*fib.Engine, error) {
	handler := gateway.Handler()
	httpConfig := gateway.HTTPConfig()

	// HTTPS: fibtls decrypts in front of the HTTP handler, which serves HTTP/2
	// and HTTP/1.1 alike once ConfigureTLS has offered h2 through ALPN.
	tcpConfig := fib.DefaultConfig()
	tcp, err := fib.New(tcpConfig, fib.Binding{Addr: addr, Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandlerWithConfig(httpConfig, handler))})
	if err != nil {
		return nil, err
	}
	engines := []*fib.Engine{tcp}

	// HTTP/3: a UDP engine whose handler is http3's, on the port the HTTPS
	// one got, which is addr's own unless that was 0.
	host, _, _ := net.SplitHostPort(addr)
	tcpAddr, _ := tcp.LocalAddr()
	udpConfig := fib.DefaultConfig()
	udpConfig.Network = "udp"
	udp, err := fib.New(udpConfig, fib.Binding{Addr: net.JoinHostPort(host, strconv.Itoa(tcpAddr.Port)), Handler: http3.NewHandlerWithConfig(gateway.HTTP3Config(tlsConfig), handler)})
	if err != nil {
		closeAll(engines)
		return nil, err
	}
	engines = append(engines, udp)

	if plainAddr != "" {
		plainConfig := fib.DefaultConfig()
		plain, err := fib.New(plainConfig, fib.Binding{Addr: plainAddr, Handler: fibhttp.NewHandlerWithConfig(httpConfig, handler)})
		if err != nil {
			closeAll(engines)
			return nil, err
		}
		engines = append(engines, plain)
	}
	return engines, nil
}

func closeAll(engines []*fib.Engine) {
	for _, engine := range engines {
		_ = engine.Close()
	}
}

// serve runs the engines until ctx ends, or one of them stops on its own,
// which it reports. Then it closes the upstream clients, which has to be done
// before the engines.
func serve(ctx context.Context, gateway *proxy.Gateway, engines []*fib.Engine, out io.Writer, ready func(addr string), banner string) error {
	done := make(chan error, len(engines))
	for _, engine := range engines {
		go func() { done <- engine.Run() }()
	}
	fmt.Fprintln(out, banner)
	if ready != nil {
		ready(example.Addr(engines[0]))
	}
	running := len(engines)
	var failed error
	select {
	case <-ctx.Done():
	case err := <-done:
		running--
		if err == nil {
			err = errors.New("stopped")
		}
		failed = fmt.Errorf("an engine stopped serving: %w", err)
	}
	gateway.Close()
	for _, engine := range engines {
		engine.Stop()
	}
	for ; running > 0; running-- {
		<-done
	}
	closeAll(engines)
	return failed
}
