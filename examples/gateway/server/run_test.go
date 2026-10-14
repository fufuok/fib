//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/server/proxy"
	"github.com/lesismal/fib/examples/gateway/upstream/backend"
	"github.com/lesismal/fib/tlstest"
)

// run starts the gateway the way the command does, on ports the system
// chooses, forwards a request to the route it was given, announces HTTP/3 on
// the port it got, and stops when its context ends.
func TestRunForwardsUntilCancelled(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := backend.NewEngines(serverTLS, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	start(t, upstream...)

	certOut := filepath.Join(t.TempDir(), "gateway.pem")
	args := []string{
		"-addr", "127.0.0.1:0", "-plain-addr", "127.0.0.1:0", "-cert-out", certOut, "-insecure",
		"-route", "/b=http://" + example.Addr(upstream[2]), "-subprotocols", "chat superchat", "-timeout", "30s",
	}
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, args, &out, func(addr string) { addrs <- addr }) }()
	var addr string
	select {
	case addr = <-addrs:
	case err := <-done:
		t.Fatalf("did not start: %v", err)
	}

	pem, err := os.ReadFile(certOut)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	transport := &stdhttp.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 30 * time.Second}
	resp, err := client.Post("https://"+addr+"/b/echo", "text/plain", strings.NewReader("through"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	_, port, _ := strings.Cut(addr, ":")
	if resp.StatusCode != 200 || !strings.Contains(string(body), "body: through") || !strings.Contains(resp.Header.Get("Alt-Svc"), ":"+port) {
		t.Fatalf("%d %q, Alt-Svc %q", resp.StatusCode, body, resp.Header.Get("Alt-Svc"))
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "gateway listening on https://"+addr) || !strings.Contains(text, "(HTTP/1.1, h2c, ws://)") {
		t.Fatalf("output %q", text)
	}

	// With the default routes and no cleartext listener, and nothing to serve.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	out.Reset()
	if err := run(ctx2, []string{"-addr", "127.0.0.1:0", "-plain-addr", "", "-cert-out", certOut, "-insecure"}, &out, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "ws://") {
		t.Fatalf("a cleartext listener announced: %q", out.String())
	}
}

func TestRunStartupErrors(t *testing.T) {
	ctx := context.Background()
	certOut := filepath.Join(t.TempDir(), "gateway.pem")
	for _, args := range [][]string{
		{"-nope"},
		{"-route", "nonsense"},
		{"-cert", "missing.pem", "-key", "missing.key"},
		{"-cert-out", certOut, "-ca", filepath.Join(t.TempDir(), "missing.pem")},
		{"-cert-out", certOut, "-insecure", "-addr", "not an address"},
		{"-cert-out", certOut, "-insecure", "-addr", "127.0.0.1:0", "-plain-addr", "not an address"},
	} {
		if err := run(ctx, args, io.Discard, nil); err == nil {
			t.Errorf("%q: started", args)
		}
	}
}

// An engine that stops serving on its own takes the gateway down with it, and
// says so.
func TestServeReportsAnEngineThatStops(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	route, err := proxy.ParseRoute("/=http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := proxy.New(proxy.Config{Routes: []proxy.Route{route}})
	if err != nil {
		t.Fatal(err)
	}
	engines, err := newEngines(gateway, serverTLS, "127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	gateway.Attach(engines[0])
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serve(context.Background(), gateway, engines, io.Discard, func(string) { close(ready) }, "banner")
	}()
	<-ready
	engines[1].Stop()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "an engine stopped serving") {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not return")
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
