//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"testing"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
)

// serve binds handler on a port of its own until the test ends.
func serve(t *testing.T, handler fib.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return addr.String()
}

func TestRunFetchesThroughTheProxy(t *testing.T) {
	web := serve(t, fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("hello through socks"))
	})))
	proxy := serve(t, socks.NewServer(socks.Config{}))
	var out bytes.Buffer
	if err := run([]string{"-proxy", "socks5h://" + proxy, "-url", "http://" + web + "/"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if out.String() != "HTTP/1.1 200 OK\nhello through socks" {
		t.Fatalf("output %q", out.String())
	}
}

func TestRunFailures(t *testing.T) {
	for _, args := range [][]string{
		{"-nope"},
		{"-proxy", "http://127.0.0.1:1"},
		{"-proxy", "socks5h://127.0.0.1:1", "-url", "http://bad host/"},
		{"-proxy", "socks5h://127.0.0.1:1", "-url", "http://127.0.0.1:2/"},
	} {
		if err := run(args, io.Discard); err == nil {
			t.Errorf("%q ran", args)
		}
	}
}
