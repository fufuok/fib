//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

// serveEcho runs an HTTP echo until the test ends.
func serveEcho(t *testing.T) string {
	t.Helper()
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	}))})
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

func TestRunPrintsTheReplies(t *testing.T) {
	addr := serveEcho(t)
	var out bytes.Buffer
	if err := run([]string{"-url", "http://" + addr + "/echo", "-n", "2"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if out.String() != "200 OK: hello from fib #1\n200 OK: hello from fib #2\n" {
		t.Fatalf("output %q", out.String())
	}
}

// A URL that does not parse and a server that is not there are reported.
func TestRunFailures(t *testing.T) {
	if err := run([]string{"-url", "http://bad host/"}, io.Discard); err == nil {
		t.Error("no error for a URL that does not parse")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-url", "http://" + refused + "/echo"}, io.Discard); err == nil {
		t.Error("no error from an address nothing listens on")
	}
	if err := run([]string{"-nope"}, io.Discard); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("unknown flag: %v", err)
	}
}

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"client", "-nope"}
	main()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
