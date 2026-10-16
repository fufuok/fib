//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// serveTCP runs handler on a free port until the test ends.
func serveTCP(t *testing.T, handler fib.Handler) string {
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

// The client prints every echo, which an echo server may split: this one
// sends each back a byte at a time.
func TestRunPrintsTheEchoes(t *testing.T) {
	addr := serveTCP(t, fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) {
		for i := range data {
			_ = c.Send(data[i : i+1])
		}
	}})
	var out bytes.Buffer
	if err := run([]string{"-addr", addr, "-n", "3"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	for i := 1; i <= 3; i++ {
		if !strings.Contains(out.String(), fmt.Sprintf("echo: hello from fib #%d\n", i)) {
			t.Fatalf("output %q", out.String())
		}
	}
}

// A server that hangs up, one that never answers, and none at all are each
// reported as an error.
func TestRunFailures(t *testing.T) {
	hangup := serveTCP(t, fib.HandlerFuncs{Data: func(c *fib.Connection, _ []byte) { c.Close() }})
	if err := run([]string{"-addr", hangup}, io.Discard); err == nil {
		t.Error("no error from a server that hangs up")
	}

	old := echoTimeout
	defer func() { echoTimeout = old }()
	echoTimeout = 50 * time.Millisecond
	silent := serveTCP(t, fib.HandlerFuncs{})
	if err := run([]string{"-addr", silent}, io.Discard); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("silent server: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-addr", refused}, io.Discard); err == nil {
		t.Error("no error from an address nothing listens on")
	}
	if err := run([]string{"-nope"}, io.Discard); err == nil {
		t.Error("accepted an unknown flag")
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
