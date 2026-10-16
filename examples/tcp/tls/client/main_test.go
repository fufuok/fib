//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/http/upload/uploadtest"
	fibtls "github.com/lesismal/fib/tls"
)

// serveTLS runs handler behind TLS on a free port until the test ends, and
// returns its address and the certificate file a client trusts it by.
func serveTLS(t *testing.T, handler fib.Handler) (addr, certFile string) {
	t.Helper()
	serverTLS, _, certFile := uploadtest.TLS(t)
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: fibtls.NewServer(serverTLS, handler)})
	if err != nil {
		t.Fatal(err)
	}
	local, err := engine.LocalAddr()
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
	return local.String(), certFile
}

func echoHandler() fib.Handler {
	return fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) { _ = c.Send(data) }}
}

// The client verifies the server against the certificate it is given, says
// which TLS it negotiated, and prints every echo.
func TestRunPrintsTheEchoes(t *testing.T) {
	addr, certFile := serveTLS(t, echoHandler())
	var out bytes.Buffer
	if err := run([]string{"-addr", addr, "-n", "2", "-ca", certFile}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if !strings.Contains(out.String(), "TLS TLS 1.3") || !strings.Contains(out.String(), "echo: hello from fib #2\n") {
		t.Fatalf("output %q", out.String())
	}
}

// A server that hangs up, one that never answers, none at all, and a missing
// certificate are each reported as an error.
func TestRunFailures(t *testing.T) {
	hangup, certFile := serveTLS(t, fib.HandlerFuncs{Data: func(c *fib.Connection, _ []byte) { c.Close() }})
	if err := run([]string{"-addr", hangup, "-ca", certFile}, io.Discard); err == nil {
		t.Error("no error from a server that hangs up")
	}

	old := echoTimeout
	defer func() { echoTimeout = old }()
	echoTimeout = 50 * time.Millisecond
	silent, certFile := serveTLS(t, fib.HandlerFuncs{})
	if err := run([]string{"-addr", silent, "-ca", certFile}, io.Discard); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("silent server: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-addr", refused, "-insecure"}, io.Discard); err == nil {
		t.Error("no error from an address nothing listens on")
	}
	if err := run([]string{"-ca", filepath.Join(t.TempDir(), "missing.pem")}, io.Discard); err == nil {
		t.Error("no error for a missing certificate")
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
