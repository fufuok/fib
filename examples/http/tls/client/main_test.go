//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/http/upload/uploadtest"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
)

// serveEcho runs an HTTPS echo, which offers HTTP/2, until the test ends, and
// returns its address and the certificate file a client trusts it by.
func serveEcho(t *testing.T) (string, string) {
	t.Helper()
	serverTLS, _, certFile := uploadtest.TLS(t)
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	})))})
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
	return addr.String(), certFile
}

// The client speaks HTTP/2 when the server offers it, and HTTP/1.1 with
// -http1.
func TestRunPrintsTheReplies(t *testing.T) {
	addr, certFile := serveEcho(t)
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, "HTTP/2.0 200 OK: hello from fib #1\n"},
		{[]string{"-http1"}, "HTTP/1.1 200 OK: hello from fib #1\n"},
	} {
		var out bytes.Buffer
		args := append([]string{"-url", "https://" + addr + "/echo", "-n", "1", "-ca", certFile}, c.args...)
		if err := run(args, &out); err != nil {
			t.Fatalf("%v: %s", err, out.String())
		}
		if out.String() != c.want {
			t.Fatalf("output %q, want %q", out.String(), c.want)
		}
	}
}

// A URL that does not parse, a server that is not there, and a certificate
// that is not there are reported.
func TestRunFailures(t *testing.T) {
	if err := run([]string{"-url", "https://bad host/", "-insecure"}, io.Discard); err == nil {
		t.Error("no error for a URL that does not parse")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-url", "https://" + refused + "/echo", "-insecure"}, io.Discard); err == nil {
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
