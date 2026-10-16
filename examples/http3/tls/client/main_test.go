//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/http/upload/uploadtest"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

// serveEcho runs an HTTP/3 echo until the test ends, and returns its address
// and the certificate file a client trusts it by.
func serveEcho(t *testing.T) (string, string) {
	t.Helper()
	serverTLS, _, certFile := uploadtest.TLS(t)
	config := fib.DefaultConfig()
	config.Network = "udp"
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: http3.NewHandler(serverTLS, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	}))})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalUDPAddr()
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

func TestRunPrintsTheReplies(t *testing.T) {
	addr, certFile := serveEcho(t)
	var out bytes.Buffer
	if err := run([]string{"-url", "https://" + addr + "/echo", "-n", "2", "-ca", certFile}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if out.String() != "HTTP/3.0 200 OK: hello from fib #1\nHTTP/3.0 200 OK: hello from fib #2\n" {
		t.Fatalf("output %q", out.String())
	}
}

// A URL that does not parse, one HTTP/3 cannot reach, and a certificate that
// is not there are reported.
func TestRunFailures(t *testing.T) {
	if err := run([]string{"-url", "https://bad host/", "-insecure"}, io.Discard); err == nil {
		t.Error("no error for a URL that does not parse")
	}
	if err := run([]string{"-url", "http://127.0.0.1:1/echo", "-insecure"}, io.Discard); err == nil {
		t.Error("no error for a URL that is not https")
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
