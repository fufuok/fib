//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// serveUDP answers each datagram it gets with what reply makes of it, until
// the test ends.
func serveUDP(t *testing.T, reply func([]byte) []byte) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64<<10)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if answer := reply(buf[:n]); answer != nil {
				_, _ = conn.WriteTo(answer, from)
			}
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-done
	})
	return conn.LocalAddr().String()
}

func TestRunPrintsTheEchoes(t *testing.T) {
	addr := serveUDP(t, func(b []byte) []byte { return b })
	var out bytes.Buffer
	if err := run([]string{"-addr", addr, "-n", "3"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if !strings.Contains(out.String(), "echo: hello from fib #3\n") {
		t.Fatalf("output %q", out.String())
	}
}

// A server that never answers is reported, after a while, as an error; so is
// a port nothing listens on, which on most systems the client hears of at
// once as a refusal, and otherwise as the same silence.
func TestRunFailures(t *testing.T) {
	old := echoTimeout
	defer func() { echoTimeout = old }()
	echoTimeout = 100 * time.Millisecond
	silent := serveUDP(t, func([]byte) []byte { return nil })
	if err := run([]string{"-addr", silent}, io.Discard); err == nil || !strings.Contains(err.Error(), "no echo") {
		t.Errorf("silent server: %v", err)
	}

	echoTimeout = 5 * time.Second
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := conn.LocalAddr().String()
	_ = conn.Close()
	if err := run([]string{"-addr", closed, "-n", "3"}, io.Discard); err == nil {
		t.Error("no error from a port nothing listens on")
	}
	if err := run([]string{"-addr", "not an address"}, io.Discard); err == nil {
		t.Error("dialed a bad address")
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
