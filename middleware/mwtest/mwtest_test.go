//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package mwtest

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
)

// fatalTB records the failure a helper reports, and ends the goroutine it
// was called on as testing.T.Fatal does, so that the helper goes no further.
type fatalTB struct {
	testing.TB
	msg string
}

func (tb *fatalTB) Helper() {}

func (tb *fatalTB) Fatal(args ...any) {
	tb.msg = fmt.Sprint(args...)
	runtime.Goexit()
}

func (tb *fatalTB) Fatalf(format string, args ...any) {
	tb.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// fails runs fn with a fatalTB, and returns what it failed with.
func fails(t *testing.T, fn func(tb testing.TB)) string {
	tb := &fatalTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
	return tb.msg
}

// TestDoFailures checks that Do fails the test, rather than returning, when
// it cannot build the request, when the request fails, and when the body is
// cut short.
func TestDoFailures(t *testing.T) {
	c := Clients(t)[0]
	if msg := fails(t, func(tb testing.TB) { c.Do(tb, "BAD METHOD", "http://127.0.0.1/", nil, "") }); msg == "" {
		t.Error("an invalid method did not fail")
	}

	// A listener that is closed at once refuses the request.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + ln.Addr().String()
	ln.Close()
	if msg := fails(t, func(tb testing.TB) { c.Do(tb, "GET", closed, nil, "") }); !strings.Contains(msg, "over HTTP/1.1") {
		t.Errorf("a refused request failed with %q", msg)
	}

	// A server that promises more body than it sends.
	ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 4096)
		_, _ = conn.Read(buf)
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort"))
		conn.Close()
	}()
	if msg := fails(t, func(tb testing.TB) { c.Do(tb, "GET", "http://"+ln.Addr().String(), nil, "") }); !strings.Contains(msg, "reading body") {
		t.Errorf("a short body failed with %q", msg)
	}
}
