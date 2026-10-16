//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
)

// serveArpc runs an arpc server with what register gives it until the test
// ends.
func serveArpc(t *testing.T, register func(h *arpc.Handler)) string {
	t.Helper()
	server := arpc.NewServer()
	register(server.Handler)
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: server})
	if err != nil {
		t.Fatal(err)
	}
	stop := example.Start(engine)
	t.Cleanup(func() { _ = stop() })
	return example.Addr(engine)
}

type math struct{}

func (math) Sum(_ context.Context, req *SumReq, rsp *SumRsp) { rsp.Sum = req.A + req.B }

// echo answers like the example server's /echo, except that it refuses fail.
func echo(fail string) func(*arpc.Context) {
	return func(ctx *arpc.Context) {
		var s string
		_ = ctx.Bind(&s)
		if s == fail {
			ctx.Error(errors.New("refused"))
			return
		}
		ctx.Write(s)
	}
}

func upper(s *arpc.Stream) {
	defer s.CloseSend()
	for {
		var msg string
		if s.Recv(&msg) != nil {
			return
		}
		_ = s.Send(strings.ToUpper(msg))
	}
}

func TestRunMakesEveryKindOfCall(t *testing.T) {
	addr := serveArpc(t, func(h *arpc.Handler) {
		h.Handle("/echo", echo(""), false)
		if err := h.Register("Math", math{}); err != nil {
			t.Fatal(err)
		}
		h.HandleStream("/upper", upper)
	})
	var out bytes.Buffer
	if err := run([]string{"-addr", addr, "-n", "2"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	want := "echo: hello from fib #1\necho: hello from fib #2\nasync echo: \"async\", <nil>\nMath.Sum(2, 3) = 5\nstream: FIB\nstream: ARPC\n"
	if out.String() != want {
		t.Fatalf("output %q", out.String())
	}
}

// Each call that fails ends the run with its error; an asynchronous call's
// error is printed, since its callback has nowhere else to put it.
func TestRunFailures(t *testing.T) {
	refusing := serveArpc(t, func(h *arpc.Handler) { h.Handle("/echo", echo("hello from fib #1"), false) })
	if err := run([]string{"-addr", refusing}, io.Discard); err == nil {
		t.Error("no error from a refused call")
	}

	noMath := serveArpc(t, func(h *arpc.Handler) { h.Handle("/echo", echo("async"), false) })
	var out bytes.Buffer
	if err := run([]string{"-addr", noMath, "-n", "0"}, &out); err == nil {
		t.Error("no error from a missing service")
	}
	if !strings.Contains(out.String(), "async echo: \"\", refused") {
		t.Errorf("output %q", out.String())
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
