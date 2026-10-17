//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package example

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

func bindForTest(t *testing.T, network string) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Network = network
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: fib.HandlerFuncs{
		Data: func(c *fib.Connection, data []byte) { _ = c.Send(data) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// Serve prints the banner with the address the engine got, reports that
// address, serves on it until the context ends, and leaves the engines closed.
func TestServeRunsEnginesUntilTheContextEnds(t *testing.T) {
	tcp, udp := bindForTest(t, "tcp"), bindForTest(t, "udp")
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, &out, func(addr string) { addrs <- addr }, "serving on %s", tcp, udp)
	}()
	addr := <-addrs
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 4)
	if _, err := conn.Read(reply); err != nil || string(reply) != "ping" {
		t.Fatalf("echo %q, %v", reply, err)
	}
	_ = conn.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if out.String() != "serving on "+addr+"\n" {
		t.Fatalf("output %q", out.String())
	}
}

// A UDP-only engine reports its UDP address, and one with no listener at all
// still prints something.
func TestAddrOfEachKindOfEngine(t *testing.T) {
	udp := bindForTest(t, "udp")
	defer udp.Close()
	want, err := udp.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	if got := Addr(udp); got != want.String() {
		t.Fatalf("Addr %q, want %q", got, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, &bytes.Buffer{}, nil, "%s", bindForTest(t, "udp")); err != nil {
		t.Fatal(err)
	}

	engine, stop := ClientEngine()
	defer stop()
	if got := Addr(engine); got != "<nil>" {
		t.Fatalf("Addr of a client engine %q", got)
	}
}

func TestMessage(t *testing.T) {
	if got := Message(3); got != "hello from fib #3" {
		t.Fatalf("Message(3) = %q", got)
	}
}

// Fatal exits with a status a shell sees as failure, saying why on stderr.
func TestFatalPrintsAndExits(t *testing.T) {
	if os.Getenv("EXAMPLE_FATAL") == "1" {
		Fatal(errors.New("exit now"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalPrintsAndExits$")
	cmd.Env = append(os.Environ(), "EXAMPLE_FATAL=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "error: exit now") {
		t.Fatalf("child: %v %q", err, out)
	}
}
