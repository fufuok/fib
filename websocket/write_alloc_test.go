//go:build (linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows) && !race

package websocket

import (
	"io"
	stdhttp "net/http"
	"testing"

	fib "github.com/lesismal/fib"
)

// A server frame's header is built on sendFrame's stack and written beside the
// payload, so WriteMessage allocates nothing. Every platform's two-part write
// has to keep it there: on Darwin and OpenBSD, a writev the compiler could not
// see into put the header and the iovec array on the heap, two allocations
// for every message to every connection, and on Windows sys.WSABufs did the
// same. The race detector's hooks in the syscall package let it escape anyway,
// so this does not run under it.
func TestServerWriteMessageAllocs(t *testing.T) {
	opened := make(chan *Connection, 1)
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, NewHandler(HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) { opened <- c },
	}))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()

	conn, reader := dialUpgraded(t, addr.String())
	defer conn.Close()
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	c := <-opened

	// Small enough that the socket never pushes back, which would queue the
	// unsent rest and count a pool's allocations instead.
	payload := make([]byte, 16)
	if err := c.WriteMessage(Binary, payload); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := c.WriteMessage(Binary, payload); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("WriteMessage allocated %v times a message", allocs)
	}
}
