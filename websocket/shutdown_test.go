//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	epollhttp "github.com/lesismal/fib/http"
)

// shutdownEngine runs handler on an engine the test shuts down, and returns
// it, its address, and what its Run returns.
func shutdownEngine(t *testing.T, handler fib.Handler) (*fib.Engine, string, chan error) {
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
	return engine, addr.String(), done
}

// shutdownAndWait shuts engine down and checks that Shutdown and Run both
// return without error, the connections having closed.
func shutdownAndWait(t *testing.T, engine *fib.Engine, done chan error, shut <-chan error) {
	t.Helper()
	select {
	case err := <-shut:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

func startShutdown(engine *fib.Engine) <-chan error {
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	return shut
}

// expectGoingAway reads a close frame with 1001 from reader.
func expectGoingAway(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	opcode, payload, err := readServerFrame(reader)
	if err != nil || opcode != Close || len(payload) < 2 || binary.BigEndian.Uint16(payload) != CloseGoingAway {
		t.Fatalf("frame %v %x %v, want Close with %d", opcode, payload, err, CloseGoingAway)
	}
}

// TestShutdownSendsGoingAway checks that a server shutting down sends its
// open WebSockets a close frame with 1001 and closes them, closes one still
// in its opening handshake, and that Shutdown returns once they are gone.
func TestShutdownSendsGoingAway(t *testing.T) {
	closed := make(chan closeReport, 1)
	engine, addr, done := shutdownEngine(t, NewHandler(HandlerFuncs{
		Close: func(_ *Connection, code uint16, reason string, _ error) { closed <- closeReport{code, reason} },
	}))
	conn, reader := dialWebSocket(t, addr)
	defer conn.Close()
	opening, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer opening.Close()
	// The opening connection is open on the server before Shutdown begins.
	_ = opening.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	_, _ = opening.Read(make([]byte, 1))

	shut := startShutdown(engine)
	expectGoingAway(t, reader)
	if _, err := reader.ReadByte(); err == nil {
		t.Error("the connection stayed open after its close frame")
	}
	_ = opening.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := opening.Read(make([]byte, 1)); err == nil {
		t.Errorf("the connection in its handshake read %d bytes; want it closed", n)
	}
	expectClose(t, closed, CloseGoingAway)
	shutdownAndWait(t, engine, done, shut)
}

// shutdownUpgradeServer serves WebSocket through Upgrade, over HTTP/1 and
// HTTP/2, on an engine the test shuts down.
func shutdownUpgradeServer(t *testing.T) (*fib.Engine, string, chan error, chan closeReport) {
	t.Helper()
	closed := make(chan closeReport, 4)
	ws := NewHandler(HandlerFuncs{
		Close: func(_ *Connection, code uint16, reason string, _ error) { closed <- closeReport{code, reason} },
	})
	engine, addr, done := shutdownEngine(t, epollhttp.NewHandler(epollhttp.HandlerFunc(func(c *epollhttp.Context) {
		if _, err := ws.Upgrade(c, nil); err != nil {
			t.Logf("upgrade: %v", err)
		}
	})))
	return engine, addr, done, closed
}

// TestShutdownSendsGoingAwayThroughHTTP1Upgrade checks the same of a
// WebSocket that an HTTP/1 request switched to with Upgrade.
func TestShutdownSendsGoingAwayThroughHTTP1Upgrade(t *testing.T) {
	engine, addr, done, closed := shutdownUpgradeServer(t)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	if _, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "+key+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil || resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %v %v", resp, err)
	}

	shut := startShutdown(engine)
	expectGoingAway(t, reader)
	if _, err := reader.ReadByte(); err == nil {
		t.Error("the connection stayed open after its close frame")
	}
	expectClose(t, closed, CloseGoingAway)
	shutdownAndWait(t, engine, done, shut)
}

// TestShutdownSendsGoingAwayThroughHTTP2ExtendedConnect checks it of a
// WebSocket on an HTTP/2 stream: the connection sends GOAWAY, the tunnel a
// close frame with 1001 and END_STREAM, and once the client ends its side
// too, the connection closes and Shutdown returns.
func TestShutdownSendsGoingAwayThroughHTTP2ExtendedConnect(t *testing.T) {
	engine, addr, done, closed := shutdownUpgradeServer(t)
	c, _ := dialH2(t, addr)
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
		":path", "/ws", ":authority", "test", "sec-websocket-version", "13")
	if f := c.next(1); f.typ != 0x1 {
		t.Fatalf("frame type %d, want HEADERS", f.typ)
	}

	shut := startShutdown(engine)
	goAway := false
	var data []byte
	ended := false
	for !ended {
		f := c.read()
		switch {
		case f.stream == 0 && f.typ == 0x7:
			goAway = true
		case f.stream == 1 && f.typ == 0x0:
			data = append(data, f.payload...)
			ended = f.flags&0x1 != 0
		case f.stream == 0:
		default:
			t.Fatalf("frame type %d on stream %d", f.typ, f.stream)
		}
	}
	if !goAway {
		t.Error("no GOAWAY before the stream ended")
	}
	if len(data) < 4 || Opcode(data[0]&0xf) != Close || binary.BigEndian.Uint16(data[2:]) != CloseGoingAway {
		t.Fatalf("stream carried %x, want a close frame with %d", data, CloseGoingAway)
	}
	c.write(h2TestFrameBytes(0x0, 0x1, 1, nil))
	expectClose(t, closed, CloseGoingAway)
	shutdownAndWait(t, engine, done, shut)
}
