//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

// serveEcho runs fib's WebSocket echo until the test ends.
func serveEcho(t *testing.T) string {
	t.Helper()
	config := fib.DefaultConfig()
	wsConfig := websocket.DefaultConfig()
	wsConfig.EnableCompression = true
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: websocket.NewHandlerWithConfig(wsConfig, websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) { _ = c.WriteMessage(opcode, data) },
	})})
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

// serveRaw is a WebSocket server written by hand, so that it can misbehave:
// after the opening handshake, each text message the client sends gets what
// answer makes of it (nil for nothing), and hangUp ends the connection
// instead.
func serveRaw(t *testing.T, answer func(string) []byte, hangUp bool) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go handleRaw(conn, answer, hangUp)
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return l.Addr().String()
}

func handleRaw(conn net.Conn, answer func(string) []byte, hangUp bool) {
	r := bufio.NewReader(conn)
	req, err := stdhttp.ReadRequest(r)
	if err != nil {
		return
	}
	sum := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		base64.StdEncoding.EncodeToString(sum[:]))
	for {
		var head [2]byte
		if _, err := io.ReadFull(r, head[:]); err != nil {
			return
		}
		n := int(head[1] & 0x7f)
		switch n {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(r, ext[:]); err != nil {
				return
			}
			n = int(binary.BigEndian.Uint16(ext[:]))
		case 127:
			return
		}
		var mask [4]byte
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
		if head[0]&0x0f != 1 {
			continue // a close, which this server ignores
		}
		if hangUp {
			_ = conn.Close()
			return
		}
		if reply := answer(string(payload)); reply != nil {
			_, _ = conn.Write(append([]byte{0x81, byte(len(reply))}, reply...))
		}
	}
}

func TestRunPrintsTheEchoes(t *testing.T) {
	addr := serveEcho(t)
	var out bytes.Buffer
	if err := run([]string{"-url", "ws://" + addr + "/ws", "-n", "2", "-compress"}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "connected: 101") || !strings.Contains(text, "echo: hello from fib #2\n") || !strings.Contains(text, "closed: code=1000") {
		t.Fatalf("output %q", text)
	}
}

// A server that hangs up, one that never answers, and none at all are each
// reported as an error.
func TestRunFailures(t *testing.T) {
	hangup := serveRaw(t, nil, true)
	if err := run([]string{"-url", "ws://" + hangup + "/ws"}, io.Discard); err == nil || !strings.Contains(err.Error(), "connection closed") {
		t.Errorf("a server that hangs up: %v", err)
	}

	old := echoTimeout
	defer func() { echoTimeout = old }()
	echoTimeout = 50 * time.Millisecond
	silent := serveRaw(t, func(string) []byte { return nil }, false)
	if err := run([]string{"-url", "ws://" + silent + "/ws"}, io.Discard); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("silent server: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	if err := run([]string{"-url", "ws://" + refused + "/ws"}, io.Discard); err == nil {
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
