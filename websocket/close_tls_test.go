//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// TestCloseAnsweredOverTLSWithFramesBehind sends a Close and then a Ping as
// TLS records of their own, as Autobahn's case 7.1.3 does: the server
// answers the Close, ignores what follows it, and ends the stream cleanly,
// with its Close frame ahead of close_notify.
func TestCloseAnsweredOverTLSWithFramesBehind(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	server, err := fib.Bind(engineConfig, fibtls.NewServer(serverTLS, NewHandler(echoServerHandler())))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	for i := 0; i < 10; i++ {
		conn, err := tls.Dial("tcp", addr.String(), clientTLS)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
		reader := bufio.NewReader(conn)
		resp, err := stdhttp.ReadResponse(reader, nil)
		if err != nil || resp.StatusCode != stdhttp.StatusSwitchingProtocols {
			t.Fatalf("upgrade: %v %v", resp, err)
		}
		// Two writes, so two records: a masked Close 1000, then a Ping.
		if _, err := conn.Write([]byte{0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe8}); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte{0x89, 0x80, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		got, err := io.ReadAll(reader)
		_ = conn.Close()
		if len(got) < 4 || got[0] != 0x88 || got[2] != 0x03 || got[3] != 0xe8 {
			t.Fatalf("run %d: the server sent %x (%v), want its Close 1000 first", i, got, err)
		}
		if err != nil {
			t.Fatalf("run %d: the stream ended with %v after the Close, want close_notify", i, err)
		}
	}
}
