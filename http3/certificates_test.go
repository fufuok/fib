//go:build linux || darwin || windows

package http3

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	stdhttp "net/http"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// A server whose certificate GetCertificate picks, taking its time, and that
// asks clients for theirs, serves HTTP/3 with the handshake that takes no
// goroutine: the QUIC connection is woken once GetCertificate has returned on
// a worker, and the request sees the certificate the client sent, which a
// client handshake without a goroutine sent from its Config.
func TestGetCertificateAndClientCertificates(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	cert := serverTLS.Certificates[0]
	var calls atomic.Int64
	serverTLS.Certificates = nil
	serverTLS.GetCertificate = func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		calls.Add(1)
		if hello.ServerName != "localhost" {
			return nil, fmt.Errorf("hello for %q", hello.ServerName)
		}
		if strings.Contains(string(debug.Stack()), "HandleData") {
			return nil, errors.New("GetCertificate was called on the goroutine that handles the connection's data")
		}
		time.Sleep(20 * time.Millisecond)
		return &cert, nil
	}
	// The test certificate is for servers, so it is not verified as a
	// client's.
	serverTLS.ClientAuth = stdtls.RequireAnyClientCert
	if fibtls.NewQUICServer(serverTLS) == nil {
		t.Fatal("the server Config does not qualify for the handshake without a goroutine")
	}

	fc := fib.DefaultConfig()
	fc.Network, fc.Addr = "udp", "127.0.0.1:0"
	engine, err := fib.Bind(fc, NewHandlerWithConfig(Config{TLSConfig: serverTLS}, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		peer := "none"
		if c.Request.TLS != nil && len(c.Request.TLS.PeerCertificates) == 1 {
			peer = c.Request.TLS.PeerCertificates[0].Subject.CommonName
		}
		_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Body: []byte(peer)})
	})))
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("https://localhost:%d/", addr.Port)

	client := newClient(t, func(c *ClientConfig) {
		c.TLSConfig.Certificates = []stdtls.Certificate{cert}
		if fibtls.NewQUICClient(c.TLSConfig) == nil {
			t.Fatal("the client Config does not qualify for the handshake without a goroutine")
		}
	})
	for i := 0; i < 3; i++ {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if body := readBody(t, resp); body != "fib test" {
			t.Fatalf("the server saw the client as %q", body)
		}
	}
	if calls.Load() == 0 {
		t.Fatal("GetCertificate was never called")
	}
}
