//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// clientHandshakeConfigs are the client Configs the client handshake benchmark
// runs: the server's chain verified, verified with each of the callbacks that
// check the server, and not verified at all.
func clientHandshakeConfigs(pool *x509.CertPool) []struct {
	name   string
	config *stdtls.Config
} {
	verify := &stdtls.Config{RootCAs: pool, ServerName: "bench.test", SessionTicketsDisabled: true}
	peer := verify.Clone()
	peer.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { return nil }
	conn := verify.Clone()
	conn.VerifyConnection = func(stdtls.ConnectionState) error { return nil }
	insecure := verify.Clone()
	insecure.InsecureSkipVerify = true
	return []struct {
		name   string
		config *stdtls.Config
	}{
		{"verify", verify},
		{"VerifyPeerCertificate", peer},
		{"VerifyConnection", conn},
		{"insecure", insecure},
	}
}

// echoOnce is a client that sends a byte once its connection is up and closes
// it, with a reset, once the byte comes back.
type echoOnce struct{ done chan error }

func (e *echoOnce) OnOpen(*fib.Connection)                 {}
func (e *echoOnce) OnPriorityData(*fib.Connection, []byte) {}
func (e *echoOnce) OnData(c *fib.Connection, _ []byte) {
	// A reset rather than a FIN leaves no TIME_WAIT behind, which would use
	// up the local ports.
	_ = c.SetLinger(0)
	_ = c.Close()
	e.done <- nil
}
func (e *echoOnce) OnClose(_ *fib.Connection, err error) {
	if err == nil {
		err = errors.New("closed before the echo")
	}
	select {
	case e.done <- err:
	default:
	}
}

// BenchmarkClientHandshake measures full client handshakes of the state
// machine, TLS 1.3 and 1.2: a client of a fib engine dials a fib server, has a
// byte echoed and closes, so every operation is one handshake. "serial" is one
// at a time, the latency of a handshake; "parallel" has GOMAXPROCS in flight.
// Both ends are in this process, so the figures are relative.
func BenchmarkClientHandshake(b *testing.B) {
	ca := newTestCA(b, "bench CA")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{ca.issue(b, "ecdsa-p256", "bench.test", false)}}
	_, addr := startEchoServer(b, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	client, _ := startEchoServer(b, fib.DefaultConfig(), nil)
	handshake := func(b *testing.B, config *stdtls.Config) {
		e := &echoOnce{done: make(chan error, 2)}
		err := Dial(client, "tcp", addr, 10*time.Second, config, e, func(c *fib.Connection, err error) {
			if err == nil {
				_ = c.Send([]byte{1})
			}
		})
		if err == nil {
			err = <-e.done
		}
		if err != nil {
			b.Error(err)
		}
	}
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		for _, tc := range clientHandshakeConfigs(ca.pool) {
			config := tc.config.Clone()
			config.MinVersion, config.MaxVersion = version, version
			name := stdtls.VersionName(version) + "/" + tc.name
			b.Run(name+"/serial", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					handshake(b, config)
				}
			})
			b.Run(name+"/parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						handshake(b, config)
					}
				})
			})
		}
	}
}
