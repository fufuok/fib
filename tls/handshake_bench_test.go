//go:build linux || darwin || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// serverHandshakeConfigs are the server Configs the handshake benchmarks run:
// one that holds its certificate, one that picks it with GetCertificate, and
// one that also asks the client for a certificate of its own. Before the state
// machines took the last two, they ran in crypto/tls on a worker apiece.
func serverHandshakeConfigs(b *testing.B) []struct {
	name   string
	server *stdtls.Config
	client *stdtls.Config
} {
	ca := newTestCA(b, "bench CA")
	serverCert := ca.issue(b, "ecdsa-p256", "bench.test", false)
	clientCert := ca.issue(b, "ecdsa-p256", "client", true)
	client := &stdtls.Config{RootCAs: ca.pool, ServerName: "bench.test"}
	withClientCert := client.Clone()
	withClientCert.Certificates = []stdtls.Certificate{clientCert}
	return []struct {
		name   string
		server *stdtls.Config
		client *stdtls.Config
	}{
		{"certificates", &stdtls.Config{Certificates: []stdtls.Certificate{serverCert}}, client},
		{"GetCertificate", &stdtls.Config{GetCertificate: func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
			return &serverCert, nil
		}}, client},
		{"mTLS", &stdtls.Config{Certificates: []stdtls.Certificate{serverCert},
			ClientAuth: stdtls.RequireAndVerifyClientCert, ClientCAs: ca.pool}, withClientCert},
	}
}

// BenchmarkServerHandshake measures full handshakes against a fib server: a
// crypto/tls client connects, has a byte echoed and closes, so every
// operation is one handshake, with session resumption out of the way. It
// compares the state machine with crypto/tls on a handshake worker for each
// Config. The clients are in this process, so the figures are relative.
func BenchmarkServerHandshake(b *testing.B) {
	for _, config := range serverHandshakeConfigs(b) {
		for _, mode := range []struct {
			name     string
			blocking bool
		}{{"state machine", false}, {"crypto/tls", true}} {
			b.Run(config.name+"/"+mode.name, func(b *testing.B) {
				handler := NewServer(config.server, echoHandler())
				handler.Blocking = mode.blocking
				_, addr := startEchoServer(b, fib.DefaultConfig(), handler)
				client := config.client.Clone()
				client.SessionTicketsDisabled = true
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					one := make([]byte, 1)
					for pb.Next() {
						raw, err := net.Dial("tcp", addr)
						if err != nil {
							b.Error(err)
							return
						}
						// A reset rather than a FIN leaves no TIME_WAIT
						// behind, which would use up the local ports.
						_ = raw.(*net.TCPConn).SetLinger(0)
						conn := stdtls.Client(raw, client)
						_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
						if _, err = conn.Write(one); err == nil {
							_, err = io.ReadFull(conn, one)
						}
						conn.Close()
						if err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

// stallAfterHello passes the client's first write, its ClientHello, and drops
// the rest, so that the server answers and then waits for a Finished that
// never comes.
type stallAfterHello struct {
	net.Conn
	wrote bool
}

func (s *stallAfterHello) Write(p []byte) (int, error) {
	if s.wrote {
		return len(p), nil
	}
	s.wrote = true
	return s.Conn.Write(p)
}

// BenchmarkServerStalledHandshakes measures what a server's handshakes cost
// while their clients do not finish them: each client sends its ClientHello,
// reads the server's flight and says nothing more. The metric is the
// goroutines parked in a server handshake while they wait, per handshake:
// counted from their stacks, since the handshake pool keeps idle workers
// from round to round, which a count of all goroutines would take in.
func BenchmarkServerStalledHandshakes(b *testing.B) {
	const handshakes = 400
	for _, config := range serverHandshakeConfigs(b) {
		for _, mode := range []struct {
			name     string
			blocking bool
		}{{"state machine", false}, {"crypto/tls", true}} {
			b.Run(config.name+"/"+mode.name, func(b *testing.B) {
				handler := NewServer(config.server, echoHandler())
				handler.Blocking = mode.blocking
				handler.HandshakeTimeout = time.Minute
				_, addr := startEchoServer(b, fib.DefaultConfig(), handler)
				var most int
				for i := 0; i < b.N; i++ {
					conns := make([]net.Conn, 0, handshakes)
					for j := 0; j < handshakes; j++ {
						raw, err := net.Dial("tcp", addr)
						if err != nil {
							b.Fatal(err)
						}
						_ = raw.(*net.TCPConn).SetLinger(0)
						conns = append(conns, raw)
						tc := stdtls.Client(&stallAfterHello{Conn: raw}, config.client)
						_ = tc.SetDeadline(time.Now().Add(10 * time.Second))
						if err := tc.Handshake(); err != nil {
							b.Fatal(err)
						}
					}
					time.Sleep(200 * time.Millisecond)
					if n := goroutinesIn("crypto/tls.(*Conn).serverHandshake"); n > most {
						most = n
					}
					for _, c := range conns {
						c.Close()
					}
				}
				b.ReportMetric(float64(most)/handshakes, "goroutines/handshake")
			})
		}
	}
}

// goroutinesIn counts the goroutines with fn on their stack.
func goroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.Contains(g, []byte(fn)) {
			count++
		}
	}
	return count
}
