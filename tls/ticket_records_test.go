//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"crypto/ed25519"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"slices"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// longClientChain is a client certificate chain of more than size bytes: a
// few copies of a self-signed Ed25519 certificate made long with names.
func longClientChain(t *testing.T, size int) *stdtls.Certificate {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	for i := 0; i < 100; i++ {
		template.DNSNames = append(template.DNSNames, fmt.Sprintf("name-%03d.client.example", i))
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return &stdtls.Certificate{Certificate: slices.Repeat([][]byte{der}, size/len(der)+1), PrivateKey: key}
}

// ticketEcho is earlyEcho that also reports the server's connection state for
// the data that follows a handshake.
type ticketEcho struct {
	earlyEcho
	states chan stdtls.ConnectionState
}

func (e ticketEcho) OnData(c *fib.Connection, b []byte) {
	if state, ok := ConnectionState(c); ok {
		select {
		case e.states <- state:
		default:
		}
	}
	e.earlyEcho.OnData(c, b)
}

// A ticket carries the client's certificates, so the ticket of a client whose
// chain is longer than a record is too, and goes across records: both when the
// handshake seals it and when the layer does, after early data. Our client and
// crypto/tls's take it and resume from it.
func TestTicketLongerThanRecord(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.ClientAuth = stdtls.RequireAnyClientCert
	clientConfig.MinVersion = stdtls.VersionTLS13
	chain := longClientChain(t, 2*maxPlaintext)
	clientConfig.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) { return chain, nil }

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	go engine.Run()
	t.Cleanup(engine.Stop)

	// waitTicket waits for the cache to hold a ticket other than old, and
	// checks that it is longer than a record.
	waitTicket := func(t *testing.T, cache QUICSessionCache, old *QUICSession) *QUICSession {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if s := cache.Get("localhost"); s != nil && s != old {
				if len(s.ticket) <= maxPlaintext {
					t.Fatalf("a ticket of %d bytes, want more than a record", len(s.ticket))
				}
				return s
			}
		}
		t.Fatal("no ticket")
		return nil
	}

	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprintf("early data %v", early), func(t *testing.T) {
			handler := ticketEcho{newEarlyEcho(), make(chan stdtls.ConnectionState, 4)}
			server := NewServer(serverConfig, handler)
			server.EarlyData = early
			_, addr := startEchoServer(t, fib.DefaultConfig(), server)
			cache := NewQUICSessionCache(4)
			nextState := func(t *testing.T) stdtls.ConnectionState {
				t.Helper()
				select {
				case s := <-handler.states:
					return s
				case <-time.After(5 * time.Second):
					t.Fatal("the server reported no connection state")
				}
				return stdtls.ConnectionState{}
			}

			if got := resumingClient(t, engine, addr, clientConfig, cache, early, "first"); got != "data:first" {
				t.Fatalf("first: %q", got)
			}
			if s := nextState(t); s.DidResume || len(s.PeerCertificates) != len(chain.Certificate) {
				t.Fatalf("first: resumed %v, %d peer certificates", s.DidResume, len(s.PeerCertificates))
			}
			// The handshake sealed this ticket: no early data came before it.
			ticket := waitTicket(t, cache, nil)

			if !early {
				if got := resumingClient(t, engine, addr, clientConfig, cache, false, "second"); got != "data:second" {
					t.Fatalf("resumed: %q", got)
				}
				if s := nextState(t); !s.DidResume || len(s.PeerCertificates) != len(chain.Certificate) {
					t.Fatalf("resumed %v, %d peer certificates, want %d", s.DidResume, len(s.PeerCertificates), len(chain.Certificate))
				}
				return
			}
			// The early data is taken, which only a resumed session's is, and
			// the layer seals the ticket that follows; the session it holds
			// is resumed in turn, with early data again.
			if got := resumingClient(t, engine, addr, clientConfig, cache, true, "second"); got != "early:second" {
				t.Fatalf("resumed: %q", got)
			}
			waitTicket(t, cache, ticket)
			if got := resumingClient(t, engine, addr, clientConfig, cache, true, "third"); got != "early:third" {
				t.Fatalf("resumed from the layer's ticket: %q", got)
			}
		})
	}

	// The client retries its hello, which offers the session again, for a
	// server that wants another key share.
	t.Run("hello retried", func(t *testing.T) {
		config := serverConfig.Clone()
		config.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256}
		handler := ticketEcho{newEarlyEcho(), make(chan stdtls.ConnectionState, 4)}
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(config, handler))
		cache := NewQUICSessionCache(4)
		for i, write := range []string{"first", "second"} {
			if got := resumingClient(t, engine, addr, clientConfig, cache, false, write); got != "data:"+write {
				t.Fatalf("connection %d: %q", i, got)
			}
			select {
			case s := <-handler.states:
				if s.DidResume != (i == 1) || len(s.PeerCertificates) != len(chain.Certificate) {
					t.Fatalf("connection %d: resumed %v, %d peer certificates", i, s.DidResume, len(s.PeerCertificates))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the server reported no connection state")
			}
			if i == 0 {
				waitTicket(t, cache, nil)
			}
		}
	})

	t.Run("crypto/tls client", func(t *testing.T) {
		handler := ticketEcho{newEarlyEcho(), make(chan stdtls.ConnectionState, 4)}
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, handler))
		config := clientConfig.Clone()
		config.ClientSessionCache = stdtls.NewLRUClientSessionCache(4)
		for i := 0; i < 2; i++ {
			// The echo also has the client read the ticket.
			state, err := dialEcho(addr, config)
			if err != nil {
				t.Fatalf("connection %d: %v", i, err)
			}
			if state.DidResume != (i == 1) {
				t.Fatalf("connection %d resumed: %v", i, state.DidResume)
			}
		}
	})
}
