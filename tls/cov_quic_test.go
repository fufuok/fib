//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	stdtls "crypto/tls"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// covQuicLevels are the levels QUICConn takes data at, by index.
var covQuicLevels = [3]stdtls.QUICEncryptionLevel{
	stdtls.QUICEncryptionLevelInitial, stdtls.QUICEncryptionLevelHandshake, stdtls.QUICEncryptionLevelApplication,
}

// covQuicOut is what an end reported through its events.
type covQuicOut struct {
	out    [3][]byte
	kinds  []stdtls.QUICEventKind
	err    error
	resume *stdtls.SessionState
}

// covQuicEvents reads every event an end has, without failing on an error
// event, which some tests want to see.
func covQuicEvents(conn quicEnd) covQuicOut {
	var o covQuicOut
	for {
		e := conn.NextEvent()
		if e.Kind == stdtls.QUICNoEvent {
			return o
		}
		o.kinds = append(o.kinds, e.Kind)
		switch e.Kind {
		case stdtls.QUICWriteData:
			o.out[levelIndex(e.Level)] = append(o.out[levelIndex(e.Level)], e.Data...)
		case stdtls.QUICErrorEvent:
			o.err = e.Err
		case stdtls.QUICResumeSession:
			o.resume = e.SessionState
		}
	}
}

// covQuicStart sets an end's transport parameters and starts it.
func covQuicStart(t *testing.T, conn quicEnd, params string) {
	t.Helper()
	conn.SetTransportParameters([]byte(params))
	if err := conn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// covQuicAlert checks that err carries the alert want and, if contains is
// set, says it.
func covQuicAlert(t *testing.T, err error, want int, contains string) {
	t.Helper()
	var alert stdtls.AlertError
	if err == nil || !errors.As(err, &alert) || alert != stdtls.AlertError(want) || contains != "" && !strings.Contains(err.Error(), contains) {
		t.Fatalf("got %v, want alert %d saying %q", err, want, contains)
	}
}

// covQuicRand is a Rand that can be made to fail.
type covQuicRand struct{ fail atomic.Bool }

func (r *covQuicRand) Read(p []byte) (int, error) {
	if r.fail.Load() {
		return 0, errors.New("covquic: no randomness")
	}
	return rand.Read(p)
}

// covQuicWait waits for a wake of SetWake.
func covQuicWait(t *testing.T, woke chan struct{}) {
	t.Helper()
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("not woken")
	}
}

func TestCovQuicQualification(t *testing.T) {
	cert, _ := issueCert(t, keyOf(t, "ecdsa-p256"))
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		ok     bool
	}{
		{"nil", nil, false},
		{"renegotiation", &stdtls.Config{ServerName: "a", Renegotiation: stdtls.RenegotiateOnceAsClient}, false},
		{"TLS 1.2 at most", &stdtls.Config{ServerName: "a", MaxVersion: stdtls.VersionTLS12}, false},
		{"above TLS 1.3", &stdtls.Config{ServerName: "a", MinVersion: stdtls.VersionTLS13 + 1}, false},
		{"only curves it lacks", &stdtls.Config{ServerName: "a", CurvePreferences: []stdtls.CurveID{stdtls.CurveP521}}, false},
		{"one curve it has", &stdtls.Config{ServerName: "a", CurvePreferences: []stdtls.CurveID{stdtls.CurveP521, stdtls.X25519}}, true},
	} {
		if got := NewQUICClient(tc.config) != nil; got != tc.ok {
			t.Errorf("client %s: qualifies %v, want %v", tc.name, got, tc.ok)
		}
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, MinVersion: stdtls.VersionTLS12, MaxVersion: stdtls.VersionTLS12}) != nil {
		t.Error("a server of TLS 1.2 alone qualifies for QUIC")
	}
}

func TestCovQuicStartAndHandleDataMisuse(t *testing.T) {
	_, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	t.Run("HandleData before Start", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		if err := c.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{2, 0, 0, 0}); err == nil || !strings.Contains(err.Error(), "before Start") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Start before SetTransportParameters, and twice", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "before SetTransportParameters") {
			t.Fatalf("got %v", err)
		}
		if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "more than once") {
			t.Fatalf("got %v", err)
		}
		if e := c.NextEvent(); e.Kind != stdtls.QUICNoEvent {
			t.Fatalf("a client that did not start wrote %v", e.Kind)
		}
	})
	t.Run("no ServerName", func(t *testing.T) {
		c := NewQUICClient(&stdtls.Config{})
		if c == nil {
			t.Fatal("the client Config does not qualify")
		}
		c.SetTransportParameters(nil)
		if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "ServerName") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("an unexpected level", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "p")
		if err := c.HandleData(stdtls.QUICEncryptionLevelEarly, []byte{2, 0, 0, 0}); err == nil || !strings.Contains(err.Error(), "unexpected level") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a message spanning levels", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "p")
		if err := c.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{2, 0}); err != nil {
			t.Fatal(err)
		}
		err := c.HandleData(stdtls.QUICEncryptionLevelHandshake, []byte{8, 0, 0, 0})
		covQuicAlert(t, err, alertUnexpectedMessage, "")
		// The failure sticks.
		if again := c.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{0, 0}); again != err {
			t.Fatalf("then got %v", again)
		}
	})
	t.Run("Close drops what is held", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "p")
		if err := c.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{2, 0}); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		// With the partial message gone, data at another level is no longer
		// a message spanning levels.
		if err := c.HandleData(stdtls.QUICEncryptionLevelHandshake, []byte{8, 0}); err != nil {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("an oversized message", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "p")
		covQuicAlert(t, c.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{2, 1, 0, 1}), alertInternalError, "")
	})
	t.Run("a client's message at the wrong level", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "p")
		covQuicAlert(t, c.HandleData(stdtls.QUICEncryptionLevelHandshake, []byte{hsFinished, 0, 0, 0}), alertUnexpectedMessage, "wrong encryption level")
	})
}

func TestCovQuicServerMessages(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// hello is the first flight of a client of ours.
	hello := func(t *testing.T) []byte {
		t.Helper()
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "client parameters")
		o := covQuicEvents(c)
		if len(o.out[0]) == 0 {
			t.Fatal("no ClientHello")
		}
		return o.out[0]
	}
	t.Run("before the hello", func(t *testing.T) {
		s := ourServer(t, quicServerConfig(cert, nil))
		if state := s.ConnectionState(); state.Version != 0 || state.HandshakeComplete {
			t.Fatalf("state %+v", state)
		}
		if err := s.SendSessionTicket(stdtls.QUICSessionTicketOptions{}); err == nil {
			t.Fatal("a ticket was sent before the handshake")
		}
		covQuicStart(t, s, "server parameters")
		covQuicAlert(t, s.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{hsFinished, 0, 0, 0}), alertUnexpectedMessage, "unexpected handshake message")
	})
	t.Run("no transport parameters", func(t *testing.T) {
		s := ourServer(t, quicServerConfig(cert, nil))
		if err := s.Start(context.Background()); err == nil {
			t.Fatal("started without transport parameters")
		}
		covQuicAlert(t, s.HandleData(stdtls.QUICEncryptionLevelInitial, hello(t)), alertInternalError, "no transport parameters")
	})
	t.Run("data after the ClientHello", func(t *testing.T) {
		s := ourServer(t, quicServerConfig(cert, nil))
		covQuicStart(t, s, "server parameters")
		covQuicAlert(t, s.HandleData(stdtls.QUICEncryptionLevelInitial, append(hello(t), 0)), alertUnexpectedMessage, "after the ClientHello")
	})
	t.Run("a second ClientHello that is not one", func(t *testing.T) {
		s := ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }))
		covQuicStart(t, s, "server parameters")
		first := hello(t)
		if err := s.HandleData(stdtls.QUICEncryptionLevelInitial, first); err != nil {
			t.Fatal(err)
		}
		if o := covQuicEvents(s); len(o.out[0]) == 0 || !s.server.retried {
			t.Fatal("no HelloRetryRequest")
		}
		// The first hello again, whose key share is not the one asked for.
		err := s.HandleData(stdtls.QUICEncryptionLevelInitial, first)
		var alert stdtls.AlertError
		if !errors.As(err, &alert) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("data after the Finished, and after the handshake", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		s := ourServer(t, quicServerConfig(cert, nil))
		covQuicStart(t, c, "client parameters")
		covQuicStart(t, s, "server parameters")
		if err := s.HandleData(stdtls.QUICEncryptionLevelInitial, covQuicEvents(c).out[0]); err != nil {
			t.Fatal(err)
		}
		so := covQuicEvents(s)
		for level := 0; level < 2; level++ {
			if err := c.HandleData(covQuicLevels[level], so.out[level]); err != nil {
				t.Fatal(err)
			}
		}
		fin := covQuicEvents(c).out[1]
		if len(fin) == 0 {
			t.Fatal("no client Finished")
		}
		// The client's Finished, with a byte more.
		covQuicAlert(t, s.HandleData(stdtls.QUICEncryptionLevelHandshake, append(bytes.Clone(fin), 0)), alertUnexpectedMessage, "after the Finished")
	})
	t.Run("a finished server", func(t *testing.T) {
		for _, tc := range []struct {
			level stdtls.QUICEncryptionLevel
			text  string
		}{
			{stdtls.QUICEncryptionLevelHandshake, "wrong encryption level"},
			{stdtls.QUICEncryptionLevelApplication, "unexpected handshake message"},
		} {
			s := ourServer(t, quicServerConfig(cert, nil))
			cli, srv, err := quicRun(t, ourClient(t, quicClientConfig(pool, nil)), s, 4096)
			if err != nil {
				t.Fatal(err)
			}
			quicCheck(t, cli, srv)
			covQuicAlert(t, s.HandleData(tc.level, []byte{24, 0, 0, 1, 0}), alertUnexpectedMessage, tc.text)
		}
	})
}

func TestCovQuicClientMessages(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	t.Run("data after the server's flight", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		s := ourServer(t, quicServerConfig(cert, nil))
		covQuicStart(t, c, "client parameters")
		covQuicStart(t, s, "server parameters")
		if err := s.HandleData(stdtls.QUICEncryptionLevelInitial, covQuicEvents(c).out[0]); err != nil {
			t.Fatal(err)
		}
		so := covQuicEvents(s)
		if err := c.HandleData(stdtls.QUICEncryptionLevelInitial, so.out[0]); err != nil {
			t.Fatal(err)
		}
		covQuicAlert(t, c.HandleData(stdtls.QUICEncryptionLevelHandshake, append(so.out[1], 1, 2, 3)), alertUnexpectedMessage, "after the last message of a flight")
	})
	t.Run("a KeyUpdate after the handshake", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, nil))
		cli, srv, err := quicRun(t, c, ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		covQuicAlert(t, c.HandleData(stdtls.QUICEncryptionLevelApplication, []byte{24, 0, 0, 1, 0}), alertUnexpectedMessage, "unexpected handshake message 24")
		if err := c.SendSessionTicket(stdtls.QUICSessionTicketOptions{}); err == nil {
			t.Fatal("a client sent a ticket")
		}
	})
	t.Run("VerifyConnection refuses", func(t *testing.T) {
		c := ourClient(t, quicClientConfig(pool, func(c *stdtls.Config) {
			c.VerifyConnection = func(stdtls.ConnectionState) error { return errors.New("covquic refused") }
		}))
		_, _, err := quicRun(t, c, stdServer(quicServerConfig(cert, nil)), 4096)
		covQuicAlert(t, err, alertBadCertificate, "covquic refused")
	})
	t.Run("a client key that cannot sign", func(t *testing.T) {
		serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
		serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
		broken := clientCA.issue(t, "ecdsa-p256", "alice", true)
		broken.PrivateKey = failingSigner{broken.PrivateKey.(crypto.Signer)}
		c := ourClient(t, quicClientConfig(serverCA.pool, func(c *stdtls.Config) { c.Certificates = []stdtls.Certificate{broken} }))
		server := stdServer(quicServerConfig(serverCert, func(c *stdtls.Config) {
			c.ClientAuth, c.ClientCAs = stdtls.RequireAndVerifyClientCert, clientCA.pool
		}))
		defer server.Close()
		_, _, err := quicRun(t, c, server, 4096)
		covQuicAlert(t, err, alertInternalError, "failed to sign handshake")
	})
	t.Run("GetClientCertificate on a worker", func(t *testing.T) {
		serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
		serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
		alice := clientCA.issue(t, "ecdsa-p256", "alice", true)
		var calls atomic.Int64
		c := ourClient(t, quicClientConfig(serverCA.pool, func(c *stdtls.Config) {
			c.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
				calls.Add(1)
				time.Sleep(5 * time.Millisecond)
				return &alice, nil
			}
		}))
		woke := make(chan struct{}, 1)
		c.SetWake(func() { woke <- struct{}{} })
		server := stdServer(quicServerConfig(serverCert, func(c *stdtls.Config) {
			c.ClientAuth, c.ClientCAs = stdtls.RequireAndVerifyClientCert, clientCA.pool
		}))
		cli, srv, err := quicRunWith(t, c, server, 4096, quicRunOptions{wakes: []chan struct{}{woke}})
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if peers := server.ConnectionState().PeerCertificates; len(peers) != 1 || peers[0].Subject.CommonName != "alice" || calls.Load() != 1 {
			t.Fatalf("the server saw %d certificates; %d calls", len(peers), calls.Load())
		}
	})
}

// covQuicTicket is a NewSessionTicket: lifetime, a ticket, the extensions
// exts writes, and trailing bytes inside the message.
func covQuicTicket(lifetime uint32, ticket []byte, exts func(*builder), trailing ...byte) []byte {
	var b builder
	b.u8(hsNewSessionTicket)
	b.u24(0)
	b.u32(lifetime)
	b.u32(7)
	b.vec8(func(b *builder) { b.raw([]byte{0}) })
	b.vec16(func(b *builder) { b.raw(ticket) })
	b.vec16(func(b *builder) {
		if exts != nil {
			exts(b)
		}
	})
	b.raw(trailing)
	return finishMessage(&b)
}

// covQuicEarlyExt writes an early_data extension of size.
func covQuicEarlyExt(size uint32) func(*builder) {
	return func(b *builder) {
		b.u16(extEarlyData)
		b.vec16(func(b *builder) { b.u32(size) })
	}
}

func TestCovQuicClientTickets(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// done is a client of ours that has finished its handshake, with a cache
	// emptied of the ticket the server sent.
	done := func(t *testing.T) (*QUICConn, QUICSessionCache) {
		t.Helper()
		cache := NewQUICSessionCache(2)
		c := NewQUICClientWithSessions(quicClientConfig(pool, nil), cache)
		cli, srv, err := quicRun(t, c, ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if cache.Get("localhost") == nil {
			t.Fatal("the server's ticket was not kept")
		}
		cache.Put("localhost", nil)
		return c, cache
	}
	app := stdtls.QUICEncryptionLevelApplication
	for _, tc := range []struct {
		name  string
		msg   []byte
		alert int
		text  string
	}{
		{"no ticket", covQuicTicket(100, nil, nil), alertDecodeError, "malformed NewSessionTicket"},
		{"trailing bytes", covQuicTicket(100, []byte("t"), nil, 9), alertDecodeError, "malformed NewSessionTicket"},
		{"a duplicate extension", covQuicTicket(100, []byte("t"), func(b *builder) {
			covQuicEarlyExt(0xffffffff)(b)
			covQuicEarlyExt(0xffffffff)(b)
		}), alertIllegalParameter, "duplicate extension"},
		{"some of the early data", covQuicTicket(100, []byte("t"), covQuicEarlyExt(1000)), alertIllegalParameter, "max_early_data_size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cache := done(t)
			covQuicAlert(t, c.HandleData(app, tc.msg), tc.alert, tc.text)
			if cache.Get("localhost") != nil {
				t.Fatal("a bad ticket was kept")
			}
		})
	}
	t.Run("lifetime zero, then a good one", func(t *testing.T) {
		c, cache := done(t)
		if err := c.HandleData(app, covQuicTicket(0, []byte("t"), covQuicEarlyExt(0xffffffff))); err != nil {
			t.Fatal(err)
		}
		if cache.Get("localhost") != nil {
			t.Fatal("a ticket of no lifetime was kept")
		}
		if err := c.HandleData(app, covQuicTicket(100, []byte("good"), covQuicEarlyExt(0xffffffff))); err != nil {
			t.Fatal(err)
		}
		s := cache.Get("localhost")
		if s == nil || string(s.ticket) != "good" || !s.earlyData || s.lifetime != 100*time.Second ||
			len(s.extra) != 1 || string(s.extra[0][1:]) != "server parameters" {
			t.Fatalf("kept %+v", s)
		}
	})
}

func TestCovQuicFallBack(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// A client that offers only P-521, which this package does not do and
	// crypto/tls does.
	p521 := func() *stdtls.Config {
		return quicClientConfig(pool, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP521} })
	}
	t.Run("served by crypto/tls", func(t *testing.T) {
		fellBack := countFallbacks(t)
		server := ourServer(t, quicServerConfig(cert, nil))
		// quicRun has the server send a ticket, which goes through to crypto/tls.
		cli, srv, err := quicRun(t, stdClient(p521()), server, 100)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if n := fellBack.Load(); n != 1 || server.fallback == nil {
			t.Fatalf("%d fell back", n)
		}
		if state := server.ConnectionState(); state.Version != stdtls.VersionTLS13 || state.CurveID != stdtls.CurveP521 || !state.HandshakeComplete {
			t.Fatalf("state %+v", state)
		}
		// Parameters set after the handshake change nothing, and do not block.
		server.SetTransportParameters([]byte("late"))
		if string(server.q.localParams) != "late" {
			t.Fatal("the parameters were not kept")
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCovQuicServerResumeFails(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	rnd := &covQuicRand{}
	server := NewQUICServer(quicServerConfig(cert, func(c *stdtls.Config) {
		c.SessionTicketKey = [32]byte{5}
		c.Rand = rnd
	}))
	// hello is the first flight of crypto/tls's client, resuming a fresh
	// ticket that allows early data; a ticket carries it once.
	hello := func(t *testing.T) []byte {
		t.Helper()
		rnd.fail.Store(false)
		cli, _, err := quicRunWith(t, quicEarlyClient(pool, stdtls.NewLRUClientSessionCache(2)), server.NewConn(), 4096,
			quicRunOptions{ticket: stdtls.QUICSessionTicketOptions{EarlyData: true}})
		if err != nil || cli.stored == nil || !cli.stored.EarlyData {
			t.Fatalf("no ticket for early data: %v", err)
		}
		cache := stdtls.NewLRUClientSessionCache(2)
		if err := quicEarlyClient(pool, cache).StoreSession(cli.stored); err != nil {
			t.Fatal(err)
		}
		client := quicEarlyClient(pool, cache)
		t.Cleanup(func() { client.Close() })
		covQuicStart(t, client, "client parameters")
		return covQuicEvents(client).out[0]
	}
	// asked is a server connection that has the hello, and asks the layer
	// about early data.
	asked := func(t *testing.T) *QUICConn {
		t.Helper()
		conn := server.NewConn()
		covQuicStart(t, conn, "server parameters")
		if err := conn.HandleData(stdtls.QUICEncryptionLevelInitial, hello(t)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	t.Run("answered by HandleData", func(t *testing.T) {
		conn := asked(t)
		rnd.fail.Store(true)
		covQuicAlert(t, conn.HandleData(stdtls.QUICEncryptionLevelInitial, nil), alertInternalError, "")
		o := covQuicEvents(conn)
		if o.resume == nil || o.err == nil || len(o.out[0]) != 0 {
			t.Fatalf("events %v", o.kinds)
		}
	})
	t.Run("answered by NextEvent", func(t *testing.T) {
		conn := asked(t)
		// The layer reads up to the question, and is asked nothing else.
		for e := conn.NextEvent(); e.Kind != stdtls.QUICResumeSession; e = conn.NextEvent() {
			if e.Kind == stdtls.QUICNoEvent || e.Kind == stdtls.QUICWriteData {
				t.Fatalf("got %v before the question", e.Kind)
			}
		}
		rnd.fail.Store(true)
		e := conn.NextEvent()
		if e.Kind != stdtls.QUICErrorEvent {
			t.Fatalf("got %v", e.Kind)
		}
		covQuicAlert(t, e.Err, alertInternalError, "")
	})
	t.Run("a ticket without randomness", func(t *testing.T) {
		rnd.fail.Store(false)
		conn := server.NewConn()
		// A client that resumes, and so takes tickets.
		cli, srv, err := quicRun(t, quicEarlyClient(pool, stdtls.NewLRUClientSessionCache(2)), conn, 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		rnd.fail.Store(true)
		covQuicAlert(t, conn.SendSessionTicket(stdtls.QUICSessionTicketOptions{}), alertInternalError, "no randomness")
	})
}

func TestCovQuicServerCertificateWait(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// waiting is a server whose GetCertificate answers with answer once
	// released, and which has the hello of a client of ours.
	waiting := func(t *testing.T, answer func() (*stdtls.Certificate, error)) (*QUICConn, chan struct{}, chan struct{}) {
		t.Helper()
		release, woke := make(chan struct{}), make(chan struct{}, 1)
		s := ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) {
			c.Certificates, c.GetCertificate = nil, func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
				<-release
				return answer()
			}
		}))
		s.SetWake(func() { woke <- struct{}{} })
		covQuicStart(t, s, "server parameters")
		c := ourClient(t, quicClientConfig(pool, nil))
		covQuicStart(t, c, "client parameters")
		if err := s.HandleData(stdtls.QUICEncryptionLevelInitial, covQuicEvents(c).out[0]); err != nil {
			t.Fatal(err)
		}
		if o := covQuicEvents(s); len(o.out[0]) != 0 || o.err != nil {
			t.Fatalf("a server waiting for its certificate reported %v", o.kinds)
		}
		return s, release, woke
	}
	t.Run("GetCertificate fails", func(t *testing.T) {
		s, release, woke := waiting(t, func() (*stdtls.Certificate, error) { return nil, errors.New("covquic no certificate") })
		close(release)
		covQuicWait(t, woke)
		o := covQuicEvents(s)
		if o.err == nil || !strings.Contains(o.err.Error(), "covquic no certificate") || len(o.out[0]) != 0 {
			t.Fatalf("events %v, error %v", o.kinds, o.err)
		}
		if err := s.HandleData(stdtls.QUICEncryptionLevelHandshake, []byte{hsFinished, 0, 0, 0}); err != o.err {
			t.Fatalf("then got %v", err)
		}
	})
	t.Run("data that arrived meanwhile", func(t *testing.T) {
		s, release, woke := waiting(t, func() (*stdtls.Certificate, error) { return &cert, nil })
		// Held while the server waits: a message at the Initial level, where
		// the server will have nothing more to read.
		if err := s.HandleData(stdtls.QUICEncryptionLevelInitial, []byte{hsFinished, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		if e := s.NextEvent(); e.Kind != stdtls.QUICNoEvent {
			t.Fatalf("got %v", e.Kind)
		}
		close(release)
		covQuicWait(t, woke)
		o := covQuicEvents(s)
		if len(o.out[0]) == 0 || len(o.out[1]) == 0 {
			t.Fatalf("the server did not answer: %v", o.kinds)
		}
		covQuicAlert(t, o.err, alertUnexpectedMessage, "wrong encryption level")
		if o.kinds[len(o.kinds)-1] != stdtls.QUICErrorEvent {
			t.Fatalf("events %v", o.kinds)
		}
	})
}
