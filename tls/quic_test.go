//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// quicEnd is what crypto/tls's QUICConn and this package's both are.
type quicEnd interface {
	Start(context.Context) error
	HandleData(stdtls.QUICEncryptionLevel, []byte) error
	NextEvent() stdtls.QUICEvent
	SetTransportParameters([]byte)
	SendSessionTicket(stdtls.QUICSessionTicketOptions) error
	ConnectionState() stdtls.ConnectionState
	Close() error
}

var (
	_ quicEnd = (*QUICConn)(nil)
	_ quicEnd = (*stdtls.QUICConn)(nil)
)

// quicSide is one end of a handshake as the test sees it: what it wrote at each
// level, the secrets it set, the parameters its peer sent, and whether it is done.
type quicSide struct {
	conn   quicEnd
	out    [3][]byte
	reads  map[stdtls.QUICEncryptionLevel][]byte
	writes map[stdtls.QUICEncryptionLevel][]byte
	peer   []byte
	done   bool
	suite  uint16
	// Sessions: what crypto/tls's client has to store, what a server was
	// asked to resume, whether the server turned a client's early data away,
	// and whether this side declines early data when asked.
	stored   *stdtls.SessionState
	resumed  *stdtls.SessionState
	rejected bool
	decline  bool
}

func newQuicSide(conn quicEnd) *quicSide {
	return &quicSide{conn: conn, reads: map[stdtls.QUICEncryptionLevel][]byte{}, writes: map[stdtls.QUICEncryptionLevel][]byte{}}
}

func levelIndex(level stdtls.QUICEncryptionLevel) int {
	switch level {
	case stdtls.QUICEncryptionLevelInitial:
		return 0
	case stdtls.QUICEncryptionLevelHandshake:
		return 1
	}
	return 2
}

// events reads what the side has to report, and returns an error for an event
// the test does not know.
func (s *quicSide) events(t *testing.T) {
	t.Helper()
	for {
		e := s.conn.NextEvent()
		switch e.Kind {
		case stdtls.QUICNoEvent:
			return
		case stdtls.QUICSetReadSecret:
			s.reads[e.Level] = bytes.Clone(e.Data)
			s.suite = e.Suite
		case stdtls.QUICSetWriteSecret:
			s.writes[e.Level] = bytes.Clone(e.Data)
			s.suite = e.Suite
		case stdtls.QUICWriteData:
			s.out[levelIndex(e.Level)] = append(s.out[levelIndex(e.Level)], e.Data...)
		case stdtls.QUICTransportParameters:
			s.peer = bytes.Clone(e.Data)
		case stdtls.QUICTransportParametersRequired:
			t.Fatal("the transport parameters were asked for after they were set")
		case stdtls.QUICHandshakeDone:
			s.done = true
		case stdtls.QUICStoreSession:
			s.stored = e.SessionState
			if std, ok := s.conn.(*stdtls.QUICConn); ok {
				if err := std.StoreSession(e.SessionState); err != nil {
					t.Fatal(err)
				}
			}
		case stdtls.QUICResumeSession:
			s.resumed = e.SessionState
			if s.decline {
				e.SessionState.EarlyData = false
			}
		case stdtls.QUICRejectedEarlyData:
			s.rejected = true
		case stdtls.QUICErrorEvent:
			t.Fatalf("error event: %v", e.Err)
		}
	}
}

// quicRun runs a handshake between client and server, delivering what each
// writes to the other in pieces of at most chunk bytes, and returns the first
// error either gave. The server sends a ticket when it is done.
func quicRun(t *testing.T, client, server quicEnd, chunk int) (*quicSide, *quicSide, error) {
	t.Helper()
	return quicRunWith(t, client, server, chunk, quicRunOptions{})
}

// quicRunOptions are what a run of quicRun sets differently: the ticket the
// server sends, and whether the server declines early data when it is asked.
type quicRunOptions struct {
	ticket         stdtls.QUICSessionTicketOptions
	serverDeclines bool
	clientDeclines bool
	// wakes are where the ends that SetWake was given are woken, for the
	// run to wait on when neither has anything to say.
	wakes []chan struct{}
}

func quicRunWith(t *testing.T, client, server quicEnd, chunk int, opts quicRunOptions) (*quicSide, *quicSide, error) {
	t.Helper()
	cli, srv := newQuicSide(client), newQuicSide(server)
	srv.decline = opts.serverDeclines
	cli.decline = opts.clientDeclines
	client.SetTransportParameters([]byte("client parameters"))
	server.SetTransportParameters([]byte("server parameters"))
	if err := client.Start(context.Background()); err != nil {
		return cli, srv, err
	}
	if err := server.Start(context.Background()); err != nil {
		return cli, srv, err
	}
	ticketSent := false
	for step := 0; step < 10000; step++ {
		cli.events(t)
		srv.events(t)
		moved := false
		for _, pair := range []struct{ from, to *quicSide }{{cli, srv}, {srv, cli}} {
			// The lowest level that has anything, and no higher one until it
			// is all delivered: packets of a level cannot be read before the
			// keys that the level below gives, so a message is never partly
			// delivered when data of another level arrives.
			for level := range pair.from.out {
				data := pair.from.out[level]
				if len(data) == 0 {
					continue
				}
				n := min(chunk, len(data))
				pair.from.out[level] = data[n:]
				moved = true
				lv := [...]stdtls.QUICEncryptionLevel{stdtls.QUICEncryptionLevelInitial, stdtls.QUICEncryptionLevelHandshake, stdtls.QUICEncryptionLevelApplication}[level]
				if err := pair.to.conn.HandleData(lv, data[:n]); err != nil {
					return cli, srv, err
				}
				break
			}
		}
		if srv.done && !ticketSent {
			ticketSent = true
			if err := server.SendSessionTicket(opts.ticket); err != nil {
				return cli, srv, err
			}
			moved = true
		}
		if !moved && cli.done && srv.done {
			return cli, srv, nil
		}
		if !moved {
			cli.events(t)
			srv.events(t)
			if !cli.pending() && !srv.pending() && !(cli.done && srv.done) && len(opts.wakes) > 0 {
				// An end waits for a certificate callback on another
				// goroutine.
				var a, b chan struct{}
				a = opts.wakes[0]
				if len(opts.wakes) > 1 {
					b = opts.wakes[1]
				}
				select {
				case <-a:
				case <-b:
				case <-time.After(5 * time.Second):
					return cli, srv, errors.New("neither end was woken")
				}
			}
		}
	}
	return cli, srv, errors.New("the handshake never finished")
}

// pending reports whether the side has written what its peer has not been
// given.
func (s *quicSide) pending() bool {
	for _, out := range s.out {
		if len(out) > 0 {
			return true
		}
	}
	return false
}

// quicCheck checks that two ends that finished agree.
func quicCheck(t *testing.T, cli, srv *quicSide) {
	t.Helper()
	if !cli.done || !srv.done {
		t.Fatalf("client done %v, server done %v", cli.done, srv.done)
	}
	for _, level := range []stdtls.QUICEncryptionLevel{stdtls.QUICEncryptionLevelHandshake, stdtls.QUICEncryptionLevelApplication} {
		if len(cli.writes[level]) == 0 || !bytes.Equal(cli.writes[level], srv.reads[level]) {
			t.Errorf("level %d: what the client writes under, %x, is not what the server reads under, %x", level, cli.writes[level], srv.reads[level])
		}
		if len(srv.writes[level]) == 0 || !bytes.Equal(srv.writes[level], cli.reads[level]) {
			t.Errorf("level %d: what the server writes under, %x, is not what the client reads under, %x", level, srv.writes[level], cli.reads[level])
		}
	}
	if string(cli.peer) != "server parameters" || string(srv.peer) != "client parameters" {
		t.Errorf("transport parameters: client got %q, server got %q", cli.peer, srv.peer)
	}
}

func quicClientConfig(pool *x509.CertPool, configure func(*stdtls.Config)) *stdtls.Config {
	c := &stdtls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h3test"}, MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(c)
	}
	return c
}

func quicServerConfig(cert stdtls.Certificate, configure func(*stdtls.Config)) *stdtls.Config {
	c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h3test"}, MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(c)
	}
	return c
}

func ourClient(t *testing.T, config *stdtls.Config) *QUICConn {
	t.Helper()
	c := NewQUICClient(config)
	if c == nil {
		t.Fatal("the client Config does not qualify")
	}
	return c
}

func ourServer(t *testing.T, config *stdtls.Config) *QUICConn {
	t.Helper()
	s := NewQUICServer(config)
	if s == nil {
		t.Fatal("the server Config does not qualify")
	}
	return s.NewConn()
}

func stdClient(config *stdtls.Config) *stdtls.QUICConn {
	return stdtls.QUICClient(&stdtls.QUICConfig{TLSConfig: config})
}

func stdServer(config *stdtls.Config) *stdtls.QUICConn {
	return stdtls.QUICServer(&stdtls.QUICConfig{TLSConfig: config})
}

func TestQUICClientAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				client := ourClient(t, quicClientConfig(pool, nil))
				cli, srv, err := quicRun(t, client, stdServer(quicServerConfig(cert, nil)), chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				state := client.ConnectionState()
				if state.Version != stdtls.VersionTLS13 || state.NegotiatedProtocol != "h3test" || len(state.PeerCertificates) != 1 {
					t.Fatalf("state %+v", state)
				}
			})
		}
	}
}

func TestQUICServerAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				server := ourServer(t, quicServerConfig(cert, nil))
				cli, srv, err := quicRun(t, stdClient(quicClientConfig(pool, nil)), server, chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				state := server.ConnectionState()
				if state.Version != stdtls.VersionTLS13 || state.NegotiatedProtocol != "h3test" || state.ServerName != "localhost" {
					t.Fatalf("state %+v", state)
				}
			})
		}
	}
}

func TestQUICOursAgainstOurs(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	for _, suite := range quicSuites {
		for _, chunk := range []int{1, 1 << 16} {
			t.Run(strconv.Itoa(int(suite))+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				client := ourClient(t, quicClientConfig(pool, nil))
				client.suites = []uint16{suite}
				cli, srv, err := quicRun(t, client, ourServer(t, quicServerConfig(cert, nil)), chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				if cli.suite != suite || srv.suite != suite {
					t.Fatalf("suites %x and %x, want %x", cli.suite, srv.suite, suite)
				}
			})
		}
	}
	t.Run("ChaCha20 against crypto/tls", func(t *testing.T) {
		client := ourClient(t, quicClientConfig(pool, nil))
		client.suites = []uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256}
		cli, srv, err := quicRun(t, client, stdServer(quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if cli.suite != stdtls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("suite %x", cli.suite)
		}
	})
}

func TestQUICHandshakeOptions(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	t.Run("HelloRetryRequest, with our server", func(t *testing.T) {
		server := ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }))
		cli, srv, err := quicRun(t, stdClient(quicClientConfig(pool, nil)), server, 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if !server.server.retried {
			t.Fatal("no HelloRetryRequest")
		}
	})
	t.Run("HelloRetryRequest, with our client", func(t *testing.T) {
		cli, srv, err := quicRun(t, ourClient(t, quicClientConfig(pool, nil)),
			stdServer(quicServerConfig(cert, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} })), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
	})
	t.Run("no protocol in common", func(t *testing.T) {
		_, _, err := quicRun(t, stdClient(quicClientConfig(pool, func(c *stdtls.Config) { c.NextProtos = []string{"other"} })),
			ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("untrusted certificate", func(t *testing.T) {
		_, _, err := quicRun(t, ourClient(t, quicClientConfig(x509.NewCertPool(), nil)), stdServer(quicServerConfig(cert, nil)), 4096)
		// As crypto/tls's QUICConn reports it: the cause, and the alert.
		var verify *stdtls.CertificateVerificationError
		var alert stdtls.AlertError
		if err == nil || !errors.As(err, &verify) || !errors.As(err, &alert) || alert != 42 {
			t.Fatalf("got %v", err)
		}
		if !strings.Contains(err.Error(), "unknown authority") {
			t.Fatalf("the error does not say why: %v", err)
		}
	})
	t.Run("a name the certificate lacks", func(t *testing.T) {
		_, _, err := quicRun(t, ourClient(t, quicClientConfig(pool, func(c *stdtls.Config) { c.ServerName = "other.invalid" })),
			stdServer(quicServerConfig(cert, nil)), 4096)
		if err == nil || !strings.Contains(err.Error(), "not other.invalid") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a server that sends no ALPN protocol", func(t *testing.T) {
		_, _, err := quicRun(t, ourClient(t, quicClientConfig(pool, nil)),
			ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) { c.NextProtos = nil })), 4096)
		if err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var seen stdtls.ConnectionState
		client := ourClient(t, quicClientConfig(pool, func(c *stdtls.Config) {
			c.VerifyConnection = func(s stdtls.ConnectionState) error { seen = s; return nil }
		}))
		cli, srv, err := quicRun(t, client, ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if seen.NegotiatedProtocol != "h3test" {
			t.Fatalf("seen %+v", seen)
		}
	})
}

func TestQUICResumption(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	config := quicServerConfig(cert, nil)
	server := NewQUICServer(config)
	cache := stdtls.NewLRUClientSessionCache(4)
	for i := 0; i < 3; i++ {
		conn := server.NewConn()
		client := stdClient(quicClientConfig(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache }))
		cli, srv, err := quicRun(t, client, conn, 4096)
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		quicCheck(t, cli, srv)
		if resumed := i > 0; conn.ConnectionState().DidResume != resumed || client.ConnectionState().DidResume != resumed {
			t.Fatalf("connection %d: server resumed %v, client resumed %v, want %v", i,
				conn.ConnectionState().DidResume, client.ConnectionState().DidResume, resumed)
		}
	}
}

func TestQUICServerFallsBack(t *testing.T) {
	fellBack := countFallbacks(t)
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// A client that asks for something the server does not serve itself: a
	// session ID, which QUIC has no use for.
	h, err := newClientHSFor(quicClientConfig(pool, nil), &quicCtx{localParams: []byte("client parameters"), haveLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	h.sessionID = bytes.Repeat([]byte{7}, 32)
	hello := h.clientHello()

	server := ourServer(t, quicServerConfig(cert, nil))
	server.SetTransportParameters([]byte("server parameters"))
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleData(stdtls.QUICEncryptionLevelInitial, hello); err != nil {
		t.Fatal(err)
	}
	if n := fellBack.Load(); n != 1 {
		t.Fatalf("%d handed to crypto/tls, want 1", n)
	}
	var wrote bool
	for {
		e := server.NextEvent()
		if e.Kind == stdtls.QUICNoEvent {
			break
		}
		wrote = wrote || e.Kind == stdtls.QUICWriteData && e.Level == stdtls.QUICEncryptionLevelInitial
	}
	if !wrote {
		t.Fatal("crypto/tls did not answer the hello it was given")
	}
}

func TestQUICQualification(t *testing.T) {
	cert, _ := issueCert(t, keyOf(t, "ecdsa-p256"))
	if NewQUICClient(&stdtls.Config{ClientSessionCache: stdtls.NewLRUClientSessionCache(1)}) != nil {
		t.Error("a client with a session cache qualifies")
	}
	if NewQUICClient(&stdtls.Config{ServerName: "a", Certificates: []stdtls.Certificate{cert}}) == nil {
		t.Error("a client with certificates does not qualify")
	}
	if NewQUICClient(&stdtls.Config{ServerName: "a", Certificates: []stdtls.Certificate{{Certificate: [][]byte{{1}}, PrivateKey: "no"}}}) != nil {
		t.Error("a client with a key that cannot sign qualifies")
	}
	if NewQUICClient(&stdtls.Config{ServerName: "a"}) == nil {
		t.Error("an ordinary client does not qualify")
	}
	if NewQUICServer(&stdtls.Config{}) != nil {
		t.Error("a server without certificates qualifies")
	}
	if NewQUICServer(&stdtls.Config{GetCertificate: func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) { return nil, nil }}) == nil {
		t.Error("a server with GetCertificate does not qualify")
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, ClientAuth: stdtls.RequireAndVerifyClientCert}) == nil {
		t.Error("a server that authenticates clients does not qualify")
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, GetConfigForClient: func(*stdtls.ClientHelloInfo) (*stdtls.Config, error) { return nil, nil }}) != nil {
		t.Error("a server with GetConfigForClient qualifies")
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}}) == nil {
		t.Error("an ordinary server does not qualify")
	}
}

// quicEarlyClient is crypto/tls's client, with the events that let it resume
// and send early data.
func quicEarlyClient(pool *x509.CertPool, cache stdtls.ClientSessionCache) *stdtls.QUICConn {
	config := quicClientConfig(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache })
	return stdtls.QUICClient(&stdtls.QUICConfig{TLSConfig: config, EnableSessionEvents: true})
}

func TestQUICServerEarlyData(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	key := [32]byte{1, 2, 3}
	config := quicServerConfig(cert, func(c *stdtls.Config) { c.SessionTicketKey = key })
	server := NewQUICServer(config)
	ticket := stdtls.QUICSessionTicketOptions{EarlyData: true, Extra: [][]byte{[]byte("params one"), []byte("two")}}

	// connect runs a connection of crypto/tls's client against ours.
	connect := func(t *testing.T, server *QUICServer, cache stdtls.ClientSessionCache, opts quicRunOptions) (cli, srv *quicSide, conn *QUICConn) {
		t.Helper()
		conn = server.NewConn()
		var err error
		cli, srv, err = quicRunWith(t, quicEarlyClient(pool, cache), conn, 4096, opts)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		return cli, srv, conn
	}
	// withSession is a cache holding state, which a client tries to resume.
	withSession := func(state *stdtls.SessionState) stdtls.ClientSessionCache {
		cache := stdtls.NewLRUClientSessionCache(2)
		if err := quicEarlyClient(pool, cache).StoreSession(state); err != nil {
			t.Fatal(err)
		}
		return cache
	}
	issue := func(t *testing.T, opts stdtls.QUICSessionTicketOptions) *stdtls.SessionState {
		t.Helper()
		cli, _, _ := connect(t, server, stdtls.NewLRUClientSessionCache(2), quicRunOptions{ticket: opts})
		if cli.stored == nil {
			t.Fatal("no session to store")
		}
		return cli.stored
	}

	saved := issue(t, ticket)
	if !saved.EarlyData {
		t.Fatal("the ticket does not allow early data")
	}

	t.Run("accepted", func(t *testing.T) {
		cli, srv, conn := connect(t, server, withSession(saved), quicRunOptions{})
		if len(cli.writes[stdtls.QUICEncryptionLevelEarly]) == 0 {
			t.Fatal("the client did not offer early data")
		}
		if !bytes.Equal(cli.writes[stdtls.QUICEncryptionLevelEarly], srv.reads[stdtls.QUICEncryptionLevelEarly]) {
			t.Fatalf("early secrets: client %x, server %x", cli.writes[stdtls.QUICEncryptionLevelEarly], srv.reads[stdtls.QUICEncryptionLevelEarly])
		}
		if cli.rejected {
			t.Fatal("the client was told its early data was rejected")
		}
		if srv.resumed == nil || len(srv.resumed.Extra) != 2 || string(srv.resumed.Extra[0]) != "params one" || string(srv.resumed.Extra[1]) != "two" {
			t.Fatalf("the layer was asked about %+v", srv.resumed)
		}
		if !conn.ConnectionState().DidResume {
			t.Fatal("not resumed")
		}
	})

	t.Run("a ticket carries early data once", func(t *testing.T) {
		// "accepted" used it.
		cli, srv, conn := connect(t, server, withSession(saved), quicRunOptions{})
		if len(cli.writes[stdtls.QUICEncryptionLevelEarly]) == 0 || !cli.rejected {
			t.Fatalf("the client offered %d bytes of secret and was told rejected = %v", len(cli.writes[stdtls.QUICEncryptionLevelEarly]), cli.rejected)
		}
		if len(srv.reads[stdtls.QUICEncryptionLevelEarly]) != 0 || srv.resumed != nil {
			t.Fatal("the server took the replay")
		}
		if !conn.ConnectionState().DidResume {
			t.Fatal("the session should still resume")
		}
	})

	t.Run("declined by the layer", func(t *testing.T) {
		fresh := issue(t, ticket)
		cli, srv, conn := connect(t, server, withSession(fresh), quicRunOptions{serverDeclines: true})
		if srv.resumed == nil || !cli.rejected || len(srv.reads[stdtls.QUICEncryptionLevelEarly]) != 0 || !conn.ConnectionState().DidResume {
			t.Fatalf("asked %v, rejected %v, early read %d, resumed %v", srv.resumed != nil, cli.rejected,
				len(srv.reads[stdtls.QUICEncryptionLevelEarly]), conn.ConnectionState().DidResume)
		}
	})

	t.Run("a ticket that does not allow it", func(t *testing.T) {
		plain := issue(t, stdtls.QUICSessionTicketOptions{})
		if plain.EarlyData {
			t.Fatal("early data allowed")
		}
		cli, srv, conn := connect(t, server, withSession(plain), quicRunOptions{})
		if len(cli.writes[stdtls.QUICEncryptionLevelEarly]) != 0 || srv.resumed != nil || !conn.ConnectionState().DidResume {
			t.Fatal("expected a plain resumption")
		}
	})

	t.Run("a ticket too far from its age", func(t *testing.T) {
		fresh := issue(t, ticket)
		later := quicServerConfig(cert, func(c *stdtls.Config) {
			c.SessionTicketKey = key
			c.Time = func() time.Time { return time.Now().Add(time.Minute) }
		})
		cli, srv, _ := connect(t, NewQUICServer(later), withSession(fresh), quicRunOptions{})
		if !cli.rejected || len(srv.reads[stdtls.QUICEncryptionLevelEarly]) != 0 {
			t.Fatal("early data was accepted for a ticket whose age does not fit")
		}
	})
}

// stdEarlyServer is crypto/tls's server, with the events that let it take
// early data.
func stdEarlyServer(config *stdtls.Config) *stdtls.QUICConn {
	return stdtls.QUICServer(&stdtls.QUICConfig{TLSConfig: config, EnableSessionEvents: true})
}

func TestQUICClientResumes(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	key := [32]byte{9, 9, 9}
	serverConfig := func(configure func(*stdtls.Config)) *stdtls.Config {
		return quicServerConfig(cert, func(c *stdtls.Config) {
			c.SessionTicketKey = key
			if configure != nil {
				configure(c)
			}
		})
	}
	ticket := stdtls.QUICSessionTicketOptions{EarlyData: true, Extra: [][]byte{[]byte("limits")}}

	// servers: a way to make the server of a connection, ours or crypto/tls's.
	servers := map[string]func(*stdtls.Config) func() quicEnd{
		"ours": func(c *stdtls.Config) func() quicEnd {
			s := NewQUICServer(c)
			return func() quicEnd { return s.NewConn() }
		},
		"crypto/tls": func(c *stdtls.Config) func() quicEnd {
			return func() quicEnd { return stdEarlyServer(c) }
		},
	}
	for name, makeServer := range servers {
		t.Run(name, func(t *testing.T) {
			// connect runs a client with the cache against a new server connection.
			connect := func(t *testing.T, cache QUICSessionCache, newServer func() quicEnd, clientConfig func(*stdtls.Config), opts quicRunOptions) (*quicSide, *quicSide, *QUICConn, quicEnd) {
				t.Helper()
				client := NewQUICClientWithSessions(quicClientConfig(pool, clientConfig), cache)
				if client == nil {
					t.Fatal("the client Config does not qualify")
				}
				server := newServer()
				opts.ticket = ticket
				cli, srv, err := quicRunWith(t, client, server, 4096, opts)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				return cli, srv, client, server
			}
			newServer := makeServer(serverConfig(nil))
			cache := NewQUICSessionCache(4)

			cli, _, client, _ := connect(t, cache, newServer, nil, quicRunOptions{})
			if cli.resumed != nil || client.ConnectionState().DidResume {
				t.Fatal("the first connection resumed")
			}
			if cache.Get("localhost") == nil {
				t.Fatal("no session was kept")
			}

			t.Run("accepted", func(t *testing.T) {
				cli, srv, client, server := connect(t, cache, newServer, nil, quicRunOptions{})
				if !client.ConnectionState().DidResume || !server.ConnectionState().DidResume {
					t.Fatal("not resumed")
				}
				if cli.resumed == nil || len(cli.resumed.Extra) != 1 || cli.resumed.Extra[0][0] != 1 || string(cli.resumed.Extra[0][1:]) != "server parameters" {
					t.Fatalf("the layer was asked about %+v", cli.resumed)
				}
				early := cli.writes[stdtls.QUICEncryptionLevelEarly]
				if len(early) == 0 || !bytes.Equal(early, srv.reads[stdtls.QUICEncryptionLevelEarly]) {
					t.Fatalf("early secrets: client %x, server %x", early, srv.reads[stdtls.QUICEncryptionLevelEarly])
				}
				if cli.rejected {
					t.Fatal("rejected")
				}
				if len(client.ConnectionState().PeerCertificates) != 1 {
					t.Fatal("no peer certificate on the resumed connection")
				}
				if cache.Get("localhost") == nil {
					t.Fatal("the resumed connection kept no session")
				}
			})

			// fresh fills the cache with a session of a full handshake.
			fresh := func(t *testing.T, newServer func() quicEnd) {
				t.Helper()
				cache.Put("localhost", nil)
				connect(t, cache, newServer, nil, quicRunOptions{})
				if cache.Get("localhost") == nil {
					t.Fatal("no session was kept")
				}
			}
			earlyOf := func(side *quicSide) int { return len(side.writes[stdtls.QUICEncryptionLevelEarly]) }

			t.Run("the client declines early data", func(t *testing.T) {
				fresh(t, newServer)
				cli, srv, client, server := connect(t, cache, newServer, nil, quicRunOptions{clientDeclines: true})
				if !client.ConnectionState().DidResume || !server.ConnectionState().DidResume {
					t.Fatal("not resumed")
				}
				if cli.resumed == nil || earlyOf(cli) != 0 || len(srv.reads[stdtls.QUICEncryptionLevelEarly]) != 0 || cli.rejected {
					t.Fatalf("asked %v, early secret %d, rejected %v", cli.resumed != nil, earlyOf(cli), cli.rejected)
				}
			})

			t.Run("the server declines early data", func(t *testing.T) {
				fresh(t, newServer)
				cli, srv, client, server := connect(t, cache, newServer, nil, quicRunOptions{serverDeclines: true})
				if !client.ConnectionState().DidResume || !server.ConnectionState().DidResume {
					t.Fatal("not resumed")
				}
				if earlyOf(cli) == 0 || !cli.rejected || len(srv.reads[stdtls.QUICEncryptionLevelEarly]) != 0 {
					t.Fatalf("early secret %d, rejected %v", earlyOf(cli), cli.rejected)
				}
			})

			t.Run("a server that does not know the ticket", func(t *testing.T) {
				fresh(t, newServer)
				other := makeServer(quicServerConfig(cert, func(c *stdtls.Config) { c.SessionTicketKey = [32]byte{7} }))
				cli, _, client, _ := connect(t, cache, other, nil, quicRunOptions{})
				if client.ConnectionState().DidResume {
					t.Fatal("resumed")
				}
				if earlyOf(cli) == 0 || !cli.rejected {
					t.Fatalf("early secret %d, rejected %v", earlyOf(cli), cli.rejected)
				}
				if len(client.ConnectionState().PeerCertificates) != 1 {
					t.Fatal("the certificate was not checked")
				}
				if cache.Get("localhost") == nil {
					t.Fatal("the new session was not kept")
				}
			})

			t.Run("a HelloRetryRequest", func(t *testing.T) {
				fresh(t, newServer)
				retry := makeServer(serverConfig(func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }))
				cli, _, client, server := connect(t, cache, retry, nil, quicRunOptions{})
				if !client.ConnectionState().DidResume || !server.ConnectionState().DidResume {
					t.Fatal("the session was not resumed after the retry")
				}
				if !cli.rejected {
					t.Fatal("early data survived a HelloRetryRequest")
				}
			})

			t.Run("a session that has expired", func(t *testing.T) {
				fresh(t, newServer)
				s := cache.Get("localhost")
				s.received = s.received.Add(-2 * s.lifetime)
				cli, _, client, _ := connect(t, cache, newServer, nil, quicRunOptions{})
				if cli.resumed != nil || client.ConnectionState().DidResume {
					t.Fatal("an expired session was offered")
				}
			})

			t.Run("a ticket is used once", func(t *testing.T) {
				fresh(t, newServer)
				connect(t, cache, newServer, nil, quicRunOptions{})
				s := cache.Get("localhost")
				connect(t, cache, newServer, nil, quicRunOptions{})
				if cache.Get("localhost") == s {
					t.Fatal("the session that was used is still in the cache")
				}
			})
		})
	}
}
