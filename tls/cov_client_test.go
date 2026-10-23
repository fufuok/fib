//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"crypto/rand"
	stdtls "crypto/tls"
	"io"
	"testing"
	"time"
)

// covCliRand is a Config.Rand that reads crypto/rand until it is told to
// fail.
type covCliRand struct{ fail bool }

func (r *covCliRand) Read(p []byte) (int, error) {
	if r.fail {
		return 0, io.ErrUnexpectedEOF
	}
	return rand.Read(p)
}

// covCliCustomRand makes crypto/ecdh read the Config's Rand, as it ignores
// it otherwise since Go 1.26, so that a failing one reaches key generation.
func covCliCustomRand(t *testing.T) {
	t.Helper()
	t.Setenv("GODEBUG", "cryptocustomrand=1")
}

// A Config whose suites leave no TLS 1.2 suite to offer offers TLS 1.3 alone.
func TestCovCliNoTLS12Suites(t *testing.T) {
	h, err := newClientHS(&stdtls.Config{ServerName: "localhost", CipherSuites: []uint16{stdtls.TLS_AES_128_GCM_SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.suites12) != 0 || h.offers(stdtls.VersionTLS12) || !h.offers(stdtls.VersionTLS13) {
		t.Fatalf("suites12 %v, versions %x", h.suites12, h.versions)
	}
	// A TLS 1.2 suite among them keeps TLS 1.2 offered.
	h, err = newClientHS(&stdtls.Config{ServerName: "localhost", CipherSuites: []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	if !h.offers(stdtls.VersionTLS12) {
		t.Fatalf("TLS 1.2 not offered: %x", h.versions)
	}
}

// A Rand that runs out at the key share fails the handshake before it starts.
func TestCovCliKeyShareRandFails(t *testing.T) {
	covCliCustomRand(t)
	// The client random and the session ID take 64 bytes; the key share finds
	// none left.
	if _, err := newClientHS(&stdtls.Config{ServerName: "localhost", Rand: &shortRand{n: 64}}); err == nil {
		t.Fatal("a Rand that ran out at the key share was not noticed")
	}
	if _, err := newClientHS(&stdtls.Config{ServerName: "localhost", Rand: &covCliRand{}}); err != nil {
		t.Fatalf("a working Rand: %v", err)
	}
}

// A Rand that fails when a HelloRetryRequest asks for a share of another group
// fails the handshake with internal_error.
func TestCovCliHRRKeyShareRandFails(t *testing.T) {
	covCliCustomRand(t)
	config, pool := ecdsaServer(t)
	r := &covCliRand{}
	cli := newHS(t, pool, func(c *stdtls.Config) { c.Rand = r })
	f := newFlight13(t, cli, config)
	hrr := parseHelloFields(f.hello).helloRetry(msgExt{extKeyShare, []byte{0, byte(stdtls.CurveP256)}})
	hrr.suite = stdtls.TLS_AES_128_GCM_SHA256
	r.fail = true
	_, _, herr := f.send(cli, hrr.bytes(), nil)
	wantAlert(t, "HelloRetryRequest key share", herr, alertInternalError)
	if cli.retried {
		t.Error("the HelloRetryRequest was taken")
	}
}

// A Rand that fails at the TLS 1.2 client's key exchange fails the handshake
// with internal_error, and nothing is sent.
func TestCovCliTLS12KeyExchangeRandFails(t *testing.T) {
	covCliCustomRand(t)
	config, pool := ecdsaServer(t)
	r := &covCliRand{}
	cli := newCli12(t, pool, func(c *stdtls.Config) { c.Rand = r })
	f := newFlight12(t, cli, config)
	r.fail = true
	out, _, herr := f.send(cli, f.msgs...)
	wantAlert(t, "TLS 1.2 key exchange", herr, alertInternalError)
	if len(out) != 0 {
		t.Errorf("%d bytes sent", len(out))
	}
}

// With Config.Time, sessions are judged by the time it tells.
func TestCovCliNowIsConfigTime(t *testing.T) {
	received := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		later time.Duration
		want  bool
	}{
		{"within the lifetime", 30 * time.Minute, true},
		{"past the lifetime", 2 * time.Hour, false},
	} {
		now := received.Add(tc.later)
		h, err := newClientHS(&stdtls.Config{ServerName: "localhost", InsecureSkipVerify: true, Time: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		if !h.now().Equal(now) {
			t.Fatalf("now %v, want %v", h.now(), now)
		}
		h.sessions = NewQUICSessionCache(1)
		h.sessions.Put("localhost", &QUICSession{ticket: []byte("ticket"), psk: make([]byte, 32),
			suite: stdtls.TLS_AES_128_GCM_SHA256, received: received, lifetime: time.Hour})
		if got := h.pickSession(); got != tc.want {
			t.Errorf("%s: picked %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Without Config.Time, sessions are judged by the clock.
func TestCovCliNowIsTheClock(t *testing.T) {
	for _, tc := range []struct {
		name     string
		received time.Time
		want     bool
	}{
		{"fresh", time.Now(), true},
		{"expired", time.Now().Add(-2 * time.Hour), false},
	} {
		h, err := newClientHS(&stdtls.Config{ServerName: "localhost", InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		if now := h.now(); time.Since(now) > time.Minute || time.Until(now) > time.Minute {
			t.Fatalf("now %v", now)
		}
		h.sessions = NewQUICSessionCache(1)
		h.sessions.Put("localhost", &QUICSession{ticket: []byte("ticket"), psk: make([]byte, 32),
			suite: stdtls.TLS_AES_128_GCM_SHA256, received: tc.received, lifetime: time.Hour})
		if got := h.pickSession(); got != tc.want {
			t.Errorf("%s: picked %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A ServerHello's pre_shared_key that is not a two-byte index is malformed.
func TestCovCliMalformedPreSharedKey(t *testing.T) {
	config, pool := ecdsaServer(t)
	for _, data := range [][]byte{{0}, {0, 0, 0}} {
		cli := newHS(t, pool, nil)
		cli.sessions = NewQUICSessionCache(1)
		cli.session = &QUICSession{ticket: []byte("ticket"), psk: make([]byte, 48), suite: stdtls.TLS_AES_256_GCM_SHA384,
			received: time.Now(), lifetime: time.Hour}
		f := newFlight13(t, cli, config)
		sh := parseHelloFields(f.hello).with(extPreSharedKey, data)
		sh.suite = stdtls.TLS_AES_256_GCM_SHA384
		_, _, herr := f.send(cli, sh.bytes(), nil)
		wantAlert(t, "pre_shared_key of "+string(rune('0'+len(data)))+" bytes", herr, alertDecodeError)
	}
}

// A QUIC server's EncryptedExtensions must carry its transport parameters.
func TestCovCliQUICTransportParameters(t *testing.T) {
	h := &clientHS{config: &stdtls.Config{}, quic: &quicCtx{}}
	herr := h.encryptedExtensions(hsMsg(hsEncryptedExtensions, []byte{0, 0}))
	wantAlert(t, "no quic_transport_parameters", herr, alertMissingExtension)
	if len(h.quic.events) != 0 {
		t.Errorf("events %v", h.quic.events)
	}

	h = &clientHS{config: &stdtls.Config{}, quic: &quicCtx{}}
	body := []byte{0, 6, 0, extQUICParams, 0, 2, 7, 8}
	if herr := h.encryptedExtensions(hsMsg(hsEncryptedExtensions, body)); herr != nil {
		t.Fatal(herr)
	}
	if ev := h.quic.events; len(ev) != 1 || ev[0].Kind != stdtls.QUICTransportParameters || string(ev[0].Data) != "\x07\x08" {
		t.Fatalf("events %v", ev)
	}
	if h.state != hsExpectCertificateOrRequest {
		t.Errorf("state %v", h.state)
	}
}
