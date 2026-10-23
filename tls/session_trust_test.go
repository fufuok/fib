package tls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"testing"
	"time"
)

// A session is offered to a server only if the Config would trust the server
// it was made with: any session if the Config does not verify, otherwise one
// whose chain was verified, is current, is the ServerName's and has its root
// among the RootCAs the Config names.
func TestSessionTrusted(t *testing.T) {
	ca := newTestCA(t, "session CA")
	other := newTestCA(t, "session other CA")
	leaf, err := x509.ParseCertificate(ca.issue(t, "ecdsa-p256", "localhost", false).Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	verified := [][]*x509.Certificate{{leaf, ca.cert}}
	later := func() time.Time { return time.Now().Add(48 * time.Hour) }
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		chains [][]*x509.Certificate
		want   bool
	}{
		{"verified", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool}, verified, true},
		{"the system's roots", &stdtls.Config{ServerName: "localhost"}, verified, true},
		{"not verified, nor now", &stdtls.Config{ServerName: "localhost", InsecureSkipVerify: true}, nil, true},
		{"not verified", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool}, nil, false},
		{"another server's", &stdtls.Config{ServerName: "example.com", RootCAs: ca.pool}, verified, false},
		{"expired", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool, Time: later}, verified, false},
		{"a root no longer trusted", &stdtls.Config{ServerName: "localhost", RootCAs: other.pool}, verified, false},
	} {
		h, err := newClientHS(tc.config)
		if err != nil {
			t.Fatal(err)
		}
		if got := h.sessionTrusted(&QUICSession{chains: tc.chains}); got != tc.want {
			t.Errorf("%s: trusted %v, want %v", tc.name, got, tc.want)
		}
	}
}
