package certs

import (
	"crypto/x509"
	"encoding/pem"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func serverFlags(t *testing.T, args ...string) ServerFlags {
	t.Helper()
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	f := ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f
}

func clientFlags(t *testing.T, args ...string) ClientFlags {
	t.Helper()
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	f := ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f
}

// A server given -cert and -key serves that pair, and a client trusts the
// certificate a server wrote for it.
func TestServerLoadsAKeyPairAndClientTrustsIt(t *testing.T) {
	dir := t.TempDir()
	issued, err := serverFlags(t, "-cert-out", "").Config()
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	key, err := x509.MarshalPKCS8PrivateKey(issued.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issued.Certificates[0].Certificate[0]}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := serverFlags(t, "-cert", certFile, "-key", keyFile).Config()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Certificates) != 1 {
		t.Fatalf("%d certificates", len(loaded.Certificates))
	}
	client, err := clientFlags(t, "-ca", certFile).Config()
	if err != nil || client.RootCAs == nil {
		t.Fatalf("client: %v", err)
	}
	insecure, err := clientFlags(t, "-insecure").Config()
	if err != nil || !insecure.InsecureSkipVerify {
		t.Fatalf("insecure client: %v", err)
	}
	if system, err := clientFlags(t, "-ca", "").Config(); err != nil || system.RootCAs != nil {
		t.Fatalf("system roots: %v", err)
	}
}

// The errors are each reported: a certificate that cannot be written, a CA
// file that is not there, and one with no certificate in it.
func TestConfigErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := serverFlags(t, "-cert-out", filepath.Join(dir, "missing", "cert.pem")).Config(); err == nil {
		t.Error("wrote a certificate into a directory that is not there")
	}
	if _, err := clientFlags(t, "-ca", filepath.Join(dir, "missing.pem")).Config(); err == nil {
		t.Error("trusted a file that is not there")
	}
	empty := filepath.Join(dir, "empty.pem")
	if err := os.WriteFile(empty, []byte("no certificate here"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := clientFlags(t, "-ca", empty).Config(); err == nil {
		t.Error("trusted a file with no certificate in it")
	}
	// The flag set's default -cert-out can be chosen.
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	ServerFlagsWithOut(flags, "elsewhere.pem")
	if got := flags.Lookup("cert-out").DefValue; got != "elsewhere.pem" {
		t.Errorf("-cert-out default %q", got)
	}
}
