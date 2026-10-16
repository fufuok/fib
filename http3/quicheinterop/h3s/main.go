// Command h3s is a fib HTTP/3 echo server on the benchmark's ports.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

func main() {
	blocking := flag.Bool("blocking", false, "use crypto/tls's QUICConn for the handshake")
	early := flag.Bool("early", false, "accept 0-RTT data, serving only safe methods in it")
	unsafeEarly := flag.Bool("early-unsafe", false, "with -early, serve every method in 0-RTT data")
	flag.Parse()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"}}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}

	config := http3.DefaultConfig()
	config.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	config.BlockingTLSHandshake = *blocking
	config.EarlyData = *early || *unsafeEarly
	config.EarlyDataUnsafe = *unsafeEarly
	config.MaxDatagramSize = 1350
	config.MaxConcurrentStreams = 100
	handler := http3.NewHandlerWithConfig(config, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/early" {
			_ = c.Respond(200, "text/plain", []byte("early="+c.Request.Header.Get("Early-Data")))
			return
		}
		if c.Request.URL.Path != "/echo" {
			_ = c.Respond(404, "text/plain", []byte("not found\n"))
			return
		}
		_ = c.Respond(200, "application/octet-stream", c.Body())
	}))
	var addrs []string
	for p := 11001; p <= 11050; p++ {
		addrs = append(addrs, fmt.Sprintf("0.0.0.0:%d", p))
	}
	ec := fib.DefaultConfig()
	ec.UDPIdleTimeout = 150 * time.Second
	engine, err := fib.New(ec, fib.Binding{Network: "udp", Addrs: addrs, Handler: handler})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go engine.Run()
	fmt.Println("h3s listening, blocking =", *blocking)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	engine.Stop()
}
