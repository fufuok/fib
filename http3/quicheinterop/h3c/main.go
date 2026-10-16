// Command h3c sends requests to an HTTP/3 echo server with fib's client:
// by default a load of connections that each handshake and echo bodies of
// several sizes, with -verify instead the certificate checks (the server's
// own certificate as the only root, a root that is not the server's, a name
// the certificate does not carry).
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:3301", "server")
	blocking := flag.Bool("blocking", false, "use crypto/tls's QUICConn for the handshake")
	conns := flag.Int("n", 200, "connections, each with its own handshake")
	verify := flag.Bool("verify", false, "check certificate verification (RootCAs, ServerName) instead of the load")
	resume := flag.Int("resume", 0, "connect this many times in turn, each resuming the session of the one before, instead of the load")
	wantEarly := flag.Bool("want-early", false, "with -resume, require the requests after the first to go as 0-RTT data the server accepts")
	flag.Parse()

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	go engine.Run()
	time.Sleep(200 * time.Millisecond)

	config := http3.DefaultClientConfig()
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	config.BlockingTLSHandshake = *blocking
	config.StreamResponseBody = true
	config.Timeout = 10 * time.Second

	var ok, bad atomic.Int64
	var firstBad atomic.Value
	sizes := []int{0, 1, 1024, 70000, 300000}
	echo := func(client *http3.Client, size int) error {
		body := make([]byte, size)
		rand.Read(body)
		req, _ := stdhttp.NewRequest(stdhttp.MethodPost, "https://"+*addr+"/echo", bytes.NewReader(body))
		done := make(chan struct{})
		var got []byte
		var rerr error
		client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
			if err != nil {
				rerr = err
				close(done)
				return
			}
			if resp.StatusCode != 200 {
				rerr = fmt.Errorf("status %d", resp.StatusCode)
			}
			resp.OnBody(func(data []byte, fin bool, err error) {
				got = append(got, data...)
				if err != nil {
					rerr = err
				}
				if fin {
					close(done)
				}
			})
		})
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			rerr = fmt.Errorf("timeout")
		}
		if rerr != nil {
			return rerr
		}
		if !bytes.Equal(got, body) {
			return fmt.Errorf("echoed %d bytes, want %d, and not the same", len(got), len(body))
		}
		return nil
	}
	post := func(client *http3.Client, size int) {
		if err := echo(client, size); err != nil {
			bad.Add(1)
			firstBad.CompareAndSwap(nil, fmt.Sprintf("size %d: %v", size, err))
			return
		}
		ok.Add(1)
	}

	if *verify {
		os.Exit(verifyCertificates(engine, config, echo))
	}
	if *resume > 0 {
		os.Exit(resumeSessions(engine, config, *addr, *resume, *wantEarly, *blocking))
	}

	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			client := http3.NewClient(engine, config) // a handshake apiece
			defer client.Close()
			post(client, sizes[i%len(sizes)])
			// and the same connection again
			post(client, sizes[(i+1)%len(sizes)])
		}(i)
	}
	wg.Wait()
	fmt.Printf("blocking=%v: %d requests echoed correctly, %d failed, %v for %d connections\n",
		*blocking, ok.Load(), bad.Load(), time.Since(start).Round(time.Millisecond), *conns)
	if bad.Load() > 0 {
		fmt.Println("first failure:", firstBad.Load())
		os.Exit(1)
	}
}

// verifyCertificates learns the server's certificate through
// VerifyPeerCertificate, then checks that a client given exactly that
// certificate as RootCAs connects, and that a client given another root, or
// asking for a name the certificate lacks, does not. It returns the exit code.
func verifyCertificates(engine *fib.Engine, base http3.ClientConfig, echo func(*http3.Client, int) error) int {
	var leaf []byte
	learn := base
	learn.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: "localhost",
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			leaf = append([]byte(nil), raw[0]...)
			return nil
		}}
	c := http3.NewClient(engine, learn)
	err := echo(c, 1024)
	c.Close()
	if err != nil || leaf == nil {
		fmt.Println("FAIL: could not learn the server's certificate:", err)
		return 1
	}
	cert, err := x509.ParseCertificate(leaf)
	if err != nil {
		fmt.Println("FAIL: the server's certificate does not parse:", err)
		return 1
	}
	fmt.Printf("server certificate: subject %q, DNS names %v\n", cert.Subject.CommonName, cert.DNSNames)

	other := x509.NewCertPool()
	{
		// a root that is not the server's: a certificate made here
		other.AddCert(selfSigned())
	}
	trusted := x509.NewCertPool()
	trusted.AddCert(cert)

	cases := []struct {
		name    string
		tls     *tls.Config
		wantErr bool
	}{
		{"the server's certificate as RootCAs", &tls.Config{RootCAs: trusted, ServerName: "localhost"}, false},
		{"a root that is not the server's", &tls.Config{RootCAs: other, ServerName: "localhost"}, true},
		{"the right root, a name the certificate lacks", &tls.Config{RootCAs: trusted, ServerName: "example.invalid"}, true},
		{"no RootCAs set (the system roots)", &tls.Config{ServerName: "localhost"}, true},
	}
	code := 0
	for _, blocking := range []bool{false, true} {
		for _, tc := range cases {
			cfg := base
			cfg.TLSConfig = tc.tls
			cfg.BlockingTLSHandshake = blocking
			cfg.Timeout = 5 * time.Second
			c := http3.NewClient(engine, cfg)
			err := echo(c, 1024)
			c.Close()
			verdict := "ok"
			if (err != nil) != tc.wantErr {
				verdict = "FAIL"
				code = 1
			}
			fmt.Printf("%-4s blocking=%-5v %-46s -> %v\n", verdict, blocking, tc.name, err)
		}
	}
	return code
}

// selfSigned is a certificate for localhost that the server does not have.
func selfSigned() *x509.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "somebody else"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, DNSNames: []string{"localhost"}}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	cert, _ := x509.ParseCertificate(der)
	return cert
}

// resumeSessions connects n times, each connection a new client that resumes
// the session of the one before and, if the session allows, sends its GET as
// 0-RTT data. It returns the exit code.
func resumeSessions(engine *fib.Engine, base http3.ClientConfig, addr string, n int, wantEarly, blocking bool) int {
	cache := fibtls.NewQUICSessionCache(4)
	// crypto/tls's handshake keeps its sessions in the TLS config's cache.
	stdCache := tls.NewLRUClientSessionCache(4)
	code := 0
	for i := 0; i < n; i++ {
		cfg := base
		cfg.EarlyData = true
		cfg.SessionCache = cache
		if blocking {
			cfg.TLSConfig = cfg.TLSConfig.Clone()
			cfg.TLSConfig.ClientSessionCache = stdCache
		}
		client := http3.NewClient(engine, cfg)
		req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "https://"+addr+"/echo", nil)
		resp, err := client.Go(req).Wait()
		if err != nil {
			fmt.Printf("FAIL round %d: %v\n", i, err)
			client.Close()
			return 1
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		st := client.Stats()
		resumed := resp.TLS != nil && resp.TLS.DidResume
		fmt.Printf("round %d: status=%d resumed=%v early-sent=%d accepted=%d rejected=%d too-early=%d\n",
			i, resp.StatusCode, resumed, st.EarlySent, st.EarlyAccepted, st.EarlyRejected, st.TooEarly)
		want := i > 0
		if resp.StatusCode != 200 || resumed != want {
			fmt.Println("FAIL: round", i, "should resume:", want)
			code = 1
		}
		if wantEarly && i > 0 && (st.EarlySent != 1 || st.EarlyAccepted != 1) {
			fmt.Println("FAIL: round", i, "did not send its request as accepted 0-RTT data")
			code = 1
		}
		if !wantEarly && st.EarlySent != 0 {
			fmt.Println("FAIL: round", i, "sent early data to a server that does not offer it")
			code = 1
		}
		client.Close()
		// The ticket comes after the handshake.
		if blocking {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		for deadline := time.Now().Add(3 * time.Second); cache.Get("localhost") == nil && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}
	}
	return code
}
