//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// holdingServer answers each request with cvCliOK once release is closed,
// over keep-alive connections, and counts the connections it accepted.
func holdingServer(t *testing.T, release <-chan struct{}) (string, *atomic.Int32) {
	var conns atomic.Int32
	u := rawServer(t, func(conn net.Conn, _ int) {
		conns.Add(1)
		r := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(r); err != nil {
				return
			}
			<-release
			if _, err := io.WriteString(conn, cvCliOK); err != nil {
				return
			}
		}
	})
	return u, &conns
}

// sendAll sends n requests to u at once and returns their outcomes, which
// come once release is closed for those let in, and at once for those
// refused.
func sendAll(t *testing.T, client *Client, u string, n int) chan error {
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		client.Do(mustRequest(t, "GET", u, nil), func(_ *ClientResponse, err error) { results <- err })
	}
	return results
}

// A burst to a cold host within MaxConnsPerHost is let in whole, however
// small MaxPendingPerHost: the requests the client is dialing connections
// for are not waiting beyond them.
func TestMaxPendingPerHostColdBurst(t *testing.T) {
	release := make(chan struct{})
	u, conns := holdingServer(t, release)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 256
	config.MaxPendingPerHost = 8
	client := newTestClient(t, config)
	results := sendAll(t, client, u, 256)
	deadline := time.Now().Add(10 * time.Second)
	for conns.Load() < 256 && time.Now().Before(deadline) && len(results) == 0 {
		time.Sleep(time.Millisecond)
	}
	if n := conns.Load(); n != 256 {
		t.Errorf("%d connections opened, want 256", n)
	}
	close(release)
	for i := 0; i < 256; i++ {
		if err := cvCliWait(t, results, "a request of the burst"); err != nil {
			t.Fatalf("a request of the burst got %v", err)
		}
	}
}

// With MaxConnsPerHost 1 and MaxPendingPerHost 2, a cold host takes the
// request its connection is dialed for and two more, and turns the fourth
// away: the queue is bounded at the two together.
func TestMaxPendingPerHostWhileDialing(t *testing.T) {
	release := make(chan struct{})
	u, _ := holdingServer(t, release)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	config.MaxPendingPerHost = 2
	client := newTestClient(t, config)
	results := sendAll(t, client, u, 4)
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the first outcome is %v, want %v", err, ErrTooManyPending)
	}
	close(release)
	for i := 0; i < 3; i++ {
		if err := cvCliWait(t, results, "the requests let in"); err != nil {
			t.Fatalf("a request let in got %v", err)
		}
	}
}

// For a host that may speak HTTP/2, which the client dials once for until it
// knows, a dial that hangs leaves room for the connections MaxConnsPerHost
// allows and MaxPendingPerHost more, and no more.
func TestMaxPendingPerHostMaybeHTTP2(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// The TLS handshake never gets an answer.
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 4
	config.MaxPendingPerHost = 2
	client := newTestClient(t, config)
	if !client.mayMultiplex(hostTarget{secure: true}) {
		t.Fatal("the client does not offer HTTP/2")
	}
	results := sendAll(t, client, "https://"+listener.Addr().String()+"/", 7)
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the first outcome is %v, want %v", err, ErrTooManyPending)
	}
	select {
	case err := <-results:
		t.Fatalf("a second request ended with %v while the dial hangs", err)
	case <-time.After(100 * time.Millisecond):
	}
}
