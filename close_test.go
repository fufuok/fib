package fib

import (
	"io"
	"net"
	"testing"
	"time"
)

// TestCloseDoesNotWaitForAHandler checks that Close cuts a connection whose
// handler is still running rather than wait for it: Close returns at once,
// the peer sees its connection end without the reply the handler sends late,
// and that send is refused.
func TestCloseDoesNotWaitForAHandler(t *testing.T) {
	for _, pollers := range []int{0, 2} {
		entered, release := make(chan struct{}), make(chan struct{})
		sent := make(chan error, 1)
		engine, addr, done := startShutdownEngine(t, pollers, HandlerFuncs{Data: func(c *Connection, b []byte) {
			close(entered)
			<-release
			sent <- c.Send([]byte("late"))
		}})
		client, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		<-entered
		engine.Stop()
		if err := <-done; err != nil {
			t.Errorf("pollers %d: Run: %v", pollers, err)
		}

		start := time.Now()
		if err := engine.Close(); err != nil {
			t.Errorf("pollers %d: Close: %v", pollers, err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("pollers %d: Close took %v with a handler running", pollers, elapsed)
		}
		_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
		if n, err := client.Read(make([]byte, 8)); err == nil {
			t.Errorf("pollers %d: read %d bytes after Close; want the connection ended", pollers, n)
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Errorf("pollers %d: the connection was still open after Close", pollers)
		}

		close(release)
		if err := <-sent; err == nil {
			t.Errorf("pollers %d: a send after Close was taken", pollers)
		}
		if n, err := client.Read(make([]byte, 8)); err != io.EOF && n > 0 {
			t.Errorf("pollers %d: the late reply reached the peer", pollers)
		}
		_ = client.Close()
	}
}

// TestCloseReleasesAtOnceWhenIdle checks that Close, with no handler
// running, has released everything by the time it returns: the port it
// listened on can be bound again.
func TestCloseReleasesAtOnceWhenIdle(t *testing.T) {
	engine, addr, done := startShutdownEngine(t, 0, shutdownEcho())
	client, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := echoOnce(client, "x"); err != nil {
		t.Fatal(err)
	}
	engine.Stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Addr = addr
	again, err := Bind(config, shutdownEcho())
	if err != nil {
		t.Fatalf("the port is still held after Close: %v", err)
	}
	_ = again.Close()
}
