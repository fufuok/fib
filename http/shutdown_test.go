//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// shutdownServer serves handler on an engine the test shuts down itself, and
// returns it, its address, and what its Run returns.
func shutdownServer(t *testing.T, handler fib.Handler) (*fib.Engine, string, chan error) {
	t.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine, addr.String(), done
}

// slowHandler answers /slow once release is closed, telling entered when it
// starts, and anything else at once.
func slowHandler(entered chan<- struct{}, release <-chan struct{}) HandlerFunc {
	return func(c *Context) {
		if c.Request.URL.Path == "/slow" {
			entered <- struct{}{}
			<-release
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("slow"))
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}
}

// awaitShuttingDown waits for Shutdown, started on another goroutine, to
// have begun.
func awaitShuttingDown(t *testing.T, engine *fib.Engine) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !engine.ShuttingDown() {
		if time.Now().After(deadline) {
			t.Fatal("Shutdown did not begin")
		}
		time.Sleep(time.Millisecond)
	}
}

// expectClosed checks that the server closes c within a few seconds.
func expectClosed(t *testing.T, c net.Conn, what string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("%s: read %d bytes, %v; want the connection closed", what, n, err)
	}
}

// TestShutdownHTTP1 checks Shutdown against HTTP/1, as net/http.Server's
// Shutdown behaves: new clients are refused, a connection idle between
// requests is closed, a request in flight is answered with Connection: close,
// and Shutdown returns once that response has gone and its connection closed.
func TestShutdownHTTP1(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	engine, addr, done := shutdownServer(t, NewHandler(slowHandler(entered, release)))

	idle, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	_ = idle.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(idle, "GET /fast HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(idle)
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	client, _ := stdClient(t)
	type result struct {
		resp *stdhttp.Response
		body string
		err  error
	}
	slow := make(chan result, 1)
	go func() {
		resp, err := client.Get("http://" + addr + "/slow")
		if err != nil {
			slow <- result{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		slow <- result{resp: resp, body: string(body), err: err}
	}()
	<-entered

	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	awaitShuttingDown(t, engine)

	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("a connection was taken after Shutdown began")
	}
	_ = idle.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := reader.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("the idle connection read %d bytes, %v; want it closed", n, err)
	}
	select {
	case err := <-shut:
		t.Fatalf("Shutdown returned %v with a request in flight", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	r := <-slow
	if r.err != nil {
		t.Fatalf("request in flight: %v", r.err)
	}
	if r.resp.StatusCode != stdhttp.StatusOK || r.body != "slow" || !r.resp.Close {
		t.Errorf("request in flight: %d %q, Close %v; want 200 \"slow\" closing the connection",
			r.resp.StatusCode, r.body, r.resp.Close)
	}
	select {
	case err := <-shut:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return once the last response had gone")
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestShutdownHTTP1Quiet checks that a connection that has sent nothing is
// given shutdownQuietGrace to send its first request, which is answered, and
// is closed once the grace has passed without one.
func TestShutdownHTTP1Quiet(t *testing.T) {
	grace := shutdownQuietGrace
	shutdownQuietGrace = 300 * time.Millisecond
	t.Cleanup(func() { shutdownQuietGrace = grace })
	engine, addr, done := shutdownServer(t, NewHandler(slowHandler(nil, nil)))

	late, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	quiet, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer quiet.Close()
	// Both connections are open on the server before Shutdown begins.
	for _, c := range []net.Conn{late, quiet} {
		_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		_, _ = c.Read(make([]byte, 1))
	}

	shut := make(chan error, 1)
	start := time.Now()
	go func() { shut <- engine.Shutdown(context.Background()) }()
	awaitShuttingDown(t, engine)

	time.Sleep(shutdownQuietGrace / 3)
	_ = late.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(late, "GET /late HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(late), nil)
	if err != nil {
		t.Fatalf("a request sent within the grace: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusOK || !resp.Close {
		t.Errorf("a request sent within the grace: %d, Close %v; want 200 closing the connection",
			resp.StatusCode, resp.Close)
	}

	expectClosed(t, quiet, "the quiet connection")
	if err := <-shut; err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed < shutdownQuietGrace {
		t.Errorf("Shutdown returned after %v, within the grace of %v", elapsed, shutdownQuietGrace)
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestShutdownHTTP2 checks Shutdown against HTTP/2: the connection sends
// GOAWAY, finishes the stream in flight, and closes, and Shutdown returns
// then.
func TestShutdownHTTP2(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	engine, addr, done := shutdownServer(t, NewHandler(slowHandler(entered, release)))
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	transport := &stdhttp.Transport{Protocols: &protocols}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: transport}

	resp, err := client.Get("http://" + addr + "/fast")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("served over %s, want HTTP/2", resp.Proto)
	}
	slow := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + addr + "/slow")
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil && (resp.StatusCode != stdhttp.StatusOK || string(body) != "slow") {
				err = errors.New("answered " + resp.Status + " " + string(body))
			}
		}
		slow <- err
	}()
	<-entered

	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	awaitShuttingDown(t, engine)
	select {
	case err := <-shut:
		t.Fatalf("Shutdown returned %v with a stream in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatalf("stream in flight: %v", err)
	}
	select {
	case err := <-shut:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return once the last stream had finished")
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	done <- nil
}

// TestShutdownOverTLS checks that the TLS handler passes the shutdown on to
// the HTTP server it wraps, which closes its idle connections.
func TestShutdownOverTLS(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	for _, proto := range []string{"http/1.1", "h2"} {
		engine, addr, done := shutdownServer(t, fibtls.NewServer(ConfigureTLS(serverConfig.Clone()), NewHandler(slowHandler(nil, nil))))
		config := clientConfig.Clone()
		config.NextProtos = []string{proto}
		transport := &stdhttp.Transport{TLSClientConfig: config, ForceAttemptHTTP2: proto == "h2"}
		client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: transport}
		resp, err := client.Get("https://" + addr + "/fast")
		if err != nil {
			t.Fatalf("%s: %v", proto, err)
		}
		resp.Body.Close()
		if want := map[string]int{"http/1.1": 1, "h2": 2}[proto]; resp.ProtoMajor != want {
			t.Fatalf("%s: served over %s", proto, resp.Proto)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("%s: Shutdown: %v", proto, err)
		}
		cancel()
		transport.CloseIdleConnections()
		if err := <-done; err != nil {
			t.Errorf("%s: Run: %v", proto, err)
		}
		done <- nil
	}
}
