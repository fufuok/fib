//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// unsized hides a reader's length, so that its request goes out without a
// Content-Length: chunked on HTTP/1.1, and ended by its last frame on HTTP/2
// and HTTP/3.
type unsized struct{ r io.Reader }

func (u unsized) Read(p []byte) (int, error) { return u.r.Read(p) }

// TestStreamedUploadOfUnknownLength sends bodies of no declared length, which
// the gateway forwards as they arrive, on every protocol to every upstream.
func TestStreamedUploadOfUnknownLength(t *testing.T) {
	l := newLab(t, labOptions{})
	body := pattern(6 << 20)
	sum := sha256.Sum256(body)
	want := fmt.Sprintf("received %d bytes sha256=%s", len(body), hex.EncodeToString(sum[:]))
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(client.name+"/"+upstream, func(t *testing.T) {
				req := request(t, "POST", l.url("/"+upstream+"/upload"), nil)
				req.Body = io.NopCloser(unsized{strings.NewReader(string(body))})
				r := get(t, client.doer, req)
				if r.err != nil || r.status != 200 || !strings.Contains(string(r.body), want) {
					t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
				}
			})
		}
	}
}

// TestUploadIsForwardedAsItArrives sends the start of a body and then waits:
// the upstream answers on the first bytes it sees, which reach it only if the
// gateway forwards them without waiting for the rest.
func TestUploadIsForwardedAsItArrives(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			for _, declared := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/declared=%v", client.name, upstream, declared), func(t *testing.T) {
					pr, pw := io.Pipe()
					defer pw.Close()
					req := request(t, "POST", l.url("/"+upstream+"/early"), nil)
					req.Body = pr
					if declared {
						// Past what the clients read whole before sending.
						req.ContentLength = 8 << 20
					}
					go func() { _, _ = pw.Write(pattern(256 << 10)) }()
					r := get(t, client.doer, req)
					if r.err != nil || r.status != 200 || string(r.body) != "early" {
						t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
					}
				})
			}
		}
	}
}

// TestStreamedUploadTooLarge sends a body of no declared length past
// MaxRequestBody, which the gateway finds out only on its way upstream.
func TestStreamedUploadTooLarge(t *testing.T) {
	l := newLab(t, labOptions{maxRequestBody: 1 << 20})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			req := request(t, "POST", l.url("/h1/upload"), nil)
			req.Body = io.NopCloser(unsized{strings.NewReader(string(pattern(4 << 20)))})
			r := get(t, client.doer, req)
			if r.err != nil && client.name == "HTTP/1.1" {
				// The server closes an HTTP/1.1 connection whose body outgrew
				// its limit, which may reset the client before it reads the
				// answer; see TestRequestBodyTooLarge.
				return
			}
			if r.err != nil || r.status != stdhttp.StatusRequestEntityTooLarge {
				t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
			}
		})
	}
}

// TestSmallBodyTooLarge sends a body that arrives with its request, under the
// size from which bodies stream, past a MaxRequestBody smaller still.
func TestSmallBodyTooLarge(t *testing.T) {
	l := newLab(t, labOptions{maxRequestBody: 1 << 10})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			r := get(t, client.doer, request(t, "POST", l.url("/h1/upload"), pattern(8<<10)))
			if r.err != nil || r.status != stdhttp.StatusRequestEntityTooLarge {
				t.Fatalf("err %v, status %d", r.err, r.status)
			}
		})
	}
}

// rawPost opens a cleartext connection to the gateway and sends the head of a
// POST with a declared length, leaving the body to the caller.
func rawPost(t *testing.T, l *lab, path string, length int) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", l.plainAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: gateway\r\nContent-Length: %d\r\n\r\n", path, length); err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestSmallUploadStillArriving sends a declared body under the size the
// upstream clients read a body whole at, in two halves with a pause between:
// a pushed body streams whatever its size, so the gateway forwards it as it
// arrives too.
func TestSmallUploadStillArriving(t *testing.T) {
	l := newLab(t, labOptions{})
	body := pattern(512 << 10)
	sum := sha256.Sum256(body)
	want := fmt.Sprintf("received %d bytes sha256=%s", len(body), hex.EncodeToString(sum[:]))
	for _, upstream := range []string{"h1", "h2", "h3"} {
		t.Run(upstream, func(t *testing.T) {
			conn := rawPost(t, l, "/"+upstream+"/upload", len(body))
			half := len(body) / 2
			if _, err := conn.Write(body[:half]); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
			if _, err := conn.Write(body[half:]); err != nil {
				t.Fatal(err)
			}
			resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 || !strings.Contains(string(got), want) {
				t.Fatalf("status %d, body %q", resp.StatusCode, got)
			}
		})
	}
}

// TestUploadAbandonedByClient has clients leave halfway through their body.
// The upstream sees each request abandoned, and the gateway serves the next
// request as before.
func TestUploadAbandonedByClient(t *testing.T) {
	l := newLab(t, labOptions{})
	t.Run("raw", func(t *testing.T) {
		conn := rawPost(t, l, "/h1/upload", 512<<10)
		if _, err := conn.Write(pattern(100 << 10)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		conn.Close()
	})
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run("streamed/"+client.name+"/"+upstream, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				ctx, cancel := context.WithCancel(context.Background())
				req := request(t, "POST", l.url("/"+upstream+"/hang"), nil).WithContext(ctx)
				req.Body = pr
				go func() { _, _ = pw.Write(pattern(256 << 10)) }()
				done := make(chan error, 1)
				client.doer.Do(req, func(_ *fibhttp.ClientResponse, err error) { done <- err })
				time.Sleep(300 * time.Millisecond)
				cancel()
				<-done
				select {
				case <-l.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("the upstream never saw the request abandoned")
				}
			})
		}
	}
	for _, client := range l.clients {
		if r := get(t, client.doer, request(t, "POST", l.url("/h1/upload"), pattern(10))); r.err != nil || r.status != 200 {
			t.Fatalf("%s after the abandoned uploads: err %v, status %d", client.name, r.err, r.status)
		}
	}
}

// TestUpstreamFailsBeforeBody has the upstream send its header and hang up
// before the body: nothing has gone downstream yet, so it is a 502.
func TestUpstreamFailsBeforeBody(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			r := get(t, client.doer, request(t, "GET", l.url("/h1/headonly"), nil))
			if r.err != nil || r.status != stdhttp.StatusBadGateway {
				t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
			}
		})
	}
}

// TestUploadToStalledUpstream sends an upload to an upstream that stops
// taking it: once the gateway has held the client's body for MaxBodyHold, it
// gives the request up with a 504 rather than hold the client for good.
func TestUploadToStalledUpstream(t *testing.T) {
	l := newLab(t, labOptions{maxBodyHold: 300 * time.Millisecond})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			pr, pw := io.Pipe()
			defer pw.Close()
			req := request(t, "POST", l.url("/h1/stall"), nil)
			req.Body = pr
			go func() {
				piece := pattern(64 << 10)
				for {
					if _, err := pw.Write(piece); err != nil {
						return
					}
				}
			}()
			r := get(t, client.doer, req)
			if r.err != nil && client.name == "HTTP/1.1" {
				// The gateway closes an HTTP/1.1 connection whose body it
				// gave up on, which may reset the client before it reads the
				// answer.
				return
			}
			if r.err != nil || r.status != stdhttp.StatusGatewayTimeout {
				t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
			}
		})
	}
}

// TestUploadFromStalledClient has clients stop sending their body. One that
// stops once its body streams upstream is, past the gateway's
// ReadBodyTimeout, answered with 408 on HTTP/2 and HTTP/3, its connection
// closed on HTTP/1.1, and its upstream request abandoned. One that stops
// before the body streams never reaches the upstream: its stream is reset,
// or its connection closed.
func TestUploadFromStalledClient(t *testing.T) {
	l := newLab(t, labOptions{readBodyTimeout: 300 * time.Millisecond})
	for _, stall := range []struct {
		name string
		sent int
	}{{"streaming", 128 << 10}, {"collected", 8 << 10}} {
		for _, client := range l.clients {
			t.Run(stall.name+"/"+client.name, func(t *testing.T) {
				pr, pw := io.Pipe()
				defer pw.Close()
				req := request(t, "POST", l.url("/h1/hang"), nil)
				req.Body = pr
				go func() { _, _ = pw.Write(pattern(stall.sent)) }()
				r := get(t, client.doer, req)
				switch {
				case client.name == "HTTP/1.1" || stall.name == "collected":
					if r.err == nil {
						t.Fatalf("status %d for a body that outlived its timeout", r.status)
					}
				case r.err != nil || r.status != stdhttp.StatusRequestTimeout:
					t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
				}
				if stall.name == "collected" {
					return
				}
				select {
				case <-l.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("the upstream never saw the request abandoned")
				}
			})
		}
	}
}
