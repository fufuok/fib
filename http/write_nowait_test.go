//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// noWaitPump writes payload to c with WriteNoWait, a piece at a time, and
// when told it is above the limit waits for OnWritable's go-ahead, without
// a goroutine of its own: it is what a proxy passing a body on does. It
// reports how it ended on done, and what it saw on stats.
type noWaitPump struct {
	c        *Context
	payload  []byte
	sent     int
	above    int
	slowest  time.Duration
	returned chan struct{}
	done     chan error
}

func (p *noWaitPump) run(err error) {
	if err != nil {
		p.finish(err)
		return
	}
	for p.sent < len(p.payload) {
		piece := p.payload[p.sent:min(p.sent+16<<10, len(p.payload))]
		start := time.Now()
		above, err := p.c.WriteNoWait(piece)
		p.slowest = max(p.slowest, time.Since(start))
		if err != nil {
			p.finish(err)
			return
		}
		p.sent += len(piece)
		if above {
			p.above++
			if p.c.OnWritable(p.run) {
				return
			}
		}
	}
	p.finish(nil)
}

func (p *noWaitPump) finish(err error) {
	p.c.Release()
	p.done <- err
}

// TestWriteNoWait checks, over HTTP/1.1 and h2c, that a handler writing a
// body far larger than the client takes in at once with WriteNoWait never
// waits, is told it is above the limit, and gets the whole body through by
// resuming from OnWritable; and that one whose client goes away hears so,
// from WriteNoWait or from OnWritable's function.
func TestWriteNoWait(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<20) // 16MB
	pumps := make(chan *noWaitPump, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		p := &noWaitPump{c: c, payload: payload, returned: make(chan struct{}), done: make(chan error, 1)}
		pumps <- p
		p.run(nil)
		close(p.returned)
	})))
	for _, client := range hookClients(t) {
		t.Run(client.proto+"/whole", func(t *testing.T) {
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			p := <-pumps
			// The client reads nothing until the handler has returned,
			// which it does only if WriteNoWait never waits.
			select {
			case <-p.returned:
			case <-time.After(10 * time.Second):
				t.Fatal("the handler did not return while its client read nothing")
			}
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
			}
			if err := <-p.done; err != nil {
				t.Fatalf("the pump ended with %v", err)
			}
			if p.above == 0 {
				t.Error("WriteNoWait never reported above the limit")
			}
			if p.slowest > time.Second {
				t.Errorf("a WriteNoWait took %v", p.slowest)
			}
		})
		t.Run(client.proto+"/gone", func(t *testing.T) {
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				t.Fatal(err)
			}
			p := <-pumps
			<-p.returned
			resp.Body.Close()
			client.CloseIdleConnections()
			select {
			case err := <-p.done:
				if err == nil {
					t.Fatalf("the pump ended without an error, with %d of %d bytes written", p.sent, len(payload))
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the pump never heard that its client went away")
			}
		})
	}
}

// TestOnWritableNotStreaming checks that OnWritable reports false, and does
// not keep its function, for a response that has not begun to stream.
func TestOnWritableNotStreaming(t *testing.T) {
	results := make(chan []bool, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		ran := errors.New("ran")
		fn := func(error) { panic(ran) }
		before := c.OnWritable(fn)
		above, err := c.WriteNoWait([]byte("short"))
		results <- []bool{before, above, err != nil, c.OnWritable(fn)}
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "short" {
			t.Errorf("%s: body %q", client.proto, body)
		}
		for i, v := range <-results {
			if v {
				t.Errorf("%s: result %d is true, want false", client.proto, i)
			}
		}
	}
}
