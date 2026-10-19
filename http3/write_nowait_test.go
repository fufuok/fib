//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// noWaitPump writes payload with WriteNoWait, a piece at a time, resuming
// from OnWritable whenever it is told it is above the limit, as a proxy
// passing a body on does, and reports how it ended on done.
type noWaitPump struct {
	c       *fibhttp.Context
	payload []byte
	sent    int
	above   int
	slowest time.Duration
	done    chan error
}

func (p *noWaitPump) run(err error) {
	for err == nil && p.sent < len(p.payload) {
		piece := p.payload[p.sent:min(p.sent+16<<10, len(p.payload))]
		start := time.Now()
		var above bool
		above, err = p.c.WriteNoWait(piece)
		p.slowest = max(p.slowest, time.Since(start))
		if err != nil {
			break
		}
		p.sent += len(piece)
		if above {
			p.above++
			if p.c.OnWritable(p.run) {
				return
			}
		}
	}
	p.c.Release()
	p.done <- err
}

// TestWriteNoWait checks that over HTTP/3 a handler writing with WriteNoWait
// never waits, is told when it is above the limit, and gets the whole body
// through by resuming from OnWritable.
func TestWriteNoWait(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB
	pumps := make(chan *noWaitPump, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.Retain()
		p := &noWaitPump{c: c, payload: payload, done: make(chan error, 1)}
		pumps <- p
		p.run(nil)
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", base+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(payload) {
		t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
	}
	p := <-pumps
	if err := <-p.done; err != nil {
		t.Fatalf("the pump ended with %v", err)
	}
	if p.above == 0 {
		t.Error("WriteNoWait never reported above the limit")
	}
	if p.slowest > time.Second {
		t.Errorf("a WriteNoWait took %v", p.slowest)
	}
}

// TestOnWritableWriteTimeout checks that a response whose client stops
// taking it, written with WriteNoWait, hears of WriteTimeout through
// OnWritable's function.
func TestOnWritableWriteTimeout(t *testing.T) {
	pumps := make(chan *noWaitPump, 1)
	base := startServer(t, Config{WriteTimeout: 300 * time.Millisecond}, func(c *fibhttp.Context) {
		c.Retain()
		p := &noWaitPump{c: c, payload: make([]byte, 4<<20), done: make(chan error, 1)}
		pumps <- p
		p.run(nil)
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/stall"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	p := <-pumps
	select {
	case err := <-p.done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the pump ended with %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pump never heard of WriteTimeout")
	}
	if p.above == 0 {
		t.Error("WriteNoWait never reported above the limit")
	}
}
