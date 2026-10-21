package socks

import (
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// tunnel relays a CONNECT: what the client sends goes to the destination,
// from the callback that received it, and the other way round. Its two
// halves are the two connections' handlers.
//
// Each side is paced by the other's backlog rather than its own, which
// SetWriteBackpressure turns off on both: a peer that waits to send its
// answers before it reads more would otherwise wait for the relay forever,
// the relay having stopped reading it for the backlog of what it sends it.
type tunnel struct {
	sc     *serverConn
	client *fib.Connection
	// target is set when the destination opens, before the client can
	// relay anything to it.
	target *fib.Connection
	limit  int64

	// idle, when set, closes both sides once nothing has gone either way
	// for that long: last is when something last did, in Unix nanoseconds,
	// and timer, set under sc.mu, looks at it from the reply until a side
	// closes.
	idle  time.Duration
	last  atomic.Int64
	timer *time.Timer
	ended atomic.Bool
}

// startIdle starts the idle timeout, if the tunnel has one, once the
// destination is open, with sc.mu held.
func (t *tunnel) startIdle() {
	if t.idle <= 0 {
		return
	}
	t.last.Store(time.Now().UnixNano())
	t.timer = time.AfterFunc(t.idle, t.checkIdle)
}

// checkIdle closes a tunnel that carried nothing for the idle timeout, and
// looks again when it last carried something otherwise.
func (t *tunnel) checkIdle() {
	if t.ended.Load() {
		return
	}
	since := time.Since(time.Unix(0, t.last.Load()))
	if since >= t.idle {
		t.client.Close()
		t.target.Close()
		return
	}
	t.sc.mu.Lock()
	timer := t.timer
	t.sc.mu.Unlock()
	timer.Reset(t.idle - since)
}

// stopIdle stops the idle timeout once a side has closed.
func (t *tunnel) stopIdle() {
	if !t.ended.CompareAndSwap(false, true) {
		return
	}
	t.sc.mu.Lock()
	timer := t.timer
	t.sc.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// forward sends data, which from received, on to to. When to has more
// queued than the tunnel allows, it stops reading from from until to has
// sent half of it, so the slower side's socket holds the faster side back,
// and reports that it did.
func (t *tunnel) forward(from, to *fib.Connection, data []byte) bool {
	if t.idle > 0 {
		t.last.Store(time.Now().UnixNano())
	}
	if to.Send(data) != nil {
		// to is closing, and its OnClose ends from.
		return false
	}
	if outputwait.Pending == nil || outputwait.Pending(to) <= t.limit {
		return false
	}
	from.HoldReads(true)
	// The wait can last as long as the peer is slow to read, so it takes a
	// goroutine of its own rather than one of a bounded pool's.
	go func() {
		_ = outputwait.Wait(to, t.limit)
		from.HoldReads(false)
	}()
	return true
}

// tunnelClient is the client's handler once its CONNECT is relayed.
type tunnelClient tunnel

func (h *tunnelClient) OnOpen(*fib.Connection) {}

func (h *tunnelClient) OnData(c *fib.Connection, data []byte) {
	t := (*tunnel)(h)
	t.forward(c, t.target, data)
}

func (h *tunnelClient) OnPriorityData(*fib.Connection, []byte) {}

func (h *tunnelClient) OnClose(*fib.Connection, error) {
	(*tunnel)(h).stopIdle()
	h.target.CloseAfterSend()
}

// tunnelTarget is the destination's handler.
type tunnelTarget tunnel

func (h *tunnelTarget) OnOpen(c *fib.Connection) {
	t := (*tunnel)(h)
	t.target = c
	t.sc.opened(t, c)
}

func (h *tunnelTarget) OnData(c *fib.Connection, data []byte) {
	t := (*tunnel)(h)
	t.forward(c, t.client, data)
}

func (h *tunnelTarget) OnPriorityData(*fib.Connection, []byte) {}

func (h *tunnelTarget) OnClose(*fib.Connection, error) {
	(*tunnel)(h).stopIdle()
	h.client.CloseAfterSend()
}
