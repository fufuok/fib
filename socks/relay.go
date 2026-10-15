package socks

import (
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
}

// forward sends data, which from received, on to to. When to has more
// queued than the tunnel allows, it stops reading from from until to has
// sent half of it, so the slower side's socket holds the faster side back,
// and reports that it did.
func (t *tunnel) forward(from, to *fib.Connection, data []byte) bool {
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

func (h *tunnelClient) OnClose(*fib.Connection, error) { h.target.CloseAfterSend() }

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

func (h *tunnelTarget) OnClose(*fib.Connection, error) { h.client.CloseAfterSend() }
