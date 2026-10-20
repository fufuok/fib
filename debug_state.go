//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/internal/debughook"
)

// traceOn turns on traceLocked; traces holds each connection's ring of the
// steps it recorded. Temporary, for a Windows CI hang.
var (
	traceOn    atomic.Bool
	traces     sync.Map
	traceEpoch = time.Now()
)

type traceRing struct {
	mu   sync.Mutex
	ev   [256]string
	next int
}

// traceLocked records a step of c's I/O, with the state it left behind,
// while tracing is on. Callers hold c.mu.
func (c *Connection) traceLocked(format string, args ...any) {
	if !traceOn.Load() {
		return
	}
	v, _ := traces.LoadOrStore(c, &traceRing{})
	r := v.(*traceRing)
	line := fmt.Sprintf("%8.3fms %s | q=%d/%d pend=%d corked=%v flushing=%v sched=%v pev=%#x def=%v %s",
		float64(time.Since(traceEpoch).Microseconds())/1000, fmt.Sprintf(format, args...),
		c.sendHead, len(c.sends), c.pendingBytes.Load(), c.corked, c.flushing, c.scheduled,
		c.pendingEvents, c.readDeferred, c.platformStateLocked())
	r.mu.Lock()
	r.ev[r.next%len(r.ev)] = line
	r.next++
	r.mu.Unlock()
}

func init() {
	debughook.SetTrace = func(on bool) { traceOn.Store(on) }
	debughook.ConnTrace = func(v any) string {
		c, ok := v.(*Connection)
		if !ok || c == nil {
			return "not a *fib.Connection"
		}
		r, ok := traces.Load(c)
		if !ok {
			return "no trace"
		}
		ring := r.(*traceRing)
		ring.mu.Lock()
		defer ring.mu.Unlock()
		var lines []string
		start := max(0, ring.next-len(ring.ev))
		for i := start; i < ring.next; i++ {
			lines = append(lines, ring.ev[i%len(ring.ev)])
		}
		return strings.Join(lines, "\n")
	}
	debughook.ConnState = func(v any) string {
		c, ok := v.(*Connection)
		if !ok || c == nil {
			return "not a *fib.Connection"
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		var fileBytes int64
		if d := c.deadlines; d != nil {
			fileBytes = d.fileBytes
		}
		return fmt.Sprintf("queue=%d/%d pendingBytes=%d corkedBytes=%d fileBytes=%d corked=%v flushing=%v "+
			"readPaused=%v budgetPaused=%v readStalled=%v readDeferred=%v scheduled=%v pendingEvents=%#x "+
			"closing=%v closed=%v closePending=%v hangupNoted=%v closeAfterSend=%v writeShut=%v flags=%#x %s",
			c.sendHead, len(c.sends), c.pendingBytes.Load(), c.corkedBytes, fileBytes, c.corked, c.flushing,
			c.readPaused, c.budgetPaused, c.readStalled, c.readDeferred, c.scheduled, c.pendingEvents,
			c.closing, c.closed, c.closePending, c.hangupNoted, c.closeAfterSend, c.writeShut, c.flags.bits.Load(),
			c.platformStateLocked())
	}
}
