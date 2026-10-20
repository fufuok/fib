//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"fmt"

	"github.com/lesismal/fib/internal/debughook"
)

func init() {
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
