//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"testing"

	fib "github.com/lesismal/fib"
)

// shutdownRecorder is a wrapped handler that takes word of a shutdown.
type shutdownRecorder struct {
	fib.HandlerFuncs
	calls int
}

func (s *shutdownRecorder) OnShutdown(*fib.Connection) { s.calls++ }

// The engine's word of a shutdown reaches a wrapped handler that takes it,
// so that the protocol inside the TLS stream can end its connection once it
// is idle, and is dropped for one that does not.
func TestOnShutdownPassesThrough(t *testing.T) {
	inner := &shutdownRecorder{}
	NewServer(nil, inner).OnShutdown(nil)
	if inner.calls != 1 {
		t.Fatalf("the wrapped handler was told %d times", inner.calls)
	}
	NewServer(nil, fib.HandlerFuncs{}).OnShutdown(nil)
	(&Handler{}).OnShutdown(nil)
}
