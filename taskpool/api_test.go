package taskpool

import (
	"strings"
	"testing"
)

// expectPanic runs f and fails the test unless it panics with a message
// containing want.
func expectPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if s, _ := r.(string); !strings.Contains(s, want) {
			t.Errorf("panic %v, want one containing %q", r, want)
		}
	}()
	f()
}

// The constructors refuse sizes no pool can run with, and a mode that does not
// exist, at once rather than when the first task finds the pool unusable.
func TestConstructorsRefuseInvalidSizes(t *testing.T) {
	expectPanic(t, "maxConcurrent", func() { NewWithMode("t", ModeElastic, 0, 1) })
	expectPanic(t, "queueSize", func() { NewWithMode("t", ModeElastic, 1, -1) })
	expectPanic(t, "invalid mode", func() { NewWithMode("t", Mode(99), 1, 1) })
	expectPanic(t, "queueSize", func() { NewAdaptive(AdaptiveConfig{MaxWorkers: 1, QueueSize: -1}) })
	expectPanic(t, "queueSize", func() { NewAdaptiveChan(AdaptiveConfig{MaxWorkers: 1, QueueSize: -1}) })
	if got := Mode(99).String(); got != "Mode(99)" {
		t.Errorf("Mode(99).String() = %q", got)
	}
	if Mode(99).Valid() {
		t.Error("Mode(99) is valid")
	}
}

// Nothing submitted is nothing to run: a nil task and an empty batch are
// accepted without reaching the pool, and Call runs its function in place,
// reporting a panic as a task's would be rather than letting it escape.
func TestNilTasksAndCall(t *testing.T) {
	tp := NewWithMode("t", ModeAdaptiveChan, 1, 1)
	defer tp.Stop()
	if !tp.Go(nil) || !tp.GoTask(nil) {
		t.Error("a nil task was refused")
	}
	if n := tp.GoTasks(nil); n != 0 {
		t.Errorf("GoTasks(nil) = %d", n)
	}
	ran := false
	tp.Call(func() { ran = true })
	if !ran {
		t.Error("Call did not run its function")
	}
	var panicked any
	tp.SetPanicHandler(func(v any, _ []byte) { panicked = v })
	tp.Call(func() { panic("boom") })
	if panicked != "boom" {
		t.Errorf("panic handler got %v", panicked)
	}
}

// Resizing a stopped adaptive-chan pool changes nothing: there are no workers
// left to start or retire.
func TestResizeAfterStop(t *testing.T) {
	tp := NewWithMode("t", ModeAdaptiveChan, 2, 1)
	tp.Stop()
	tp.Resize(1, 4)
	if n := tp.Workers(); n != 0 {
		t.Errorf("a stopped pool has %d workers after Resize", n)
	}
}
