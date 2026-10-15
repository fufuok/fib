package taskpool

import (
	"sync"
	"testing"
	"time"
)

// TryGo turns a task away while the queue is full, where Go would wait for
// room, runs every task it took, and turns tasks away once the pool stops.
func TestTryGo(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func() *TaskPool
	}{
		{"adaptive", func() *TaskPool { return NewAdaptive(AdaptiveConfig{Name: "try", MaxWorkers: 1, QueueSize: 1}) }},
		{"adaptive chan", func() *TaskPool { return NewAdaptiveChan(AdaptiveConfig{Name: "try", MaxWorkers: 1, QueueSize: 1}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := tc.make()
			release := make(chan struct{})
			var ran sync.WaitGroup
			accepted := 0
			deadline := time.Now().Add(5 * time.Second)
			for {
				ran.Add(1)
				if !pool.TryGo(func() { <-release; ran.Done() }) {
					ran.Done()
					break
				}
				accepted++
				if accepted > 8 {
					t.Fatalf("%d tasks taken by a pool of one worker and a queue of one", accepted)
				}
				if time.Now().After(deadline) {
					t.Fatal("the queue never filled")
				}
			}
			if accepted == 0 {
				t.Fatal("no task was taken")
			}
			close(release)
			ran.Wait()
			pool.Stop()
			if pool.TryGo(func() {}) {
				t.Fatal("a stopped pool took a task")
			}
		})
	}
	t.Run("other modes", func(t *testing.T) {
		pool := NewInline("try")
		defer pool.Stop()
		ran := false
		if !pool.TryGo(func() { ran = true }) || !ran {
			t.Fatal("an inline pool did not run the task")
		}
		if !pool.TryGo(nil) {
			t.Fatal("a nil task was refused")
		}
	})
}
