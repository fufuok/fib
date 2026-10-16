package sidepool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// Callbacks posted while the pool's one worker is busy and its queue full
// wait in the backlog, start no goroutine, and all run once the worker is
// free, each once.
func TestPostOverflow(t *testing.T) {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "post-test", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	var b backlog
	release := make(chan struct{})
	started := make(chan struct{})
	post(pool, &b, func() { close(started); <-release })
	<-started
	var mu sync.Mutex
	var order []int
	before := runtime.NumGoroutine()
	const posts = 50
	for i := range posts {
		post(pool, &b, func() {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	if grew := runtime.NumGoroutine() - before; grew > 2 {
		t.Fatalf("posting to a full pool started %d goroutines", grew)
	}
	b.mu.Lock()
	waiting := len(b.fns)
	b.mu.Unlock()
	if waiting == 0 {
		t.Fatal("nothing waited in the backlog with the queue full")
	}
	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == posts {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d callbacks ran", n, posts)
		}
		time.Sleep(time.Millisecond)
	}
	seen := make(map[int]bool, posts)
	for _, i := range order {
		if seen[i] {
			t.Fatalf("callback %d ran twice: %v", i, order)
		}
		seen[i] = true
	}
}

// Post runs its callback on the notify pool.
func TestPost(t *testing.T) {
	var ran atomic.Bool
	done := make(chan struct{})
	Post(func() { ran.Store(true); close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the callback never ran")
	}
	if !ran.Load() || Notify() != Notify() {
		t.Fatal("Post did not run on one notify pool")
	}
}
