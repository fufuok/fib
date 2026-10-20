package taskpool

import (
	"sync"
	"sync/atomic"
	"testing"
)

type indexTask int

func (indexTask) RunTask() {}

// Producers push through rings of every small size while consumers race to
// pop. Every task must come out exactly once: a ring of one cell used to let
// the producer write over a task a consumer had claimed but not yet read.
func TestTaskRingDeliversEachTaskOnce(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 5, 8} {
		const tasks, producers, consumers = 200000, 3, 4
		r := newTaskRing(limit)
		var seen [tasks]atomic.Int32
		var taken atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < consumers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for taken.Load() < tasks {
					if task, ok := r.pop(); ok {
						seen[task.(indexTask)].Add(1)
						taken.Add(1)
					}
				}
			}()
		}
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := p; i < tasks; i += producers {
					for !r.push(indexTask(i)) {
					}
				}
			}()
		}
		wg.Wait()
		for i := range seen {
			if got := seen[i].Load(); got != 1 {
				t.Fatalf("limit %d: task %d came out %d times", limit, i, got)
			}
		}
	}
}

func TestTaskRingHoldsItsLimit(t *testing.T) {
	r := newTaskRing(3)
	for i := 0; i < 3; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused below the limit", i)
		}
	}
	if r.push(indexTask(3)) {
		t.Fatal("push accepted past the limit")
	}
	if task, ok := r.pop(); !ok || task.(indexTask) != 0 {
		t.Fatalf("pop = %v, %v, want the oldest task", task, ok)
	}
	if !r.push(indexTask(3)) {
		t.Fatal("push refused after a pop made room")
	}
	if got := r.len(); got != 3 {
		t.Fatalf("len = %d, want 3", got)
	}
}

// A new ring writes none of its cells, so that their pages, and under the
// race detector the sync objects its atomics cost, come only as the queue
// reaches them; and a zeroed cell is free for its first lap.
func TestTaskRingStartsUntouched(t *testing.T) {
	r := newTaskRing(1000)
	for i := range r.cells {
		if seq := r.cells[i].seq.Load(); seq != 0 {
			t.Fatalf("cell %d starts at seq %d", i, seq)
		}
	}
	// Many laps of a ring of four, through each position's cell in turn.
	r = newTaskRing(4)
	for lap := 0; lap < 1000; lap++ {
		for i := 0; i < 4; i++ {
			if !r.push(indexTask(lap*4 + i)) {
				t.Fatalf("lap %d: push %d refused", lap, i)
			}
		}
		if r.push(indexTask(-1)) {
			t.Fatalf("lap %d: push past the limit taken", lap)
		}
		for i := 0; i < 4; i++ {
			task, ok := r.pop()
			if !ok || task.(indexTask) != indexTask(lap*4+i) {
				t.Fatalf("lap %d: pop %d gave %v, %v", lap, i, task, ok)
			}
		}
		if _, ok := r.pop(); ok {
			t.Fatalf("lap %d: pop from an empty ring", lap)
		}
	}
}

func BenchmarkTaskRingPushPop(b *testing.B) {
	r := newTaskRing(1024)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for !r.push(indexTask(0)) {
			}
			for {
				if _, ok := r.pop(); ok {
					break
				}
			}
		}
	})
}

func BenchmarkNewTaskRing(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = newTaskRing(100000)
	}
}
