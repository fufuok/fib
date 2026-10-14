package bufferpool

import "testing"

// A negative budget means no reserves, as zero does, rather than a budget
// every class is already over.
func TestNegativeRetainedBytesTurnsTheReservesOff(t *testing.T) {
	t.Cleanup(func() { SetRetainedBytes(DefaultRetainedBytes) })
	SetRetainedBytes(-1)
	if got := budget.Load(); got != 0 {
		t.Fatalf("budget %d after SetRetainedBytes(-1), want 0", got)
	}
	if got := RetainedBytes(); got != 0 {
		t.Fatalf("%d bytes still retained", got)
	}
}

// An empty reserve hands out nothing, and a class whose burst of returns is
// spent keeps nothing more until the next collection starts another, however
// far below its target it is.
func TestReserveTakeAndKeepAtTheirLimits(t *testing.T) {
	c := &class{size: MinSize}
	if p := c.take(); p != nil {
		t.Fatal("an empty reserve handed out a buffer")
	}
	c.target.Store(4)
	buf := make([]byte, MinSize)
	if c.keep(&buf[0]) {
		t.Fatal("a class with no burst left kept a buffer")
	}
	if c.refill.Load() != 0 {
		t.Fatalf("refill went to %d", c.refill.Load())
	}
	c.refill.Store(1)
	if !c.keep(&buf[0]) {
		t.Fatal("a class within its burst and target did not keep a buffer")
	}
	if p := c.take(); p != &buf[0] {
		t.Fatal("the reserve did not hand back the buffer it kept")
	}
}
