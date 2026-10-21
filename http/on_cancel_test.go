//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"io"
	"net"
	stdhttp "net/http"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestOnCancelRunsEveryCallback registers several callbacks on a request
// whose connection is then cut: each runs once, in the reverse of the order
// they were registered, and one registered after the cancellation runs at
// once.
func TestOnCancelRunsEveryCallback(t *testing.T) {
	var mu sync.Mutex
	var order []int
	errs := make(chan error, 8)
	ran := func(i int) func(error) {
		return func(err error) {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			errs <- err
		}
	}
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		c.Retain()
		for i := range 3 {
			c.OnCancel(ran(i))
		}
		go func() {
			// The context is cancelled before the callbacks run, so this one
			// may be registered while they do: it runs at once all the same.
			<-c.Context().Done()
			c.OnCancel(ran(9))
			c.Release()
		}()
	})
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(conn, "GET /held HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	conn.Close()
	for i := range 4 {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatalf("callback %d was given no reason", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 4 callbacks ran", i)
		}
	}
	select {
	case <-errs:
		t.Fatal("a callback ran twice")
	case <-time.After(200 * time.Millisecond):
	}
	mu.Lock()
	defer mu.Unlock()
	got := slices.DeleteFunc(slices.Clone(order), func(i int) bool { return i == 9 })
	if want := []int{2, 1, 0}; !slices.Equal(got, want) {
		t.Fatalf("callbacks ran in the order %v, want %v", order, want)
	}
}

// TestOnCancelCallbacksGoWithTheirRequest checks that callbacks registered on
// a request that was answered never run, and are not carried over to the
// next request its recycled Context serves: only that request's own run when
// its connection is cut.
func TestOnCancelCallbacksGoWithTheirRequest(t *testing.T) {
	calls := make(chan string, 8)
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		path := c.Request.URL.Path
		for range 3 {
			c.OnCancel(func(error) { calls <- path })
		}
		if path == "/answered" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
			// Registering once the response is written does nothing.
			c.OnCancel(func(error) { calls <- "after the response" })
			return
		}
		c.Retain()
		c.OnCancel(func(error) { c.Release() })
	})
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	for range 2 {
		if _, err = io.WriteString(conn, "GET /answered HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		resp, err := stdhttp.ReadResponse(r, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if _, err = io.WriteString(conn, "GET /held HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	conn.Close()
	for range 3 {
		select {
		case path := <-calls:
			if path != "/held" {
				t.Fatalf("a callback of %s ran", path)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the held request's callbacks never ran")
		}
	}
	select {
	case path := <-calls:
		t.Fatalf("a callback of %s ran", path)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestOnCancelOneCallbackAllocatesNothing holds OnCancel with one callback to
// what it cost before it kept more than one: a field set, once the Context's
// hooks exist, and no slice.
func TestOnCancelOneCallbackAllocatesNothing(t *testing.T) {
	c := &Context{}
	c.reopen()
	fn := func(error) {}
	c.OnCancel(fn)
	allocs := testing.AllocsPerRun(100, func() {
		c.cancel.Load().reset()
		c.OnCancel(fn)
	})
	if allocs != 0 {
		t.Fatalf("OnCancel with one callback allocated %v times", allocs)
	}
	// A second and third grow the slice once, and a recycled Context keeps
	// it for the next request.
	allocs = testing.AllocsPerRun(100, func() {
		c.cancel.Load().reset()
		c.OnCancel(fn)
		c.OnCancel(fn)
		c.OnCancel(fn)
	})
	if allocs != 0 {
		t.Fatalf("OnCancel with three callbacks on a recycled Context allocated %v times", allocs)
	}
}

// BenchmarkOnCancel measures registering OnCancel callbacks on a request
// and recycling its Context for the next, as a server that reuses Contexts
// does on every request.
func BenchmarkOnCancel(b *testing.B) {
	for _, n := range []int{1, 2} {
		b.Run([]string{"", "one", "two"}[n], func(b *testing.B) {
			c := &Context{}
			c.reopen()
			fn := func(error) {}
			b.ReportAllocs()
			for b.Loop() {
				for range n {
					c.OnCancel(fn)
				}
				c.recycle()
				c.reopen()
			}
		})
	}
}
