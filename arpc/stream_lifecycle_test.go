//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package arpc

import (
	"context"
	"io"
	"testing"
	"time"
)

// TestStreamContextVariants pins down the context and "With" forms of the
// Stream methods, which behave as the plain ones, and the end of a Stream
// each side closes with its last message: Recv returns the messages queued
// and then io.EOF, and a second SendAndClose gets ErrStreamClosedSend.
func TestStreamContextVariants(t *testing.T) {
	s := NewServer()
	s.Handler.HandleStream("/echo", func(st *Stream) {
		ctx := context.Background()
		for {
			var m string
			if err := st.RecvWith(ctx, &m); err == io.EOF {
				_ = st.SendAndCloseWith(ctx, "bye")
				st.CloseRecvContext(ctx)
				return
			} else if err != nil {
				t.Errorf("server Recv: %v", err)
				return
			}
			if err := st.SendContext(ctx, m); err != nil {
				t.Errorf("server Send: %v", err)
				return
			}
		}
	})
	c := dial(t, startServer(t, s), nil)
	ctx := context.Background()
	st := c.NewStream("/echo")
	if st.Id() == 0 || st.Method() != "/echo" {
		t.Fatalf("Id %d, Method %q", st.Id(), st.Method())
	}
	if err := st.SendWith(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := st.RecvContext(ctx, &got); err != nil || got != "a" {
		t.Fatalf("RecvContext = %q, %v", got, err)
	}
	if err := st.SendAndCloseContext(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"b", "bye"} {
		if err := st.RecvWith(ctx, &got); err != nil || got != want {
			t.Fatalf("RecvWith = %q, %v, want %q", got, err, want)
		}
	}
	if err := st.Recv(&got); err != io.EOF {
		t.Fatalf("Recv after the server closed = %v", err)
	}
	if err := st.SendAndClose("again"); err != ErrStreamClosedSend {
		t.Fatalf("a second SendAndClose = %v", err)
	}
}

// TestStreamFirstMessageIsLast pins down a Stream whose opening message is
// also its last, handled where the connection is read: the handler receives
// the message and then io.EOF, and can still answer.
func TestStreamFirstMessageIsLast(t *testing.T) {
	s := NewServer()
	s.Handler.HandleStream("/once", func(st *Stream) {
		var m string
		if err := st.Recv(&m); err != nil {
			t.Errorf("Recv = %v", err)
		}
		if err := st.Recv(nil); err != io.EOF {
			t.Errorf("Recv after the only message = %v", err)
		}
		_ = st.SendAndClose("got " + m)
	}, false)
	c := dial(t, startServer(t, s), nil)
	st := c.NewStream("/once")
	if err := st.SendAndClose("only"); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := st.Recv(&got); err != nil || got != "got only" {
		t.Fatalf("Recv = %q, %v", got, err)
	}
}

// TestStreamUnknownMethod pins down that a Stream opened for a method the
// peer has no handler for ends at once: the peer answers its first message
// with an EOF, so the opener's Recv returns io.EOF rather than waiting for a
// message that cannot come.
func TestStreamUnknownMethod(t *testing.T) {
	s := NewServer()
	c := dial(t, startServer(t, s), nil)
	st := c.NewStream("/nobody")
	if err := st.Send("hello?"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), arpcWait)
	defer cancel()
	if err := st.RecvContext(ctx, nil); err != io.EOF {
		t.Fatalf("RecvContext on a Stream nobody handles = %v, want io.EOF", err)
	}
}

// TestStreamRecvContextTimeout pins down that RecvContext gives up with
// ErrTimeout when its context ends before a message arrives.
func TestStreamRecvContextTimeout(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	defer close(release)
	s.Handler.HandleStream("/silent", func(*Stream) { <-release })
	c := dial(t, startServer(t, s), nil)
	st := c.NewStream("/silent")
	if err := st.Send("hello?"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := st.RecvContext(ctx, nil); err != ErrTimeout {
		t.Fatalf("RecvContext = %v, want ErrTimeout", err)
	}
}

// TestStreamRecvOnStoppedClient pins down that Recv on a Stream whose Client
// has stopped returns ErrClientStopped rather than waiting.
func TestStreamRecvOnStoppedClient(t *testing.T) {
	c := newClient(DefaultCodec, NewHandler())
	st := c.NewStream("/s")
	c.Stop()
	if err := st.Recv(nil); err != ErrClientStopped {
		t.Fatalf("Recv = %v, want ErrClientStopped", err)
	}
}

// TestStreamMessagesAfterCloseRecv pins down that messages reaching a Stream
// whose receiving half is closed are dropped, whether they arrive after
// CloseRecv or are waiting for room in a full queue when it is called, and
// that a full queue holds up only its own connection until then.
func TestStreamMessagesAfterCloseRecv(t *testing.T) {
	s := NewServer()
	s.Handler.SetStreamQueueSize(1)
	s.Handler.Handle("/barrier", func(ctx *Context) { ctx.Write("ok") }, false)
	accepted := make(chan *Client, 1)
	s.Handler.HandleConnected(func(c *Client) { accepted <- c })
	opened := make(chan *Stream, 1)
	s.Handler.HandleStream("/s", func(st *Stream) { opened <- st })
	addr := startServer(t, s)

	t.Run("after", func(t *testing.T) {
		c := dial(t, addr, nil)
		recvArpc(t, accepted, "the accepted Client")
		st := c.NewStream("/s")
		if err := st.Send("first"); err != nil {
			t.Fatal(err)
		}
		recvArpc(t, opened, "the server's Stream").CloseRecv()
		if err := st.Send("dropped"); err != nil {
			t.Fatal(err)
		}
		// Messages on a connection are handled in order, so once the sync
		// handler answers the stream message has been handled.
		if err := c.Call("/barrier", nil, nil, arpcWait); err != nil {
			t.Fatal(err)
		}
	})

	for _, how := range []string{"CloseRecv", "Stop"} {
		t.Run("full queue, "+how, func(t *testing.T) {
			c := dial(t, addr, nil)
			sc := recvArpc(t, accepted, "the accepted Client")
			st := c.NewStream("/s")
			for _, m := range []string{"queued", "waiting"} {
				if err := st.Send(m); err != nil {
					t.Fatal(err)
				}
			}
			server := recvArpc(t, opened, "the server's Stream")
			// Let the second message reach the full queue and wait there.
			time.Sleep(100 * time.Millisecond)
			if how == "Stop" {
				sc.Stop()
				return
			}
			server.CloseRecv()
			if err := c.Call("/barrier", nil, nil, arpcWait); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := server.Recv(&got); err != nil || got != "queued" {
				t.Fatalf("Recv = %q, %v", got, err)
			}
			if err := server.Recv(&got); err != io.EOF {
				t.Fatalf("Recv after the queue = %q, %v", got, err)
			}
		})
	}
}
