//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// recordCounter counts the TLS records the server sends, as the client's
// transport reads them.
type recordCounter struct {
	net.Conn
	mu      sync.Mutex
	header  []byte
	body    int
	records int
}

func (r *recordCounter) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.mu.Lock()
	for data := p[:n]; len(data) > 0; {
		if r.body > 0 {
			k := min(r.body, len(data))
			r.body -= k
			data = data[k:]
			continue
		}
		k := min(recordHeaderLen-len(r.header), len(data))
		r.header = append(r.header, data[:k]...)
		data = data[k:]
		if len(r.header) == recordHeaderLen {
			r.records++
			r.body = int(r.header[3])<<8 | int(r.header[4])
			r.header = r.header[:0]
		}
	}
	r.mu.Unlock()
	return n, err
}

func (r *recordCounter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.records
}

// holdMessages is what the server answers each command with: several small
// sends, made where the connection is corked.
func holdMessages(cmd byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = fmt.Appendf(nil, "%c%03d;", cmd, i)
	}
	return out
}

// holdServer answers 'r' with sends from its read round, 'g' with sends a
// goroutine makes each after a Cork, and then a Flush, as h2 sends, 'b' with a send larger than a
// record between two small ones, and 'c' with small sends and then
// CloseAfterSend.
func holdServer(t *testing.T) (addr string, client *stdtls.Config) {
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.SessionTicketsDisabled = true
	serverConfig.MinVersion = stdtls.VersionTLS13
	handler := fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		for _, cmd := range b {
			switch cmd {
			case 'r':
				for _, m := range holdMessages(cmd, 16) {
					_ = c.Send(m)
				}
			case 'g':
				go func() {
					for _, m := range holdMessages(cmd, 16) {
						c.Cork()
						_ = c.Send(m)
					}
					_ = c.Flush()
				}()
			case 'b':
				_ = c.Send([]byte("b-first;"))
				_ = c.Send(bytes.Repeat([]byte{'B'}, 3*maxPlaintext+5))
				_ = c.Send([]byte("b-last;"))
			case 'c':
				for _, m := range holdMessages(cmd, 4) {
					_ = c.Send(m)
				}
				c.CloseAfterSend()
			}
		}
	}}
	_, addr = startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, handler))
	return addr, clientConfig
}

func dialCounted(t *testing.T, addr string, config *stdtls.Config) (*stdtls.Conn, *recordCounter) {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	counter := &recordCounter{Conn: raw}
	conn := stdtls.Client(counter, config)
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if err := conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	return conn, counter
}

func expectRead(t *testing.T, conn net.Conn, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("reading %d bytes: %v", len(want), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %.80q, want %.80q", got, want)
	}
}

// The sends of a read round reach the client in one record, rather than a
// record each.
func TestCorkedRoundIsOneRecord(t *testing.T) {
	addr, config := holdServer(t)
	conn, counter := dialCounted(t, addr, config)
	for range 3 {
		before := counter.count()
		if _, err := conn.Write([]byte{'r'}); err != nil {
			t.Fatal(err)
		}
		expectRead(t, conn, bytes.Join(holdMessages('r', 16), nil))
		if got := counter.count() - before; got != 1 {
			t.Fatalf("a round's 16 sends came in %d records, want 1", got)
		}
	}
}

// What a goroutine sends corked goes out at the Flush, in order. The read
// round that started the goroutine may end meanwhile, and its end takes the
// cork off and sends what was held, as fib's Cork allows: a cork is a flag,
// not a count. So the sends come in at most three records: what the round's
// end sent, a send made between a Cork and the round's end taking it off,
// and what the Flush sent.
func TestCorkFlushReleasesHeld(t *testing.T) {
	addr, config := holdServer(t)
	conn, counter := dialCounted(t, addr, config)
	for range 3 {
		before := counter.count()
		if _, err := conn.Write([]byte{'g'}); err != nil {
			t.Fatal(err)
		}
		expectRead(t, conn, bytes.Join(holdMessages('g', 16), nil))
		if got := counter.count() - before; got > 3 {
			t.Fatalf("16 corked sends came in %d records, want at most 3", got)
		}
	}
}

// A send larger than a record goes out at once, behind what was held, and
// what follows it is held again: the bytes arrive in the order sent.
func TestHeldOrderAroundLargeSend(t *testing.T) {
	addr, config := holdServer(t)
	conn, _ := dialCounted(t, addr, config)
	if _, err := conn.Write([]byte{'b'}); err != nil {
		t.Fatal(err)
	}
	want := append([]byte("b-first;"), bytes.Repeat([]byte{'B'}, 3*maxPlaintext+5)...)
	expectRead(t, conn, append(want, "b-last;"...))
}

// CloseAfterSend on a corked connection sends what was held before its
// close_notify.
func TestHeldBeforeCloseNotify(t *testing.T) {
	addr, config := holdServer(t)
	conn, _ := dialCounted(t, addr, config)
	if _, err := conn.Write([]byte{'c'}); err != nil {
		t.Fatal(err)
	}
	expectRead(t, conn, bytes.Join(holdMessages('c', 4), nil))
	if n, err := conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("after the held sends: %d, %v; want close_notify", n, err)
	}
}
