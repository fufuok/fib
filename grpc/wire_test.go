//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"encoding/binary"
	json "encoding/json/v2"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/hpack"
)

// The helpers here speak HTTP/2 by hand, as one end of a connection whose
// other end is this package's transport: the frames gRPC's own peers never
// send, and a check of what the transport made of them, are what reach its
// error paths.

// wireConn is one end of an HTTP/2 connection spoken by hand. Its methods
// are for the test's goroutine.
type wireConn struct {
	t    *testing.T
	conn net.Conn
	enc  *hpack.Encoder
	dec  *hpack.Decoder
}

func newWireConn(t *testing.T, conn net.Conn) *wireConn {
	t.Helper()
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	return &wireConn{t: t, conn: conn, enc: hpack.NewEncoder(), dec: hpack.NewDecoder(defaultHeaderTable)}
}

// dialWire connects to a server and, with handshake, sends the client's
// preface and an empty SETTINGS.
func dialWire(t *testing.T, addr string, handshake bool) *wireConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := newWireConn(t, conn)
	if handshake {
		c.write([]byte(preface), appendSettings(nil))
	}
	return c
}

// wireServer listens on loopback for clients spoken to by hand; accept
// returns the next connection, its client's preface read and an empty
// SETTINGS sent.
func wireServer(t *testing.T) (addr string, accept func() *wireConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conns := make(chan net.Conn, 16)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- conn
		}
	}()
	return ln.Addr().String(), func() *wireConn {
		t.Helper()
		var conn net.Conn
		select {
		case conn = <-conns:
		case <-time.After(10 * time.Second):
			t.Fatal("no client connected")
		}
		c := newWireConn(t, conn)
		head := make([]byte, len(preface))
		if _, err := io.ReadFull(conn, head); err != nil || string(head) != preface {
			t.Fatalf("client preface %q, %v", head, err)
		}
		c.write(appendSettings(nil))
		return c
	}
}

func (c *wireConn) write(frames ...[]byte) {
	c.t.Helper()
	if _, err := c.conn.Write(bytes.Join(frames, nil)); err != nil {
		c.t.Fatal(err)
	}
}

// wireFrame is a frame read, with the fields of a header block decoded.
type wireFrame struct {
	frame
	fields map[string]string
}

// read returns the next frame, joining a header block's CONTINUATIONs to
// its HEADERS.
func (c *wireConn) read() (wireFrame, error) {
	f, err := c.readOne()
	if err != nil || f.typ != frameHeaders {
		return wireFrame{frame: f}, err
	}
	block := f.payload
	for !f.has(flagEndHeaders) {
		next, err := c.readOne()
		if err != nil {
			return wireFrame{frame: f}, err
		}
		block = append(block, next.payload...)
		f.flags |= next.flags & flagEndHeaders
	}
	fields := map[string]string{}
	if err := c.dec.Decode(block, func(hf hpack.HeaderField) error {
		fields[hf.Name] = hf.Value
		return nil
	}); err != nil {
		return wireFrame{frame: f}, err
	}
	return wireFrame{frame: f, fields: fields}, nil
}

func (c *wireConn) readOne() (frame, error) {
	head := make([]byte, frameHeaderLen)
	if _, err := io.ReadFull(c.conn, head); err != nil {
		return frame{}, err
	}
	f := parseFrameHeader(head)
	f.payload = make([]byte, f.length)
	_, err := io.ReadFull(c.conn, f.payload)
	return f, err
}

// expect reads frames until one of typ on stream id, which it returns.
func (c *wireConn) expect(typ frameType, id uint32) wireFrame {
	c.t.Helper()
	for {
		f, err := c.read()
		if err != nil {
			c.t.Fatalf("waiting for frame type %d on stream %d: %v", typ, id, err)
		}
		if f.typ == typ && f.streamID == id {
			return f
		}
	}
}

// next reads frames until one of typ, on whichever stream.
func (c *wireConn) next(typ frameType) wireFrame {
	c.t.Helper()
	for {
		f, err := c.read()
		if err != nil {
			c.t.Fatalf("waiting for frame type %d: %v", typ, err)
		}
		if f.typ == typ {
			return f
		}
	}
}

// sync sends a PING and reads until its ack: the peer has taken everything
// sent before it.
func (c *wireConn) sync() {
	c.t.Helper()
	c.write(appendPing(nil, false, []byte("syncsync")))
	for {
		f := c.expect(framePing, 0)
		if f.has(flagAck) && string(f.payload) == "syncsync" {
			return
		}
	}
}

// goAway reads frames until a GOAWAY and returns its error code.
func (c *wireConn) goAway() errCode {
	c.t.Helper()
	f := c.expect(frameGoAway, 0)
	return errCode(binary.BigEndian.Uint32(f.payload[4:]))
}

// reset reads frames until a RST_STREAM on stream id and returns its code.
func (c *wireConn) reset(id uint32) errCode {
	c.t.Helper()
	return errCode(binary.BigEndian.Uint32(c.expect(frameRSTStream, id).payload))
}

// trailers reads frames until the header block that ends stream id.
func (c *wireConn) trailers(id uint32) map[string]string {
	c.t.Helper()
	for {
		f := c.expect(frameHeaders, id)
		if f.has(flagEndStream) {
			return f.fields
		}
	}
}

// closed reads until the connection ends, which it must.
func (c *wireConn) closed() {
	c.t.Helper()
	for {
		if _, err := c.readOne(); err != nil {
			if isTimeout(err) {
				c.t.Fatalf("the connection was not closed: %v", err)
			}
			return
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// block encodes a header block of name, value pairs.
func (c *wireConn) block(kv ...string) []byte {
	block := c.enc.Begin(nil)
	for i := 0; i < len(kv); i += 2 {
		block = c.enc.AppendField(block, kv[i], kv[i+1], false)
	}
	return block
}

// request is the header block of a gRPC call of path, with extra fields.
func (c *wireConn) request(path string, extra ...string) []byte {
	return c.block(append([]string{":method", "POST", ":scheme", "http", ":path", path,
		":authority", "wire", "content-type", "application/grpc+json", "te", "trailers"}, extra...)...)
}

// response is the header block of a gRPC response, with extra fields.
func (c *wireConn) response(extra ...string) []byte {
	return c.block(append([]string{":status", "200", "content-type", "application/grpc+json"}, extra...)...)
}

func rawFrame(typ frameType, flags uint8, id uint32, payload []byte) []byte {
	return append(appendFrameHeader(nil, typ, flags, id, len(payload)), payload...)
}

func headersFrame(id uint32, block []byte, endStream bool) []byte {
	flags := uint8(flagEndHeaders)
	if endStream {
		flags |= flagEndStream
	}
	return rawFrame(frameHeaders, flags, id, block)
}

func dataFrame(id uint32, payload []byte, endStream bool) []byte {
	var flags uint8
	if endStream {
		flags = flagEndStream
	}
	return rawFrame(frameData, flags, id, payload)
}

// grpcMessage is v, in JSON, as one length-prefixed gRPC message.
func grpcMessage(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return lengthPrefixed(false, b)
}

func lengthPrefixed(compressed bool, b []byte) []byte {
	out := make([]byte, 5, 5+len(b))
	if compressed {
		out[0] = 1
	}
	binary.BigEndian.PutUint32(out[1:], uint32(len(b)))
	return append(out, b...)
}

func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

func settingsFrame(kv ...uint32) []byte { return appendSettings(nil, kv...) }

// oversizedHeaderList frames, as HEADERS and a CONTINUATION, a header block
// of some 17KB that decodes to more than maxHeaderBlock: a field of the
// static table, named by its index over and over. Referring only to the
// static table keeps the dynamic tables of both ends as they were.
func oversizedHeaderList(id uint32) []byte {
	const acceptEncoding = 16 // "accept-encoding: gzip, deflate", 60 bytes as listed
	block := bytes.Repeat([]byte{0x80 | acceptEncoding}, maxHeaderBlock/60+1)
	return appendHeaderBlock(nil, id, block, false, defaultMaxFrame)
}

// goCall runs call on a goroutine of its own and returns its outcome.
func goCall(call func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- call() }()
	return done
}

func waitCall(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("the call never returned")
		return nil
	}
}
