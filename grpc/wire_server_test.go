//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"testing"
	"time"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

// holdService serves every method no service registers by holding the call
// until its context ends, and then reporting what RecvMsg says of it: how
// the transport ended a call it was given no other way to see.
type holdService struct {
	entered chan string
	ended   chan holdEnd
}

type holdEnd struct {
	method string
	code   codes.Code
}

func newHoldService() *holdService {
	return &holdService{entered: make(chan string, 64), ended: make(chan holdEnd, 64)}
}

func (h *holdService) handle(_ any, stream ServerStream) error {
	method, _ := Method(stream.Context())
	h.entered <- method
	<-stream.Context().Done()
	err := stream.RecvMsg(&Req{})
	h.ended <- holdEnd{method, status.Code(err)}
	return err
}

func (h *holdService) waitEntered(t *testing.T, method string) {
	t.Helper()
	for {
		select {
		case m := <-h.entered:
			if m == method {
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never ran", method)
		}
	}
}

// waitEnded collects how the calls of methods ended.
func (h *holdService) waitEnded(t *testing.T, methods ...string) map[string]codes.Code {
	t.Helper()
	got := map[string]codes.Code{}
	for len(got) < len(methods) {
		select {
		case e := <-h.ended:
			got[e.method] = e.code
		case <-time.After(10 * time.Second):
			t.Fatalf("calls ended: %v, waiting for %v", got, methods)
		}
	}
	return got
}

// TestWireServerConnectionErrors sends a server, each on a connection of
// its own, the frames HTTP/2 makes connection errors of, and checks that
// the server answers each with a GOAWAY of the code the protocol names and
// closes the connection.
func TestWireServerConnectionErrors(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	settings := settingsFrame()
	bigBlock := bytes.Repeat([]byte{0}, defaultMaxFrame)
	tooLarge := [][]byte{settings, rawFrame(frameHeaders, 0, 1, bigBlock)}
	// The last CONTINUATION takes the block past the limit: nothing is
	// left unread when the server closes.
	for range maxHeaderBlock / defaultMaxFrame {
		tooLarge = append(tooLarge, rawFrame(frameContinuation, 0, 1, bigBlock))
	}
	for _, tc := range []struct {
		name   string
		frames [][]byte
		want   errCode
	}{
		{"first frame not SETTINGS", [][]byte{appendPing(nil, false, []byte("12345678"))}, errProtocol},
		{"first frame a SETTINGS ack", [][]byte{rawFrame(frameSettings, flagAck, 0, nil)}, errProtocol},
		{"HEADERS on stream 0", [][]byte{settings, rawFrame(frameHeaders, flagEndHeaders, 0, nil)}, errProtocol},
		{"HEADERS padding past its end", [][]byte{settings, rawFrame(frameHeaders, flagEndHeaders|flagPadded, 1, []byte{5, 'a'})}, errProtocol},
		{"HEADERS priority cut short", [][]byte{settings, rawFrame(frameHeaders, flagEndHeaders|flagPriority, 1, []byte{0, 0, 0})}, errProtocol},
		{"CONTINUATION of nothing", [][]byte{settings, rawFrame(frameContinuation, flagEndHeaders, 1, nil)}, errProtocol},
		{"no CONTINUATION after HEADERS", [][]byte{settings, rawFrame(frameHeaders, 0, 1, nil), appendPing(nil, false, []byte("12345678"))}, errProtocol},
		{"CONTINUATION on another stream", [][]byte{settings, rawFrame(frameHeaders, 0, 1, nil), rawFrame(frameContinuation, flagEndHeaders, 3, nil)}, errProtocol},
		{"header block too large", tooLarge, errEnhanceCalm},
		{"PRIORITY size", [][]byte{settings, rawFrame(framePriority, 0, 1, make([]byte, 4))}, errFrameSize},
		{"RST_STREAM size", [][]byte{settings, rawFrame(frameRSTStream, 0, 1, make([]byte, 3))}, errProtocol},
		{"RST_STREAM on stream 0", [][]byte{settings, rawFrame(frameRSTStream, 0, 0, make([]byte, 4))}, errProtocol},
		{"PUSH_PROMISE", [][]byte{settings, rawFrame(framePushPromise, flagEndHeaders, 1, make([]byte, 4))}, errProtocol},
		{"PING size", [][]byte{settings, rawFrame(framePing, 0, 0, make([]byte, 7))}, errProtocol},
		{"PING on a stream", [][]byte{settings, rawFrame(framePing, 0, 1, make([]byte, 8))}, errProtocol},
		{"GOAWAY size", [][]byte{settings, rawFrame(frameGoAway, 0, 0, make([]byte, 4))}, errProtocol},
		{"GOAWAY on a stream", [][]byte{settings, rawFrame(frameGoAway, 0, 1, make([]byte, 8))}, errProtocol},
		{"SETTINGS on a stream", [][]byte{settings, rawFrame(frameSettings, 0, 1, nil)}, errProtocol},
		{"SETTINGS ack with a payload", [][]byte{settings, rawFrame(frameSettings, flagAck, 0, make([]byte, 6))}, errFrameSize},
		{"SETTINGS size", [][]byte{settings, rawFrame(frameSettings, 0, 0, make([]byte, 5))}, errFrameSize},
		{"initial window too large", [][]byte{settings, settingsFrame(settingInitialWindowSize, 1<<31)}, errFlowControl},
		{"max frame size too small", [][]byte{settings, settingsFrame(settingMaxFrameSize, 100)}, errProtocol},
		{"max frame size too large", [][]byte{settings, settingsFrame(settingMaxFrameSize, 1<<24)}, errProtocol},
		{"WINDOW_UPDATE size", [][]byte{settings, rawFrame(frameWindowUpdate, 0, 0, make([]byte, 3))}, errFrameSize},
		{"WINDOW_UPDATE of nothing", [][]byte{settings, rawFrame(frameWindowUpdate, 0, 0, u32(0))}, errProtocol},
		{"connection window overflow", [][]byte{settings, rawFrame(frameWindowUpdate, 0, 0, u32(maxWindow))}, errFlowControl},
		{"DATA on stream 0", [][]byte{settings, dataFrame(0, nil, false)}, errProtocol},
		{"DATA padding past its end", [][]byte{settings, rawFrame(frameData, flagPadded, 1, []byte{9})}, errProtocol},
		{"DATA on an idle stream", [][]byte{settings, dataFrame(5, []byte("x"), false)}, errProtocol},
		{"frame over the max size", [][]byte{settings, appendFrameHeader(nil, frameData, 0, 1, defaultMaxFrame+1)}, errFrameSize},
		{"undecodable header block", [][]byte{settings, rawFrame(frameHeaders, flagEndHeaders, 1, []byte{0x80})}, errCompression},
		{"HEADERS on an even stream", [][]byte{settings, headersFrame(2, nil, true)}, errProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dialWire(t, addr, false)
			c.write(append([][]byte{[]byte(preface)}, tc.frames...)...)
			if got := c.goAway(); got != tc.want {
				t.Fatalf("GOAWAY code %d, want %d", got, tc.want)
			}
			c.closed()
		})
	}

	// A stream id is never used twice, even one the server turned away.
	c := dialWire(t, addr, true)
	get := c.block(":method", "GET", ":scheme", "http", ":path", "/", ":authority", "wire")
	c.write(headersFrame(3, get, true))
	if f := c.expect(frameHeaders, 3); f.fields[":status"] != "415" {
		t.Fatalf("a request that is not gRPC got %v", f.fields)
	}
	c.write(headersFrame(1, c.request("/test.Echo/Unary"), true))
	if got := c.goAway(); got != errProtocol {
		t.Fatalf("a reused stream id got GOAWAY %d", got)
	}
}

// TestWireServerStreamErrors sends a server, on one connection, what HTTP/2
// and gRPC make errors of one stream, and frames a client seldom sends,
// checking that each stream is answered as the protocol says while the
// connection goes on serving the others.
func TestWireServerStreamErrors(t *testing.T) {
	hold := newHoldService()
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(hold.handle))
	c := dialWire(t, addr, true)
	open := func(id uint32, path string) {
		c.write(headersFrame(id, c.request(path), false))
		hold.waitEntered(t, path)
	}

	// A WINDOW_UPDATE of nothing on a stream resets it.
	c.write(rawFrame(frameWindowUpdate, 0, 1, u32(0)))
	if got := c.reset(1); got != errProtocol {
		t.Fatalf("an empty WINDOW_UPDATE on a stream: RST %d", got)
	}
	// A header list over the limit resets its stream.
	c.write(oversizedHeaderList(3))
	if got := c.reset(3); got != errEnhanceCalm {
		t.Fatalf("an oversized header list: RST %d", got)
	}
	// Binary metadata that is not base64.
	c.write(headersFrame(5, c.request("/test.Echo/Unary", "k-bin", "!!!"), true))
	if got := c.reset(5); got != errProtocol {
		t.Fatalf("malformed binary metadata: RST %d", got)
	}
	// A second header block that does not end the client's side.
	open(7, "/hold/7")
	c.write(headersFrame(7, c.block("x", "y"), false))
	if got := c.reset(7); got != errProtocol {
		t.Fatalf("trailers without END_STREAM: RST %d", got)
	}
	// Trailers of the client end its side, as an empty DATA would.
	c.write(headersFrame(9, c.request("/test.Echo/Chat"), false), dataFrame(9, grpcMessage(t, Req{Msg: "hi"}), false),
		headersFrame(9, c.block("client-trailer", "x"), true))
	if st := c.trailers(9); st["grpc-status"] != "0" {
		t.Fatalf("a call the client ended with trailers: %v", st)
	}
	// A window grown past 2^31-1 resets the stream.
	open(11, "/hold/11")
	c.write(rawFrame(frameWindowUpdate, 0, 11, u32(maxWindow)))
	if got := c.reset(11); got != errFlowControl {
		t.Fatalf("a stream window overflow: RST %d", got)
	}
	// More DATA than the stream's window, which a handler that reads
	// nothing never grows.
	open(13, "/hold/13")
	chunk := make([]byte, defaultMaxFrame)
	var flood [][]byte
	for range streamWindow/defaultMaxFrame + 1 {
		flood = append(flood, dataFrame(13, chunk, false))
	}
	c.write(flood...)
	if got := c.reset(13); got != errFlowControl {
		t.Fatalf("DATA past the window: RST %d", got)
	}
	// A request that ends in the middle of a message.
	c.write(headersFrame(15, c.request("/test.Echo/Unary"), false), dataFrame(15, []byte{0, 0, 0, 0, 10, 'a', 'b', 'c'}, true))
	if st := c.trailers(15); st["grpc-status"] != "13" {
		t.Fatalf("a truncated request: %v", st)
	}
	// A content subtype no codec serves, and a compressor the server lacks.
	c.write(headersFrame(17, c.block(":method", "POST", ":scheme", "http", ":path", "/test.Echo/Unary", ":authority", "wire", "content-type", "application/grpc+nope"), true))
	if st := c.trailers(17); st["grpc-status"] != "13" || st["content-type"] != "application/grpc" {
		t.Fatalf("an unknown content subtype: %v", st)
	}
	c.write(headersFrame(19, c.request("/test.Echo/Unary", "grpc-encoding", "bogus"), true))
	if st := c.trailers(19); st["grpc-status"] != "12" {
		t.Fatalf("an unknown grpc-encoding: %v", st)
	}
	// A gRPC content type with the wrong method gets 405, and a reset of the
	// request it did not wait for.
	c.write(headersFrame(21, c.block(":method", "GET", ":scheme", "http", ":path", "/test.Echo/Unary", ":authority", "wire", "content-type", "application/grpc"), false))
	if f := c.expect(frameHeaders, 21); f.fields[":status"] != "405" {
		t.Fatalf("a GET of a gRPC method: %v", f.fields)
	}
	if got := c.reset(21); got != errNo {
		t.Fatalf("the rest of the GET: RST %d", got)
	}

	// Padding and priority on HEADERS, a header block continued, padded
	// DATA, and the frames that change nothing of the call, mid-call.
	block := c.request("/test.Echo/Unary")
	headers := append([]byte{2}, 0, 0, 0, 0, 16)
	headers = append(append(headers, block[:4]...), 0, 0)
	msg := grpcMessage(t, Req{Msg: "padded", N: 7})
	c.write(rawFrame(frameHeaders, flagPadded|flagPriority, 23, headers),
		rawFrame(frameContinuation, flagEndHeaders, 23, block[4:]),
		rawFrame(framePriority, 0, 23, make([]byte, 5)),
		rawFrame(0x20, 0, 0, []byte("an extension frame")),
		settingsFrame(settingHeaderTableSize, 100, settingMaxFrameSize, 20000, settingMaxConcurrentStreams, 50, settingMaxHeaderListSize, 1<<20),
		rawFrame(frameWindowUpdate, 0, 99, u32(10)),
		rawFrame(frameData, flagPadded|flagEndStream, 23, append(append([]byte{3}, msg...), 0, 0, 0)))
	if f := c.expect(frameData, 23); !bytes.Contains(f.payload, []byte(`"padded"`)) {
		t.Fatalf("the padded call's reply %q", f.payload)
	}
	if st := c.trailers(23); st["grpc-status"] != "0" {
		t.Fatalf("the padded call: %v", st)
	}
	// DATA for a stream that has ended is dropped.
	c.write(dataFrame(23, []byte("late"), false))

	// A new initial window applies to the streams already open.
	open(25, "/hold/25")
	c.write(settingsFrame(settingInitialWindowSize, 1000))
	c.expect(frameSettings, 0)

	// Resets from the client end the calls with the codes they map to.
	open(27, "/hold/27")
	open(29, "/hold/29")
	open(31, "/hold/31")
	c.write(rawFrame(frameRSTStream, 0, 25, u32(uint32(errNo))),
		rawFrame(frameRSTStream, 0, 27, u32(uint32(errRefusedStream))),
		rawFrame(frameRSTStream, 0, 29, u32(uint32(errEnhanceCalm))),
		rawFrame(frameRSTStream, 0, 31, u32(uint32(errInternal))),
		rawFrame(frameRSTStream, 0, 31, u32(uint32(errCancel))))
	got := hold.waitEnded(t, "/hold/7", "/hold/11", "/hold/13", "/hold/25", "/hold/27", "/hold/29", "/hold/31")
	want := map[string]codes.Code{
		"/hold/7": codes.Internal, "/hold/11": codes.Internal, "/hold/13": codes.Internal,
		"/hold/25": codes.Canceled, "/hold/27": codes.Unavailable, "/hold/29": codes.ResourceExhausted, "/hold/31": codes.Internal,
	}
	for m, code := range want {
		if got[m] != code {
			t.Errorf("%s ended with %v, want %v", m, got[m], code)
		}
	}

	// The connection still serves calls.
	c.write(headersFrame(33, c.request("/test.Echo/Unary"), false), dataFrame(33, grpcMessage(t, Req{Msg: "after"}), true))
	if st := c.trailers(33); st["grpc-status"] != "0" {
		t.Fatalf("a call after the errors: %v", st)
	}

	// Messages the server cannot read: compressed without a grpc-encoding,
	// compressed wrongly, and not in the codec's encoding.
	c.write(headersFrame(35, c.request("/test.Echo/Unary"), false), dataFrame(35, lengthPrefixed(true, []byte("{}")), true),
		headersFrame(37, c.request("/test.Echo/Unary", "grpc-encoding", "gzip"), false), dataFrame(37, lengthPrefixed(true, []byte("not gzip")), true),
		headersFrame(39, c.request("/test.Echo/Unary"), false), dataFrame(39, lengthPrefixed(false, []byte(`"str"`)), true))
	// The three are served at once and may end in any order.
	ended := map[uint32]string{}
	for len(ended) < 3 {
		if f := c.next(frameHeaders); f.has(flagEndStream) {
			ended[f.streamID] = f.fields["grpc-status"]
		}
	}
	for _, id := range []uint32{35, 37, 39} {
		if ended[id] != "13" {
			t.Fatalf("an unreadable message on stream %d: grpc-status %q", id, ended[id])
		}
	}
}

// TestWireServerRefusesStreams checks the streams a server turns away with
// REFUSED_STREAM: one past MaxConcurrentStreams, which a client that heeds
// SETTINGS never opens, and one opened after GracefulStop's GOAWAY. It also
// checks that GracefulStop closes an idle connection at once, waits for the
// call in progress, and leaves the server closing connections that arrive
// after it.
func TestWireServerRefusesStreams(t *testing.T) {
	hold := newHoldService()
	_, addr := startServer(t, &echoImpl{}, MaxConcurrentStreams(1), UnknownServiceHandler(hold.handle))
	c := dialWire(t, addr, true)
	c.write(headersFrame(1, c.request("/hold/1"), false))
	hold.waitEntered(t, "/hold/1")
	c.write(headersFrame(3, c.request("/hold/3"), false))
	if got := c.reset(3); got != errRefusedStream {
		t.Fatalf("a stream past MaxConcurrentStreams: RST %d", got)
	}

	s, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(hold.handle))
	busy := dialWire(t, addr, true)
	busy.write(headersFrame(1, busy.request("/hold/busy"), false))
	hold.waitEntered(t, "/hold/busy")
	idle := dialWire(t, addr, true)
	idle.expect(frameSettings, 0)
	stopped := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(stopped)
	}()
	if got := idle.goAway(); got != errNo {
		t.Fatalf("GracefulStop's GOAWAY to an idle connection: %d", got)
	}
	idle.closed()
	if got := busy.goAway(); got != errNo {
		t.Fatalf("GracefulStop's GOAWAY: %d", got)
	}
	busy.write(headersFrame(3, busy.request("/test.Echo/Unary"), true))
	if got := busy.reset(3); got != errRefusedStream {
		t.Fatalf("a stream after GOAWAY: RST %d", got)
	}
	select {
	case <-stopped:
		t.Fatal("GracefulStop returned with a call in progress")
	default:
	}
	busy.write(rawFrame(frameRSTStream, 0, 1, u32(uint32(errCancel))))
	busy.closed()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("GracefulStop never returned")
	}
	late := dialWire(t, addr, false)
	late.closed()
}

// TestWireServerGoAwayBehindAFlush checks that a GOAWAY framed while another
// goroutine is handing the connection's frames over still goes out before
// the connection closes. flush returns at once while another goroutine is
// flushing, and GracefulStop and fail used to close the connection as soon
// as it returned, dropping the GOAWAY, and with it, for a connection
// draining its last call, that call's trailers.
func TestWireServerGoAwayBehindAFlush(t *testing.T) {
	s, addr := startServer(t, &echoImpl{})
	c := dialWire(t, addr, true)
	c.sync()
	s.mu.Lock()
	var tr *transport
	for x := range s.transports {
		tr = x
	}
	s.mu.Unlock()
	// Another goroutine is, as far as the transport knows, mid-flush.
	tr.mu.Lock()
	tr.writing = true
	tr.mu.Unlock()
	stopped := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(stopped)
	}()
	for deadline := time.Now().Add(10 * time.Second); ; {
		tr.mu.Lock()
		framed := tr.goingAway
		tr.mu.Unlock()
		if framed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GracefulStop never framed its GOAWAY")
		}
		time.Sleep(time.Millisecond)
	}
	// That goroutine finishes, taking the GOAWAY with it.
	tr.mu.Lock()
	tr.writing = false
	tr.mu.Unlock()
	tr.flush()
	if got := c.goAway(); got != errNo {
		t.Fatalf("GOAWAY %d", got)
	}
	c.closed()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("GracefulStop never returned")
	}
}
