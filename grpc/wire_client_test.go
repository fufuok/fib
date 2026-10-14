//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

// TestWireClientResponses answers a ClientConn's calls by hand with the
// responses a server may send and a gRPC server never should, checking the
// status each call ends with: an HTTP error, a response that is not gRPC,
// an unknown compressor, misplaced or missing trailers, messages that
// cannot be read, resets of each code, and a unary call answered with the
// wrong number of messages.
func TestWireClientResponses(t *testing.T) {
	addr, accept := wireServer(t)
	_, c := dial(t, addr, WithAuthority("authority.test"), WithUserAgent("wire-test"), WithConnectTimeout(5*time.Second))
	ctx := testCtx(t)
	var srv *wireConn
	nextID := uint32(1)
	// call starts a unary call and returns its outcome and its stream.
	call := func() (<-chan error, uint32) {
		t.Helper()
		done := goCall(func() error {
			_, err := c.Unary(ctx, &Req{Msg: "wire"})
			return err
		})
		if srv == nil {
			srv = accept()
		}
		id := nextID
		nextID += 2
		f := srv.expect(frameHeaders, id)
		if id == 1 && (f.fields[":authority"] != "authority.test" || !strings.HasPrefix(f.fields["user-agent"], "wire-test ") || f.fields["grpc-timeout"] == "") {
			t.Fatalf("the request's header %v", f.fields)
		}
		return done, id
	}
	ok := grpcMessage(t, Rsp{Msg: "ok"})
	for _, tc := range []struct {
		name    string
		answer  func(id uint32) [][]byte
		code    codes.Code
		message string
		reset   errCode // the client's RST_STREAM, if it sends one that says more than CANCEL
	}{
		{"an HTTP error", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.block(":status", "503"), true)}
		}, codes.Unavailable, "unexpected HTTP status code", 0},
		{"a response that is not gRPC", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.block(":status", "200", "content-type", "text/html"), false)}
		}, codes.Internal, "unexpected content-type", 0},
		{"an unknown compressor", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response("grpc-encoding", "bogus"), false)}
		}, codes.Internal, "Decompressor is not installed", 0},
		{"a second header", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), headersFrame(id, srv.block("x", "y"), false)}
		}, codes.Internal, "", errProtocol},
		{"trailers without a status", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), dataFrame(id, ok, false), headersFrame(id, srv.block("x", "y"), true)}
		}, codes.Internal, "without sending trailers", 0},
		{"a status with details", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response("grpc-status", "5", "grpc-message", "gone%20away", "grpc-status-details-bin", encodeBin([]byte("details"))), true)}
		}, codes.NotFound, "gone away", 0},
		{"a compressed message without grpc-encoding", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), dataFrame(id, lengthPrefixed(true, []byte("{}")), false), headersFrame(id, srv.block("grpc-status", "0"), true)}
		}, codes.Internal, "compressed message without grpc-encoding", 0},
		{"a message that does not decompress", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response("grpc-encoding", "gzip"), false), dataFrame(id, lengthPrefixed(true, []byte("not gzip")), false), headersFrame(id, srv.block("grpc-status", "0"), true)}
		}, codes.Internal, "failed to decompress", 0},
		{"a message that does not decode", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), dataFrame(id, lengthPrefixed(false, []byte(`"str"`)), false), headersFrame(id, srv.block("grpc-status", "0"), true)}
		}, codes.Internal, "failed to unmarshal", 0},
		{"REFUSED_STREAM", func(id uint32) [][]byte {
			return [][]byte{rawFrame(frameRSTStream, 0, id, u32(uint32(errRefusedStream)))}
		}, codes.Unavailable, "", 0},
		{"ENHANCE_YOUR_CALM", func(id uint32) [][]byte {
			return [][]byte{rawFrame(frameRSTStream, 0, id, u32(uint32(errEnhanceCalm)))}
		}, codes.ResourceExhausted, "", 0},
		{"NO_ERROR before the status", func(id uint32) [][]byte {
			return [][]byte{rawFrame(frameRSTStream, 0, id, u32(uint32(errNo)))}
		}, codes.Internal, "", 0},
		{"CANCEL", func(id uint32) [][]byte {
			return [][]byte{rawFrame(frameRSTStream, 0, id, u32(uint32(errCancel)))}
		}, codes.Canceled, "", 0},
		{"two replies to a unary call", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), dataFrame(id, append(append([]byte(nil), ok...), ok...), false), headersFrame(id, srv.block("grpc-status", "0"), true)}
		}, codes.Internal, "cardinality violation: expected <EOF>", 0},
		{"no reply to a unary call", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), headersFrame(id, srv.block("grpc-status", "0"), true)}
		}, codes.Internal, "received no response message", 0},
		{"a reply and then an error", func(id uint32) [][]byte {
			return [][]byte{headersFrame(id, srv.response(), false), dataFrame(id, ok, false), headersFrame(id, srv.block("grpc-status", "3"), true)}
		}, codes.InvalidArgument, "", 0},
	} {
		done, id := call()
		srv.write(tc.answer(id)...)
		err := waitCall(t, done)
		if s := status.Convert(err); s.Code() != tc.code || !strings.Contains(s.Message(), tc.message) {
			t.Errorf("%s: %v, want %v %q", tc.name, err, tc.code, tc.message)
		}
		if tc.code == codes.NotFound && string(status.Convert(err).Details()) != "details" {
			t.Errorf("%s: details %q", tc.name, status.Convert(err).Details())
		}
		if tc.reset != 0 {
			if got := srv.reset(id); got != tc.reset {
				t.Errorf("%s: the client reset the stream with %d, want %d", tc.name, got, tc.reset)
			}
		}
	}

	// Header waits for the header, and for a response that is only
	// trailers gives their status, or no metadata for OK.
	desc := &StreamDesc{ServerStreams: true}
	for _, tc := range []struct {
		grpcStatus string
		code       codes.Code
	}{{"5", codes.NotFound}, {"0", codes.OK}} {
		cs, err := c.cc.NewStream(ctx, desc, "/test.Echo/Count")
		if err != nil {
			t.Fatal(err)
		}
		id := nextID
		nextID += 2
		srv.expect(frameHeaders, id)
		srv.write(headersFrame(id, srv.response("grpc-status", tc.grpcStatus, "t", "v"), true))
		md, err := cs.Header()
		if status.Code(err) != tc.code || err == nil && md == nil {
			t.Errorf("Header of a trailers-only %s response = %v, %v", tc.grpcStatus, md, err)
		}
		if err := cs.RecvMsg(&Rsp{}); tc.code == codes.OK && err != io.EOF || tc.code != codes.OK && status.Code(err) != tc.code {
			t.Errorf("RecvMsg of a trailers-only %s response = %v", tc.grpcStatus, err)
		}
		if cs.Trailer().Get("t")[0] != "v" || cs.Context() != ctx {
			t.Errorf("Trailer = %v", cs.Trailer())
		}
	}
	// Header of a call reset before its header arrived.
	cs, err := c.cc.NewStream(ctx, desc, "/test.Echo/Count")
	if err != nil {
		t.Fatal(err)
	}
	srv.expect(frameHeaders, nextID)
	srv.write(rawFrame(frameRSTStream, 0, nextID, u32(uint32(errRefusedStream))))
	nextID += 2
	if _, err := cs.Header(); status.Code(err) != codes.Unavailable {
		t.Errorf("Header of a reset call = %v", err)
	}
	// Header of a call canceled before its header arrived.
	cctx, cancel := context.WithCancel(ctx)
	cs, err = c.cc.NewStream(cctx, desc, "/test.Echo/Count")
	if err != nil {
		t.Fatal(err)
	}
	srv.expect(frameHeaders, nextID)
	nextID += 2
	cancel()
	if _, err := cs.Header(); status.Code(err) != codes.Canceled {
		t.Errorf("Header of a canceled call = %v", err)
	}

	// A PING is answered, and a header block for a stream the client has
	// let go of is read and dropped.
	srv.write(headersFrame(1, srv.response(), false))
	srv.sync()
	// DATA on a stream the client never opened ends the connection.
	srv.write(dataFrame(nextID+100, []byte("x"), false))
	if got := srv.goAway(); got != errProtocol {
		t.Fatalf("DATA on an idle stream: GOAWAY %d", got)
	}
	srv.closed()
}

// TestWireClientGoAway checks a client told GOAWAY: a call above the last
// stream the server will serve fails Unavailable while the one below it
// completes, the connection closes once that one has, the next call
// connects again, and an idle connection told GOAWAY closes at once.
func TestWireClientGoAway(t *testing.T) {
	addr, accept := wireServer(t)
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	invoke := func(method string) <-chan error {
		return goCall(func() error {
			var r Rsp
			err := cc.Invoke(ctx, method, &Req{}, &r)
			if err == nil && r.Msg != method {
				t.Errorf("%s got %q", method, r.Msg)
			}
			return err
		})
	}
	a := invoke("/wire/A")
	srv := accept()
	ids := map[string]uint32{}
	f := srv.next(frameHeaders)
	ids[f.fields[":path"]] = f.streamID
	b := invoke("/wire/B")
	f = srv.next(frameHeaders)
	ids[f.fields[":path"]] = f.streamID
	srv.write(appendGoAway(nil, ids["/wire/A"], errNo, "draining"))
	if err := waitCall(t, b); status.Code(err) != codes.Unavailable {
		t.Fatalf("the call above GOAWAY's last stream = %v", err)
	}
	id := ids["/wire/A"]
	srv.write(headersFrame(id, srv.response(), false), dataFrame(id, grpcMessage(t, Rsp{Msg: "/wire/A"}), false),
		headersFrame(id, srv.block("grpc-status", "0"), true))
	if err := waitCall(t, a); err != nil {
		t.Fatalf("the call below GOAWAY's last stream = %v", err)
	}
	srv.closed()

	c := invoke("/wire/C")
	srv = accept()
	f = srv.next(frameHeaders)
	srv.write(headersFrame(f.streamID, srv.response(), false), dataFrame(f.streamID, grpcMessage(t, Rsp{Msg: "/wire/C"}), false),
		headersFrame(f.streamID, srv.block("grpc-status", "0"), true))
	if err := waitCall(t, c); err != nil {
		t.Fatalf("a call on a new connection = %v", err)
	}
	srv.write(appendGoAway(nil, f.streamID, errNo, ""))
	srv.closed()
}

// TestWireClientFlowControl checks that a client resets a stream whose
// server sends past the window the client gave it, and that the call then
// fails both ways.
func TestWireClientFlowControl(t *testing.T) {
	addr, accept := wireServer(t)
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	cs, err := cc.NewStream(ctx, &echoDesc.Streams[2], "/test.Echo/Chat")
	if err != nil {
		t.Fatal(err)
	}
	srv := accept()
	srv.expect(frameHeaders, 1)
	frames := [][]byte{headersFrame(1, srv.response(), false)}
	chunk := make([]byte, defaultMaxFrame)
	for range streamWindow/defaultMaxFrame + 1 {
		frames = append(frames, dataFrame(1, chunk, false))
	}
	srv.write(frames...)
	if got := srv.reset(1); got != errFlowControl {
		t.Fatalf("DATA past the window: RST %d", got)
	}
	if err := cs.RecvMsg(&Rsp{}); status.Code(err) != codes.Internal {
		t.Fatalf("RecvMsg of the reset call = %v", err)
	}
	if err := cs.SendMsg(&Req{}); err != io.EOF {
		t.Fatalf("SendMsg on the reset call = %v", err)
	}
}

// TestWireClientWindows checks a client within what the server's SETTINGS
// allow: a call opened past MaxConcurrentStreams waits for room, or fails
// once its context ends; a message waits for the window; a call ended
// leaves SendMsg failing as the call did; and a closed connection fails
// what is sent on it.
func TestWireClientWindows(t *testing.T) {
	addr, accept := wireServer(t)
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	chat := &echoDesc.Streams[2]
	a, err := cc.NewStream(ctx, chat, "/test.Echo/Chat")
	if err != nil {
		t.Fatal(err)
	}
	srv := accept()
	srv.expect(frameHeaders, 1)
	srv.write(settingsFrame(settingMaxConcurrentStreams, 1, settingInitialWindowSize, 0))
	srv.sync()

	// A's window is now empty: its message waits for a WINDOW_UPDATE.
	sent := goCall(func() error { return a.SendMsg(&Req{Msg: "a"}) })
	// B waits for A to end, and C gives up waiting.
	bctx, cancelB := context.WithCancel(ctx)
	defer cancelB()
	type opened struct {
		cs  ClientStream
		err error
	}
	bOpened := make(chan opened, 1)
	go func() {
		cs, err := cc.NewStream(bctx, chat, "/test.Echo/Chat")
		bOpened <- opened{cs, err}
	}()
	cctx, cancelC := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelC()
	if _, err := cc.NewStream(cctx, chat, "/test.Echo/Chat"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a call past MaxConcurrentStreams whose context ended = %v", err)
	}
	select {
	case <-sent:
		t.Fatal("a message went out with no window")
	default:
	}
	srv.write(rawFrame(frameWindowUpdate, 0, 1, u32(1000)))
	if f := srv.expect(frameData, 1); !strings.Contains(string(f.payload), `"a"`) {
		t.Fatalf("A's message %q", f.payload)
	}
	if err := waitCall(t, sent); err != nil {
		t.Fatal(err)
	}

	// A ends; B opens in its place.
	srv.write(headersFrame(1, srv.response("grpc-status", "0"), true))
	if err := a.RecvMsg(&Rsp{}); err != io.EOF {
		t.Fatalf("A's end = %v", err)
	}
	// The call ended without CloseSend: SendMsg says so with io.EOF, as
	// ClientStream documents, not as a misuse. It used to report
	// "SendMsg called after CloseSend", as it also did, now and then, for a
	// call canceled while a message waited for the window.
	if err := a.SendMsg(&Req{}); err != io.EOF {
		t.Fatalf("SendMsg after the call ended = %v", err)
	}
	srv.expect(frameHeaders, 3)
	var b opened
	select {
	case b = <-bOpened:
	case <-time.After(10 * time.Second):
		t.Fatal("B never opened")
	}
	if b.err != nil {
		t.Fatal(b.err)
	}
	// B's message waits for a window that never comes, until B is canceled.
	sent = goCall(func() error { return b.cs.SendMsg(&Req{Msg: "b"}) })
	cancelB()
	if err := waitCall(t, sent); err != io.EOF {
		t.Fatalf("SendMsg of a canceled call = %v", err)
	}
	if err := b.cs.RecvMsg(&Rsp{}); status.Code(err) != codes.Canceled {
		t.Fatalf("RecvMsg of a canceled call = %v", err)
	}

	// D is on a connection that closes.
	d, err := cc.NewStream(ctx, chat, "/test.Echo/Chat")
	if err != nil {
		t.Fatal(err)
	}
	srv.expect(frameHeaders, 5)
	srv.conn.Close()
	if err := d.RecvMsg(&Rsp{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("RecvMsg on a closed connection = %v", err)
	}
	if err := d.SendMsg(&Req{}); err != io.EOF {
		t.Fatalf("SendMsg on a closed connection = %v", err)
	}
}
