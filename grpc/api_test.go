//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/peer"
	"github.com/lesismal/fib/grpc/status"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/taskpool"
)

// boomCompressor passes messages through unchanged, failing to compress
// one that holds "boom": a compressor that fails on one side of a call
// and not the other.
type boomCompressor struct{}

func (boomCompressor) Name() string                              { return "test-boom" }
func (boomCompressor) Decompress(r io.Reader) (io.Reader, error) { return r, nil }
func (boomCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return boomWriter{w}, nil
}

type boomWriter struct{ w io.Writer }

func (b boomWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("boom")) {
		return 0, errors.New("boom")
	}
	return b.w.Write(p)
}

func (boomWriter) Close() error { return nil }

func init() { RegisterCompressor(boomCompressor{}) }

// apiHandler serves the methods of TestServerHandlerAPI, each exercising
// what a handler may do with its stream; gone reports what SendMsg said
// once the client of /api/gone had gone.
type apiHandler struct{ gone chan error }

func (h *apiHandler) handle(_ any, stream ServerStream) error {
	ctx := stream.Context()
	method, _ := Method(ctx)
	switch method {
	case "/api/headers":
		if err := stream.SendHeader(metadata.Pairs("A", "1")); err != nil {
			return err
		}
		if stream.SetHeader(metadata.Pairs("b", "2")) == nil || stream.SendHeader(nil) == nil ||
			SetHeader(ctx, nil) == nil || SendHeader(ctx, nil) == nil {
			return status.Error(codes.Internal, "the header was set after it was sent")
		}
		if err := stream.SendMsg(&Rsp{Msg: "headers"}); err != nil {
			return err
		}
		return status.WithDetails(codes.Aborted, "with details", []byte("det")).Err()
	case "/api/marshal":
		return stream.SendMsg(make(chan int))
	case "/api/big":
		return stream.SendMsg(&Rsp{Msg: strings.Repeat("b", 2000)})
	case "/api/boom":
		var req Req
		if err := stream.RecvMsg(&req); err != nil {
			return err
		}
		return stream.SendMsg(&Rsp{Msg: "boom"})
	case "/api/gone":
		// Err first, so that Done is first asked for once the call has
		// ended.
		for deadline := time.Now().Add(15 * time.Second); ctx.Err() == nil && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		<-ctx.Done()
		if err := SendHeader(ctx, metadata.Pairs("late", "1")); err != nil {
			h.gone <- err
			return err
		}
		err := stream.SendMsg(&Rsp{})
		h.gone <- err
		return err
	}
	return status.Errorf(codes.Unimplemented, "no %s", method)
}

// TestServerHandlerAPI checks what a handler can and cannot do with its
// stream: the header goes once, a status carries its details, and a
// message that will not marshal, is over MaxSendMsgSize, fails to
// compress, or is for a client that has gone fails the call as it should.
// It also checks options that change how a server reads calls: a codec
// forced whatever the content subtype, a receive limit applied after
// decompression, a single unary interceptor, and chained stream ones.
func TestServerHandlerAPI(t *testing.T) {
	testHandlerAPI(t, func(opts ...ServerOption) string {
		_, addr := startServer(t, &echoImpl{}, opts...)
		return addr
	})
}

// testHandlerAPI runs TestServerHandlerAPI's calls against the server serve
// starts with opts.
func testHandlerAPI(t *testing.T, serve func(opts ...ServerOption) string) {
	var mu sync.Mutex
	var trace []string
	record := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	streamInt := func(name string) StreamServerInterceptor {
		return func(srv any, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error {
			record(name)
			return handler(srv, ss)
		}
	}
	h := &apiHandler{gone: make(chan error, 1)}
	addr := serve(
		UnknownServiceHandler(h.handle),
		MaxSendMsgSize(1000), MaxRecvMsgSize(1000), ForceServerCodec(jsonCodec{}),
		UnaryInterceptor(func(ctx context.Context, req any, info *UnaryServerInfo, handler UnaryHandler) (any, error) {
			record("unary")
			return handler(ctx, req)
		}),
		ChainStreamInterceptor(streamInt("s1"), streamInt("s2")))
	// The client names a codec the server does not have, which the forced
	// one decodes.
	cc, c := dial(t, addr, WithDefaultCallOptions(ForceCodec(namedCodec("other"))))
	ctx := testCtx(t)

	var header metadata.MD
	err := cc.Invoke(ctx, "/api/headers", &Req{}, &Rsp{}, Header(&header))
	if s := status.Convert(err); s.Code() != codes.Aborted || string(s.Details()) != "det" || header.Get("a")[0] != "1" {
		t.Fatalf("/api/headers = %v, details %q, header %v", err, s.Details(), header)
	}
	for method, want := range map[string]codes.Code{"/api/marshal": codes.Internal, "/api/big": codes.ResourceExhausted} {
		if err := cc.Invoke(ctx, method, &Req{}, &Rsp{}); status.Code(err) != want {
			t.Errorf("%s = %v, want %v", method, err, want)
		}
	}
	if err := cc.Invoke(ctx, "/api/boom", &Req{Msg: "x"}, &Rsp{}, UseCompressor("test-boom")); status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "compressing") {
		t.Errorf("a reply that fails to compress = %v", err)
	}
	if err := cc.Invoke(ctx, "/api/boom", &Req{Msg: "boom"}, &Rsp{}, UseCompressor("test-boom")); status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "compressing") {
		t.Errorf("a request that fails to compress = %v", err)
	}
	// Limits apply to what a message decompresses to, both ways.
	zeros := strings.Repeat("z", 2000)
	if _, err := c.Unary(ctx, &Req{Msg: zeros}, UseCompressor("gzip")); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("a request over the server's limit once decompressed = %v", err)
	}
	if _, err := c.Unary(ctx, &Req{Msg: zeros[:800]}, UseCompressor("gzip"), MaxCallRecvMsgSize(200)); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("a reply over the client's limit once decompressed = %v", err)
	}

	// A handler whose client has gone sends nothing.
	gctx, cancel := context.WithCancel(ctx)
	cs, err := cc.NewStream(gctx, &StreamDesc{ServerStreams: true, ClientStreams: true}, "/api/gone")
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&Req{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-h.gone:
		if err == nil {
			t.Fatal("SendMsg to a client that has gone succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("/api/gone never ended")
	}

	mu.Lock()
	got := strings.Join(trace, " ")
	mu.Unlock()
	if !strings.HasPrefix(got, "s1 s2 s1 s2") || !strings.Contains(got, "unary") {
		t.Errorf("interceptors ran %q", got)
	}
}

// TestServerAPI covers what a Server tells and takes outside its calls:
// its services, TLS config, registration mistakes, the functions of a
// handler's context called without one, and a stream done error as a call
// sees it.
func TestServerAPI(t *testing.T) {
	s := NewServer()
	s.RegisterService(&echoDesc, &echoImpl{})
	info := s.GetServiceInfo()["test.Echo"]
	kinds := map[string][2]bool{}
	for _, m := range info.Methods {
		kinds[m.Name] = [2]bool{m.IsClientStream, m.IsServerStream}
	}
	if len(kinds) != 4 || kinds["Unary"] != [2]bool{} || kinds["Chat"] != [2]bool{true, true} || kinds["Count"] != [2]bool{false, true} {
		t.Fatalf("GetServiceInfo = %+v", info)
	}
	if c := ConfigureTLS(nil); len(c.NextProtos) != 1 || c.NextProtos[0] != "h2" {
		t.Fatalf("ConfigureTLS(nil) = %v", c.NextProtos)
	}
	for name, register := range map[string]func(){
		"a duplicate":            func() { s.RegisterService(&echoDesc, &echoImpl{}) },
		"the wrong handler type": func() { NewServer().RegisterService(&echoDesc, struct{}{}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("registering %s did not panic", name)
				}
			}()
			register()
		}()
	}
	s.OnPriorityData(nil, nil)
	(&transport{}).OnPriorityData(nil, nil)

	ctx := context.Background()
	if SetHeader(ctx, nil) == nil || SendHeader(ctx, nil) == nil || SetTrailer(ctx, nil) == nil {
		t.Fatal("the functions of a handler's context took a context without a call")
	}
	if _, ok := Method(ctx); ok {
		t.Fatal("Method of a context without a call")
	}
	if status.Code(toRPCErr(errStreamDone)) != codes.Internal {
		t.Fatal("toRPCErr(errStreamDone)")
	}
}

// refusingPool turns every task away, as a full pool does.
type refusingPool struct{}

func (refusingPool) GoTask(taskpool.Task) bool   { return false }
func (refusingPool) GoTasks([]taskpool.Task) int { return 0 }

// TestTaskPoolRefuses checks that a call the pool turns away runs on a
// goroutine of its own, as it does when there is no pool at all.
func TestTaskPoolRefuses(t *testing.T) {
	_, addr := startServer(t, &echoImpl{}, TaskPool(refusingPool{}))
	_, c := dial(t, addr)
	if r, err := c.Unary(testCtx(t), &Req{Msg: "refused"}); err != nil || r.Msg != "refused" {
		t.Fatalf("a call the pool refused = %+v, %v", r, err)
	}
	tr := &transport{server: NewServer()}
	if tr.taskPool() != nil {
		t.Fatal("a transport without a pool has one")
	}
	ran := make(chan struct{})
	tr.run(taskFunc(func() { close(ran) }))
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("a task without a pool never ran")
	}
}

type taskFunc func()

func (f taskFunc) RunTask() { f() }

// TestServeLifecycle checks Serve: it fails for an address it cannot bind,
// Engine reports the engine it made, and once stopped it ends the calls of
// connections the closing engine ended without OnClose.
func TestServeLifecycle(t *testing.T) {
	config := fib.DefaultConfig()
	if err := NewServer().Serve(config, "not an address"); err == nil {
		t.Fatal("Serve of a bad address succeeded")
	}

	s := NewServer()
	s.RegisterService(&echoDesc, &echoImpl{})
	if s.Engine() != nil {
		t.Fatal("Engine before Serve")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	served := make(chan error, 1)
	go func() { served <- s.Serve(config, addr) }()
	cc, c := dial(t, addr)
	ctx := testCtx(t)
	if _, err := c.Unary(ctx, &Req{Msg: "x"}, WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	if s.Engine() == nil {
		t.Fatal("no Engine while serving")
	}
	// A transport whose connection is not the engine's: Stop closes the
	// connection, and nothing tells the server, as when the engine closes.
	cc.mu.Lock()
	orphan := newTransport(false)
	orphan.server, orphan.conn = s, cc.t.conn
	cc.mu.Unlock()
	s.mu.Lock()
	s.transports[orphan] = struct{}{}
	s.mu.Unlock()
	s.Stop()
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve never returned")
	}
	orphan.mu.Lock()
	closed := orphan.closed
	orphan.mu.Unlock()
	s.mu.Lock()
	left := len(s.transports)
	s.mu.Unlock()
	if !closed || left != 0 {
		t.Fatalf("after Serve: the orphan closed %v, %d transports left", closed, left)
	}

	// A call that arrives on a connection closed under it is dropped.
	tr := newTransport(false)
	tr.server, tr.conn, tr.closed = s, orphan.conn, true
	s.openStream(tr, 1, []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":path", Value: "/test.Echo/Unary"},
		{Name: "content-type", Value: "application/grpc+json"}}, true)
	if len(tr.streams) != 0 {
		t.Fatal("a stream opened on a closed connection")
	}
	tr.onClose(nil)
}

// TestLookupMalformed checks a method name without a service: Unimplemented,
// or the UnknownServiceHandler's to serve.
func TestLookupMalformed(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	if err := cc.Invoke(ctx, "/nope", &Req{}, &Rsp{}); status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "malformed method name") {
		t.Fatalf("a malformed method = %v", err)
	}
	s := NewServer(UnknownServiceHandler(func(_ any, stream ServerStream) error {
		method, _ := Method(stream.Context())
		return stream.SendMsg(&Rsp{Msg: method})
	}))
	cc2, _ := dial(t, startHandler(t, s))
	var r Rsp
	if err := cc2.Invoke(ctx, "/nope", &Req{}, &r); err != nil || r.Msg != "/nope" {
		t.Fatalf("a malformed method with an unknown service handler = %+v, %v", r, err)
	}
}

// TestClientConnAPI covers a ClientConn outside the happy path: targets it
// cannot use, the package's own engine, Connect and Close, a call option
// it cannot honor, a context already done, a message it cannot marshal,
// SendMsg after CloseSend, and the client's interceptor chains.
func TestClientConnAPI(t *testing.T) {
	if _, err := NewClient("no port"); err == nil {
		t.Fatal("NewClient of a target without a port")
	}
	_, addr := startServer(t, &echoImpl{})
	ctx := testCtx(t)

	// Without WithEngine, the package's own engine.
	shared, err := NewClient("dns:///"+addr, WithDefaultCallOptions(CallContentSubtype("json")))
	if err != nil {
		t.Fatal(err)
	}
	if shared.Target() != "dns:///"+addr {
		t.Fatalf("Target = %q", shared.Target())
	}
	if err := shared.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	var p peer.Peer
	if r, err := (echoClient{shared}).Unary(ctx, &Req{Msg: "shared"}, Peer(&p)); err != nil || r.Msg != "shared" || p.Addr.String() != addr || p.LocalAddr == nil {
		t.Fatalf("a call on the shared engine = %+v, %v, peer %v", r, err, p)
	}
	shared.Close()
	shared.Close()
	if _, err := (echoClient{shared}).Unary(ctx, &Req{}); status.Code(err) != codes.Canceled {
		t.Fatalf("a call after Close = %v", err)
	}

	var mu sync.Mutex
	var trace []string
	record := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	unaryInt := func(name string) UnaryClientInterceptor {
		return func(ctx context.Context, method string, req, reply any, cc *ClientConn, invoker UnaryInvoker, opts ...CallOption) error {
			record(name)
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}
	streamInt := func(name string) StreamClientInterceptor {
		return func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, streamer Streamer, opts ...CallOption) (ClientStream, error) {
			record(name)
			return streamer(ctx, desc, cc, method, opts...)
		}
	}
	cc, c := dial(t, addr, WithChainUnaryInterceptor(unaryInt("u1"), unaryInt("u2")), WithChainStreamInterceptor(streamInt("s1"), streamInt("s2")))
	for _, tc := range []struct {
		name string
		opts []CallOption
		req  any
		code codes.Code
	}{
		{"an unknown content subtype", []CallOption{CallContentSubtype("nope")}, &Req{}, codes.Internal},
		{"an unknown compressor", []CallOption{UseCompressor("nope")}, &Req{}, codes.Internal},
		{"a message that will not marshal", nil, make(chan int), codes.Internal},
		{"a forced codec", []CallOption{ForceCodec(jsonCodec{})}, &Req{}, codes.OK},
	} {
		if err := cc.Invoke(ctx, "/test.Echo/Unary", tc.req, &Rsp{}, tc.opts...); status.Code(err) != tc.code {
			t.Errorf("%s = %v, want %v", tc.name, err, tc.code)
		}
	}
	// Without a content subtype, the proto codec, which a message that
	// does not encode itself defeats.
	plain, err := NewClient(addr, WithEngine(startClientEngine(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if err := plain.Invoke(ctx, "/test.Echo/Unary", &plainMessage{}, &plainMessage{}); status.Code(err) != codes.Internal {
		t.Errorf("a message the proto codec cannot marshal = %v", err)
	}
	// Header returns the header a server sent before its messages.
	count, err := c.Count(ctx, &Req{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if md, err := count.Header(); err != nil || md == nil {
		t.Errorf("Header = %v, %v", md, err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.Unary(done, &Req{}); status.Code(err) != codes.Canceled {
		t.Errorf("a call whose context is done = %v", err)
	}
	sum, err := c.Sum(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sum.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := sum.Send(&Req{}); status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "after CloseSend") {
		t.Errorf("SendMsg after CloseSend = %v", err)
	}
	if r, err := sum.CloseAndRecv(); err != nil || r.N != 0 {
		t.Errorf("CloseAndRecv = %+v, %v", r, err)
	}
	mu.Lock()
	got := strings.Join(trace, " ")
	mu.Unlock()
	if !strings.HasPrefix(got, "u1 u2") || !strings.HasSuffix(got, "s1 s2") {
		t.Errorf("client interceptors ran %q", got)
	}
}

// TestClientConnDialing checks a ClientConn's connects: a call whose
// context ends while a connect is under way, a connect the engine refuses
// at once, which a call waiting for ready tries again until its context
// ends, a connect that completes after Close, and a connection out of
// stream ids, on which a call connects again.
func TestClientConnDialing(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	cc, c := dial(t, addr)
	ctx := testCtx(t)

	// A connect under way that never completes.
	cc.mu.Lock()
	pending := make(chan struct{})
	cc.dialing = pending
	cc.mu.Unlock()
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := cc.Connect(short); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Connect while a connect hangs = %v", err)
	}
	cc.mu.Lock()
	cc.dialing = nil
	cc.mu.Unlock()
	close(pending)

	// A connection that has used up its stream ids is drained for a new one.
	if err := cc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	cc.mu.Lock()
	old := cc.t
	cc.mu.Unlock()
	old.mu.Lock()
	old.nextID = maxWindow + 2
	old.mu.Unlock()
	if r, err := c.Unary(ctx, &Req{Msg: "fresh"}); err != nil || r.Msg != "fresh" {
		t.Fatalf("a call after the stream ids ran out = %v", err)
	}
	cc.mu.Lock()
	renewed := cc.t != old
	cc.mu.Unlock()
	if !renewed {
		t.Fatal("the call went out on the exhausted connection")
	}
	old.conn.Close()
	// A connection draining that the ClientConn has not let go of yet is
	// replaced as well.
	cc.mu.Lock()
	draining := cc.t
	cc.mu.Unlock()
	draining.mu.Lock()
	draining.goingAway = true
	draining.mu.Unlock()
	if err := cc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	cc.mu.Lock()
	renewed = cc.t != draining
	cc.mu.Unlock()
	if !renewed {
		t.Fatal("Connect kept a draining connection")
	}
	draining.conn.Close()

	// A connect that completes after Close is closed.
	late, err := NewClient(addr, WithEngine(startClientEngine(t)))
	if err != nil {
		t.Fatal(err)
	}
	late.mu.Lock()
	late.dialing = make(chan struct{})
	dialing := late.dialing
	late.dial()
	late.closed = true
	late.mu.Unlock()
	<-dialing
	late.mu.Lock()
	kept := late.t
	late.mu.Unlock()
	if kept != nil {
		t.Fatal("a ClientConn closed while connecting kept the connection")
	}

	// An engine that has stopped refuses every connect, at once. Such a call
	// used to wait on a nil channel until its context ended, forever without
	// a deadline, instead of failing Unavailable.
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine.Stop()
	t.Cleanup(func() { _ = engine.Close() })
	_, refused := dial(t, addr, WithEngine(engine))
	if _, err := refused.Unary(ctx, &Req{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a call whose connect failed = %v", err)
	}
	wait, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := refused.Unary(wait, &Req{}, WaitForReady(true)); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a call waiting for ready on a connect that fails = %v", err)
	}
}

// failingStreams fail as their fields say, for the generic wrappers.
type failingClientStream struct {
	ClientStream
	closeErr error
}

func (s failingClientStream) CloseSend() error  { return s.closeErr }
func (s failingClientStream) RecvMsg(any) error { return io.ErrUnexpectedEOF }

type failingServerStream struct{ ServerStream }

func (failingServerStream) RecvMsg(any) error { return io.ErrUnexpectedEOF }

// TestGenericStreamErrors checks that the typed wrappers of generated code
// hand back the errors of the streams they wrap.
func TestGenericStreamErrors(t *testing.T) {
	closeErr := errors.New("close")
	if _, err := (&GenericClientStream[Req, Rsp]{failingClientStream{closeErr: closeErr}}).CloseAndRecv(); err != closeErr {
		t.Errorf("CloseAndRecv with CloseSend failing = %v", err)
	}
	if _, err := (&GenericClientStream[Req, Rsp]{failingClientStream{}}).CloseAndRecv(); err != io.ErrUnexpectedEOF {
		t.Errorf("CloseAndRecv with RecvMsg failing = %v", err)
	}
	if _, err := (&GenericServerStream[Req, Rsp]{failingServerStream{}}).Recv(); err != io.ErrUnexpectedEOF {
		t.Errorf("Recv = %v", err)
	}
}

// TestTransportWhiteBox covers the transport's guards that no peer can
// reach in order: a preface that arrives in pieces or wrong in its first
// bytes, a connection closed twice, and a stream forgotten twice.
func TestTransportWhiteBox(t *testing.T) {
	tr := newTransport(false)
	if err := tr.read([]byte(preface[:10])); err != nil || !tr.awaitPreface {
		t.Fatalf("a preface cut short = %v", err)
	}
	if err := tr.read(append([]byte(preface[10:]), appendSettings(nil)...)); err != nil || tr.awaitPreface || !tr.sawSettings {
		t.Fatalf("the rest of the preface = %v", err)
	}
	if err := newTransport(false).read([]byte("PRX")); err == nil {
		t.Fatal("a wrong start of a preface was taken")
	}
	tr.onClose(nil)
	tr.onClose(errors.New("again"))
	if tr.closeErr != io.EOF {
		t.Fatalf("closeErr = %v", tr.closeErr)
	}
	tr.mu.Lock()
	tr.removeLocked(&stream{id: 1})
	tr.mu.Unlock()
}
