// Package multiproto serves several protocols on one port, or on one group of
// ports, through a single handler that is told which protocols to take. It
// reads the first bytes each client sends, as many as it takes to tell the
// protocols apart, and hands the connection to the handler of the protocol it
// found:
//
//	web := fibhttp.NewHandler(router)
//	engine, err := fib.Bind(config, multiproto.New(multiproto.Config{
//		TLS:       fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), web),
//		HTTP1:     web,
//		HTTP2:     web,
//		WebSocket: websocket.NewHandler(wsHandler),
//		GRPC:      grpcServer,
//		SOCKS:     socks.NewServer(socks.Config{}),
//		HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
//		ARPC:      arpcServer,
//	}))
//
// A protocol whose handler is nil is not served: its connections go to
// Config.Fallback, or are closed. Each protocol is recognised by what its
// client sends first:
//
//   - TLS by its ClientHello. What runs inside TLS is the TLS handler's to
//     tell apart, as fibhttp.ConfigureTLS does HTTP/1.1 and HTTP/2 by ALPN.
//   - SOCKS by a SOCKS5 greeting or a SOCKS4 or SOCKS4a request.
//   - HTTPProxy by a CONNECT or a request for an absolute URL, WebSocket by
//     an HTTP/1.1 GET with an Upgrade: websocket header, and HTTP1 by any
//     other HTTP/1.x request. The first request of a connection decides for
//     all of it. The last of them served takes every HTTP/1.x connection the
//     others did not.
//   - GRPC by an HTTP/2 connection preface (h2c, with prior knowledge) whose
//     first request has a gRPC content-type, and HTTP2 by any other. With
//     only one of them served, it takes every h2c connection.
//   - ARPC by the header of an arpc request, notify, stream message or ping.
//
// The mux reads only as far as the protocols it serves need: with WebSocket
// and HTTP1 both served it reads a request's whole head to tell them apart,
// with only one of them the method is enough, and likewise for GRPC and
// HTTP2. Once it has decided, package mux steps aside, so the chosen handler
// serves the connection as if it had accepted it itself, with no dispatch in
// between; see package mux for how the handoff goes.
package multiproto

import (
	"slices"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/mux"
)

// DefaultMaxPeek is how many bytes the handler reads at most before it
// decides, when Config.MaxPeek is zero: enough for the request head of most
// HTTP/1.1 clients, browsers' WebSocket handshakes with their cookies among
// them, and for the first headers of an HTTP/2 client.
const DefaultMaxPeek = 8192

// Config says which protocols a Handler serves, and with which handlers: a
// protocol whose handler is nil is not served. One handler may serve several
// of them, as fibhttp.NewHandler serves HTTP1 and HTTP2.
type Config struct {
	// TLS serves the connections that open with a TLS ClientHello.
	TLS fib.Handler
	// HTTP1 serves HTTP/1.x, but for the proxy requests HTTPProxy takes and
	// the WebSocket handshakes WebSocket takes, when they are set.
	HTTP1 fib.Handler
	// HTTPProxy serves the HTTP/1.x connections whose first request is for a
	// proxy: a CONNECT, or a request whose target is an absolute http:// or
	// https:// URL, as fibhttp.NewHandler(fibhttp.NewConnectProxy(...))
	// does. It makes one port an HTTP proxy beside a SOCKS one and a web
	// server.
	HTTPProxy fib.Handler
	// HTTP2 serves HTTP/2 with prior knowledge (h2c), but for the gRPC
	// connections that GRPC takes when it is set.
	HTTP2 fib.Handler
	// WebSocket serves the HTTP/1.1 connections whose first request opens a
	// WebSocket, as websocket.NewHandler's ServerHandler does.
	WebSocket fib.Handler
	// GRPC serves the h2c connections whose first request is a gRPC call, as
	// a grpc.Server does.
	GRPC fib.Handler
	// SOCKS serves SOCKS5, SOCKS4 and SOCKS4a, as a socks.Server does.
	SOCKS fib.Handler
	// ARPC serves arpc, as an arpc.Server does.
	ARPC fib.Handler
	// ARPCMaxBodyLen is the longest body an arpc header may announce for
	// ARPC to recognise it; zero means DefaultARPCMaxBodyLen. Set it as the
	// arpc.Server's Handler.SetMaxBodyLen is.
	ARPCMaxBodyLen int

	// Routes are protocols of the caller's own, tried after the built-in
	// ones.
	Routes []mux.Route
	// Fallback serves the connections that no protocol recognises. Nil
	// closes them, with mux.ErrNoMatch.
	Fallback fib.Handler
	// MaxPeek is how many bytes the handler reads at most before it decides.
	// A protocol still waiting for more once that many have arrived counts
	// as not matching. Zero means DefaultMaxPeek.
	MaxPeek int
	// Timeout closes a connection that has not let the handler decide this
	// long after it opened. Zero means mux.DefaultTimeout, and a negative
	// value none.
	Timeout time.Duration
}

// New returns the handler that serves config's protocols. It is a
// mux.Handler, bound as any other fib.Handler.
func New(config Config) *mux.Handler {
	maxPeek := config.MaxPeek
	if maxPeek <= 0 {
		maxPeek = DefaultMaxPeek
	}
	return mux.NewWithConfig(mux.Config{Fallback: config.Fallback, MaxPeek: maxPeek, Timeout: config.Timeout},
		Routes(config)...)
}

// Routes returns the routes New gives package mux for config, in the order
// it tries them, for a caller that builds its own mux.Handler.
func Routes(config Config) []mux.Route {
	var routes []mux.Route
	add := func(m mux.Matcher, h fib.Handler) {
		if h != nil {
			routes = append(routes, mux.Route{Match: m, Handler: h})
		}
	}
	arpcMatch := ARPC(config.ARPCMaxBodyLen)
	// An arpc header has no magic number, and the length that opens one can
	// begin the way a ClientHello or a SOCKS greeting does: when arpc is
	// served too, those take only what is not arpc.
	tlsMatch, socksMatch := mux.TLS(), mux.SOCKS()
	if config.ARPC != nil {
		tlsMatch = except(tlsMatch, arpcMatch, nil)
		socksMatch = except(socksMatch, arpcMatch, socksSettled)
	}
	add(tlsMatch, config.TLS)
	add(socksMatch, config.SOCKS)

	// A protocol served alone takes its whole family, which it answers
	// itself when a client asks for what it is not, as a WebSocket handler
	// answers a plain request 400 and a gRPC server an HTTP/2 one 415: only
	// with more than one served is there a choice worth reading further for.
	// Of the HTTP/1.x protocols, each but the last one served reads as far
	// as it needs to, and the last takes the rest.
	http1 := []mux.Route{{Match: HTTPProxy(), Handler: config.HTTPProxy},
		{Match: WebSocket(), Handler: config.WebSocket}, {Handler: config.HTTP1}}
	http1 = slices.DeleteFunc(http1, func(r mux.Route) bool { return r.Handler == nil })
	for i, r := range http1 {
		if i == len(http1)-1 {
			r.Match = mux.HTTP1()
		}
		routes = append(routes, r)
	}

	switch {
	case config.GRPC != nil && config.HTTP2 != nil:
		routes = append(routes, mux.Route{Match: GRPC(), Handler: config.GRPC, Nudge: NudgeHTTP2})
		add(mux.HTTP2(), config.HTTP2)
	case config.GRPC != nil:
		add(mux.HTTP2(), config.GRPC)
	default:
		add(mux.HTTP2(), config.HTTP2)
	}

	add(arpcMatch, config.ARPC)
	return append(routes, config.Routes...)
}
