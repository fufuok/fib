package fib

import "errors"

// Binding is one group of addresses an engine listens on, the network they
// share, and the handler that serves the connections, or the UDP peers, they
// take. Config.Bindings lists them, so that one engine serves several
// protocols at once, each on addresses of its own: HTTP on one port, HTTPS on
// another, HTTP/3 on a UDP port, gRPC on a fourth and a Unix socket for
// administration, all on a single event loop, descriptor table, task pool and
// buffer pool, where an engine per protocol gives each its own copy of all
// four.
//
// The handler is whatever stack the protocol needs, as Bind would take it:
//
//	config := fib.DefaultConfig()
//	config.Bindings = []fib.Binding{
//		{Addr: ":80", Handler: fibhttp.NewHandler(router)},
//		{Addrs: []string{":443", ":8443"}, Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(router))},
//		{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, router)},
//		{Addr: ":9000", Handler: grpcServer},
//		{Network: "unix", Addr: "/run/app.sock", Handler: admin},
//	}
//	engine, err := fib.Bind(config, nil)
//
// Everything else, the backlog, the buffers, the watermarks, the pollers and
// ReusePort, is the engine's and applies to every binding alike. With
// IOPollers, each binding's connections are spread over the same pollers as
// every other binding's.
type Binding struct {
	// Network is what Config.Network is for the engine, for this binding's
	// addresses alone: "tcp", "tcp4", "tcp6", "unix", "udp", "udp4" or
	// "udp6". Empty means Config.Network.
	Network string
	// Addr and Addrs are what Config.Addr and Config.Addrs are for the
	// engine: Addrs, when it is not empty, is the complete set of the
	// binding's addresses and Addr is ignored. An empty Addr means ":0", so
	// a binding always listens somewhere; BindingAddrs reports where.
	Addr  string
	Addrs []string
	// Handler serves the binding's connections. Nil means the handler Bind
	// was given.
	Handler Handler
}

// errNoBinding is what BindingAddrs reports for an index that names none of
// the engine's bindings.
var errNoBinding = errors.New("fib: engine has no such binding")

// bindingsOf returns what Bind is to listen on: config.Bindings, with each
// one's network, addresses and handler filled in from config and handler
// where it leaves them out, or else the one binding that config.Network,
// config.Addr or config.Addrs and handler make, which is what an engine
// listened on before it had bindings.
func bindingsOf(config Config, handler Handler) []Binding {
	if len(config.Bindings) == 0 {
		return []Binding{resolveBinding(Binding{Network: config.Network, Addr: config.Addr, Addrs: config.Addrs},
			config, handler)}
	}
	bindings := make([]Binding, len(config.Bindings))
	for i, b := range config.Bindings {
		bindings[i] = resolveBinding(b, config, handler)
	}
	return bindings
}

func resolveBinding(b Binding, config Config, handler Handler) Binding {
	if b.Network == "" {
		b.Network = config.Network
	}
	if b.Network == "" {
		b.Network = "tcp"
	}
	if len(b.Addrs) == 0 {
		b.Addrs = []string{b.Addr}
	} else {
		// The caller's slice stays the caller's.
		b.Addrs = append([]string(nil), b.Addrs...)
	}
	b.Addr = ""
	if b.Handler == nil {
		b.Handler = handler
	}
	return b
}
