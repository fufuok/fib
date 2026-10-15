//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"encoding/pem"
	"flag"
	"io"
	"log"
	"net"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/server/proxy"
	"github.com/lesismal/fib/examples/gateway/upstream/backend"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func TestMain(m *testing.M) {
	// The upstream logs each WebSocket it closes.
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// startEngines runs engines until the test ends.
func startEngines(t *testing.T, engines ...*fib.Engine) {
	t.Helper()
	for _, engine := range engines {
		stop := example.Start(engine)
		t.Cleanup(func() { _ = stop() })
	}
}

// bindPair binds engines that share a TCP and a UDP port, which bind gets
// as an address with port 0 to choose. A UDP port another program already
// has fails the bind now and then, so it is tried again.
func bindPair(t *testing.T, bind func(addr string) ([]*fib.Engine, error)) []*fib.Engine {
	t.Helper()
	var err error
	for i := 0; i < 20; i++ {
		var engines []*fib.Engine
		if engines, err = bind("127.0.0.1:0"); err == nil {
			startEngines(t, engines...)
			return engines
		}
	}
	t.Fatal(err)
	return nil
}

// gatewayLab is an upstream with the gateway in front of it, as the three
// commands would run them, and the gateway's certificate as a PEM file for
// the client's -ca.
type gatewayLab struct {
	tlsAddr, plainAddr, caFile string
}

func newGatewayLab(t *testing.T) gatewayLab {
	t.Helper()
	serverTLS, upstreamTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	upstream := bindPair(t, func(addr string) ([]*fib.Engine, error) {
		return backend.NewEngines(serverTLS, addr, "127.0.0.1:0")
	})
	upTLS, upPlain := example.Addr(upstream[0]), example.Addr(upstream[2])

	var routes []proxy.Route
	for _, spec := range []string{"/h1=http://" + upPlain, "/h2=https://" + upTLS, "/h3=h3://" + upTLS} {
		route, err := proxy.ParseRoute(spec)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, route)
	}
	gateway, err := proxy.New(proxy.Config{Routes: routes, TLSConfig: upstreamTLS, Subprotocols: []string{"chat", "superchat"}})
	if err != nil {
		t.Fatal(err)
	}
	// What the server command binds, built here since a command cannot be
	// imported.
	engines := bindPair(t, func(addr string) ([]*fib.Engine, error) {
		handler, httpConfig := gateway.Handler(), gateway.HTTPConfig()
		tcpConfig := fib.DefaultConfig()
		tcpConfig.Addr = addr
		tcp, err := fib.Bind(tcpConfig, fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandlerWithConfig(httpConfig, handler)))
		if err != nil {
			return nil, err
		}
		udpConfig := fib.DefaultConfig()
		udpConfig.Network = "udp"
		udpConfig.Addr = example.Addr(tcp)
		udp, err := fib.Bind(udpConfig, http3.NewHandlerWithConfig(gateway.HTTP3Config(serverTLS), handler))
		if err != nil {
			_ = tcp.Close()
			return nil, err
		}
		plainConfig := fib.DefaultConfig()
		plainConfig.Addr = "127.0.0.1:0"
		plain, err := fib.Bind(plainConfig, fibhttp.NewHandlerWithConfig(httpConfig, handler))
		if err != nil {
			_ = udp.Close()
			_ = tcp.Close()
			return nil, err
		}
		gateway.Attach(tcp)
		return []*fib.Engine{tcp, udp, plain}, nil
	})
	// Registered last, so it runs first: the upstream clients close before the
	// engines they dial through.
	t.Cleanup(gateway.Close)

	caFile := filepath.Join(t.TempDir(), "gateway.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverTLS.Certificates[0].Certificate[0]})
	if err := os.WriteFile(caFile, block, 0o644); err != nil {
		t.Fatal(err)
	}
	return gatewayLab{tlsAddr: example.Addr(engines[0]), plainAddr: example.Addr(engines[2]), caFile: caFile}
}

// TestRunPassesThroughTheGateway runs the client against a real gateway and
// upstream: every protocol in, every protocol out, and WebSockets over both
// ws:// and wss://.
func TestRunPassesThroughTheGateway(t *testing.T) {
	l := newGatewayLab(t)
	var out bytes.Buffer
	err := run([]string{"-addr", l.tlsAddr, "-plain-addr", l.plainAddr, "-ca", l.caFile, "-size", strconv.Itoa(256 << 10)}, &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	passed := 0
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "ok   ") {
			passed++
		}
	}
	// Three checks for each client and upstream, and a ws:// and a wss:// one
	// for each upstream.
	if passed != 3*3*3+3*2 || !strings.HasSuffix(out.String(), "all checks passed\n") {
		t.Fatalf("%d checks passed:\n%s", passed, out.String())
	}
}

// TestRunReportsFailedChecks points the client at a gateway it does not
// trust, so that every check fails, and run reports how many did.
func TestRunReportsFailedChecks(t *testing.T) {
	l := newGatewayLab(t)
	// A certificate of its own, which the gateway's is not.
	otherCA := filepath.Join(t.TempDir(), "other.pem")
	flags := flag.NewFlagSet("certs", flag.ContinueOnError)
	serverFlags := certs.ServerFlagsWithOut(flags, otherCA)
	if err := flags.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := serverFlags.Config(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := run([]string{"-addr", l.tlsAddr, "-plain-addr", "", "-ca", otherCA, "-size", "1024"}, &out)
	if err == nil || err.Error() != "30 checks failed" {
		t.Fatalf("err %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "ws://  ") {
		t.Fatalf("ran the ws:// checks without -plain-addr:\n%s", out.String())
	}
}

func TestRunStartupErrors(t *testing.T) {
	if err := run([]string{"-nope"}, io.Discard); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run([]string{"-ca", filepath.Join(t.TempDir(), "missing.pem")}, io.Discard); err == nil {
		t.Error("ran without the certificate to trust")
	}
}

func TestMainReportsAnError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"client", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}

// shortenTimeouts makes the waits for a response, a dial, an echo and a close
// short enough for a test that expects them to run out.
func shortenTimeouts(t *testing.T) {
	old := [4]time.Duration{responseTimeout, dialTimeout, echoTimeout, closeTimeout}
	responseTimeout, dialTimeout, echoTimeout, closeTimeout = 300*time.Millisecond, 300*time.Millisecond, 300*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { responseTimeout, dialTimeout, echoTimeout, closeTimeout = old[0], old[1], old[2], old[3] })
}

// misbehaving answers /<fault>/echo, /<fault>/download and /<fault>/stream
// wrongly, each in its own way, and upgrades /mismatch/ws and /silent/ws to
// WebSockets that echo the wrong thing or nothing.
func misbehaving(c *fibhttp.Context) {
	const lines = "line 1 of 4\nline 2 of 4\nline 3 of 4\nline 4 of 4\n"
	text := func(code int, body string) { _ = c.Respond(code, "text/plain", []byte(body)) }
	switch c.Request.URL.Path {
	case "/status/echo", "/status/download":
		// The start of what a download would send, under the wrong status.
		text(stdhttp.StatusInternalServerError, "abc")
	case "/body/echo":
		text(stdhttp.StatusOK, "not an echo")
	case "/wrong/download":
		text(stdhttp.StatusOK, strings.Repeat("z", 1024))
	case "/cut/download":
		// Promises more than it sends, then hangs up.
		c.Header().Set("Content-Length", "1024")
		_, _ = c.Write([]byte("abc"))
		c.Flush()
		c.Conn.Close()
	case "/hang/echo":
		c.Retain()
		c.OnCancel(func(error) { c.Release() })
	case "/body/stream":
		text(stdhttp.StatusOK, "not the lines")
	case "/trailer/stream":
		text(stdhttp.StatusOK, lines)
	case "/fast/stream":
		// The right lines and trailer, all at once.
		c.Header().Set("Trailer", "X-Lines")
		_, _ = c.Write([]byte(lines))
		c.Header().Set(stdhttp.TrailerPrefix+"X-Lines", "4")
		_ = c.Finish()
	case "/mismatch/ws":
		_, _ = websocket.NewHandler(websocket.HandlerFuncs{
			Message: func(conn *websocket.Connection, opcode websocket.Opcode, _ []byte) {
				_ = conn.WriteMessage(opcode, []byte("something else"))
			},
		}).Upgrade(c, nil)
	case "/silent/ws":
		_, _ = websocket.NewHandler(websocket.HandlerFuncs{}).Upgrade(c, nil)
	default:
		text(stdhttp.StatusNotFound, "not found")
	}
}

// TestChecksCatchAMisbehavingGateway runs each check against a server that
// gets it wrong, and the failure each one should report.
func TestChecksCatchAMisbehavingGateway(t *testing.T) {
	shortenTimeouts(t)
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, fibhttp.NewHandler(fibhttp.HandlerFunc(misbehaving)))
	if err != nil {
		t.Fatal(err)
	}
	startEngines(t, server)
	addr := example.Addr(server)
	base := func(fault string) string { return "http://" + addr + "/" + fault }
	// Nothing listens on port 1. Windows retries a connect the port refuses
	// for longer than the shortened timeouts, which then report it.
	const refused = "http://127.0.0.1:1"
	refusal := "refused"
	if runtime.GOOS == "windows" {
		refusal = "timed out"
	}

	engine, stop := example.ClientEngine()
	t.Cleanup(stop)
	client := newHTTPClient(engine, nil, true)
	dialer := websocket.NewDialer(engine, websocket.DefaultDialerConfig())

	// A listener that takes connections and never answers a handshake.
	mute, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var muteConns sync.WaitGroup
	muteConns.Add(1)
	go func() {
		defer muteConns.Done()
		var held []net.Conn
		for {
			conn, err := mute.Accept()
			if err != nil {
				break
			}
			held = append(held, conn)
		}
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = mute.Close(); muteConns.Wait() })

	for _, tc := range []struct {
		name string
		got  func() outcome
		want string
	}{
		{"echo refused", func() outcome { return echo(client, refused) }, refusal},
		{"echo status", func() outcome { return echo(client, base("status")) }, "status 500"},
		{"echo body", func() outcome { return echo(client, base("body")) }, "unexpected reply"},
		{"echo hangs", func() outcome { return echo(client, base("hang")) }, "timed out"},
		{"download refused", func() outcome { return download(client, refused, 10) }, refusal},
		{"download status", func() outcome { return download(client, base("status"), 10) }, "status 500"},
		{"download wrong bytes", func() outcome { return download(client, base("wrong"), 1024) }, `byte 0 is 'z'`},
		{"download cut short", func() outcome { return download(client, base("cut"), 1024) }, ""},
		{"stream refused", func() outcome { return stream(client, refused) }, refusal},
		{"stream body", func() outcome { return stream(client, base("body")) }, "body "},
		{"stream trailer", func() outcome { return stream(client, base("trailer")) }, "trailer"},
		{"stream not streamed", func() outcome { return stream(client, base("fast")) }, "did not stream"},
		{"websocket refused", func() outcome { return relay(dialer, "ws://127.0.0.1:1/ws") }, refusal},
		{"websocket dial hangs", func() outcome { return relay(dialer, "ws://"+mute.Addr().String()+"/ws") }, "dial timed out"},
		{"websocket wrong echo", func() outcome { return relay(dialer, "ws://"+addr+"/mismatch/ws") }, "echoed 14 bytes, want 25"},
		{"websocket no echo", func() outcome { return relay(dialer, "ws://"+addr+"/silent/ws") }, "no echo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.got()
			if got.err == nil || !strings.Contains(got.err.Error(), tc.want) {
				t.Fatalf("%s: err %v, want one that says %q", got.what, got.err, tc.want)
			}
			var out bytes.Buffer
			if check(&out, tc.name, got) != 1 || !strings.HasPrefix(out.String(), "FAIL "+tc.name) {
				t.Fatalf("check printed %q", out.String())
			}
		})
	}
}
