//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package socks_test

import (
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func run(t *testing.T, config fib.Config, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config.Name = "socks-" + t.Name()
	engine, err := fib.New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine
}

// serve binds each handler on a port of its own on 127.0.0.1, and returns
// the ports' addresses in order.
func serve(t *testing.T, handlers ...fib.Handler) []string {
	t.Helper()
	bindings := make([]fib.Binding, len(handlers))
	for i, h := range handlers {
		bindings[i] = fib.Binding{Addr: "127.0.0.1:0", Handler: h}
	}
	engine := run(t, fib.DefaultConfig(), bindings...)
	addrs := make([]string, len(handlers))
	for i := range handlers {
		a, err := engine.BindingAddrs(i)
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = a[0].String()
	}
	return addrs
}

func web() fibhttp.Handler {
	router := fibhttp.NewRouter()
	router.Get("/hello", func(c *fibhttp.Context) {
		reply := c.Request.Proto + " tls=" + map[bool]string{false: "no", true: "yes"}[c.Request.TLS != nil]
		if err := c.WriteResponse(fibhttp.Response{StatusCode: 200, Body: []byte(reply)}); err != nil {
			c.Conn.Close()
		}
	})
	return router
}

func get(t *testing.T, client *fibhttp.Client, url string) string {
	t.Helper()
	req, err := stdhttp.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// The HTTP client reaches HTTP/1.1 and, over TLS, HTTP/2 servers through
// the proxy, which resolves their names.
func TestHTTPClientThroughProxy(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addrs := serve(t, socks.NewServer(socks.Config{}), fibhttp.NewHandler(web()),
		fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(web())))
	proxy, err := socks.ParseURL("socks5h://" + addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	config := fibhttp.DefaultClientConfig()
	config.Dial = proxy.Dial
	config.TLSConfig = clientTLS
	client := fibhttp.NewClient(run(t, fib.DefaultConfig()), config)
	t.Cleanup(client.Close)

	if got := get(t, client, "http://"+addrs[1]+"/hello"); got != "HTTP/1.1 tls=no" {
		t.Errorf("http: %q", got)
	}
	_, port, _ := net.SplitHostPort(addrs[2])
	if got := get(t, client, "https://localhost:"+port+"/hello"); got != "HTTP/2.0 tls=yes" {
		t.Errorf("https: %q", got)
	}
}

func TestWebSocketThroughProxy(t *testing.T) {
	echo := websocket.NewHandler(websocket.HandlerFuncs{Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
		_ = c.WriteMessage(op, data)
	}})
	addrs := serve(t, socks.NewServer(socks.Config{}), echo)
	config := websocket.DefaultDialerConfig()
	config.Dial = (&socks.Dialer{Addr: addrs[0]}).Dial
	got := make(chan string, 1)
	handler := websocket.HandlerFuncs{Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
		got <- string(data)
	}}
	conn, _, err := websocket.NewDialer(run(t, fib.DefaultConfig()), config).
		Go("ws://"+addrs[1]+"/", nil, handler).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(1000, "")
	if err := conn.WriteText("through the proxy"); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if msg != "through the proxy" {
			t.Fatalf("got %q", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no echo")
	}
}

// SOCKS shares a port with HTTP behind the mux, and reaches the HTTP
// server on that same port.
func TestMuxRoutesSOCKS(t *testing.T) {
	addrs := serve(t, mux.New(
		mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{})},
		mux.Route{Match: mux.HTTP(), Handler: fibhttp.NewHandler(web())},
	))
	engine := run(t, fib.DefaultConfig())
	for _, url := range []string{"socks5h://", "socks4://"} {
		proxy, err := socks.ParseURL(url + addrs[0])
		if err != nil {
			t.Fatal(err)
		}
		config := fibhttp.DefaultClientConfig()
		config.Dial = proxy.Dial
		client := fibhttp.NewClient(engine, config)
		if got := get(t, client, "http://"+addrs[0]+"/hello"); got != "HTTP/1.1 tls=no" {
			t.Errorf("%s: %q", url, got)
		}
		client.Close()
	}
}
