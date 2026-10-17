//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Command fibhub tunnels TCP connections to devices behind pods, through a
// hub, on fib's engine. It runs in one of three modes:
//
//	fibhub server  the hub: pods and clients register with it over arpc,
//	               and it routes each tunnel a client opens to its pod
//	fibhub pod     runs next to the nodes (devices) and connects to them
//	               for the tunnels the server routes to it
//	fibhub client  listens on local ports and tunnels each connection to a
//	               node through the server and the node's pod
//
// For example, to reach the SSH server of a device 10.0.0.5 that only a pod
// host can reach:
//
//	fibhub server -password s3cret
//	fibhub pod    -name lab -password s3cret -server hub.example:7000 -node ssh=10.0.0.5:22
//	fibhub client -name me  -password s3cret -server hub.example:7000 -forward 127.0.0.1:2222=lab/ssh
//	ssh -p 2222 127.0.0.1
//
// Where only HTTP gets through, the server also serves arpc over WebSocket
// (-arpc-ws), and pods and clients name it as ws://host:port/fibhub. With
// -body-password on all three, every arpc message body is AES-encrypted.
//
// Each mode has a web page; run "fibhub <mode> -h" for the flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lesismal/fib/tools/fibhub/client"
	"github.com/lesismal/fib/tools/fibhub/internal/common"
	"github.com/lesismal/fib/tools/fibhub/internal/proto"
	"github.com/lesismal/fib/tools/fibhub/internal/uplink"
	"github.com/lesismal/fib/tools/fibhub/internal/wsarpc"
	"github.com/lesismal/fib/tools/fibhub/pod"
	"github.com/lesismal/fib/tools/fibhub/server"
)

const usage = `usage: fibhub <mode> [flags]

modes:
  server   run the hub pods and clients register with
  pod      connect to servers and to the nodes their tunnels ask for
  client   tunnel local ports to nodes through servers and pods
  version  print the version

Run "fibhub <mode> -h" for the flags of a mode.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "fibhub:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return errors.New("no mode given")
	}
	mode, args := args[0], args[1:]
	switch mode {
	case "server":
		return runServer(ctx, args, out)
	case "pod":
		return runPod(ctx, args, out)
	case "client":
		return runClient(ctx, args, out)
	case "version", "-v", "--version":
		fmt.Fprintln(out, "fibhub", proto.Version)
		return nil
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return nil
	}
	fmt.Fprint(out, usage)
	return fmt.Errorf("unknown mode %q", mode)
}

func runServer(ctx context.Context, args []string, out io.Writer) error {
	var cfg server.Config
	flags := flag.NewFlagSet("fibhub server", flag.ContinueOnError)
	flags.SetOutput(out)
	flags.StringVar(&cfg.Name, "name", "fibhub", "the server's name, which peers are told")
	flags.StringVar(&cfg.Addr, "addr", ":7000", "arpc address pods and clients connect to")
	flags.StringVar(&cfg.WebAddr, "web", "127.0.0.1:7080", "address of the web page")
	flags.StringVar(&cfg.WebAuth, "web-auth", "", "user:password protecting the web page")
	flags.StringVar(&cfg.WSAddr, "ws", "", "address of the encrypted WebSocket to the web page (empty: none)")
	flags.StringVar(&cfg.Password, "password", os.Getenv("FIBHUB_PASSWORD"), "password pods and clients authenticate with (default $FIBHUB_PASSWORD)")
	flags.StringVar(&cfg.WSPassword, "ws-password", "", "password encrypting the WebSocket (default: -password)")
	flags.StringVar(&cfg.ArpcWSAddr, "arpc-ws", "", "address pods and clients reach arpc on over WebSocket (empty: none)")
	flags.StringVar(&cfg.ArpcWSPath, "arpc-ws-path", "/fibhub", "path of the arpc WebSocket")
	flags.StringVar(&cfg.BodyPassword, "body-password", os.Getenv("FIBHUB_BODY_PASSWORD"), "AES-encrypt every arpc message body with a key from this password; peers need the same (default $FIBHUB_BODY_PASSWORD, empty: off)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return server.Run(ctx, cfg, out, nil)
}

// peerFlags are the flags pods and clients share.
type peerFlags struct {
	flags      *flag.FlagSet
	config     string
	name       string
	web        string
	webAuth    string
	password   string
	bodyPass   string
	servers    common.StringList
	meta       common.StringList
	visited    map[string]bool
	configured bool
}

func newPeerFlags(mode, web string, out io.Writer) *peerFlags {
	pf := &peerFlags{flags: flag.NewFlagSet("fibhub "+mode, flag.ContinueOnError)}
	f := pf.flags
	f.SetOutput(out)
	f.StringVar(&pf.config, "config", "", "JSON file the settings are loaded from and the web page saves them to")
	host, _ := os.Hostname()
	f.StringVar(&pf.name, "name", host, "the "+mode+"'s name")
	f.StringVar(&pf.web, "web", web, "address of the web page (empty: none)")
	f.StringVar(&pf.webAuth, "web-auth", "", "user:password protecting the web page")
	f.StringVar(&pf.password, "password", os.Getenv("FIBHUB_PASSWORD"), "password of the servers given with -server (default $FIBHUB_PASSWORD)")
	f.StringVar(&pf.bodyPass, "body-password", os.Getenv("FIBHUB_BODY_PASSWORD"), "AES-encrypt arpc message bodies to the servers given with -server, as they are configured to (default $FIBHUB_BODY_PASSWORD, empty: off)")
	f.Var(&pf.servers, "server", "server to connect to, [name=]host:port or [name=]ws://host:port/path (repeatable)")
	f.Var(&pf.meta, "meta", "key=value told to the servers (repeatable)")
	return pf
}

func (pf *peerFlags) parse(args []string) error {
	if err := pf.flags.Parse(args); err != nil {
		return err
	}
	if pf.flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", pf.flags.Args())
	}
	pf.visited = map[string]bool{}
	pf.flags.Visit(func(f *flag.Flag) { pf.visited[f.Name] = true })
	return nil
}

// apply puts the flags over what the config file set: a name, web address
// or meta flag replaces the file's, and servers are added unless the file
// has one of the same name.
func (pf *peerFlags) apply(name, web, webAuth *string, servers *[]uplink.Config, meta *map[string]string) error {
	if pf.visited["name"] || *name == "" {
		*name = pf.name
	}
	if pf.visited["web"] || *web == "" && !pf.configured {
		*web = pf.web
	}
	if pf.visited["web-auth"] {
		*webAuth = pf.webAuth
	}
	for _, s := range pf.servers {
		sc := uplink.Config{Name: s, Addr: s, Password: pf.password, BodyPassword: pf.bodyPass}
		if n, addr, ok := strings.Cut(s, "="); ok && !wsarpc.IsURL(s) {
			sc.Name, sc.Addr = n, addr
		}
		if sc.Password == "" {
			return fmt.Errorf("-server %s needs a -password", s)
		}
		dup := false
		for _, have := range *servers {
			if have.Name == sc.Name {
				dup = true
			}
		}
		if !dup {
			*servers = append(*servers, sc)
		}
	}
	if len(pf.meta) > 0 {
		m := map[string]string{}
		for _, kv := range pf.meta {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("-meta %q is not key=value", kv)
			}
			m[k] = v
		}
		*meta = m
	}
	return nil
}

func runPod(ctx context.Context, args []string, out io.Writer) error {
	pf := newPeerFlags("pod", "127.0.0.1:7180", out)
	var nodes common.StringList
	pf.flags.Var(&nodes, "node", "node the pod can reach, name=host:port[,description] (repeatable)")
	if err := pf.parse(args); err != nil {
		return err
	}
	var cfg pod.Config
	if err := common.LoadJSON(pf.config, &cfg); err != nil {
		return err
	}
	pf.configured = pf.config != "" && fileExists(pf.config)
	if err := pf.apply(&cfg.Name, &cfg.WebAddr, &cfg.WebAuth, &cfg.Servers, &cfg.Meta); err != nil {
		return err
	}
	for _, n := range nodes {
		name, rest, ok := strings.Cut(n, "=")
		if !ok {
			return fmt.Errorf("-node %q is not name=host:port", n)
		}
		node := proto.Node{Name: name}
		node.Addr, node.Desc, _ = strings.Cut(rest, ",")
		replaced := false
		for i := range cfg.Nodes {
			if cfg.Nodes[i].Name == name {
				cfg.Nodes[i], replaced = node, true
			}
		}
		if !replaced {
			cfg.Nodes = append(cfg.Nodes, node)
		}
	}
	return pod.Run(ctx, cfg, pf.config, out, nil)
}

func runClient(ctx context.Context, args []string, out io.Writer) error {
	pf := newPeerFlags("client", "127.0.0.1:7280", out)
	var forwards, proxies common.StringList
	pf.flags.Var(&forwards, "forward", "local address tunneled to a node, listen=pod/node[@server] (repeatable)")
	pf.flags.Var(&proxies, "webproxy", "serve a server's web page locally through its encrypted WebSocket, listen=ws://host:port/ (repeatable)")
	wsPassword := pf.flags.String("ws-password", "", "password of the -webproxy WebSockets (default: -password)")
	if err := pf.parse(args); err != nil {
		return err
	}
	var cfg client.Config
	if err := common.LoadJSON(pf.config, &cfg); err != nil {
		return err
	}
	pf.configured = pf.config != "" && fileExists(pf.config)
	if err := pf.apply(&cfg.Name, &cfg.WebAddr, &cfg.WebAuth, &cfg.Servers, &cfg.Meta); err != nil {
		return err
	}
	for _, s := range forwards {
		f, err := parseForward(s)
		if err != nil {
			return err
		}
		dup := false
		for _, have := range cfg.Forwards {
			dup = dup || have.Listen == f.Listen
		}
		if !dup {
			cfg.Forwards = append(cfg.Forwards, f)
		}
	}
	password := *wsPassword
	if password == "" {
		password = pf.password
	}
	for _, s := range proxies {
		listen, url, ok := strings.Cut(s, "=")
		if !ok || listen == "" || url == "" {
			return fmt.Errorf("-webproxy %q is not listen=ws://host:port/", s)
		}
		if password == "" {
			return fmt.Errorf("-webproxy %s needs a -ws-password or -password", s)
		}
		cfg.WebProxies = append(cfg.WebProxies, client.WebProxy{Listen: listen, URL: url, Password: password})
	}
	return client.Run(ctx, cfg, pf.config, out, nil)
}

// parseForward parses listen=pod/node[@server].
func parseForward(s string) (client.Forward, error) {
	listen, target, ok := strings.Cut(s, "=")
	target, server, _ := strings.Cut(target, "@")
	podName, node, ok2 := strings.Cut(target, "/")
	if !ok || !ok2 || listen == "" || podName == "" || node == "" {
		return client.Forward{}, fmt.Errorf("-forward %q is not listen=pod/node[@server]", s)
	}
	return client.Forward{Listen: listen, Pod: podName, Node: node, Server: server}, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
