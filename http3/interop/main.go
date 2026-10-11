// Command interop is the peer the HTTP/3 conformance tests run against: an
// HTTP/3 server and an HTTP/3 client built on quic-go, driven over stdin and
// stdout with one JSON object per line.
//
// It is a module of its own so that quic-go stays out of fib's own module:
// nothing in github.com/lesismal/fib imports it, and the tests run it as
// a subprocess.
//
//	interop -mode server -cert cert.pem -key key.pem [-retry]
//	interop -mode client -ca cert.pem -url https://127.0.0.1:1234
//
// In server mode it prints "listening <address>" once it is up, then reads
// commands; in client mode it reads request specifications. Either way a
// line of JSON goes back for each line that comes in, so a test can drive it
// step by step.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "127.0.0.1:0", "server listen address")
	certFile := flag.String("cert", "", "PEM certificate, server mode")
	keyFile := flag.String("key", "", "PEM private key, server mode")
	caFile := flag.String("ca", "", "PEM certificate to trust, client mode")
	url := flag.String("url", "", "base URL, client mode")
	retry := flag.Bool("retry", false, "server mode: validate every source address with a Retry")
	early0 := flag.Bool("early-server", false, "server mode: accept 0-RTT data, and say in X-Used-0RTT whether a request used it")
	early := flag.Bool("early", false, "client mode: keep session tickets, and send GET_0RTT and HEAD_0RTT requests as 0-RTT data")
	flag.Parse()

	var err error
	switch *mode {
	case "server":
		err = runServer(*addr, *certFile, *keyFile, *retry, *early0)
	case "client":
		err = runClient(*url, *caFile, *early)
	default:
		err = errors.New("-mode must be server or client")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "interop:", err)
		os.Exit(1)
	}
}

// request is what the test asks the client to send.
type request struct {
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Header   map[string][]string `json:"header"`
	Trailer  map[string][]string `json:"trailer"`
	Body     string              `json:"body"`
	BodySize int                 `json:"bodySize"`
	// Count requests sent at once on the connection, all identical.
	Count int `json:"count"`
	// Command is for the server side: "shutdown" or "quit".
	Command string `json:"command"`
}

// result is what comes back for each request.
type result struct {
	Status   int                 `json:"status"`
	Proto    string              `json:"proto"`
	Header   map[string][]string `json:"header"`
	Trailer  map[string][]string `json:"trailer"`
	Body     string              `json:"body"`
	BodyLen  int                 `json:"bodyLen"`
	BodyHash string              `json:"bodyHash"`
	Count    int                 `json:"count"`
	Error    string              `json:"error"`
	// Resumed says the connection resumed a session.
	Resumed bool `json:"resumed"`
	// Sent0RTT says, in client mode with -early, whether the first request
	// on the connection went out in a 0-RTT packet: its stream's first data
	// was sent under the early keys. A request quic-go writes once its
	// handshake has finished goes under the 1-RTT keys even when the server
	// would have taken it early. The 0-RTT tests make one request per
	// connection, which this is.
	Sent0RTT bool `json:"sent0rtt"`
}

// sentStreams records, for each request stream of a client connection, the
// type of the packet its first data went out in, from quic-go's qlog events.
type sentStreams struct {
	mu    sync.Mutex
	first map[int64]qlog.PacketType
}

func (s *sentStreams) AddProducer() qlogwriter.Recorder   { return s }
func (s *sentStreams) SupportsSchemas(schema string) bool { return schema == qlog.EventSchema }
func (s *sentStreams) Close() error                       { return nil }

func (s *sentStreams) RecordEvent(e qlogwriter.Event) {
	sent, ok := e.(qlog.PacketSent)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range sent.Frames {
		frame, ok := f.Frame.(*qlog.StreamFrame)
		// Client-initiated bidirectional streams are the requests.
		if !ok || frame.StreamID%4 != 0 {
			continue
		}
		if _, seen := s.first[int64(frame.StreamID)]; !seen {
			s.first[int64(frame.StreamID)] = sent.Header.PacketType
		}
	}
}

// startedEarly reports whether the connection's first request stream sent
// its first data in a 0-RTT packet.
func (s *sentStreams) startedEarly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first[0] == qlog.PacketType0RTT
}

// ticketCache is the client's session cache, counting the tickets stored in
// it, so that "waitticket" can wait for the one the current connection gets
// rather than a guess at how long it takes: a connection dialed before it
// arrives resumes with the ticket it already used, whose early data a
// server turns away, since a ticket carries early data once.
type ticketCache struct {
	tls.ClientSessionCache
	mu     sync.Mutex
	cond   *sync.Cond
	stored int
	// mark is the count when the current connection was dialed.
	mark int
}

func newTicketCache() *ticketCache {
	c := &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(8)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *ticketCache) Put(key string, cs *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, cs)
	if cs == nil {
		return
	}
	c.mu.Lock()
	c.stored++
	c.cond.Broadcast()
	c.mu.Unlock()
}

// newConn marks the start of a connection: the tickets waited for from here
// are the ones it gets.
func (c *ticketCache) newConn() {
	c.mu.Lock()
	c.mark = c.stored
	c.mu.Unlock()
}

// wait waits until a ticket has arrived since the current connection began.
func (c *ticketCache) wait(timeout time.Duration) error {
	timer := time.AfterFunc(timeout, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer timer.Stop()
	deadline := time.Now().Add(timeout)
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.stored == c.mark {
		if !time.Now().Before(deadline) {
			return errors.New("no session ticket arrived")
		}
		c.cond.Wait()
	}
	return nil
}

// connTraces keeps the trace of the connection the client dialed last: the
// 0-RTT tests dial one at a time, each after a "newconn".
type connTraces struct {
	mu   sync.Mutex
	last *sentStreams
}

func (c *connTraces) tracer(_ context.Context, isClient bool, _ quic.ConnectionID) qlogwriter.Trace {
	if !isClient {
		return nil
	}
	s := &sentStreams{first: make(map[int64]qlog.PacketType)}
	c.mu.Lock()
	c.last = s
	c.mu.Unlock()
	return s
}

func (c *connTraces) startedEarly() bool {
	c.mu.Lock()
	s := c.last
	c.mu.Unlock()
	return s != nil && s.startedEarly()
}

// hashBody is the FNV-1a of a body, so that large ones are checked without
// being carried around.
func hashBody(b []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// filler is the deterministic body both sides can generate.
func filler(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func runClient(base, caFile string, early bool) error {
	if base == "" {
		return errors.New("-url is required")
	}
	pool := x509.NewCertPool()
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return err
		}
		if !pool.AppendCertsFromPEM(pem) {
			return errors.New("no certificate in " + caFile)
		}
	}
	tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}
	var traces *connTraces
	var tickets *ticketCache
	if early {
		traces = &connTraces{}
		tickets = newTicketCache()
		tr.TLSClientConfig.ClientSessionCache = tickets
		tr.QUICConfig = &quic.Config{Allow0RTT: true, Tracer: traces.tracer}
	}
	defer tr.Close()
	client := &http.Client{Transport: tr, Timeout: 60 * time.Second}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var spec request
		if err := json.Unmarshal([]byte(line), &spec); err != nil {
			return err
		}
		if spec.Command == "quit" {
			return nil
		}
		if spec.Command == "waitticket" {
			// The current connection's ticket has arrived, so the next one
			// resumes with a ticket it has not used.
			var res result
			if tickets == nil {
				res.Error = "waitticket needs -early"
			} else if err := tickets.wait(10 * time.Second); err != nil {
				res.Error = err.Error()
			}
			if err := out.Encode(res); err != nil {
				return err
			}
			continue
		}
		if spec.Command == "newconn" {
			// The next request dials again, resuming the session if it can.
			tr.CloseIdleConnections()
			if tickets != nil {
				tickets.newConn()
			}
			if err := out.Encode(result{}); err != nil {
				return err
			}
			continue
		}
		res := doRequests(client, base, spec)
		if traces != nil {
			res.Sent0RTT = traces.startedEarly()
		}
		if err := out.Encode(res); err != nil {
			return err
		}
	}
	return in.Err()
}

// doRequests sends a specification's requests, all at once when it asks for
// more than one, and returns the last result, or the first failure.
func doRequests(client *http.Client, base string, spec request) result {
	count := max(spec.Count, 1)
	results := make([]result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = doRequest(client, base, spec)
		}(i)
	}
	wg.Wait()
	for _, r := range results {
		if r.Error != "" {
			r.Count = count
			return r
		}
	}
	last := results[count-1]
	last.Count = count
	return last
}

func doRequest(client *http.Client, base string, spec request) result {
	method := spec.Method
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	switch {
	case spec.BodySize > 0:
		body = strings.NewReader(string(filler(spec.BodySize)))
	case spec.Body != "":
		body = strings.NewReader(spec.Body)
	}
	req, err := http.NewRequest(method, base+spec.Path, body)
	if err != nil {
		return result{Error: err.Error()}
	}
	for key, values := range spec.Header {
		req.Header[http.CanonicalHeaderKey(key)] = values
	}
	if len(spec.Trailer) > 0 {
		req.Trailer = make(http.Header, len(spec.Trailer))
		for key, values := range spec.Trailer {
			req.Trailer[http.CanonicalHeaderKey(key)] = values
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{Error: err.Error()}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return result{Status: resp.StatusCode, Error: err.Error()}
	}
	res := result{
		Status:   resp.StatusCode,
		Proto:    resp.Proto,
		Header:   resp.Header,
		Trailer:  resp.Trailer,
		BodyLen:  len(data),
		BodyHash: hashBody(data),
		Resumed:  resp.TLS != nil && resp.TLS.DidResume,
	}
	if len(data) <= 4096 {
		res.Body = string(data)
	}
	return res
}

func runServer(addr, certFile, keyFile string, retry, early bool) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	pconn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	server := &http3.Server{
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
		Handler:   http.HandlerFunc(serveHTTP),
		ConnContext: func(ctx context.Context, c *quic.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, c)
		},
	}
	var quicConfig *quic.Config
	if early {
		quicConfig = &quic.Config{Allow0RTT: true}
	}
	transport := &quic.Transport{Conn: pconn}
	if retry {
		// Every connection is answered with a Retry, which is what makes a
		// client prove its address before the handshake goes on.
		transport.VerifySourceAddress = func(net.Addr) bool { return true }
	}
	listener, err := transport.ListenEarly(server.TLSConfig, quicConfig)
	if err != nil {
		return err
	}
	fmt.Printf("listening %s\n", pconn.LocalAddr().String())
	os.Stdout.Sync()

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ServeListener(listener) }()

	in := bufio.NewScanner(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var spec request
		if err := json.Unmarshal([]byte(line), &spec); err != nil {
			return err
		}
		switch spec.Command {
		case "shutdown":
			// A graceful shutdown sends GOAWAY and waits for the requests
			// already running.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := server.Shutdown(ctx)
			cancel()
			res := result{}
			if err != nil {
				res.Error = err.Error()
			}
			if err := out.Encode(res); err != nil {
				return err
			}
		case "quit":
			_ = server.Close()
			<-serveDone
			return nil
		default:
			if err := out.Encode(result{Error: "unknown command " + spec.Command}); err != nil {
				return err
			}
		}
	}
	_ = server.Close()
	<-serveDone
	return in.Err()
}

// serveHTTP answers the conformance tests' requests. The query decides what
// comes back, so that one endpoint covers every case.
// connKey is where ConnContext keeps a request's QUIC connection.
type connKey struct{}

func serveHTTP(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if c, ok := r.Context().Value(connKey{}).(*quic.Conn); ok {
		w.Header().Set("X-Used-0RTT", strconv.FormatBool(c.ConnectionState().Used0RTT))
	}
	if ms, _ := strconv.Atoi(query.Get("delay")); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("X-Read-Error", err.Error())
	}
	// Trailers of the request are only there once its body has been read.
	for key, values := range r.Trailer {
		w.Header()["X-Req-Trailer-"+key] = values
	}
	w.Header().Set("X-Method", r.Method)
	w.Header().Set("X-Path", r.URL.RequestURI())
	w.Header().Set("X-Proto", r.Proto)
	w.Header().Set("X-Host", r.Host)
	w.Header().Set("X-Body-Len", strconv.Itoa(len(body)))
	w.Header().Set("X-Body-Hash", hashBody(body))
	for key, values := range r.Header {
		if strings.HasPrefix(key, "X-Echo-") {
			w.Header()[key] = values
		}
	}
	if v := query.Get("header"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				w.Header().Add(name, value)
			}
		}
	}
	var trailers []string
	if v := query.Get("trailer"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, _, ok := strings.Cut(pair, ":"); ok {
				trailers = append(trailers, http.CanonicalHeaderKey(name))
			}
		}
		w.Header()["Trailer"] = trailers
	}
	if query.Get("interim") != "" {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Del("Link")
	}
	status := http.StatusOK
	if v, err := strconv.Atoi(query.Get("status")); err == nil && v >= 200 {
		status = v
	}
	size, _ := strconv.Atoi(query.Get("size"))
	if size > 0 && status != http.StatusNoContent && status != http.StatusNotModified {
		w.Header().Set("Content-Length", strconv.Itoa(size))
	}
	w.WriteHeader(status)
	switch {
	case size > 0:
		out := filler(size)
		for len(out) > 0 {
			n := min(len(out), 64<<10)
			if _, err := w.Write(out[:n]); err != nil {
				return
			}
			out = out[n:]
		}
	case query.Get("echo") != "":
		_, _ = w.Write(body)
	}
	if v := query.Get("trailer"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				w.Header().Set(name, value)
			}
		}
	}
}
