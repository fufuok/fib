//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"encoding/binary"
	"io"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// TestCoverH2Frame pins down h2AppendPushPromise splitting a header block
// that does not fit one frame: the PUSH_PROMISE carries the promised stream
// and as much of the block as fits beside it, CONTINUATION frames carry the
// rest at the full frame size, and only the last of them ends the headers.
func TestCoverH2Frame(t *testing.T) {
	block := bytes.Repeat([]byte{'h'}, 25)
	buf := h2AppendPushPromise(nil, 1, 2, block, 10)
	var got []byte
	var frames []h2Frame
	for len(buf) > 0 {
		f, n, err := h2ReadFrame(buf, h2DefaultMaxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, f)
		buf = buf[n:]
	}
	if len(frames) != 3 {
		t.Fatalf("%d frames, want 3", len(frames))
	}
	for i, f := range frames {
		wantType, wantEnd := h2FrameContinuation, i == len(frames)-1
		payload := f.payload
		if i == 0 {
			wantType = h2FramePushPromise
			if promised := binary.BigEndian.Uint32(payload); promised != 2 {
				t.Fatalf("promised stream %d, want 2", promised)
			}
			payload = payload[4:]
		}
		if f.typ != wantType || f.streamID != 1 || (f.flags&h2FlagEndHeaders != 0) != wantEnd || f.length > 10 {
			t.Fatalf("frame %d: %+v", i, f)
		}
		got = append(got, payload...)
	}
	if !bytes.Equal(got, block) {
		t.Fatalf("reassembled %q", got)
	}
}

// cvH2fStoppedPool is a pool that has stopped taking work, as the stream
// pool of an engine that has closed has.
func cvH2fStoppedPool(t *testing.T) *taskpool.TaskPool {
	t.Helper()
	pool := taskpool.New("cvH2f", 1, 0)
	pool.Stop()
	if pool.Go(func() {}) {
		t.Fatal("a stopped pool took a task")
	}
	return pool
}

// cvH2fRequest is a GET of path whose response stream records it.
func cvH2fRequest(stream Stream, path string) (*StreamRequest, *Context) {
	r := new(StreamRequest)
	r.URL.Path = path
	r.Request.URL = &r.URL
	r.Request.Method = stdhttp.MethodGet
	r.Request.Header = stdhttp.Header{}
	return r, r.Context(nil, stream, nil)
}

// TestCoverStreamPool pins down the StreamPool paths the protocols' own
// tests leave out: Run on the reader without a pool and on the pool with
// one, requests served where they are submitted once their connection's
// pool has stopped taking work, a response flushed after a request the pool
// ran on a connection that does not flush its own, and a nil pool serving
// a Context in place.
func TestCoverStreamPool(t *testing.T) {
	t.Run("RunWithoutPool", func(t *testing.T) {
		var nilPool *StreamPool
		var gate StreamGate
		ran := false
		nilPool.Run(&gate, nil, func() { ran = true })
		if !ran || gate.Running() != 0 {
			t.Fatalf("ran %v, %d running: want served in place, uncounted", ran, gate.Running())
		}
	})

	t.Run("RunOnPool", func(t *testing.T) {
		pool := NewStreamPool(StreamPoolConfig{})
		var gate StreamGate
		done := make(chan struct{})
		pool.Run(&gate, nil, func() { close(done) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the request never ran on the pool")
		}
	})

	t.Run("RunStoppedPoolFlushes", func(t *testing.T) {
		// A request run here, on the connection's own handler, stands in for
		// one a multiplexed reader hands over after its engine stopped the
		// pool: the gate still names that pool, so the request is served in
		// place, and the connection, which does not flush its own requests'
		// responses, is flushed once it is.
		stopped := cvH2fStoppedPool(t)
		ran := make(chan bool, 1)
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			pool := NewStreamPool(StreamPoolConfig{})
			gate := StreamGate{pool: stopped}
			inline := false
			pool.Run(&gate, c.Conn, func() { inline = true })
			ran <- inline && gate.Running() == 0
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		})))
		resp, err := stdhttp.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "ok" || !<-ran {
			t.Fatalf("body %q: the request was not served in place", body)
		}
	})

	t.Run("SubmitStoppedPool", func(t *testing.T) {
		pool := NewStreamPool(StreamPoolConfig{})
		stream := &recordingStream{done: make(chan struct{}, 2)}
		handler := HandlerFunc(func(c *Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
		})
		gate := StreamGate{pool: cvH2fStoppedPool(t)}
		var batch StreamBatch
		pool.Submit(&batch) // Nothing queued: nothing to do.
		for _, path := range []string{"/a", "/b"} {
			r, _ := cvH2fRequest(stream, path)
			pool.Queue(&batch, &gate, nil, handler, r)
		}
		pool.Submit(&batch)
		// Served in place, so both are answered by the time Submit returns.
		if len(stream.done) != 2 || len(batch.tasks) != 0 || gate.Running() != 0 {
			t.Fatalf("%d answered, %d queued, %d running", len(stream.done), len(batch.tasks), gate.Running())
		}
		if stream.paths[0] != "/a" || stream.paths[1] != "/b" {
			t.Fatalf("served %v, want in order", stream.paths)
		}
	})

	t.Run("ServeContext", func(t *testing.T) {
		handler := HandlerFunc(func(c *Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
		})
		// No pool: the Context is served on the caller.
		var nilPool *StreamPool
		stream := &recordingStream{done: make(chan struct{}, 1)}
		_, c := cvH2fRequest(stream, "/reader")
		var gate StreamGate
		nilPool.serveContext(&gate, nil, handler, c)
		if len(stream.done) != 1 {
			t.Fatal("a request without a pool was not served in place")
		}
		// A pool that has stopped: admitted, then served on the caller.
		stream = &recordingStream{done: make(chan struct{}, 1)}
		_, c = cvH2fRequest(stream, "/stopped")
		gate = StreamGate{pool: cvH2fStoppedPool(t)}
		NewStreamPool(StreamPoolConfig{}).serveContext(&gate, nil, handler, c)
		if len(stream.done) != 1 || gate.Running() != 0 {
			t.Fatal("a request on a stopped pool was not served in place")
		}
	})
}
