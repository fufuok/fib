// Package reqbody sends the body of a client request as it is produced, for
// the clients of package http and package http3: which bodies stream rather
// than being read whole before the request goes out, and the loop that reads
// one in pieces and hands each to the transport, which paces it.
package reqbody

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"sync"

	"github.com/lesismal/fib/bufferpool"
)

const (
	// BufferLimit is the largest body a request with a known length has read
	// whole before it is sent, as every body was before bodies streamed: it
	// costs little memory, goes out with the header, and can be sent again.
	BufferLimit = 1 << 20
	// ChunkSize is how much of a streamed body is read at a time.
	ChunkSize = 32 << 10
	// MaxQueued is how much of a streamed body the transport may hold unsent
	// before the next piece waits for some of it to go.
	MaxQueued = 64 << 10
)

// ErrNotReplayable is what a request whose body streamed gets when it has to
// be sent again, after a connection was lost or the server turned it away
// unprocessed, and it has no GetBody to read the body afresh with.
var ErrNotReplayable = errors.New("http: request body already sent and the request has no GetBody to send it again")

// Streams reports whether req's body is sent as it is read rather than read
// whole first. Bodies that are small and known, with a ContentLength of at
// most BufferLimit, as net/http.NewRequest sets for an in-memory reader, are
// read whole as before, and so is the body of an HTTP/1.0 request, which has no chunked
// framing for a body of unknown length.
func Streams(req *stdhttp.Request) bool {
	body := req.Body
	if body == nil || body == stdhttp.NoBody {
		return false
	}
	if req.ProtoMajor == 1 && req.ProtoMinor == 0 {
		return false
	}
	if req.ContentLength > 0 {
		return req.ContentLength > BufferLimit
	}
	// A length of zero with GetBody set is what net/http.NewRequest leaves
	// for an empty in-memory body.
	return req.GetBody == nil
}

// Declared is the length a streamed body declares, or -1 when it is unknown.
func Declared(req *stdhttp.Request) int64 {
	if req.ContentLength > 0 {
		return req.ContentLength
	}
	return -1
}

// Source is the body of a request that streams, across the attempts to send
// it: the first reads req.Body, and any later one what GetBody returns.
type Source struct {
	req *stdhttp.Request

	mu      sync.Mutex
	current *body
	taken   bool
	closed  bool
}

// NewSource returns the source of req's body.
func NewSource(req *stdhttp.Request) *Source { return &Source{req: req} }

// Take returns the body for an attempt to send the request, which Pump
// reads and closes. The attempt before it, if any, has been abandoned.
func (s *Source) Take() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrNotReplayable
	}
	if s.current != nil {
		s.current.Close()
	}
	rc := s.req.Body
	if s.taken {
		if s.req.GetBody == nil {
			return nil, ErrNotReplayable
		}
		var err error
		if rc, err = s.req.GetBody(); err != nil {
			return nil, err
		}
	}
	s.taken = true
	s.current = &body{rc: rc}
	return s.current, nil
}

// Close closes the body being sent, if any, which ends a Read on it that is
// waiting, for a body that allows that, as io.Pipe's does. It is for when
// the request is over: no attempt follows.
func (s *Source) Close() {
	s.mu.Lock()
	s.closed = true
	current := s.current
	s.mu.Unlock()
	if current != nil {
		current.Close()
	}
}

// body closes the reader under it once, whoever closes it first.
type body struct {
	rc   io.ReadCloser
	once sync.Once
}

func (b *body) Read(p []byte) (int, error) { return b.rc.Read(p) }

func (b *body) Close() error {
	b.once.Do(func() { _ = b.rc.Close() })
	return nil
}

// BodyError is a failure of the body itself rather than of the transport:
// reading it failed, or it was not as long as it declared. It fails the
// request, where a transport's failure is the transport's to report.
type BodyError struct{ Err error }

func (e *BodyError) Error() string { return e.Err.Error() }
func (e *BodyError) Unwrap() error { return e.Err }

// Pump reads rc in pieces of up to ChunkSize and hands each to send, which
// must be done with it when it returns, and closes rc. declared is the
// length the body declared, or -1. It returns nil once the body has ended at
// the length it declared, a *BodyError when the body failed, and otherwise
// the error send returned.
func Pump(rc io.ReadCloser, declared int64, send func([]byte) error) error {
	defer rc.Close()
	buf := bufferpool.Get(ChunkSize)
	defer bufferpool.Put(buf)
	var total int64
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			total += int64(n)
			if declared >= 0 && total > declared {
				return &BodyError{fmt.Errorf("http: ContentLength=%d with Body length at least %d", declared, total)}
			}
			if err := send(buf[:n]); err != nil {
				return err
			}
		}
		switch {
		case err == io.EOF:
			if declared >= 0 && total != declared {
				return &BodyError{fmt.Errorf("http: ContentLength=%d with Body length %d", declared, total)}
			}
			return nil
		case err != nil:
			return &BodyError{err}
		}
	}
}
