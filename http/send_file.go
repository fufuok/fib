//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strconv"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/sidepool"
)

// sendFilePiece is how much of a file SendFile reads from the disk at a time
// where it cannot hand the file to sendfile.
const sendFilePiece = 64 << 10

// SendFile sends n bytes of f, from offset, as the response's body, without
// the calling goroutine waiting for the disk or for the client, and runs
// done, once and on another goroutine, when all of it has been handed to the
// connection or the stream, or with the error that stopped it. It is how a
// handler serves a file of any size without a worker held up by either: on
// an HTTP/1 connection without a layer, such as TLS, the range goes from the
// file to the socket by sendfile(2), as Connection.SendFile sends it, paced
// by the socket, and what of it is still queued counts against the
// response's limit as written bytes do, so WriteNoWait and OnWritable see a
// client that has stopped reading it; elsewhere it is read a piece at a time
// on a pool of its own
// for the disk, package sidepool's File, and each piece is written with
// WriteNoWait, the next read waiting for OnWritable once the response is
// above its limit.
//
// The header is the one the handler set through Header, with a
// Content-Length of n added when it sets none and the response has not
// begun; a handler that wants another status, such as 206 with its
// Content-Range, calls WriteHeader first. SendFile keeps the response open
// until done has run, as a Retain does, and lets it go after: the handler
// writes nothing more until then, and may return at once. f must stay open
// until done runs; the range must lie within it, since a file that turns
// out shorter ends the response short, which closes an HTTP/1 connection or
// resets a stream. A response to HEAD sends none of the file.
func (c *Context) SendFile(f *os.File, offset, n int64, done func(error)) {
	if done == nil {
		done = func(error) {}
	}
	switch {
	case f == nil || offset < 0 || n < 0:
		sidepool.Go(func() { done(errors.New("http: invalid SendFile range")) })
		return
	case c.wrote:
		sidepool.Go(func() { done(ErrResponseWritten) })
		return
	}
	w := c.writer()
	if w.status == 0 {
		if _, set := w.header["Content-Length"]; !set && w.written == 0 && !c.holdsWhole() {
			c.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		}
		c.WriteHeader(stdhttp.StatusOK)
	}
	c.Retain()
	s := &fileSend{c: c, f: f, offset: offset, left: n, done: done}
	if n == 0 || c.Request.Method == stdhttp.MethodHead || !statusHasBody(w.status) {
		var err error
		switch {
		case n == 0:
		case !statusHasBody(w.status):
			err = stdhttp.ErrBodyNotAllowed
		default:
			// Counted, as Write counts what it is given for HEAD.
			w.written += n
		}
		sidepool.Go(func() { s.finish(err) })
		return
	}
	if c.isHTTP1() && !c.holdsWhole() && c.Conn.Layer() == nil && w.err == nil && n >= minSendFileSize &&
		(w.declared < 0 || w.written+n <= w.declared) {
		err := c.sendFileHTTP1(f, offset, n)
		sidepool.Go(func() { s.finish(err) })
		return
	}
	sidepool.File().Go(s.pump)
}

// sendFileHTTP1 is SendFile on an HTTP/1 connection the file can go to the
// socket from without passing through a layer.
func (c *Context) sendFileHTTP1(f *os.File, offset, n int64) error {
	w := c.w
	if !w.committed {
		if err := c.commit(false); err != nil {
			return w.fail(err)
		}
	} else if c.h1WriteExpired() {
		return w.fail(os.ErrDeadlineExceeded)
	}
	if w.chunked {
		if err := c.Conn.SendOwned(fmt.Appendf(nil, "%x\r\n", n)); err != nil {
			return w.fail(err)
		}
	}
	if err := c.Conn.SendFile(f, offset, n); err != nil {
		return w.fail(err)
	}
	if w.chunked {
		if err := c.Conn.Send(crlf); err != nil {
			return w.fail(err)
		}
	}
	w.written += n
	return nil
}

// fileSend is a SendFile reading its file a piece at a time.
type fileSend struct {
	c      *Context
	f      *os.File
	offset int64
	left   int64
	done   func(error)
}

// pump reads and writes pieces, on the file pool, until the response is
// above its limit, when it goes on from OnWritable's callback, or the range
// has all gone.
func (s *fileSend) pump() {
	buf := bufferpool.Get(int(min(s.left, sendFilePiece)))
	defer bufferpool.Put(buf)
	for s.left > 0 {
		k, err := s.f.ReadAt(buf[:min(s.left, int64(len(buf)))], s.offset)
		if k == 0 {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			s.finish(err)
			return
		}
		s.offset += int64(k)
		s.left -= int64(k)
		above, werr := s.c.WriteNoWait(buf[:k])
		if werr != nil {
			s.finish(werr)
			return
		}
		if above && s.left > 0 && s.c.OnWritable(s.resume) {
			return
		}
	}
	s.finish(nil)
}

// resume carries on once the response has room, or ends it.
func (s *fileSend) resume(err error) {
	if err != nil {
		s.finish(err)
		return
	}
	sidepool.File().Go(s.pump)
}

// finish runs done and lets the response go.
func (s *fileSend) finish(err error) {
	s.done(err)
	s.c.Release()
}
