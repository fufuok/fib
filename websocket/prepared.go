//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"sync"
	"unicode/utf8"
)

// PreparedMessage is a data message made ready once for any number of
// connections, as a broadcast sends it:
//
//	m, err := websocket.NewPreparedMessage(websocket.Text, payload)
//	if err != nil {
//		return err
//	}
//	for _, c := range conns {
//		_ = c.WritePreparedMessage(m)
//	}
//
// WriteMessage checks, frames and, with permessage-deflate, compresses its
// payload for each connection it is called on. A PreparedMessage checks the
// payload once, frames it once, and compresses it at most once for each window
// size the connections agreed, which is nearly always the one: this package's
// servers send without context takeover, so a message compresses to the same
// bytes for every connection with the same window.
//
// A server's connections all send the same frame, without copying it: what a
// slow peer cannot take yet waits in its queue as that one frame, where
// WriteMessage would leave a copy in each. A client masks every frame with a
// key of its own (RFC 6455 section 5.3), so a client's connections share the
// compressed payload but each still builds its frame.
//
// A PreparedMessage is safe for concurrent use and is never changed, so it can
// be kept and sent again.
type PreparedMessage struct {
	opcode Opcode
	// frame is the message as a server sends it uncompressed: the header and
	// a copy of the payload, which every connection shares and none writes.
	frame     []byte
	headerLen int
	// deflated holds the message compressed for each window a connection may
	// have agreed, made the first time a connection with that window sends it.
	deflated [maxWindowBits - minWindowBits + 1]preparedFrame
}

type preparedFrame struct {
	once      sync.Once
	frame     []byte
	headerLen int
}

// NewPreparedMessage prepares a Text or Binary message. A Text payload must be
// valid UTF-8. The payload is copied, so the caller may reuse it once
// NewPreparedMessage returns.
func NewPreparedMessage(opcode Opcode, payload []byte) (*PreparedMessage, error) {
	if opcode != Text && opcode != Binary {
		return nil, errDataOpcode
	}
	if opcode == Text && !utf8.Valid(payload) {
		return nil, ErrInvalidPayload
	}
	m := &PreparedMessage{opcode: opcode}
	m.frame, m.headerLen = buildFrame(opcode, payload, false)
	return m, nil
}

func (m *PreparedMessage) payload() []byte { return m.frame[m.headerLen:] }

// deflatedFrame returns the message compressed for a window of 1<<windowBits
// bytes, framed as a server sends it, and the length of the frame's header.
func (m *PreparedMessage) deflatedFrame(windowBits int) ([]byte, int) {
	p := &m.deflated[windowBits-minWindowBits]
	p.once.Do(func() {
		compressor, compressed := compressMessage(m.payload(), windowBits)
		p.frame, p.headerLen = buildFrame(m.opcode, compressed, true)
		compressor.release()
	})
	return p.frame, p.headerLen
}

// buildFrame frames payload, a data message's, as a server sends it, in an
// array of its own, and returns the frame and the length of its header.
// compressed sets RSV1.
func buildFrame(opcode Opcode, payload []byte, compressed bool) ([]byte, int) {
	var header [maxFrameHeader]byte
	// A data frame of any length has a header, so this cannot fail.
	headerLen, _ := frameHeader(header[:], opcode, len(payload))
	if compressed {
		header[0] |= 0x40
	}
	frame := make([]byte, headerLen+len(payload))
	copy(frame, header[:headerLen])
	copy(frame[headerLen:], payload)
	return frame, headerLen
}

// WritePreparedMessage sends m, as WriteMessage would send its opcode and
// payload, compressed if this connection uses permessage-deflate.
func (c *Connection) WritePreparedMessage(m *PreparedMessage) error {
	if c.closeSent.Load() {
		return errCloseSent
	}
	frame, headerLen := m.frame, m.headerLen
	if c.compress {
		frame, headerLen = m.deflatedFrame(int(c.windowBits))
	}
	if c.client {
		return c.sendMaskedFrame(m.opcode, frame[headerLen:], c.compress)
	}
	return c.conn.SendShared(frame)
}
