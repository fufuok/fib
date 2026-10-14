package websocket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// rawClientFrame is a masked frame with first as its first byte, so that a
// test can set any bits of it, and the payload length written as size says:
// 7 bits, 16 or 64, whatever the length is. A length too short for its form
// is how a peer breaks the minimal-encoding rule.
func rawClientFrame(first byte, size int, payload []byte) []byte {
	frame := []byte{first, 0x80}
	switch size {
	case 7:
		frame[1] |= byte(len(payload))
	case 16:
		frame[1] |= 126
		frame = binary.BigEndian.AppendUint16(frame, uint16(len(payload)))
	case 64:
		frame[1] |= 127
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	mask := [4]byte{9, 8, 7, 6}
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i&3])
	}
	return frame
}

// TestParserRejectsMalformedFrames runs the frames RFC 6455 section 5 says a
// receiver must fail the connection for, each through Feed and FeedOneBorrowed
// alike, which is what the Autobahn suite's sections 1 to 7 exercise from the
// outside.
func TestParserRejectsMalformedFrames(t *testing.T) {
	text := func(s string) []byte { return []byte(s) }
	closePayload := func(code uint16, reason string) []byte {
		return append(binary.BigEndian.AppendUint16(nil, code), reason...)
	}
	cases := []struct {
		name   string
		stream []byte
		want   error
	}{
		{"rsv2", rawClientFrame(0x80|0x20|byte(Text), 7, text("x")), ErrProtocol},
		{"rsv3", rawClientFrame(0x80|0x10|byte(Binary), 7, text("x")), ErrProtocol},
		{"rsv1 without deflate", rawClientFrame(0x80|0x40|byte(Text), 7, text("x")), ErrProtocol},
		{"reserved data opcode", rawClientFrame(0x80|0x3, 7, nil), ErrProtocol},
		{"reserved control opcode", rawClientFrame(0x80|0xb, 7, nil), ErrProtocol},
		{"fragmented ping", rawClientFrame(byte(Ping), 7, text("p")), ErrProtocol},
		{"ping of 126 bytes", rawClientFrame(0x80|byte(Ping), 16, make([]byte, 126)), ErrProtocol},
		{"continuation first", clientFrame(Continuation, true, text("x")), ErrProtocol},
		{"text inside a fragmented message",
			append(clientFrame(Text, false, text("a")), clientFrame(Text, true, text("b"))...), ErrProtocol},
		{"16-bit length below 126", rawClientFrame(0x80|byte(Binary), 16, make([]byte, 125)), ErrProtocol},
		{"64-bit length below 65536", rawClientFrame(0x80|byte(Binary), 64, make([]byte, 100)), ErrProtocol},
		{"64-bit length with the top bit set", func() []byte {
			frame := rawClientFrame(0x80|byte(Binary), 64, nil)
			frame[2] = 0x80
			return frame
		}(), ErrProtocol},
		{"close of one byte", clientFrame(Close, true, []byte{0x03}), ErrProtocol},
		{"close code 1005", clientFrame(Close, true, closePayload(1005, "")), ErrProtocol},
		{"close code 999", clientFrame(Close, true, closePayload(999, "")), ErrProtocol},
		{"close code 2000", clientFrame(Close, true, closePayload(2000, "")), ErrProtocol},
		{"close reason not UTF-8", clientFrame(Close, true, closePayload(CloseNormal, "\xff")), ErrInvalidPayload},
		{"first fragment not UTF-8", clientFrame(Text, false, []byte{'o', 'k', 0xff}), ErrInvalidPayload},
		{"last fragment not UTF-8",
			append(clientFrame(Text, false, text("ok")), clientFrame(Continuation, true, []byte{0xc3})...), ErrInvalidPayload},
	}
	for _, tc := range cases {
		for _, borrowed := range []bool{false, true} {
			parser := NewParser(1 << 20)
			var err error
			if borrowed {
				for data := tc.stream; err == nil; data = nil {
					var complete bool
					if _, complete, err = parser.FeedOneBorrowed(data); !complete {
						break
					}
				}
				parser.ReleaseBorrowed()
			} else {
				_, err = parser.Feed(tc.stream)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("%s (borrowed %v): error %v, want %v", tc.name, borrowed, err, tc.want)
			}
		}
	}
}

// TestParserAcceptsCloseFrames checks the close frames a receiver must take:
// one with no payload, a registered code with a UTF-8 reason, and a code from
// the private-use range.
func TestParserAcceptsCloseFrames(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		append(binary.BigEndian.AppendUint16(nil, CloseGoingAway), "κόσμε"...),
		binary.BigEndian.AppendUint16(nil, 4999),
	} {
		events, err := NewParser(1 << 10).Feed(clientFrame(Close, true, payload))
		if err != nil || len(events) != 1 || events[0].Opcode != Close || !bytes.Equal(events[0].Payload, payload) {
			t.Errorf("close %x: events %v, err %v", payload, events, err)
		}
	}
}

// TestParserWaitsForTheWholeHeader feeds a frame a byte at a time, so that
// every part of its header, the extended lengths and the mask, arrives cut
// short at some point, and checks it still parses once it is all there.
func TestParserWaitsForTheWholeHeader(t *testing.T) {
	for _, size := range []int{5, 300, 70000} {
		payload := bytes.Repeat([]byte{'z'}, size)
		frame := clientFrame(Binary, true, payload)
		parser := NewParser(1 << 20)
		var events []Event
		head := min(20, len(frame)-1)
		for i := range frame[:head] {
			got, err := parser.Feed(frame[i : i+1])
			if err != nil {
				t.Fatalf("size %d: byte %d: %v", size, i, err)
			}
			events = append(events, got...)
		}
		got, err := parser.Feed(frame[head:])
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		events = append(events, got...)
		if len(events) != 1 || !bytes.Equal(events[0].Payload, payload) {
			t.Fatalf("size %d: %d events", size, len(events))
		}
	}
}

// TestParserPartialTextFromServer checks the UTF-8 of a text frame from a
// server, which is unmasked, as its pieces arrive: invalid bytes fail it
// before it completes, and a frame whose checked prefix was valid still fails
// on an invalid end.
func TestParserPartialTextFromServer(t *testing.T) {
	frame := serverFrame(Text, true, []byte("hello \xff world"))
	parser := NewServerFrameParser(1 << 10)
	if _, err := parser.Feed(frame[:10]); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("partial invalid text: %v, want ErrInvalidPayload", err)
	}

	frame = serverFrame(Text, true, []byte("hello world \xce"))
	parser = NewServerFrameParser(1 << 10)
	if events, err := parser.Feed(frame[:8]); err != nil || len(events) != 0 {
		t.Fatalf("valid prefix: events %v, err %v", events, err)
	}
	if _, err := parser.Feed(frame[8:]); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("text ending mid-character: %v, want ErrInvalidPayload", err)
	}

	// The same for a client's masked frame, whose prefix is checked unmasked.
	frame = clientFrame(Text, true, []byte("hello world \xce"))
	parser = NewParser(1 << 10)
	if events, err := parser.Feed(frame[:10]); err != nil || len(events) != 0 {
		t.Fatalf("masked valid prefix: events %v, err %v", events, err)
	}
	if _, err := parser.Feed(frame[10:]); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("masked text ending mid-character: %v, want ErrInvalidPayload", err)
	}
}

// TestParserDirection checks that each side rejects frames masked the other
// way: a client's parser an unmasked server frame never needs masking, and a
// server's parser any frame a client did not mask.
func TestParserDirection(t *testing.T) {
	if _, err := NewServerFrameParser(0).Feed(clientFrame(Text, true, []byte("x"))); !errors.Is(err, ErrProtocol) {
		t.Errorf("masked frame from a server: %v, want ErrProtocol", err)
	}
	events, err := NewServerFrameParser(0).Feed(serverFrame(Text, true, []byte("x")))
	if err != nil || len(events) != 1 || string(events[0].Payload) != "x" {
		t.Errorf("server frame: events %v, err %v", events, err)
	}
	if p := NewParser(0); p.maxMessageBytes != 16<<20 {
		t.Errorf("NewParser(0) bounds messages at %d, want 16 MiB", p.maxMessageBytes)
	}
}

// TestParserReset checks that Reset forgets a message half received, whether
// its frames are held in the parser's own array or still in a borrowed read
// buffer, so that the next frame parses as the start of a new message.
func TestParserReset(t *testing.T) {
	first := clientFrame(Text, false, []byte("abandoned"))
	more := clientFrame(Continuation, true, []byte(" message"))
	next := clientFrame(Text, true, []byte("fresh"))

	owned := NewParser(1 << 10)
	if _, err := owned.Feed(append(first, more[:3]...)); err != nil {
		t.Fatal(err)
	}
	owned.Reset()
	if owned.fragment() != nil || len(owned.buffer) != 0 {
		t.Fatal("Reset kept the message in progress")
	}
	events, err := owned.Feed(next)
	if err != nil || len(events) != 1 || string(events[0].Payload) != "fresh" {
		t.Fatalf("after Reset: events %v, err %v", events, err)
	}

	// A frame returned from a read that holds more leaves the rest of the
	// read borrowed.
	borrowed := NewParser(1 << 10)
	read := append(clientFrame(Binary, true, []byte("whole")), first...)
	if _, complete, err := borrowed.FeedOneBorrowed(read); err != nil || !complete {
		t.Fatalf("borrowed: complete %v, err %v", complete, err)
	}
	borrowed.Reset()
	if borrowed.buffer != nil || borrowed.borrowedBuffer || borrowed.fragment() != nil {
		t.Fatal("Reset kept the borrowed buffer")
	}
	if event, complete, err := borrowed.FeedOneBorrowed(next); err != nil || !complete || string(event.Payload) != "fresh" {
		t.Fatalf("after Reset: %q, complete %v, err %v", event.Payload, complete, err)
	}
	borrowed.ReleaseBorrowed()
}

// TestParserCopiesBorrowedTailBeforeAppending covers a parser that
// FeedOneBorrowed left holding the start of a frame in the caller's read
// buffer, behind the frame it returned, and that is then fed by FeedOne: the
// bytes must be copied out before more are appended, rather than written into
// the buffer the parser was lent.
func TestParserCopiesBorrowedTailBeforeAppending(t *testing.T) {
	first := clientFrame(Binary, true, []byte("first frame"))
	second := clientFrame(Binary, true, []byte("second frame"))
	parser := NewParser(1 << 10)
	// The first frame arrives in two reads, the second of which also carries
	// the start of the next frame.
	if _, complete, err := parser.FeedOneBorrowed(first[:5]); err != nil || complete {
		t.Fatalf("partial first frame: complete %v, err %v", complete, err)
	}
	parser.ReleaseBorrowed()
	read := make([]byte, 0, 64)
	read = append(read, first[5:]...)
	read = append(read, second[:4]...)
	sentinel := read[: len(read)+1 : len(read)+1]
	sentinel[len(read)] = 0xee
	event, complete, err := parser.FeedOneBorrowed(read)
	if err != nil || !complete || string(event.Payload) != "first frame" {
		t.Fatalf("first frame: %q, complete %v, err %v", event.Payload, complete, err)
	}
	event, complete, err = parser.FeedOne(second[4:])
	if err != nil || !complete || string(event.Payload) != "second frame" {
		t.Fatalf("second frame: %q, complete %v, err %v", event.Payload, complete, err)
	}
	if sentinel[len(read)] != 0xee {
		t.Fatal("the lent read buffer was written past what it lent")
	}
}

// TestFrameSizeOverflow checks that a frame announcing more than an int can
// hold is not given a size, so that adopt does not size a buffer by it.
func TestFrameSizeOverflow(t *testing.T) {
	header := rawClientFrame(0x80|byte(Binary), 64, nil)
	binary.BigEndian.PutUint64(header[2:10], 1<<63-1)
	if size, known := frameSize(header); known {
		t.Fatalf("frameSize = %d, want unknown", size)
	}
}

// TestFrameEncodersRejectInvalidFrames checks that MarshalFrame and
// frameHeader refuse an opcode that is not a frame's, and a control frame
// longer than 125 bytes.
func TestFrameEncodersRejectInvalidFrames(t *testing.T) {
	var header [maxFrameHeader]byte
	for _, tc := range []struct {
		opcode Opcode
		size   int
	}{{Continuation, 0}, {Opcode(0x3), 1}, {Ping, 126}, {Close, 200}} {
		if _, err := MarshalFrame(tc.opcode, make([]byte, tc.size)); !errors.Is(err, ErrProtocol) {
			t.Errorf("MarshalFrame(%d, %d bytes): %v, want ErrProtocol", tc.opcode, tc.size, err)
		}
		if _, err := frameHeader(header[:], tc.opcode, tc.size); !errors.Is(err, ErrProtocol) {
			t.Errorf("frameHeader(%d, %d bytes): %v, want ErrProtocol", tc.opcode, tc.size, err)
		}
	}
}
