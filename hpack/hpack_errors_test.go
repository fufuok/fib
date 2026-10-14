package hpack

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestDecodeRejectsMalformedBlocks feeds one malformed block per way a header
// block can be cut short or point outside the tables, and checks each fails
// with the error that names it rather than decoding a field or panicking.
func TestDecodeRejectsMalformedBlocks(t *testing.T) {
	// A string longer than MaxStringLength, Huffman coded.
	longCoded := appendInt([]byte{0x00}, 0x80, 7, uint64(huffmanLen("abcdefgh")))
	longCoded = huffmanAppend(longCoded, "abcdefgh")
	cases := []struct {
		name  string
		block []byte
		want  error
	}{
		{"indexed truncated int", []byte{0xff}, ErrTruncated},
		{"indexed int continues past block", []byte{0xff, 0x80}, ErrTruncated},
		{"indexed int overflow", []byte{0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, ErrIntegerOverflow},
		{"incremental literal missing name", []byte{0x40}, ErrTruncated},
		{"size update truncated int", []byte{0x3f}, ErrTruncated},
		{"literal name index truncated", []byte{0x0f}, ErrTruncated},
		{"literal name index past table", []byte{0x0f, 0x7f}, ErrInvalidIndex},
		{"literal missing value", []byte{0x01}, ErrTruncated},
		{"string length truncated", []byte{0x00, 0x7f}, ErrTruncated},
		{"huffman EOS", []byte{0x00, 0x84, 0xff, 0xff, 0xff, 0xff}, ErrInvalidHuffman},
		{"huffman string too long", longCoded, ErrStringTooLong},
		{"raw string too long", []byte{0x00, 0x08, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, ErrStringTooLong},
	}
	for _, c := range cases {
		d := NewDecoder(DefaultTableSize)
		d.MaxStringLength = 4
		err := d.Decode(c.block, func(HeaderField) error { return nil })
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

// TestDecodeStopsOnEmitError checks that an error from emit ends decoding for
// every kind of field, so a caller rejecting a header (a forbidden name, a
// list over its size limit) is not handed the rest of the block.
func TestDecodeStopsOnEmitError(t *testing.T) {
	stop := errors.New("stop")
	blocks := map[string][]byte{
		"indexed":                {0x82, 0x84},
		"incremental literal":    {0x41, 0x01, 'a', 0x84},
		"literal without index":  {0x01, 0x01, 'a', 0x84},
		"literal never indexed":  {0x11, 0x01, 'a', 0x84},
		"literal with name text": {0x00, 0x01, 'x', 0x01, 'y', 0x84},
	}
	for name, block := range blocks {
		d := NewDecoder(DefaultTableSize)
		calls := 0
		err := d.Decode(block, func(HeaderField) error {
			calls++
			return stop
		})
		if err != stop || calls != 1 {
			t.Errorf("%s: err %v after %d calls, want stop after 1", name, err, calls)
		}
	}
}

// TestDynamicTableLookupOutOfRange pins down the bounds the table enforces on
// its own, which Decode never reaches because field checks them first.
func TestDynamicTableLookupOutOfRange(t *testing.T) {
	var table dynamicTable
	table.maxSize = DefaultTableSize
	table.add(HeaderField{"x", "y"})
	if _, ok := table.get(0); ok {
		t.Fatal("index 0 resolved")
	}
	if _, ok := table.get(2); ok {
		t.Fatal("index past the table resolved")
	}
	if f, ok := table.get(1); !ok || f != (HeaderField{"x", "y"}) {
		t.Fatalf("index 1: %v, %v", f, ok)
	}
	if _, _, err := readInt(nil, 7); err != ErrTruncated {
		t.Fatalf("readInt of nothing: %v", err)
	}
}

// TestEncoderAnnouncesSmallestTableSize checks RFC 7541 section 4.2: when the
// peer shrinks the table and grows it again between two blocks, the next
// block announces the smallest size first and then the final one, so the
// decoder evicts what the shrink evicted. A size larger than the default is
// clamped, since the encoder never uses more.
func TestEncoderAnnouncesSmallestTableSize(t *testing.T) {
	e := NewEncoder()
	d := NewDecoder(DefaultTableSize)
	value := strings.Repeat("v", 40)
	decodeAll(t, d, e.AppendField(e.Begin(nil), "x-a", value, false))
	if len(e.table.entries) != 1 {
		t.Fatalf("encoder table has %d entries, want 1", len(e.table.entries))
	}

	e.SetMaxTableSize(0)
	e.SetMaxTableSize(1 << 20)
	if e.table.maxSize != DefaultTableSize {
		t.Fatalf("max size %d, want the default %d", e.table.maxSize, DefaultTableSize)
	}
	block := e.Begin(nil)
	if want := []byte{0x20, 0x3f, 0xe1, 0x1f}; !reflect.DeepEqual(block, want) {
		t.Fatalf("size updates % x, want % x", block, want)
	}
	block = e.AppendField(block, "x-a", value, false)
	got := decodeAll(t, d, block)
	if !reflect.DeepEqual(got, []HeaderField{{"x-a", value}}) {
		t.Fatalf("got %v", got)
	}
	if len(d.table.entries) != len(e.table.entries) || d.table.size != e.table.size {
		t.Fatalf("decoder table %d entries/%d bytes, encoder %d/%d",
			len(d.table.entries), d.table.size, len(e.table.entries), e.table.size)
	}

	// A size already in effect changes nothing and announces nothing.
	e.SetMaxTableSize(DefaultTableSize)
	if block := e.Begin(nil); len(block) != 0 {
		t.Fatalf("unchanged size announced % x", block)
	}
}

// TestHuffmanDecodeErrors checks the exported decoder QPACK uses reports
// invalid codes and the length limit.
func TestHuffmanDecodeErrors(t *testing.T) {
	if _, err := HuffmanDecode([]byte{0xff, 0xff, 0xff, 0xff}, 0); err != ErrInvalidHuffman {
		t.Fatalf("EOS: %v", err)
	}
	coded := AppendHuffman(nil, "hello")
	if len(coded) != HuffmanLen("hello") {
		t.Fatalf("length %d, want %d", len(coded), HuffmanLen("hello"))
	}
	if _, err := HuffmanDecode(coded, 3); err != ErrStringTooLong {
		t.Fatalf("limit: %v", err)
	}
	if s, err := HuffmanDecode(coded, 5); err != nil || s != "hello" {
		t.Fatalf("got %q, %v", s, err)
	}
}
