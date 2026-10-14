package qpack

import (
	"errors"
	"strings"
	"testing"
)

// Every way a field section can be malformed stops the decoder with
// ErrDecompression, both through Decode and through a Decoder that
// remembers strings, so a peer's bad encoding is a connection error and
// never a panic or a half-decoded section.
func TestDecodeMalformedSections(t *testing.T) {
	cases := map[string][]byte{
		"missing delta base":          {0x00},
		"truncated indexed index":     {0x00, 0x00, 0xff},
		"indexed index never ends":    {0x00, 0x00, 0xff, 0x80},
		"indexed index overflows":     {0x00, 0x00, 0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80},
		"dynamic name reference":      {0x00, 0x00, 0x40, 0x00},
		"truncated name reference":    {0x00, 0x00, 0x5f},
		"name reference out of range": {0x00, 0x00, 0x5f, 0x7f},
		"name reference no value":     {0x00, 0x00, 0x51},
		"truncated value length":      {0x00, 0x00, 0x51, 0x7f},
		"invalid huffman value":       {0x00, 0x00, 0x51, 0x84, 0xff, 0xff, 0xff, 0xff},
		"literal name no value":       {0x00, 0x00, 0x21, 'a'},
		"truncated literal name":      {0x00, 0x00, 0x27},
	}
	for name, block := range cases {
		if _, err := decode(t, block, 0); !errors.Is(err, ErrDecompression) {
			t.Errorf("%s: Decode got %v", name, err)
		}
		var d Decoder
		if _, err := d.AppendFields(nil, block, 0); !errors.Is(err, ErrDecompression) {
			t.Errorf("%s: Decoder got %v", name, err)
		}
	}
}

// A Huffman-coded string whose code fits the limit but whose decoded text
// does not is too large, not undecodable: the limit is on what the fields
// would cost in memory.
func TestDecodeHuffmanPastLimit(t *testing.T) {
	value := strings.Repeat("a", 64) // 5 bits per 'a', so 40 bytes coded
	block := encode([]HeaderField{{":path", value}})
	if _, err := decode(t, block, 50); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

// An error from emit stops decoding there and is returned as is, so a
// caller can reject a field (a forbidden header, say) without decoding the
// rest of the section.
func TestDecodeEmitError(t *testing.T) {
	stop := errors.New("stop")
	block := encode([]HeaderField{{":method", "GET"}, {":path", "/"}})
	n := 0
	err := Decode(block, 0, func(HeaderField) error {
		n++
		return stop
	})
	if err != stop || n != 1 {
		t.Fatalf("got %v after %d fields, want stop after 1", err, n)
	}
}

// A field never to be indexed whose name is not in the static table is
// sent as a literal name with the N bit set, and decodes back unchanged.
func TestAppendFieldNeverIndexedLiteralName(t *testing.T) {
	block := append([]byte(nil), Prefix...)
	block = AppendField(block, "x-secret", "token", true)
	if block[2]&0xf0 != 0x30 {
		t.Fatalf("first byte %#x, want a literal name with the N bit", block[2])
	}
	got, err := decode(t, block, 0)
	if err != nil || len(got) != 1 || got[0] != (HeaderField{"x-secret", "token"}) {
		t.Fatalf("got %v, %v", got, err)
	}
}
