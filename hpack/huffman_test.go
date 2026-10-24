package hpack

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
)

// huffmanDecodeBits is the decoder the table replaced, which walks the tree
// a bit at a time, kept to check the table against.
func huffmanDecodeBits(s []byte) ([]byte, error) {
	var out []byte
	node := 0
	depth, ones := 0, true
	for _, b := range s {
		for i := 7; i >= 0; i-- {
			bit := (b >> uint(i)) & 1
			next := huffmanTree[node].children[bit]
			if next == 0 {
				return nil, ErrInvalidHuffman
			}
			node = int(next)
			depth++
			ones = ones && bit == 1
			if huffmanTree[node].children[0] == 0 {
				out = append(out, huffmanTree[node].sym)
				node, depth, ones = 0, 0, true
			}
		}
	}
	if depth > 7 || !ones {
		return nil, ErrInvalidHuffman
	}
	return out, nil
}

// TestHuffmanStepsMatchTheTree decodes every input of up to two bytes, and
// random ones, coded strings and garbage alike, with the table and with the
// tree, and checks they agree on what decodes and to what.
func TestHuffmanStepsMatchTheTree(t *testing.T) {
	check := func(in []byte) {
		t.Helper()
		want, wantErr := huffmanDecodeBits(in)
		got, err := huffmanDecodeAppend(nil, in, 0)
		if (err != nil) != (wantErr != nil) || !bytes.Equal(got, want) {
			t.Fatalf("%x: table %q %v, tree %q %v", in, got, err, want, wantErr)
		}
	}
	check(nil)
	for i := 0; i < 1<<16; i++ {
		check([]byte{byte(i)})
		check([]byte{byte(i >> 8), byte(i)})
	}
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		raw := make([]byte, r.IntN(40))
		for j := range raw {
			raw[j] = byte(r.Uint32())
		}
		check(raw)
		coded := AppendHuffman(nil, string(raw))
		check(coded)
		// Coded strings with their padding cut or overrun.
		if len(coded) > 0 {
			check(coded[:len(coded)-1])
			check(append(coded, 0xff))
		}
	}
}

// TestHuffmanMaxLen checks that decoding past maxLen fails.
func TestHuffmanMaxLen(t *testing.T) {
	coded := AppendHuffman(nil, "www.example.com")
	if _, err := huffmanDecodeAppend(nil, coded, 15); err != nil {
		t.Fatal(err)
	}
	if _, err := huffmanDecodeAppend(nil, coded, 14); !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("decoding 15 bytes with a limit of 14: %v", err)
	}
}

// BenchmarkHuffmanDecode decodes header values of the kinds a browser sends.
func BenchmarkHuffmanDecode(b *testing.B) {
	values := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"/static/js/main.4f2c9a1b.chunk.js?v=20261024",
		"gzip, deflate, br, zstd",
		"en-US,en;q=0.9",
	}
	var coded [][]byte
	n := 0
	for _, v := range values {
		coded = append(coded, AppendHuffman(nil, v))
		n += len(v)
	}
	out := make([]byte, 0, 256)
	b.SetBytes(int64(n))
	b.ReportAllocs()
	for b.Loop() {
		for _, c := range coded {
			out, _ = huffmanDecodeAppend(out[:0], c, 0)
		}
	}
}
