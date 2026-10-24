package hpack

// huffmanNode is a node of the decoding tree: a leaf when children is zero.
type huffmanNode struct {
	children [2]uint16
	sym      uint8
}

// huffmanTree is the decoding tree; the root is element 0. 256 leaves need
// 255 inner nodes.
var huffmanTree []huffmanNode

func buildHuffmanTree() {
	huffmanTree = make([]huffmanNode, 1, 512)
	for sym := 0; sym < 256; sym++ {
		code, length := huffmanCodes[sym], huffmanCodeLen[sym]
		node := 0
		for i := int(length) - 1; i >= 0; i-- {
			bit := (code >> uint(i)) & 1
			next := huffmanTree[node].children[bit]
			if next == 0 {
				huffmanTree = append(huffmanTree, huffmanNode{})
				next = uint16(len(huffmanTree) - 1)
				huffmanTree[node].children[bit] = next
			}
			node = int(next)
		}
		huffmanTree[node].sym = uint8(sym)
	}
}

// huffmanDecode decodes s. Padding must be at most seven bits, all ones, the
// start of EOS (RFC 7541 section 5.2); EOS itself never decodes.
func huffmanDecode(s []byte, maxLen int) (string, error) {
	out, err := huffmanDecodeAppend(make([]byte, 0, len(s)*8/5), s, maxLen)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// huffmanStep is where the decoder goes from one state on the next four
// bits of input: the state it reaches, and the symbol those bits finished, if
// they finished one. Codes are five bits or longer, so four bits finish one
// at most.
type huffmanStep struct {
	next  uint8
	sym   uint8
	flags uint8
}

const (
	// huffmanEmit says the step finished a symbol, sym.
	huffmanEmit = 1 << iota
	// huffmanFail says the bits ran into EOS, which never decodes.
	huffmanFail
)

// huffmanSteps is the decoder's table: a state is an inner node of the tree,
// 0 the root, and the decoder takes four bits a step rather than walking the
// tree a bit at a time. huffmanAccepts says which states the input may end
// in: those whose bits since the last symbol are at most seven, all ones,
// which is the padding RFC 7541 section 5.2 allows.
var (
	huffmanSteps   [256][16]huffmanStep
	huffmanAccepts [256]bool
)

// buildHuffmanSteps builds the decoder's table from the tree.
func buildHuffmanSteps() {
	// The states, numbered in the order the inner nodes were made, the
	// root first.
	var stateOf [512]uint8
	var nodes []int
	for i, n := range huffmanTree {
		if n.children[0] != 0 {
			stateOf[i] = uint8(len(nodes))
			nodes = append(nodes, i)
		}
	}
	for state, start := range nodes {
		for nibble := 0; nibble < 16; nibble++ {
			node := start
			var step huffmanStep
			for i := 3; i >= 0; i-- {
				next := huffmanTree[node].children[nibble>>uint(i)&1]
				if next == 0 {
					step.flags |= huffmanFail
					break
				}
				node = int(next)
				if huffmanTree[node].children[0] == 0 {
					step.sym = huffmanTree[node].sym
					step.flags |= huffmanEmit
					node = 0
				}
			}
			step.next = stateOf[node]
			huffmanSteps[state][nibble] = step
		}
	}
	// The root, and the nodes up to seven ones down from it.
	node := 0
	for depth := 0; depth <= 7 && (depth == 0 || huffmanTree[node].children[0] != 0); depth++ {
		huffmanAccepts[stateOf[node]] = true
		node = int(huffmanTree[node].children[1])
	}
}

// huffmanDecodeAppend decodes s onto out, as huffmanDecode does.
func huffmanDecodeAppend(out, s []byte, maxLen int) ([]byte, error) {
	state := uint8(0)
	for _, b := range s {
		step := &huffmanSteps[state][b>>4]
		if step.flags != 0 {
			if step.flags&huffmanFail != 0 {
				return nil, ErrInvalidHuffman
			}
			out = append(out, step.sym)
		}
		step = &huffmanSteps[step.next][b&0xf]
		if step.flags != 0 {
			if step.flags&huffmanFail != 0 {
				return nil, ErrInvalidHuffman
			}
			out = append(out, step.sym)
		}
		state = step.next
		if maxLen > 0 && len(out) > maxLen {
			return nil, ErrStringTooLong
		}
	}
	if !huffmanAccepts[state] {
		return nil, ErrInvalidHuffman
	}
	return out, nil
}

// huffmanLen is the length of s once Huffman coded.
func huffmanLen(s string) int {
	bits := 0
	for i := 0; i < len(s); i++ {
		bits += int(huffmanCodeLen[s[i]])
	}
	return (bits + 7) / 8
}

// huffmanAppend appends s Huffman coded, padded with ones.
func huffmanAppend(dst []byte, s string) []byte {
	var acc uint64
	var n uint
	for i := 0; i < len(s); i++ {
		c := s[i]
		acc = acc<<huffmanCodeLen[c] | uint64(huffmanCodes[c])
		n += uint(huffmanCodeLen[c])
		for n >= 8 {
			n -= 8
			dst = append(dst, byte(acc>>n))
		}
	}
	if n > 0 {
		dst = append(dst, byte(acc<<(8-n))|byte(0xff>>n))
	}
	return dst
}

// HuffmanDecode decodes s, a Huffman-coded string, failing once it would
// decode to more than maxLen bytes when maxLen is positive. QPACK uses the
// same code with prefixes of its own, so it decodes through this.
func HuffmanDecode(s []byte, maxLen int) (string, error) { return huffmanDecode(s, maxLen) }

// HuffmanLen is the length of s once Huffman coded.
func HuffmanLen(s string) int { return huffmanLen(s) }

// AppendHuffman appends s Huffman coded, padded with ones.
func AppendHuffman(dst []byte, s string) []byte { return huffmanAppend(dst, s) }
