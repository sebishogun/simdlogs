package storage

import (
	"fmt"
	"testing"
)

// BenchmarkDictBlockDecode compares decoding one 64-value hex block via the
// nibble codec vs LZ4 -- the materialize hot path. Nibble should win (no LZ4
// sequence parsing), so if it does not the unpack is the bottleneck.
func BenchmarkDictBlockDecode(b *testing.B) {
	vals := make([]string, 64)
	seed := uint32(99)
	for i := range vals {
		bs := make([]byte, 16)
		for j := range bs {
			seed = seed*1664525 + 1013904223
			bs[j] = hexChar(byte(seed & 0xf))
		}
		vals[i] = string(bs)
	}
	raw := marshalRawBlock(vals)
	hexed := hexPackBlock(raw, 64)
	lz4ed := lz4Compress(raw)
	rawLen := len(raw)
	b.Run("hex", func(b *testing.B) {
		for b.Loop() {
			sinkB = hexUnpackBlock(hexed)
		}
	})
	b.Run("lz4", func(b *testing.B) {
		for b.Loop() {
			sinkB = lz4Decompress(lz4ed, rawLen)
		}
	})
	fmt.Fprintln(sink, len(raw))
}

var sinkB []byte
