package storage

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/sebishogun/simd"
)

// forEncodeBlock frame-of-reference bit-packs a block of d-gaps (per-list
// deltas) at the block's max width, using our in-tree SIMD kernel -- the same
// simd.BitPackInto the dict-id index already uses. Returns width + packed bytes.
func forEncodeBlock(dgaps []uint32) (int, []byte) {
	if len(dgaps) == 0 {
		return 0, nil
	}
	var max uint32
	for _, d := range dgaps {
		if d > max {
			max = d
		}
	}
	w := bitWidth(int(max) + 1)
	words := (len(dgaps)*w+31)/32 + 1 // BitPackInto's sizing contract
	buf := make([]uint32, words)
	simd.BitPackInto(buf, dgaps, int32(w))
	out := make([]byte, words*4)
	for i, x := range buf {
		binary.LittleEndian.PutUint32(out[i*4:], x)
	}
	return w, out
}

// buildRowLists inverts row->id into per-id ascending row lists.
func buildRowLists(indices []uint32, dictLen int) [][]uint32 {
	lists := make([][]uint32, dictLen)
	for row, id := range indices {
		lists[id] = append(lists[id], uint32(row))
	}
	return lists
}

// currentPostingsBytes is the size of today's delta-varint + LZ4 (postBlock=64)
// encoding of the row lists (data section only, matching postings.marshal).
func currentPostingsBytes(lists [][]uint32) int {
	total := 0
	for i := 0; i < len(lists); i += postBlock {
		hi := i + postBlock
		if hi > len(lists) {
			hi = len(lists)
		}
		var raw []byte
		for _, l := range lists[i:hi] {
			var prev uint32
			for _, r := range l {
				raw = binary.AppendUvarint(raw, uint64(r-prev))
				prev = r
			}
		}
		total += len(lz4Compress(raw))
	}
	return total
}

// forPostingsBytes is the size of FOR bit-packing the same d-gaps per 64-id block.
func forPostingsBytes(lists [][]uint32) int {
	total := 0
	for i := 0; i < len(lists); i += postBlock {
		hi := i + postBlock
		if hi > len(lists) {
			hi = len(lists)
		}
		var dgaps []uint32
		for _, l := range lists[i:hi] {
			var prev uint32
			for _, r := range l {
				dgaps = append(dgaps, r-prev)
				prev = r
			}
		}
		_, packed := forEncodeBlock(dgaps)
		total += 1 + len(packed) // width byte + packed
	}
	return total
}

// BenchmarkPostingsDecodeOne compares single-id row-list retrieval through the
// production readers: the legacy LZ4+varint path (postingRows on a v7 blob) vs
// the v8 FOR path (postingRows on a FOR blob) built from the same data. This is
// the hot selective-query read, so the FOR path must not regress it.
func BenchmarkPostingsDecodeOne(b *testing.B) {
	const n = 128 * 1024
	seed := uint64(0x9e3779b97f4a7c15)
	rnd := func() uint64 { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed }
	regimes := []struct {
		name     string
		distinct int
	}{{"near-unique", n}, {"medium", n / 20}, {"low-card", 8}}
	for _, rg := range regimes {
		indices := make([]uint32, n)
		for i := range indices {
			indices[i] = uint32(rnd() % uint64(rg.distinct))
		}
		p := buildPostings(indices, rg.distinct)
		v7 := marshalV7(p)
		v8 := p.marshal(nil)
		ids := make([]int, 256)
		for i := range ids {
			ids[i] = int(rnd() % uint64(rg.distinct))
		}
		b.Run(rg.name+"/lz4varint", func(b *testing.B) {
			i := 0
			for b.Loop() {
				sinkRows = postingRows(v7, ids[i&255], n)
				i++
			}
		})
		b.Run(rg.name+"/for-simd", func(b *testing.B) {
			i := 0
			for b.Loop() {
				sinkRows = postingRows(v8, ids[i&255], n)
				i++
			}
		})
	}
}

var sinkRows []uint32

// TestBitPackBlockAccess verifies we can unpack a single 32-value block from
// the middle of a uniformly-bit-packed array (block b at packed[b*width:]),
// which is what O(1)-ish count random access in v8 needs.
func TestBitPackBlockAccess(t *testing.T) {
	const n = 200
	width := 5
	src := make([]uint32, ((n+31)/32)*32) // pad to 32
	for i := range src {
		src[i] = uint32(i % 32) // fits in 5 bits
	}
	words := (len(src)*width+31)/32 + 1
	packed := make([]uint32, words)
	simd.BitPackInto(packed, src, int32(width))

	// Whole-array unpack (the known-good reference).
	whole := make([]uint32, len(src))
	simd.BitUnpackInto(whole, packed, int32(width))

	// Per-block unpack of block 3 (values 96..127) via packed[3*width:].
	blk := 3
	out := make([]uint32, 32)
	simd.BitUnpackInto(out, packed[blk*width:], int32(width))
	for i := 0; i < 32; i++ {
		if out[i] != whole[blk*32+i] {
			t.Fatalf("per-block unpack mismatch at block %d idx %d: got %d want %d", blk, i, out[i], whole[blk*32+i])
		}
	}
	t.Logf("per-block unpack OK: block %d matches whole-array unpack", blk)
}

func TestPostingsFORvsLZ4(t *testing.T) {
	const n = 100_000
	regimes := []struct {
		name     string
		distinct int
	}{
		{"near-unique (trace_id-like)", n}, // every row a distinct value
		{"medium (path-like)", n / 20},     // ~20 rows per value
		{"low-card (level-like)", 5},       // ~20k rows per value
	}
	// Deterministic uniform-random assignment -> realistic varied d-gaps (the
	// `i % distinct` regular pattern is a constant-gap artifact LZ4 loves).
	seed := uint64(0x9e3779b97f4a7c15)
	rnd := func() uint64 { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed }
	for _, rg := range regimes {
		indices := make([]uint32, n)
		for i := range indices {
			indices[i] = uint32(rnd() % uint64(rg.distinct))
		}
		lists := buildRowLists(indices, rg.distinct)
		cur := currentPostingsBytes(lists)
		fr := forPostingsBytes(lists)
		t.Logf("%-28s distinct=%-7d | delta-varint+lz4 %6dKB | FOR-bitpack %6dKB | %.2fx smaller",
			rg.name, rg.distinct, cur/1024, fr/1024, float64(cur)/float64(fr))
	}

	// Decode-speed spot check: unpack a 4096-value block, our SIMD kernel.
	dgaps := make([]uint32, 4096)
	for i := range dgaps {
		dgaps[i] = uint32(i % 32)
	}
	w, packed := forEncodeBlock(dgaps)
	words := (len(packed)) / 4
	pk := make([]uint32, words)
	for i := 0; i < words; i++ {
		pk[i] = binary.LittleEndian.Uint32(packed[i*4:])
	}
	out := make([]uint32, 4096)
	iter := 2000
	t0 := time.Now()
	for i := 0; i < iter; i++ {
		simd.BitUnpackInto(out[:4096], pk, int32(w))
	}
	per := time.Since(t0) / time.Duration(iter)
	t.Logf("SIMD BitUnpackInto 4096 vals @ w=%d: %v/block (%.1f M vals/s)", w, per, 4096/per.Seconds()/1e6)
}
