package storage

import (
	"encoding/binary"
	"testing"
)

// TestCountLayoutEquiv pins the count table's two decode views: the SIMD-kernel
// bulk decode (decodeIndices) and the scalar O(1) single read (extractCountBits,
// used by every count consumer) must agree with what encodeIndices packed,
// across widths and lengths including partial tail blocks.
func TestCountLayoutEquiv(t *testing.T) {
	for _, n := range []int{1, 31, 32, 33, 100, 4096, 130000} {
		for _, w := range []int{1, 3, 7, 14, 20} {
			vals := make([]uint32, n)
			mask := uint32(1)<<uint(w) - 1
			seed := uint32(0x1234 + n + w)
			for i := range vals {
				seed = seed*1664525 + 1013904223
				vals[i] = seed & mask
			}
			simdEnc := encodeIndices(vals, w)
			// decodeIndices (SIMD bulk) round-trips.
			got := decodeIndices(simdEnc, n, w)
			for i := range vals {
				if got[i] != vals[i] {
					t.Fatalf("n=%d w=%d: decodeIndices[%d]=%d want %d", n, w, i, got[i], vals[i])
				}
			}
			// extractCountBits (scalar single) on the SIMD-encoded bytes.
			for _, i := range []int{0, n / 2, n - 1} {
				if got := extractCountBits(simdEnc, i, w); uint32(got) != vals[i] {
					t.Fatalf("n=%d w=%d: extractCountBits(simdEnc,%d)=%d want %d", n, w, i, got, vals[i])
				}
			}
		}
	}
}

// marshalV7 writes the legacy v7 postings blob (uncompressed rowOffsets prefix
// sums, then LZ4 delta-varint blocks) so the back-compat read path has a
// fixture. New code never writes this; the readers must still decode it. The
// v7 varint data is reconstructed from the d-gaps (row-prev == the gap).
func marshalV7(p postings) []byte {
	dictLen := len(p.rowOffsets) - 1
	byteOffsets := make([]uint32, dictLen+1)
	var data []byte
	for id := 0; id < dictLen; id++ {
		for _, g := range p.dgaps[p.rowOffsets[id]:p.rowOffsets[id+1]] {
			data = binary.AppendUvarint(data, uint64(g))
		}
		byteOffsets[id+1] = uint32(len(data))
	}
	var b []byte
	b = appU32(b, uint32(dictLen+1))
	for _, o := range p.rowOffsets {
		b = appU32(b, o)
	}
	numBlocks := 0
	if dictLen > 0 {
		numBlocks = (dictLen + postBlock - 1) / postBlock
	}
	type blkIdx struct{ compOff, compLen, rawLen uint32 }
	idx := make([]blkIdx, numBlocks)
	var comp []byte
	for k := 0; k < numBlocks; k++ {
		lo := k * postBlock
		hi := lo + postBlock
		if hi > dictLen {
			hi = dictLen
		}
		cnt := hi - lo
		dataLo := byteOffsets[lo]
		raw := make([]byte, 0, 4*(cnt+1)+int(byteOffsets[hi]-dataLo))
		for j := lo; j <= hi; j++ {
			raw = appU32(raw, byteOffsets[j]-dataLo)
		}
		raw = append(raw, data[dataLo:byteOffsets[hi]]...)
		c := lz4Compress(raw)
		idx[k] = blkIdx{uint32(len(comp)), uint32(len(c)), uint32(len(raw))}
		comp = append(comp, c...)
	}
	b = appU32(b, uint32(numBlocks))
	b = appU32(b, uint32(postBlock))
	for _, e := range idx {
		b = appU32(b, e.compOff)
		b = appU32(b, e.compLen)
		b = appU32(b, e.rawLen)
	}
	b = appU32(b, uint32(len(comp)))
	return append(b, comp...)
}

// TestV7BackCompat verifies the readers decode a legacy v7 blob identically to
// the v8 FOR blob built from the same data -- the sentinel dispatch must route
// old files to the v7 LZ4 path (first word = dictLen+1, never a v8 sentinel).
func TestV7BackCompat(t *testing.T) {
	// Mixed cardinality: singletons, repeats, one empty id, and a >64-id case
	// that spans two blocks.
	indices := make([]uint32, 5000)
	for i := range indices {
		indices[i] = uint32(i % 300) // 300 ids, ~17 rows each; id 299 last block
	}
	indices = append(indices, 300, 300) // id 300 appears twice; id 301+ absent
	dictLen := 400                      // ids 302..399 empty
	p := buildPostings(indices, dictLen)
	v7 := marshalV7(p)
	v8 := p.marshal(nil)

	if v7[0] == 0xFF && v7[1] == 0xFF && v7[2] == 0xFF && v7[3] == 0xFF {
		t.Fatal("v7 blob first word collided with postV8Magic")
	}
	for id := 0; id < dictLen; id++ {
		if c7, c8 := postCount(v7, id), postCount(v8, id); c7 != c8 {
			t.Fatalf("id %d: v7 count %d != v8 count %d", id, c7, c8)
		}
		r7, r8 := postingRows(v7, id), postingRows(v8, id)
		if len(r7) != len(r8) {
			t.Fatalf("id %d: v7 %d rows != v8 %d rows", id, len(r7), len(r8))
		}
		for k := range r7 {
			if r7[k] != r8[k] {
				t.Fatalf("id %d row %d: v7 %d != v8 %d", id, k, r7[k], r8[k])
			}
		}
	}
}
