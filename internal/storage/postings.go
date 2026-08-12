package storage

import (
	"encoding/binary"

	"github.com/sebishogun/simd"
)

// The count table is bit-packed with the SIMD kernel (encodeIndices), whose
// layout is plain linear LSB-first -- so any count reads O(1) via
// extractCountBits (one 8-byte load, shift, mask), whether EqualityCount reads
// one or ValueCounts sweeps all. Packing with the kernel keeps the layout
// identical to the dict-id index; TestCountLayoutEquiv pins pack against read.

// extractCountBits reads value index from a linear LSB-first bit stream in
// O(1): one 8-byte little-endian load at the byte holding the value's first
// bit, shifted past the sub-byte offset and masked to width -- the same trick
// DictValueAt uses. width <= 25 is required so the value never straddles the
// 8-byte window (count and d-gap widths are <= bitWidth(rows) <= 17).
func extractCountBits(b []byte, index, width int) int {
	if width == 0 {
		return 0
	}
	bit := index * width
	var v uint64
	for k := 0; k < 8 && bit/8+k < len(b); k++ {
		v |= uint64(b[bit/8+k]) << (8 * k)
	}
	return int(uint32(v>>uint(bit%8)) & (uint32(1)<<uint(width) - 1))
}

// A v8 postings blob is self-describing via its first word, a sentinel the v7
// layout (first word = dictLen+1, always < 2^31) can never produce. Both v8
// variants share the same bit-packed count table header; they differ only in
// the data block section, so postCount/ValueCounts read either. Readers dispatch
// on the sentinel and old v7 blobs still decode -- no group-version plumbing.
const (
	postV8Magic    = 0xFFFFFFFF // count table + LZ4 varint data blocks (superseded)
	postV8ForMagic = 0xFFFFFFFE // count table + FOR bit-packed data blocks (default)
)

// Postings is a per-dict-column inverted index: for each dictionary id,
// the sorted row ids holding it. It turns an equality query from "decode
// every row's index and compare" into "look up this value's rows" -- the
// difference the head-to-head showed load-bearing on selective queries,
// and the plan's decision #3 (postings once selective queries dominate).
//
// On disk (v8, FOR): a bit-packed per-value count table, then the per-id row
// lists as frame-of-reference bit-packed d-gaps, in blocks of postBlock ids.
// The count table answers EqualityCount/ValueCounts from the footer and also
// locates each id's run inside its block (a prefix sum of counts) -- so no
// intra-block offset table is stored. postingRows reads only the one id's run:
// a scalar O(1) bit read per value for a selective list, an aligned SIMD unpack
// for a large one. See marshal for the layout.
//
// rowOffsets and dgaps are build-time only. rowOffsets is the prefix sum
// marshal differences into the on-disk counts and also the run index into
// dgaps (id i's d-gaps are dgaps[rowOffsets[i]:rowOffsets[i+1]]). Neither is
// persisted -- v8 stores counts, not offsets, so the fat uncompressed prefix-sum
// table (the biggest raw chunk of a near-unique group) is gone from disk.
type postings struct {
	rowOffsets []uint32 // build-time only: dictLen+1 prefix sums (also the dgaps run index)
	dgaps      []uint32 // build-time only: per-id d-gaps concatenated, id i's run at rowOffsets[i]
}

// buildPostings inverts per-row indices into per-id ascending row lists and
// d-gaps them in place: id i's run lives at rowOffsets[i], and walking rows in
// order keeps each run ascending, so the gap is always non-negative.
func buildPostings(indices []uint32, dictLen int) postings {
	counts := make([]uint32, dictLen)
	for _, id := range indices {
		counts[id]++
	}
	rowOffsets := make([]uint32, dictLen+1)
	for i := 0; i < dictLen; i++ {
		rowOffsets[i+1] = rowOffsets[i] + counts[i]
	}
	dgaps := make([]uint32, len(indices))
	last := make([]uint32, dictLen) // last row seen per id (0 => first gap is absolute)
	pos := make([]uint32, dictLen)  // next write index per id
	copy(pos, rowOffsets[:dictLen])
	for row, id := range indices {
		dgaps[pos[id]] = uint32(row) - last[id]
		last[id] = uint32(row)
		pos[id]++
	}
	return postings{rowOffsets: rowOffsets, dgaps: dgaps}
}

// postBlock is how many ids share one postings block.
const postBlock = 64

// forBulkThreshold is the run length above which an aligned SIMD unpack of id's
// run beats a per-value scalar bit read. Below it the scalar read wins (no
// buffer, no byte->word copy); above it the kernel's throughput dominates.
const forBulkThreshold = 256

// marshal appends the v8 FOR postings blob: a bit-packed per-value count table
// (self-describing via the postV8ForMagic sentinel, so v7/v8-LZ4 blobs still
// read), then the per-id row lists as FOR bit-packed d-gaps in blocks of
// postBlock ids. Each block is packed at its own max d-gap width and padded to a
// 32-value multiple, so a sub-range aligned SIMD unpack needs no scalar tail. No
// intra-block offset table: id's run is located by summing the count table
// within the block.
//
//	postV8ForMagic u32, dictLen u32, countWidth u32, countBytes u32, packedCounts
//	numBlocks u32, blockSize u32
//	blockIndex[numBlocks]{byteOff u32, width u32}
//	packedLen u32, packed FOR blocks
func (p postings) marshal(b []byte) []byte {
	dictLen := len(p.rowOffsets) - 1
	// Per-value counts (rowOffsets deltas), bit-packed. Every consumer needs only
	// count[id] = rowOffsets[id+1]-rowOffsets[id]; the absolute offset is never
	// read, so the prefix-sum table was pure waste -- on near-unique columns the
	// identity permutation stored at 4 bytes each.
	counts := make([]uint32, dictLen)
	var maxC uint32
	for i := 0; i < dictLen; i++ {
		counts[i] = p.rowOffsets[i+1] - p.rowOffsets[i]
		if counts[i] > maxC {
			maxC = counts[i]
		}
	}
	cw := bitWidth(int(maxC) + 1)
	packedCounts := encodeIndices(counts, cw)
	b = appU32(b, postV8ForMagic)
	b = appU32(b, uint32(dictLen))
	b = appU32(b, uint32(cw))
	b = appU32(b, uint32(len(packedCounts)))
	b = append(b, packedCounts...)

	numBlocks := 0
	if dictLen > 0 {
		numBlocks = (dictLen + postBlock - 1) / postBlock
	}
	type fidx struct{ byteOff, width uint32 }
	idx := make([]fidx, numBlocks)
	var packed []byte
	for k := 0; k < numBlocks; k++ {
		lo := k * postBlock
		hi := lo + postBlock
		if hi > dictLen {
			hi = dictLen
		}
		dg := p.dgaps[p.rowOffsets[lo]:p.rowOffsets[hi]]
		w := forBlockWidth(dg)
		idx[k] = fidx{uint32(len(packed)), uint32(w)}
		packed = appendForBlock(packed, dg, w)
	}
	b = appU32(b, uint32(numBlocks))
	b = appU32(b, uint32(postBlock))
	for _, e := range idx {
		b = appU32(b, e.byteOff)
		b = appU32(b, e.width)
	}
	b = appU32(b, uint32(len(packed)))
	return append(b, packed...)
}

// forBlockWidth is the bit width that holds the block's largest d-gap.
func forBlockWidth(dg []uint32) int {
	var max uint32
	for _, d := range dg {
		if d > max {
			max = d
		}
	}
	return bitWidth(int(max) + 1)
}

// appendForBlock bit-packs dg at width w via the SIMD-kernel layout, padded to a
// 32-value multiple so no partial final block exists and a sub-range aligned
// unpack round-trips with no scalar tail. Pad waste is <= 31 values * w bits.
func appendForBlock(dst []byte, dg []uint32, w int) []byte {
	if len(dg) == 0 {
		return dst
	}
	padded := dg
	if r := len(dg) % 32; r != 0 {
		padded = make([]uint32, len(dg)+(32-r))
		copy(padded, dg)
	}
	return append(dst, encodeIndices(padded, w)...)
}

// postV8Header parses the shared count-table header of either v8 variant and
// reports which data-block form follows. ok is false for a legacy v7 blob.
func postV8Header(blob []byte) (dictLen, cw int, countStream, blocks []byte, forData, ok bool) {
	if len(blob) < 16 {
		return 0, 0, nil, nil, false, false
	}
	w := binary.LittleEndian.Uint32(blob)
	if w != postV8Magic && w != postV8ForMagic {
		return 0, 0, nil, nil, false, false
	}
	dictLen = int(binary.LittleEndian.Uint32(blob[4:]))
	cw = int(binary.LittleEndian.Uint32(blob[8:]))
	clen := int(binary.LittleEndian.Uint32(blob[12:]))
	return dictLen, cw, blob[16 : 16+clen], blob[16+clen:], w == postV8ForMagic, true
}

// postCount returns id's row count, from the bit-packed count table (v8) or the
// rowOffsets prefix-sum table (legacy v7).
func postCount(blob []byte, id int) int {
	if dictLen, cw, cs, _, _, ok := postV8Header(blob); ok {
		if id < 0 || id >= dictLen {
			return 0
		}
		return extractCountBits(cs, id, cw)
	}
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no {
		return 0
	}
	return int(binary.LittleEndian.Uint32(blob[4+(id+1)*4:]) - binary.LittleEndian.Uint32(blob[4+id*4:]))
}

// postingRows decodes id's row list, touching only the one block that holds it.
func postingRows(blob []byte, id int) []uint32 {
	if dictLen, cw, cs, blocks, forData, ok := postV8Header(blob); ok {
		if id < 0 || id >= dictLen {
			return nil
		}
		count := extractCountBits(cs, id, cw)
		if count == 0 {
			return nil
		}
		if forData {
			return decodeForBlock(blocks, cs, cw, id, count)
		}
		return decodePostingBlock(blocks, id, count, dictLen)
	}
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no {
		return nil
	}
	count := int(binary.LittleEndian.Uint32(blob[4+(id+1)*4:]) - binary.LittleEndian.Uint32(blob[4+id*4:]))
	if count == 0 {
		return nil
	}
	return decodePostingBlock(blob[4+no*4:], id, count, no-1)
}

// decodeForBlock decodes id's `count` rows from the FOR block section. It finds
// id's block, sums the count table across the block's earlier ids to get id's
// value offset within the block (no offset table on disk), then decodes the run.
func decodeForBlock(blocks, countStream []byte, cw, id, count int) []uint32 {
	numBlocks := int(binary.LittleEndian.Uint32(blocks))
	bs := int(binary.LittleEndian.Uint32(blocks[4:]))
	idxBase := 8
	packedLenPos := idxBase + numBlocks*8
	packedLen := int(binary.LittleEndian.Uint32(blocks[packedLenPos:]))
	packed := blocks[packedLenPos+4 : packedLenPos+4+packedLen]
	k := id / bs
	e := idxBase + k*8
	byteOff := int(binary.LittleEndian.Uint32(blocks[e:]))
	w := int(binary.LittleEndian.Uint32(blocks[e+4:]))
	end := packedLen
	if k+1 < numBlocks {
		end = int(binary.LittleEndian.Uint32(blocks[idxBase+(k+1)*8:]))
	}
	blk := packed[byteOff:end]
	// id's run starts at the sum of counts of the earlier ids in its block (at
	// most postBlock-1 O(1) reads, since no offset table is stored on disk).
	lo := 0
	for x := k * bs; x < id; x++ {
		lo += extractCountBits(countStream, x, cw)
	}
	return decodeForRun(blk, w, lo, count)
}

// decodeForRun prefix-sums cnt d-gaps starting at value lo in a FOR block. A
// short run reads each value O(1) scalar (no buffer); a long run unpacks the
// aligned 32-blocks covering it with the SIMD kernel (the block is padded to a
// 32-multiple, so there is no partial-block tail to read scalar).
func decodeForRun(blk []byte, w, lo, cnt int) []uint32 {
	out := make([]uint32, 0, cnt)
	var acc uint32
	if cnt >= forBulkThreshold {
		words := make([]uint32, len(blk)/4)
		for i := range words {
			words[i] = binary.LittleEndian.Uint32(blk[i*4:])
		}
		startBlk := lo / 32
		startVal := lo % 32
		nBlk := (startVal + cnt + 31) / 32
		scratch := make([]uint32, nBlk*32)
		simd.BitUnpackInto(scratch, words[startBlk*w:], int32(w))
		for i := startVal; i < startVal+cnt; i++ {
			acc += scratch[i]
			out = append(out, acc)
		}
		return out
	}
	for i := lo; i < lo+cnt; i++ {
		acc += uint32(extractCountBits(blk, i, w))
		out = append(out, acc)
	}
	return out
}

// decodePostingBlock decodes id's `count` rows from a legacy LZ4 block section
// (v7, and the superseded v8-LZ4): find id's block, decompress it, slice id's
// varint list via the block's intra-offsets, and delta-decode.
func decodePostingBlock(blocks []byte, id, count, dictLen int) []uint32 {
	numBlocks := int(binary.LittleEndian.Uint32(blocks))
	bs := int(binary.LittleEndian.Uint32(blocks[4:]))
	idxBase := 8
	compLenPos := idxBase + numBlocks*12
	compTotal := int(binary.LittleEndian.Uint32(blocks[compLenPos:]))
	comp := blocks[compLenPos+4 : compLenPos+4+compTotal]
	k := id / bs
	e := idxBase + k*12
	compOff := int(binary.LittleEndian.Uint32(blocks[e:]))
	compLen := int(binary.LittleEndian.Uint32(blocks[e+4:]))
	rawLen := int(binary.LittleEndian.Uint32(blocks[e+8:]))
	raw := lz4Decompress(comp[compOff:compOff+compLen], rawLen)
	lo := k * bs
	hi := lo + bs
	if hi > dictLen {
		hi = dictLen
	}
	cnt := hi - lo
	j := id - lo
	off0 := binary.LittleEndian.Uint32(raw[4*j:])
	off1 := binary.LittleEndian.Uint32(raw[4*(j+1):])
	listBase := 4 * (cnt + 1)
	data := raw[listBase+int(off0) : listBase+int(off1)]
	out := make([]uint32, 0, count)
	var prev uint32
	pos := 0
	for i := 0; i < count; i++ {
		d, n := binary.Uvarint(data[pos:])
		pos += n
		if i == 0 {
			prev = uint32(d)
		} else {
			prev += uint32(d)
		}
		out = append(out, prev)
	}
	return out
}
