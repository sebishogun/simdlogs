package storage

import "encoding/binary"

// The count table is bit-packed with the SIMD kernel (encodeIndices), whose
// layout is plain linear LSB-first -- so any count reads O(1) via
// extractCountBits (one 8-byte load, shift, mask), whether EqualityCount reads
// one or ValueCounts sweeps all. Packing with the kernel keeps the layout
// identical to the dict-id index; TestCountLayoutEquiv pins pack against read.

// extractCountBits reads value index from a linear LSB-first bit stream in
// O(1): one 8-byte little-endian load at the byte holding the value's first
// bit, shifted past the sub-byte offset and masked to width -- the same trick
// DictValueAt uses. width <= 25 is required so the value never straddles the
// 8-byte window (count widths are <= bitWidth(rows) <= 17).
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

// postV8Magic is the first word of a v8 postings blob (a value the v7 layout --
// whose first word is dictLen+1, always < 2^31 -- can never produce), so the
// blob is self-describing: readers dispatch on it, old v7 blobs still read, and
// no group-version plumbing is needed. v8 replaces the uncompressed rowOffsets
// prefix-sum table with bit-packed per-value counts (the only thing any
// consumer reads from it), ~30x smaller on near-unique columns.
const postV8Magic = 0xFFFFFFFF

// Postings is a per-dict-column inverted index: for each dictionary id,
// the sorted row ids holding it. It turns an equality query from "decode
// every row's index and compare" into "look up this value's rows" -- the
// difference the head-to-head showed load-bearing on selective queries,
// and the plan's decision #3 (postings once selective queries dominate).
//
// On disk (v8): a bit-packed per-value count table then the per-id
// delta-varint row lists block-compressed in groups of postBlock ids, each
// block carrying its own intra-block offsets. The count table answers
// EqualityCount/ValueCounts from the footer (no decompress); postingRows
// decompresses the one block that holds an id. See marshal for the layout.
//
// The struct's rowOffsets/byteOffsets are build-time only. rowOffsets is the
// prefix sum marshal differences into the on-disk counts; byteOffsets slices
// the lists into blocks. Neither is persisted -- v8 stores counts, not
// offsets, so the fat uncompressed prefix-sum table (the biggest raw chunk of
// a near-unique group) is gone from disk, and each block's own intra-offsets
// replace the per-id byte seek table at no on-disk cost.
type postings struct {
	rowOffsets  []uint32 // build-time only: dictLen+1 prefix sums, differenced to on-disk counts
	byteOffsets []uint32 // build-time only: byte positions used to block the data
	data        []byte   // per-id delta-varint row id lists, concatenated
}

// buildPostings inverts per-row indices into per-id row lists.
func buildPostings(indices []uint32, dictLen int) postings {
	counts := make([]uint32, dictLen)
	for _, id := range indices {
		counts[id]++
	}
	rowOffsets := make([]uint32, dictLen+1)
	for i := 0; i < dictLen; i++ {
		rowOffsets[i+1] = rowOffsets[i] + counts[i]
	}
	// Bucket row ids by id, in row order (so each list is ascending).
	lists := make([][]uint32, dictLen)
	for i := range lists {
		lists[i] = make([]uint32, 0, counts[i])
	}
	for row, id := range indices {
		lists[id] = append(lists[id], uint32(row))
	}
	byteOffsets := make([]uint32, dictLen+1)
	var data []byte
	for i, l := range lists {
		var prev uint32
		for _, row := range l {
			data = binary.AppendUvarint(data, uint64(row-prev))
			prev = row
		}
		byteOffsets[i+1] = uint32(len(data))
	}
	return postings{rowOffsets: rowOffsets, byteOffsets: byteOffsets, data: data}
}

// postBlock is how many ids share one compressed postings block.
const postBlock = 64

// marshal appends the v8 postings blob: a bit-packed per-value count table
// (self-describing via the postV8Magic sentinel, so old v7 blobs still read),
// then the per-id varint row lists block-compressed in groups of postBlock ids.
// Each block carries its own intra-block offsets inside the compressed bytes, so
// the ~4-byte-per-id seek table costs nothing on disk.
//
//	postV8Magic u32, dictLen u32, countWidth u32, countBytes u32, packedCounts
//	numBlocks u32, blockSize u32
//	blockIndex[numBlocks]{compOff u32, compLen u32, rawLen u32}
//	compLen u32, compressed blocks (each raw = [cnt+1 intra-offsets][list bytes])
func (p postings) marshal(b []byte) []byte {
	no := len(p.rowOffsets)
	dictLen := no - 1
	// Per-value counts (rowOffsets deltas), bit-packed. Every consumer only ever
	// needs count[id] = rowOffsets[id+1]-rowOffsets[id]; the absolute offset is
	// never read (postingRows locates data via the block's own intra-offsets), so
	// the prefix-sum table was pure waste -- on near-unique columns the identity
	// permutation stored at 4 bytes each.
	counts := make([]uint32, dictLen)
	var maxC uint32
	for i := 0; i < dictLen; i++ {
		counts[i] = p.rowOffsets[i+1] - p.rowOffsets[i]
		if counts[i] > maxC {
			maxC = counts[i]
		}
	}
	cw := bitWidth(int(maxC) + 1)
	packed := encodeIndices(counts, cw) // SIMD-kernel layout: bulk-decodable, word-padded
	b = appU32(b, postV8Magic)
	b = appU32(b, uint32(dictLen))
	b = appU32(b, uint32(cw))
	b = appU32(b, uint32(len(packed)))
	b = append(b, packed...)
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
		dataLo := p.byteOffsets[lo]
		raw := make([]byte, 0, 4*(cnt+1)+int(p.byteOffsets[hi]-dataLo))
		for j := lo; j <= hi; j++ {
			raw = appU32(raw, p.byteOffsets[j]-dataLo) // intra-block offsets
		}
		raw = append(raw, p.data[dataLo:p.byteOffsets[hi]]...)
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

// postV8Header returns (dictLen, countWidth, countStream, blocksSection) for a
// v8 postings blob, or ok=false for a legacy v7 blob.
func postV8Header(blob []byte) (dictLen, cw int, countStream, blocks []byte, ok bool) {
	if len(blob) < 16 || binary.LittleEndian.Uint32(blob) != postV8Magic {
		return 0, 0, nil, nil, false
	}
	dictLen = int(binary.LittleEndian.Uint32(blob[4:]))
	cw = int(binary.LittleEndian.Uint32(blob[8:]))
	clen := int(binary.LittleEndian.Uint32(blob[12:]))
	return dictLen, cw, blob[16 : 16+clen], blob[16+clen:], true
}

// postCount returns id's row count, from the bit-packed count table (v8) or the
// rowOffsets prefix-sum table (legacy v7).
func postCount(blob []byte, id int) int {
	if dictLen, cw, cs, _, ok := postV8Header(blob); ok {
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

// postingRows decodes id's row list, decompressing the one block that holds it.
// The count bounds the varint decode; the block's intra-offsets locate id's list.
func postingRows(blob []byte, id int) []uint32 {
	if dictLen, cw, cs, blocks, ok := postV8Header(blob); ok {
		if id < 0 || id >= dictLen {
			return nil
		}
		count := extractCountBits(cs, id, cw)
		if count == 0 {
			return nil
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

// decodePostingBlock decodes id's `count` rows from the block section (identical
// in v7 and v8): find id's block, decompress it, slice id's varint list via the
// block's intra-offsets, and delta-decode.
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
