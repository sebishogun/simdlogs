package storage

import "encoding/binary"

// Postings is a per-dict-column inverted index: for each dictionary id,
// the sorted row ids holding it. It turns an equality query from "decode
// every row's index and compare" into "look up this value's rows" -- the
// difference the head-to-head showed load-bearing on selective queries,
// and the plan's decision #3 (postings once selective queries dominate).
//
// On disk: the row-count offset table (dictLen+1 uint32 prefix sums,
// uncompressed) then the per-id delta-varint row lists block-compressed in
// groups of postBlock ids, each block carrying its own intra-block offsets.
// The row-count table answers EqualityCount/ValueCounts as a footer
// subtraction (no decompress); postingRows decompresses the one block that
// holds an id. See marshal for the exact layout.
//
// The struct's byteOffsets is build-time only -- marshal uses it to slice the
// per-id lists into blocks and it is not persisted, so the ~4-byte-per-id
// seek table (the largest raw chunk of a high-cardinality group) is gone
// from disk. An earlier form stored it uncompressed; block offsets replace
// it at no on-disk cost.
type postings struct {
	rowOffsets  []uint32 // dictLen+1 prefix sums of row counts
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

// marshal appends the postings blob: the row-count offset table
// (uncompressed, so EqualityCount/ValueCounts stay footer-cheap), then the
// per-id varint row lists block-compressed in groups of postBlock ids. The
// old byte-offset table is gone -- each block carries its own intra-block
// offsets inside the compressed bytes, so the ~4-byte-per-id seek table (the
// biggest raw chunk of a high-cardinality group) costs nothing on disk.
//
//	no u32, rowOffsets[no] u32
//	numBlocks u32, blockSize u32
//	blockIndex[numBlocks]{compOff u32, compLen u32, rawLen u32}
//	compLen u32, compressed blocks (each raw = [cnt+1 intra-offsets][list bytes])
func (p postings) marshal(b []byte) []byte {
	no := len(p.rowOffsets)
	dictLen := no - 1
	b = appU32(b, uint32(no))
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

// postingRows decodes id's row list, decompressing the one block that holds
// it. The row-count table bounds the decode (count = rowOffsets[id+1]-[id]);
// the block's intra-offsets locate id's list within the decompressed bytes.
func postingRows(blob []byte, id int) []uint32 {
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no {
		return nil
	}
	start := binary.LittleEndian.Uint32(blob[4+id*4:])
	end := binary.LittleEndian.Uint32(blob[4+(id+1)*4:])
	count := int(end - start)
	if count == 0 {
		return nil
	}
	p := 4 + no*4
	numBlocks := int(binary.LittleEndian.Uint32(blob[p:]))
	bs := int(binary.LittleEndian.Uint32(blob[p+4:]))
	idxBase := p + 8
	compLenPos := idxBase + numBlocks*12
	compTotal := int(binary.LittleEndian.Uint32(blob[compLenPos:]))
	comp := blob[compLenPos+4 : compLenPos+4+compTotal]
	k := id / bs
	e := idxBase + k*12
	compOff := int(binary.LittleEndian.Uint32(blob[e:]))
	compLen := int(binary.LittleEndian.Uint32(blob[e+4:]))
	rawLen := int(binary.LittleEndian.Uint32(blob[e+8:]))
	raw := lz4Decompress(comp[compOff:compOff+compLen], rawLen)
	lo := k * bs
	hi := lo + bs
	if hi > no-1 {
		hi = no - 1
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
