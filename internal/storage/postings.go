package storage

import "encoding/binary"

// Postings is a per-dict-column inverted index: for each dictionary id,
// the sorted row ids holding it. It turns an equality query from "decode
// every row's index and compare" into "look up this value's rows" -- the
// difference the head-to-head showed load-bearing on selective queries,
// and the plan's decision #3 (postings once selective queries dominate).
//
// Layout: a row-count offset table of dictLen+1 uint32 prefix sums, then a
// byte offset table of dictLen+1 uint32 positions into the varint stream,
// then the row ids of every id's list in id order, each list
// delta-varint-encoded so a rare value costs a handful of bytes.
//
// Two tables, not one, because the varints are variable-length: the
// row-count prefix sum gives a value's row count (EqualityCount, a footer
// subtraction, no data touched), and the byte offset seeks straight to its
// list. Lookup is genuinely O(1) to the bytes then O(list) to decode -- an
// earlier single-table form had to walk every preceding varint to find the
// list, which made a rare value in a high-cardinality dict O(dict), the
// exact skip-walk that lost the needle head-to-head.
type postings struct {
	rowOffsets  []uint32 // dictLen+1 prefix sums of row counts
	byteOffsets []uint32 // dictLen+1 byte positions of each id's list
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

// marshal appends the postings blob: dictLen+1 row offsets, dictLen+1 byte
// offsets, then the varint data.
func (p postings) marshal(b []byte) []byte {
	b = appU32(b, uint32(len(p.rowOffsets)))
	for _, o := range p.rowOffsets {
		b = appU32(b, o)
	}
	for _, o := range p.byteOffsets {
		b = appU32(b, o)
	}
	b = appU32(b, uint32(len(p.data)))
	return append(b, p.data...)
}

// postingRows decodes id's row list from a marshaled postings blob. The
// byte offset table seeks straight to id's varints -- no preceding list is
// touched -- and the row-count table bounds the decode. Both are O(1); the
// decode is O(list).
func postingRows(blob []byte, id int) []uint32 {
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no {
		return nil
	}
	offBase := 4
	start := binary.LittleEndian.Uint32(blob[offBase+id*4:])
	end := binary.LittleEndian.Uint32(blob[offBase+(id+1)*4:])
	byteBase := offBase + no*4
	pos := int(binary.LittleEndian.Uint32(blob[byteBase+id*4:]))
	dataOff := byteBase + no*4
	dataLen := int(binary.LittleEndian.Uint32(blob[dataOff:]))
	data := blob[dataOff+4 : dataOff+4+dataLen]
	out := make([]uint32, 0, end-start)
	var prev uint32
	for i := start; i < end; i++ {
		d, n := binary.Uvarint(data[pos:])
		pos += n
		if i == start {
			prev = uint32(d)
		} else {
			prev += uint32(d)
		}
		out = append(out, prev)
	}
	return out
}
