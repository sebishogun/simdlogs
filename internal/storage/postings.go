package storage

import "encoding/binary"

// Postings is a per-dict-column inverted index: for each dictionary id,
// the sorted row ids holding it. It turns an equality query from "decode
// every row's index and compare" into "look up this value's rows" -- the
// difference the head-to-head showed load-bearing on selective queries,
// and the plan's decision #3 (postings once selective queries dominate).
//
// Layout: an offset table of dictLen+1 uint32 prefix sums, then the row
// ids of every id's list in id order, each list delta-varint-encoded so a
// rare value costs a handful of bytes. Lookup is O(1) to the list via the
// offsets, then O(list) to decode -- for a needle, a couple of rows.
type postings struct {
	offsets []uint32 // dictLen+1 prefix sums into the ids stream (row counts)
	data    []byte   // per-id delta-varint row id lists, concatenated
}

// buildPostings inverts per-row indices into per-id row lists.
func buildPostings(indices []uint32, dictLen int) postings {
	counts := make([]uint32, dictLen)
	for _, id := range indices {
		counts[id]++
	}
	offsets := make([]uint32, dictLen+1)
	for i := 0; i < dictLen; i++ {
		offsets[i+1] = offsets[i] + counts[i]
	}
	// Bucket row ids by id, in row order (so each list is ascending).
	lists := make([][]uint32, dictLen)
	for i := range lists {
		lists[i] = make([]uint32, 0, counts[i])
	}
	for row, id := range indices {
		lists[id] = append(lists[id], uint32(row))
	}
	var data []byte
	for _, l := range lists {
		var prev uint32
		for _, row := range l {
			data = binary.AppendUvarint(data, uint64(row-prev))
			prev = row
		}
	}
	return postings{offsets: offsets, data: data}
}

// marshal appends the postings blob: dictLen+1 offsets, then data.
func (p postings) marshal(b []byte) []byte {
	b = appU32(b, uint32(len(p.offsets)))
	for _, o := range p.offsets {
		b = appU32(b, o)
	}
	b = appU32(b, uint32(len(p.data)))
	return append(b, p.data...)
}

// rowsFor decodes id's row list from a marshaled postings blob at off.
// The offset table gives the id's slice of the ids stream directly; only
// that slice is decoded -- a rare value never touches another id's rows.
func postingRows(blob []byte, id int) []uint32 {
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no {
		return nil
	}
	offBase := 4
	start := binary.LittleEndian.Uint32(blob[offBase+id*4:])
	end := binary.LittleEndian.Uint32(blob[offBase+(id+1)*4:])
	dataOff := offBase + no*4
	dataLen := int(binary.LittleEndian.Uint32(blob[dataOff:]))
	data := blob[dataOff+4 : dataOff+4+dataLen]
	// Walk varints, skipping the lists before id, then decode id's list.
	pos := 0
	var prev uint32
	skip := int(start)
	for i := 0; i < skip; i++ {
		_, n := binary.Uvarint(data[pos:])
		pos += n
	}
	out := make([]uint32, 0, end-start)
	prev = 0
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
