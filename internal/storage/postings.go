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

	// maxCountWidth is extractCountBits' documented limit: above 25 a value
	// straddles the 8-byte window it loads.
	maxCountWidth = 25
	// maxPostingRun bounds a single id's row list. A group holds far fewer
	// rows than this; the bound exists so a corrupt count cannot turn into a
	// hundred-megabyte allocation.
	maxPostingRun = 1 << 24
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
	// Every field here is four bytes of file. clen slices, and cw is the
	// shift width extractCountBits masks with -- above 25 a value straddles
	// the 8-byte window it loads and the read is wrong rather than merely
	// out of range. The writer never emits either out of range, so rejecting
	// costs nothing and a corrupt blob decodes to "no postings" instead of
	// reaching into whatever follows it in the mapping.
	if clen < 0 || 16+clen > len(blob) || cw < 0 || cw > maxCountWidth {
		return 0, 0, nil, nil, false, false
	}
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
	if len(blob) < 4 {
		return 0
	}
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no || 4+(id+2)*4 > len(blob) {
		return 0
	}
	return int(binary.LittleEndian.Uint32(blob[4+(id+1)*4:]) - binary.LittleEndian.Uint32(blob[4+id*4:]))
}

// postingRows decodes id's row list, touching only the one block that holds it.
//
// maxRow is the group's row count. A valid list is strictly ascending and
// every entry is a row of this group, so the decoders stop at the first entry
// that is not -- a corrupt blob yields a short list rather than a row id that
// indexes past every other column when the caller materializes it.
func postingRows(blob []byte, id, maxRow int) []uint32 {
	if dictLen, cw, cs, blocks, forData, ok := postV8Header(blob); ok {
		if id < 0 || id >= dictLen {
			return nil
		}
		count := extractCountBits(cs, id, cw)
		// A row list can never be longer than the group, and maxRow is the
		// group's row count -- it was in the signature and unused here, so a
		// 56-byte blob still reserved 64 MB.
		if count <= 0 || count > maxPostingRun || (maxRow > 0 && count > maxRow) {
			return nil
		}
		if forData {
			return decodeForBlock(blocks, cs, cw, id, count, maxRow)
		}
		return decodePostingBlock(blocks, id, count, dictLen, maxRow)
	}
	if len(blob) < 4 {
		return nil
	}
	no := int(binary.LittleEndian.Uint32(blob))
	if id < 0 || id+1 >= no || 4+(id+2)*4 > len(blob) || 4+no*4 > len(blob) {
		return nil
	}
	// The same clamp the v8 branch above takes. This one is a uint32
	// subtraction of two file offsets, so a corrupt pair produces a huge
	// positive count and decodePostingBlock reserves it: an 872-byte v7
	// blob allocated 16 GiB. v7 groups carry no checksum at all
	// (group_read.go's version check), so nothing upstream catches it.
	count := int(binary.LittleEndian.Uint32(blob[4+(id+1)*4:]) - binary.LittleEndian.Uint32(blob[4+id*4:]))
	if count <= 0 || count > maxPostingRun || (maxRow > 0 && count > maxRow) {
		return nil
	}
	return decodePostingBlock(blob[4+no*4:], id, count, no-1, maxRow)
}

// decodeForBlock decodes id's `count` rows from the FOR block section. It finds
// id's block, sums the count table across the block's earlier ids to get id's
// value offset within the block (no offset table on disk), then decodes the run.
func decodeForBlock(blocks, countStream []byte, cw, id, count, maxRow int) []uint32 {
	// Nothing below this line is trusted: numBlocks, bs, packedLen, byteOff
	// and w are each four bytes read straight out of the mapped file.
	// Unguarded, 96 of 7306 positions panicked when twelve-byte corruptions
	// were swept across a real postings span at stride 1 with fills 0x00 and
	// 0xff; guarded, none do.
	if len(blocks) < 8 {
		return nil
	}
	numBlocks := int(binary.LittleEndian.Uint32(blocks))
	bs := int(binary.LittleEndian.Uint32(blocks[4:]))
	if numBlocks <= 0 || bs <= 0 { // bs == 0 divides by zero below
		return nil
	}
	idxBase := 8
	packedLenPos := idxBase + numBlocks*8
	if packedLenPos < idxBase || packedLenPos+4 > len(blocks) {
		return nil
	}
	packedLen := int(binary.LittleEndian.Uint32(blocks[packedLenPos:]))
	if packedLen < 0 || packedLenPos+4+packedLen > len(blocks) {
		return nil
	}
	packed := blocks[packedLenPos+4 : packedLenPos+4+packedLen]
	k := id / bs
	if k >= numBlocks {
		return nil
	}
	e := idxBase + k*8
	byteOff := int(binary.LittleEndian.Uint32(blocks[e:]))
	w := int(binary.LittleEndian.Uint32(blocks[e+4:]))
	if w < 0 || w > 32 {
		return nil
	}
	end := packedLen
	if k+1 < numBlocks {
		end = int(binary.LittleEndian.Uint32(blocks[idxBase+(k+1)*8:]))
	}
	if byteOff < 0 || end < byteOff || end > packedLen {
		return nil
	}
	blk := packed[byteOff:end]
	// id's run starts at the sum of counts of the earlier ids in its block (at
	// most postBlock-1 O(1) reads, since no offset table is stored on disk).
	lo := 0
	for x := k * bs; x < id; x++ {
		lo += extractCountBits(countStream, x, cw)
	}
	return decodeForRun(blk, w, lo, count, maxRow)
}

// decodeForRun prefix-sums cnt d-gaps starting at value lo in a FOR block. A
// short run reads each value O(1) scalar (no buffer); a long run unpacks the
// aligned 32-blocks covering it with the SIMD kernel (the block is padded to a
// 32-multiple, so there is no partial-block tail to read scalar).
func decodeForRun(blk []byte, w, lo, cnt, maxRow int) []uint32 {
	// Guard before the allocation, not after: cnt comes from the file, and
	// reserving it first is the whole cost the guard exists to avoid.
	if w <= 0 || w > 32 || lo < 0 || cnt < 0 {
		return nil
	}
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
		// A short block with a large lo/cnt reaches past words; the writer
		// pads every block to a 32-value multiple, so a run that does not fit
		// is a corrupt header, not a tail case.
		if startBlk*w > len(words) || (startBlk+nBlk)*w > len(words) {
			return nil
		}
		scratch := make([]uint32, nBlk*32)
		simd.BitUnpackInto(scratch, words[startBlk*w:], int32(w))
		for i := startVal; i < startVal+cnt; i++ {
			acc += scratch[i]
			if !inGroup(acc, out, maxRow) {
				break
			}
			out = append(out, acc)
		}
		return out
	}
	for i := lo; i < lo+cnt; i++ {
		acc += uint32(extractCountBits(blk, i, w))
		if !inGroup(acc, out, maxRow) {
			break
		}
		out = append(out, acc)
	}
	return out
}

// decodePostingBlock decodes id's `count` rows from a legacy LZ4 block section
// (v7, and the superseded v8-LZ4): find id's block, decompress it, slice id's
// varint list via the block's intra-offsets, and delta-decode.
func decodePostingBlock(blocks []byte, id, count, dictLen, maxRow int) []uint32 {
	// Same untrusted-header treatment as decodeForBlock: every length here is
	// four bytes of file, and bs == 0 divides by zero.
	if len(blocks) < 8 {
		return nil
	}
	numBlocks := int(binary.LittleEndian.Uint32(blocks))
	bs := int(binary.LittleEndian.Uint32(blocks[4:]))
	if numBlocks <= 0 || bs <= 0 {
		return nil
	}
	idxBase := 8
	compLenPos := idxBase + numBlocks*12
	if compLenPos < idxBase || compLenPos+4 > len(blocks) {
		return nil
	}
	compTotal := int(binary.LittleEndian.Uint32(blocks[compLenPos:]))
	if compTotal < 0 || compLenPos+4+compTotal > len(blocks) {
		return nil
	}
	comp := blocks[compLenPos+4 : compLenPos+4+compTotal]
	k := id / bs
	if k >= numBlocks {
		return nil
	}
	e := idxBase + k*12
	compOff := int(binary.LittleEndian.Uint32(blocks[e:]))
	compLen := int(binary.LittleEndian.Uint32(blocks[e+4:]))
	rawLen := int(binary.LittleEndian.Uint32(blocks[e+8:]))
	if compOff < 0 || compLen < 0 || compOff+compLen > len(comp) {
		return nil
	}
	raw := lz4Decompress(comp[compOff:compOff+compLen], rawLen)
	lo := k * bs
	hi := lo + bs
	if hi > dictLen {
		hi = dictLen
	}
	cnt := hi - lo
	j := id - lo
	// Every length below comes from the file, and unchecked a corrupt offset
	// table slices past the block. (One message from the sweep,
	// "slice bounds out of range [:4294967311]", comes from postV8Header's
	// blob[16:16+clen] rather than from here -- 4294967311 is 16 + 0xFFFFFFFF.
	// The most common single message was "integer divide by zero", 12 of the
	// 96, from the unguarded bs == 0.) Postings decode is query-path
	// only, so recoverPanic turns these into 500s rather than process death;
	// a 500 per corrupt query is still the file deciding what the server
	// does.
	if j < 0 || 4*(j+1)+4 > len(raw) {
		return nil
	}
	off0 := binary.LittleEndian.Uint32(raw[4*j:])
	off1 := binary.LittleEndian.Uint32(raw[4*(j+1):])
	if off1 < off0 {
		return nil
	}
	listBase := 4 * (cnt + 1)
	lo0, hi0 := listBase+int(off0), listBase+int(off1)
	if cnt < 0 || lo0 < listBase || hi0 < lo0 || hi0 > len(raw) {
		return nil
	}
	data := raw[lo0:hi0]
	out := make([]uint32, 0, count)
	var prev uint32
	pos := 0
	for i := 0; i < count; i++ {
		if pos >= len(data) {
			break
		}
		d, n := binary.Uvarint(data[pos:])
		// A negative count means an overlong varint; adding it walks pos
		// backwards and the next slice panics.
		if n <= 0 {
			break
		}
		pos += n
		if i == 0 {
			prev = uint32(d)
		} else {
			prev += uint32(d)
		}
		if !inGroup(prev, out, maxRow) {
			break
		}
		out = append(out, prev)
	}
	return out
}

// inGroup reports whether row is a legal next entry of the posting list built
// so far: inside the group, and strictly after the previous entry, since a
// d-gap of zero cannot occur after the first row.
//
// The gaps are uint32 sums of file bytes, so a corrupt gap wraps rather than
// producing something obviously large; the ascending test is what catches the
// wrap, and the row bound catches the rest.
func inGroup(row uint32, out []uint32, maxRow int) bool {
	if maxRow > 0 && int64(row) >= int64(maxRow) {
		return false
	}
	return len(out) == 0 || row > out[len(out)-1]
}
