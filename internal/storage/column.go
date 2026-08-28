// Package storage is the immutable row-group core: the columnar format
// every other layer reads. A group is 64-128K rows; per column it holds a
// dictionary of distinct values plus bit-packed indices (low cardinality)
// or delta+varint (timestamps). The decode paths run on the simd kernels
// -- BitUnpackInto for indices, VarintDecode for timestamps -- with the
// scalar loops kept as the conformance oracle the differential compares.
package storage

import (
	"encoding/binary"
	"math"
	"math/bits"
	"sort"

	"github.com/sebishogun/simd"
)

// ColumnType tags a column's encoding.
type ColumnType uint8

const (
	ColDict      ColumnType = iota // deduped value table + bit-packed indices
	ColTimestamp                   // int64, delta + varint
	ColVector                      // dense float32 embeddings: dim u32 + n*dim float32 LE
)

// encodeVectors lays out a dense embedding column: the dimension then the row
// vectors, little-endian float32. Raw (no compression) -- embeddings are
// high-entropy and read whole for k-NN.
func encodeVectors(vec []float32, dim int) []byte {
	out := make([]byte, 4+len(vec)*4)
	binary.LittleEndian.PutUint32(out, uint32(dim))
	for i, f := range vec {
		binary.LittleEndian.PutUint32(out[4+i*4:], math.Float32bits(f))
	}
	return out
}

// DictColumn is a dictionary-encoded string column: the sorted distinct
// values and one index per row into them.
type DictColumn struct {
	Dict    []string // sorted, deduped
	Indices []uint32 // one per row, into Dict
}

// BuildDict interns values into a sorted dictionary and per-row indices.
// One hash map, not two: the first pass assigns each distinct value a
// provisional (first-seen) id and records every row's, then the distinct
// set is sorted and an array remap rewrites the provisional ids to sorted
// ones. The earlier form built a dedup set and then a second id table and
// looked the id up per row -- two maps and a per-row map lookup where one
// map and an array index do, the ingest hot path's largest single cost.
func BuildDict(values []string) DictColumn {
	id := make(map[string]uint32, len(values))
	dict := make([]string, 0, len(values))
	prov := make([]uint32, len(values))
	for i, v := range values {
		x, ok := id[v]
		if !ok {
			x = uint32(len(dict))
			id[v] = x
			dict = append(dict, v)
		}
		prov[i] = x
	}
	sorted := make([]string, len(dict))
	copy(sorted, dict)
	sort.Strings(sorted)
	// remap[provisional id] -> sorted id, from the sorted order.
	remap := make([]uint32, len(dict))
	for newID, v := range sorted {
		remap[id[v]] = uint32(newID)
	}
	idx := make([]uint32, len(values))
	for i, p := range prov {
		idx[i] = remap[p]
	}
	return DictColumn{Dict: sorted, Indices: idx}
}

// bitWidth is the bits needed to hold the largest index.
func bitWidth(n int) int {
	if n <= 1 {
		return 1
	}
	return bits.Len32(uint32(n - 1))
}

// encodeIndices bit-packs indices at the dictionary's natural width, in
// whole 32-value blocks (simd.BitPackInto's contract), tail scalar. The
// width and row count travel in the column header, written by the group.
func encodeIndices(idx []uint32, width int) []byte {
	if len(idx) == 0 {
		return nil
	}
	// simd.BitPackInto packs the whole slice; size the destination to
	// ceil(n*width/32)+1 words, its documented requirement.
	words := (len(idx)*width+31)/32 + 1
	packed := make([]uint32, words)
	simd.BitPackInto(packed, idx, int32(width))
	out := make([]byte, words*4)
	for i, w := range packed {
		binary.LittleEndian.PutUint32(out[i*4:], w)
	}
	return out
}

// decodeIndices unpacks n indices of the given width via the kernel, with
// a scalar tail for the final partial 32-block (BitUnpackInto works in
// whole blocks).
func decodeIndices(b []byte, n, width int) []uint32 {
	if n == 0 {
		return nil
	}
	words := len(b) / 4
	packed := make([]uint32, words)
	for i := 0; i < words; i++ {
		packed[i] = binary.LittleEndian.Uint32(b[i*4:])
	}
	out := make([]uint32, n)
	blocks := n / 32
	if blocks > 0 {
		simd.BitUnpackInto(out[:blocks*32], packed, int32(width))
	}
	mask := uint32(1)<<uint(width) - 1
	for j := blocks * 32; j < n; j++ {
		bit := j * width
		var v uint64
		for k := 0; k < 8 && bit/8+k < len(b); k++ {
			v |= uint64(b[bit/8+k]) << (8 * k)
		}
		out[j] = uint32(v>>uint(bit%8)) & mask
	}
	return out
}

// tsBlock is the timestamp checkpoint stride: every tsBlock rows the encoder
// records the byte position in the varint stream and the running timestamp,
// so a single row's timestamp is read by seeking to its block and decoding
// at most tsBlock deltas -- O(tsBlock), not O(rows). Materializing a
// selective query's handful of matches no longer decodes the whole column.
const tsBlock = 512

// tsHdrStride is the per-block checkpoint size: byte offset (u32), the
// running timestamp before the block (i64, for the point seek), and the
// block's min and max timestamp (i64 each, for the range skip).
const tsHdrStride = 28

// encodeTimestamps delta-encodes then zig-zag varints int64 timestamps,
// prefixed with a checkpoint header. The stream itself is just deltas (row 0
// from zero, the rest from the prior row); the header records, per block,
// the byte offset of its first row, the timestamp of the row before it (the
// seek base), and the block's min and max -- so a query skips a whole block
// whose [min,max] misses the window without decoding it, and restricts its
// per-row work to the blocks the window actually spans.
//
//	header: numBlocks u32, blockSize u32, then per block { off u32, base i64, min i64, max i64 }
//	stream: zig-zag varint deltas, one per row
func encodeTimestamps(ts []int64) []byte {
	n := len(ts)
	if n == 0 {
		return nil
	}
	numBlocks := (n + tsBlock - 1) / tsBlock
	offs := make([]uint32, numBlocks)
	bases := make([]int64, numBlocks)
	mins := make([]int64, numBlocks)
	maxs := make([]int64, numBlocks)
	var stream []byte
	var prev int64
	for i, t := range ts {
		b := i / tsBlock
		if i%tsBlock == 0 {
			offs[b] = uint32(len(stream))
			bases[b] = prev // timestamp of the row before this block
			mins[b], maxs[b] = t, t
		}
		if t < mins[b] {
			mins[b] = t
		}
		if t > maxs[b] {
			maxs[b] = t
		}
		d := t
		if i > 0 {
			d = t - prev
		}
		prev = t
		stream = binary.AppendUvarint(stream, zigzag(d))
	}
	out := make([]byte, 0, 8+numBlocks*tsHdrStride+len(stream))
	out = appU32(out, uint32(numBlocks))
	out = appU32(out, uint32(tsBlock))
	for k := 0; k < numBlocks; k++ {
		out = appU32(out, offs[k])
		out = appI64(out, bases[k])
		out = appI64(out, mins[k])
		out = appI64(out, maxs[k])
	}
	return append(out, stream...)
}

// tsHeaderLen is the byte length of the checkpoint header.
func tsHeaderLen(b []byte) int { return 8 + int(get32(b, 0))*tsHdrStride }

// tsStream returns the varint stream past the checkpoint header.
func tsStream(b []byte) []byte {
	if len(b) < 8 {
		return nil
	}
	return b[tsHeaderLen(b):]
}

func geti64(b []byte) int64 { return int64(binary.LittleEndian.Uint64(b)) }

// timeWindowSpan returns the row range [lo,hi) covering every block whose
// [min,max] overlaps [from,to) -- read from the header alone, no decode. A
// query restricts its per-row predicate scan to this span, so a narrow
// window over a big group touches a fraction of the rows. Returns 0,0 when
// no block overlaps.
func timeWindowSpan(b []byte, n int, from, to int64) (int, int) {
	if len(b) < 8 {
		return 0, n
	}
	numBlocks := int(get32(b, 0))
	bs := int(get32(b, 4))
	lo, hi := n, 0
	for k := 0; k < numBlocks; k++ {
		hdr := 8 + k*tsHdrStride
		mn := geti64(b[hdr+12:])
		mx := geti64(b[hdr+20:])
		if mx < from || mn >= to {
			continue
		}
		bl := k * bs
		bh := bl + bs
		if bh > n {
			bh = n
		}
		if bl < lo {
			lo = bl
		}
		if bh > hi {
			hi = bh
		}
	}
	if lo >= hi {
		return 0, 0
	}
	return lo, hi
}

// decodeTimeRangeInto fills out[i] = from <= ts[i] < to, skipping blocks
// whose [min,max] miss the window (never decoded), setting whole blocks that
// fall entirely inside, and decoding only the boundary blocks. out is
// cleared to n and returned.
func decodeTimeRangeInto(b []byte, n int, from, to int64, out []bool) []bool {
	if cap(out) < n {
		out = make([]bool, n)
	} else {
		out = out[:n]
		for i := range out {
			out[i] = false
		}
	}
	if len(b) < 8 {
		return out
	}
	numBlocks := int(get32(b, 0))
	bs := int(get32(b, 4))
	stream := b[8+numBlocks*tsHdrStride:]
	for k := 0; k < numBlocks; k++ {
		hdr := 8 + k*tsHdrStride
		mn := geti64(b[hdr+12:])
		mx := geti64(b[hdr+20:])
		if mx < from || mn >= to {
			continue // block entirely outside the window
		}
		lo := k * bs
		hi := lo + bs
		if hi > n {
			hi = n
		}
		if mn >= from && mx < to {
			for i := lo; i < hi; i++ {
				out[i] = true // block entirely inside
			}
			continue
		}
		// Boundary block: decode it and compare per row.
		prev := geti64(b[hdr+4:])
		pos := int(get32(b, hdr))
		for i := lo; i < hi; i++ {
			if pos >= len(stream) {
				break
			}
			d, w := binary.Uvarint(stream[pos:])
			// binary.Uvarint returns a NEGATIVE count for an overlong varint
			// (more than ten continuation bytes). Adding it walked pos
			// backwards and the next slice panicked -- and this runs in the
			// tiering goroutine, which has no recover, so a checksum-valid
			// file with a corrupt varint stream killed the process. Header
			// validation cannot reach this: it is payload, not geometry.
			if w <= 0 {
				break
			}
			pos += w
			prev += unzigzag(d)
			out[i] = prev >= from && prev < to
		}
	}
	return out
}

// decodeTimestamps reverses the whole column through simd.VarintDecode,
// undoing the zig-zag and the delta.
func decodeTimestamps(b []byte, n int) []int64 {
	if n == 0 {
		return nil
	}
	raw := make([]uint64, n)
	out := make([]int64, n)
	got, _ := decodeInto(b, raw, out)
	return out[:got]
}

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// decodeInto decodes the full timestamp column reusing caller buffers raw
// (varint scratch) and out (result); the zero-allocation scan path. It runs
// the stream from the front with the running sum starting at zero, so the
// checkpoints cost nothing here -- they exist for the point read below.
func decodeInto(b []byte, raw []uint64, out []int64) (int, error) {
	got, _ := simd.VarintDecode(raw, tsStream(b))
	var prev int64
	for i := 0; i < got; i++ {
		prev += unzigzag(raw[i])
		out[i] = prev
	}
	return got, nil
}

// decodeTsRange decodes rows [lo,hi) into a slice indexed from lo, seeking to
// the block containing lo rather than the column start -- so a windowed query
// decodes only its window's span, not the whole column.
func decodeTsRange(b []byte, lo, hi int) []int64 {
	return decodeTsRangeInto(nil, b, lo, hi)
}

// decodeTsRangeInto is decodeTsRange writing into the caller's buffer. See
// Reader.TimestampsRangeInto for who may pass one.
func decodeTsRangeInto(dst []int64, b []byte, lo, hi int) []int64 {
	if hi <= lo || len(b) < 8 {
		return nil
	}
	numBlocks := int(get32(b, 0))
	bs := int(get32(b, 4))
	stream := b[8+numBlocks*tsHdrStride:]
	k := lo / bs
	hdr := 8 + k*tsHdrStride
	prev := geti64(b[hdr+4:])
	pos := int(get32(b, hdr))
	out := dst[:0]
	if cap(out) < hi-lo {
		out = make([]int64, hi-lo)
	}
	out = out[:hi-lo]
	i := k * bs
	for ; i < hi; i++ {
		if pos >= len(stream) {
			break
		}
		d, w := binary.Uvarint(stream[pos:])
		if w <= 0 { // overlong varint: see decodeTimeRangeInto
			break
		}
		pos += w
		prev += unzigzag(d)
		if i >= lo {
			out[i-lo] = prev
		}
	}
	// A stream that ends before hi leaves the tail unwritten. That was zero
	// when this allocated its own slice every time; a reused buffer would
	// instead hand back the PREVIOUS group's timestamps, which read as
	// plausible times rather than as corruption. Zeroing keeps the answer
	// exactly what a fresh allocation gave.
	if i < hi {
		j := i - lo
		if j < 0 {
			j = 0
		}
		clear(out[j:])
	}
	return out
}

// decodeTsAt returns the timestamp of one row by seeking to its checkpoint
// block and decoding at most tsBlock deltas forward -- the selective-query
// materialize path, which needs a few rows' times, not the whole column.
func decodeTsAt(b []byte, row int) int64 {
	numBlocks := int(get32(b, 0))
	bs := int(get32(b, 4))
	k := row / bs
	if k >= numBlocks {
		return 0
	}
	hdr := 8 + k*tsHdrStride
	off := int(get32(b, hdr))
	prev := int64(binary.LittleEndian.Uint64(b[hdr+4:]))
	stream := b[8+numBlocks*tsHdrStride:]
	pos := off
	for i := k * bs; i <= row; i++ {
		if pos >= len(stream) {
			break
		}
		d, n := binary.Uvarint(stream[pos:])
		if n <= 0 { // overlong varint: see decodeTimeRangeInto
			break
		}
		pos += n
		prev += unzigzag(d)
	}
	return prev
}
