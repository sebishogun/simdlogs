// Package storage is the immutable row-group core: the columnar format
// every other layer reads. A group is 64-128K rows; per column it holds a
// dictionary of distinct values plus bit-packed indices (low cardinality)
// or delta+varint (timestamps). The decode paths run on the simd kernels
// -- BitUnpackInto for indices, VarintDecode for timestamps -- with the
// scalar loops kept as the conformance oracle the differential compares.
package storage

import (
	"encoding/binary"
	"math/bits"
	"sort"

	"github.com/sebishogun/simd"
)

// ColumnType tags a column's encoding.
type ColumnType uint8

const (
	ColDict      ColumnType = iota // deduped value table + bit-packed indices
	ColTimestamp                   // int64, delta + varint
)

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

// encodeTimestamps delta-encodes then zig-zag varints int64 timestamps,
// prefixed with a checkpoint header. The stream itself is unchanged (row 0
// as a delta from zero, the rest deltas); the header adds, per block, the
// byte offset of the block's first row and the absolute timestamp of the
// row before it, the two things a seek needs.
//
//	header: numBlocks u32, blockSize u32, then per block { off u32, base i64 }
//	stream: zig-zag varint deltas, one per row
func encodeTimestamps(ts []int64) []byte {
	n := len(ts)
	if n == 0 {
		return nil
	}
	numBlocks := (n + tsBlock - 1) / tsBlock
	offs := make([]uint32, numBlocks)
	bases := make([]int64, numBlocks)
	var stream []byte
	var prev int64
	for i, t := range ts {
		if i%tsBlock == 0 {
			offs[i/tsBlock] = uint32(len(stream))
			bases[i/tsBlock] = prev // timestamp of the row before this block
		}
		d := t
		if i > 0 {
			d = t - prev
		}
		prev = t
		stream = binary.AppendUvarint(stream, zigzag(d))
	}
	out := make([]byte, 0, 8+numBlocks*12+len(stream))
	out = appU32(out, uint32(numBlocks))
	out = appU32(out, uint32(tsBlock))
	for k := 0; k < numBlocks; k++ {
		out = appU32(out, offs[k])
		out = appI64(out, bases[k])
	}
	return append(out, stream...)
}

// tsStream returns the varint stream past the checkpoint header.
func tsStream(b []byte) []byte {
	if len(b) < 8 {
		return nil
	}
	numBlocks := int(get32(b, 0))
	return b[8+numBlocks*12:]
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
	hdr := 8 + k*12
	off := int(get32(b, hdr))
	prev := int64(binary.LittleEndian.Uint64(b[hdr+4:]))
	stream := b[8+numBlocks*12:]
	pos := off
	for i := k * bs; i <= row; i++ {
		d, n := binary.Uvarint(stream[pos:])
		pos += n
		prev += unzigzag(d)
	}
	return prev
}
