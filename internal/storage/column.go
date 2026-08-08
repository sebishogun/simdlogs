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
func BuildDict(values []string) DictColumn {
	seen := map[string]struct{}{}
	for _, v := range values {
		seen[v] = struct{}{}
	}
	dict := make([]string, 0, len(seen))
	for v := range seen {
		dict = append(dict, v)
	}
	sort.Strings(dict)
	id := make(map[string]uint32, len(dict))
	for i, v := range dict {
		id[v] = uint32(i)
	}
	idx := make([]uint32, len(values))
	for i, v := range values {
		idx[i] = id[v]
	}
	return DictColumn{Dict: dict, Indices: idx}
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

// encodeTimestamps delta-encodes then zig-zag varints int64 timestamps.
// The first value is stored raw; deltas are usually small and positive.
func encodeTimestamps(ts []int64) []byte {
	var out []byte
	var prev int64
	for i, t := range ts {
		d := t
		if i > 0 {
			d = t - prev
		}
		prev = t
		out = binary.AppendUvarint(out, zigzag(d))
	}
	return out
}

// decodeTimestamps reverses it through simd.VarintDecode, undoing the
// zig-zag and the delta.
func decodeTimestamps(b []byte, n int) []int64 {
	if n == 0 {
		return nil
	}
	raw := make([]uint64, n)
	got, _ := simd.VarintDecode(raw, b)
	out := make([]int64, got)
	var prev int64
	for i := 0; i < got; i++ {
		d := unzigzag(raw[i])
		if i == 0 {
			prev = d
		} else {
			prev += d
		}
		out[i] = prev
	}
	return out
}

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// decodeInto decodes timestamps reusing caller buffers raw (varint
// scratch) and out (result); the zero-allocation query path.
func decodeInto(b []byte, raw []uint64, out []int64) (int, error) {
	got, _ := simd.VarintDecode(raw, b)
	var prev int64
	for i := 0; i < got; i++ {
		d := unzigzag(raw[i])
		if i == 0 {
			prev = d
		} else {
			prev += d
		}
		out[i] = prev
	}
	return got, nil
}
