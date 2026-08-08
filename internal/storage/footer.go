package storage

import "github.com/sebishogun/simd"

// Reader is a parsed group: skip metadata from the footer, column data
// decoded on demand. The skip methods answer from the footer alone,
// without touching column data -- the whole basis of the speed claim.
type Reader struct {
	blob    []byte
	Rows    int
	TimeMin int64
	TimeMax int64
	cols    []colMeta
}

// TimeRangeMatches reports whether the group's time span overlaps
// [from, to]; to is exclusive, matching LogsQL and ES range semantics.
func (r *Reader) TimeRangeMatches(from, to int64) bool {
	return r.TimeMin < to && r.TimeMax >= from
}

// ColumnExists reports whether a column of that name is present.
func (r *Reader) ColumnExists(name string) bool {
	return r.col(name) != nil
}

// DictContains reports whether value MAY be in the named dict column: the
// bloom answers no exactly, yes maybe, and the exact dict scan behind it
// settles the maybe. A false skips the whole group without decoding it.
func (r *Reader) DictContains(name, value string) bool {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return false
	}
	if !bloomMaybe(m.Bloom, value) {
		return false
	}
	for _, d := range m.DictData {
		if d == value {
			return true
		}
	}
	return false
}

func (r *Reader) col(name string) *colMeta {
	for i := range r.cols {
		if r.cols[i].Name == name {
			return &r.cols[i]
		}
	}
	return nil
}

// Timestamps decodes the timestamp column named, reusing the buffers.
func (r *Reader) Timestamps(name string, raw []uint64, out []int64) []int64 {
	m := r.col(name)
	if m == nil || m.Type != ColTimestamp {
		return nil
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	if cap(raw) < r.Rows {
		raw = make([]uint64, r.Rows)
	}
	if cap(out) < r.Rows {
		out = make([]int64, r.Rows)
	}
	n, _ := decodeInto(data, raw[:r.Rows], out[:r.Rows])
	return out[:n]
}

// DictIndices decodes the per-row dictionary indices of a dict column,
// and returns them with the dict table for value lookup.
func (r *Reader) DictIndices(name string) ([]uint32, []string) {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return nil, nil
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	return decodeIndices(data, r.Rows, m.Width), m.DictData
}

// DictID returns the index of value in the named column's dictionary, or
// -1 -- the equality filter's fast path: one comparison per row against
// this id over the decoded indices, no string compares.
func (r *Reader) DictID(name, value string) int {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return -1
	}
	for i, d := range m.DictData {
		if d == value {
			return i
		}
	}
	return -1
}

// ---- dict-value bloom, on the simd hash ----

const bloomWords = 32 // 2048 bits per column; saturates like the reference

func buildDictBloom(dict []string) []uint64 {
	bloom := make([]uint64, bloomWords)
	keys := make([]uint64, len(dict))
	hashStrings(dict, keys)
	for _, h := range keys {
		set(bloom, h)
	}
	return bloom
}

func bloomMaybe(bloom []uint64, value string) bool {
	if len(bloom) == 0 {
		return true
	}
	h := hashOne(value)
	return test(bloom, h)
}

func set(bloom []uint64, h uint64) {
	for _, b := range twoBits(h, len(bloom)) {
		bloom[b>>6] |= 1 << (b & 63)
	}
}
func test(bloom []uint64, h uint64) bool {
	for _, b := range twoBits(h, len(bloom)) {
		if bloom[b>>6]&(1<<(b&63)) == 0 {
			return false
		}
	}
	return true
}

// twoBits derives two bit positions from one 64-bit hash (the standard
// double-hashing bloom), bounded to the bitset.
func twoBits(h uint64, words int) [2]int {
	nbits := words * 64
	h1 := int(h % uint64(nbits))
	h2 := int((h >> 32) % uint64(nbits))
	return [2]int{h1, h2}
}

// hashStrings hashes a batch through simd.HashUint64 over a folded key;
// hashOne is the single-value form. Both use the same fold so a value
// hashed alone matches its batch hash -- the bloom's build and probe must
// agree.
func hashStrings(ss []string, out []uint64) {
	keys := make([]uint64, len(ss))
	for i, s := range ss {
		keys[i] = fold(s)
	}
	simd.HashUint64(out, keys, bloomSeed)
}
func hashOne(s string) uint64 {
	var o [1]uint64
	simd.HashUint64(o[:], []uint64{fold(s)}, bloomSeed)
	return o[0]
}

const bloomSeed = 0x9e3779b97f4a7c15

// fold reduces a string to a 64-bit key (FNV-1a) before the simd hash
// mixes it -- the kernel hashes u64 keys, so strings fold first.
func fold(s string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}
