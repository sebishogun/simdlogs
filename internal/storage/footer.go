package storage

import (
	"sort"

	"github.com/sebishogun/simd"
)

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
	return dictLookup(m.DictData, value) >= 0
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

// DictValueAt returns the value of a dict column at one row, decoding
// only that row's index bits -- O(1), so materializing a handful of
// matched rows never decodes the whole column. The selective path's
// materialize step.
func (r *Reader) DictValueAt(name string, row int) (string, bool) {
	m := r.col(name)
	if m == nil || m.Type != ColDict || row < 0 || row >= r.Rows {
		return "", false
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	w := m.Width
	bit := row * w
	var v uint64
	for k := 0; k < 8 && bit/8+k < len(data); k++ {
		v |= uint64(data[bit/8+k]) << (8 * k)
	}
	id := uint32(v>>uint(bit%8)) & (uint32(1)<<uint(w) - 1)
	if int(id) >= len(m.DictData) {
		return "", false
	}
	return m.DictData[id], true
}

// DictID returns the index of value in the named column's dictionary, or
// -1 -- the equality filter's fast path: one comparison per row against
// this id over the decoded indices, no string compares.
func (r *Reader) DictID(name, value string) int {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return -1
	}
	return dictLookup(m.DictData, value)
}

// dictLookup binary-searches a sorted dictionary for value, returning its
// id or -1. BuildDict sorts the dict, so this is O(log n) where the naive
// scan was O(n) -- the fix that made the selective needle competitive,
// since a high-cardinality column's dict is as large as the row count.
func dictLookup(dict []string, value string) int {
	i := sort.Search(len(dict), func(i int) bool { return dict[i] >= value })
	if i < len(dict) && dict[i] == value {
		return i
	}
	return -1
}

// EqualityCount returns a dict value's id and its row count, read from
// the posting offset table alone (a difference of two prefix sums) -- so
// the query planner can choose the posting path for a rare value or the
// vectorized scan for a common one without decoding anything.
func (r *Reader) EqualityCount(name, value string) (id, count int, ok bool) {
	m := r.col(name)
	if m == nil || m.Type != ColDict || m.PostLen == 0 {
		return 0, 0, false
	}
	if !bloomMaybe(m.Bloom, value) {
		return -1, 0, true
	}
	id = dictLookup(m.DictData, value)
	if id < 0 {
		return -1, 0, true
	}
	blob := r.blob[m.PostOff : m.PostOff+m.PostLen]
	start := le32(blob[4+id*4:])
	end := le32(blob[4+(id+1)*4:])
	return id, int(end - start), true
}

// EqualityRows returns the row ids where the named dict column equals
// value, read from the posting index without decoding the column's
// per-row indices at all -- the selective-query path. ok is false if the
// value is absent (empty rows) but the lookup was valid.
func (r *Reader) EqualityRows(name, value string) (rows []uint32, has bool) {
	m := r.col(name)
	if m == nil || m.Type != ColDict || m.PostLen == 0 {
		return nil, false
	}
	if !bloomMaybe(m.Bloom, value) {
		return nil, true // provably absent
	}
	id := dictLookup(m.DictData, value)
	if id < 0 {
		return nil, true
	}
	return postingRows(r.blob[m.PostOff:m.PostOff+m.PostLen], id), true
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

// ColumnNames returns the group's column names, footer-only.
func (r *Reader) ColumnNames() []string {
	out := make([]string, len(r.cols))
	for i := range r.cols {
		out[i] = r.cols[i].Name
	}
	return out
}

// ValueCount is a dict value and its row count in this group.
type ValueCount struct {
	Value string
	Count int
}

// ValueCounts returns per-value row counts for a dict column, from the
// posting offset table alone -- each value's count is the difference of
// two prefix sums, so no per-row index or row list is decoded.
func (r *Reader) ValueCounts(name string) []ValueCount {
	m := r.col(name)
	if m == nil || m.Type != ColDict || m.PostLen == 0 {
		return nil
	}
	blob := r.blob[m.PostOff : m.PostOff+m.PostLen]
	no := int(le32(blob))
	out := make([]ValueCount, 0, len(m.DictData))
	for id := 0; id+1 < no && id < len(m.DictData); id++ {
		start := le32(blob[4+id*4:])
		end := le32(blob[4+(id+1)*4:])
		out = append(out, ValueCount{Value: m.DictData[id], Count: int(end - start)})
	}
	return out
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
