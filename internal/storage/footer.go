package storage

import (
	"sync/atomic"

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

	// Decoded per-row dictionary indices, one slot per column. A group's blob is
	// immutable once sealed and the query side only reads these, so the decode is
	// pure derived data -- re-running it per query was ~20% of the profile on a
	// repeated query (and alerting rules / logrules re-run on a timer). Bounded by
	// the package budget so it cannot grow into the whole dataset.
	//
	// A slot per column read through an atomic pointer, NOT a mutex+map: the
	// lookup is on the hot path of every selective query, and the RWMutex plus map
	// hash cost more than the decode saved on small queries (needle, stats).
	idxCache []atomic.Pointer[[]uint32]

	// How many rows hold the EMPTY value for each column, memoized: -1 means not
	// yet asked. field_names asks this per column per group, and the answer is a
	// dictionary lookup whose probes each decompress a dict block -- repeated on
	// every request against an immutable group. int32 because a group's row
	// count is bounded well below that.
	emptyCache []atomic.Int32
}

// EmptyValueCount reports how many rows of the column hold the empty value,
// i.e. how many did not set the field. Memoized per column: the group is
// immutable, so the answer cannot change.
func (r *Reader) EmptyValueCount(name string) int {
	ci := r.colIndex(name)
	if ci < 0 || ci >= len(r.emptyCache) {
		_, n, _ := r.EqualityCount(name, "")
		return n
	}
	if v := r.emptyCache[ci].Load(); v >= 0 {
		return int(v)
	}
	_, n, _ := r.EqualityCount(name, "")
	r.emptyCache[ci].Store(int32(n))
	return n
}

// indexCacheBudget bounds the total bytes of decoded indices cached across all
// readers; indexCacheUsed tracks what is held. 0 disables caching.
var (
	indexCacheBudget atomic.Int64
	indexCacheUsed   atomic.Int64
)

func init() { indexCacheBudget.Store(256 << 20) }

// SetIndexCacheBudget sets the decoded-index cache budget in bytes (0 disables
// the cache). Indices are 4 bytes per row per cached column.
func SetIndexCacheBudget(n int64) { indexCacheBudget.Store(n) }

// IndexCacheUsed reports the bytes currently held by the decoded-index cache.
func IndexCacheUsed() int64 { return indexCacheUsed.Load() }

// cachedIdx returns column ci's decoded indices, or nil. One atomic load, no
// lock and no map hash -- this is on the hot path of every selective query.
func (r *Reader) cachedIdx(ci int) []uint32 {
	if ci < 0 || ci >= len(r.idxCache) {
		return nil
	}
	if p := r.idxCache[ci].Load(); p != nil {
		return *p
	}
	return nil
}

func (r *Reader) cacheIdx(ci int, idx []uint32) {
	if ci < 0 || ci >= len(r.idxCache) {
		return
	}
	size := int64(len(idx)) * 4
	if size == 0 || indexCacheUsed.Load()+size > indexCacheBudget.Load() {
		return // over budget: serve this query, cache nothing
	}
	if r.idxCache[ci].CompareAndSwap(nil, &idx) {
		indexCacheUsed.Add(size) // only the winner of the race accounts for it
	}
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
	return dictSectionSearch(r.dictSec(m), m.DictLen, value) >= 0
}

func (r *Reader) col(name string) *colMeta {
	for i := range r.cols {
		if r.cols[i].Name == name {
			return &r.cols[i]
		}
	}
	return nil
}

// dictSec returns the column's random-access dict section, a view into the
// mmap'd blob -- no decode, no copy.
func (r *Reader) dictSec(m *colMeta) []byte {
	return r.blob[m.DictOff : m.DictOff+m.DictLen2]
}

// idxBytes is the byte length of a dict column's bit-packed indices, computed
// from the row count and width so a read slices exactly the indices and not
// the postings/dict that follow them in the blob.
func (r *Reader) idxBytes(m *colMeta) int {
	if r.Rows == 0 {
		return 0
	}
	words := (r.Rows*m.Width+31)/32 + 1
	return words * 4
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

// TimestampAt returns one row's timestamp, decoding only its checkpoint
// block -- O(tsBlock), so materializing a selective query's matches never
// decodes the whole timestamp column.
func (r *Reader) TimestampAt(name string, row int) (int64, bool) {
	m := r.col(name)
	if m == nil || m.Type != ColTimestamp || row < 0 || row >= r.Rows {
		return 0, false
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	return decodeTsAt(data, row), true
}

// TimeRangeMaskInto fills out with out[i] = from <= ts[i] < to for the named
// timestamp column, skipping blocks whose [min,max] miss the window and
// decoding only the boundary blocks -- the windowed-query time filter that
// avoids a whole-column decode.
func (r *Reader) TimeRangeMaskInto(name string, from, to int64, out []bool) []bool {
	m := r.col(name)
	if m == nil || m.Type != ColTimestamp {
		return out[:0]
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	return decodeTimeRangeInto(data, r.Rows, from, to, out)
}

// TimeWindowSpan returns the row range [lo,hi) covering the blocks that
// overlap [from,to), from the checkpoint header alone. A windowed query
// restricts its predicate scan to this span.
func (r *Reader) TimeWindowSpan(name string, from, to int64) (int, int) {
	m := r.col(name)
	if m == nil || m.Type != ColTimestamp {
		return 0, 0
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	return timeWindowSpan(data, r.Rows, from, to)
}

// TimestampsRange decodes rows [lo,hi) of the named timestamp column into a
// slice indexed from lo -- the windowed aggregation/materialize path, which
// needs only the window span's times, not the whole column.
func (r *Reader) TimestampsRange(name string, lo, hi int) []int64 {
	m := r.col(name)
	if m == nil || m.Type != ColTimestamp {
		return nil
	}
	data := r.blob[m.DataOff : m.DataOff+m.DataLen]
	return decodeTsRange(data, lo, hi)
}

// DictIndices decodes the per-row dictionary indices of a dict column,
// and returns them with the dict table for value lookup.
func (r *Reader) DictIndices(name string) ([]uint32, []string) {
	ci := r.colIndex(name) // one column scan, not one here plus one in the Raw call
	if ci < 0 || r.cols[ci].Type != ColDict {
		return nil, nil
	}
	m := &r.cols[ci]
	return r.dictIndicesAt(ci), dictSectionAll(r.dictSec(m), m.DictLen)
}

// DictLen is the number of distinct values in a dict column.
func (r *Reader) DictLen(name string) int {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return 0
	}
	return m.DictLen
}

// DictIndicesRaw decodes only the per-row dictionary indices of a dict column,
// not the dict values -- for the materialize path that needs a subset of values
// (DictDecodeSome), avoiding a full-dict string decode of unreferenced values.
func (r *Reader) DictIndicesRaw(name string) []uint32 {
	ci := r.colIndex(name)
	if ci < 0 || r.cols[ci].Type != ColDict {
		return nil
	}
	return r.dictIndicesAt(ci)
}

// dictIndicesAt is the cached decode for an already-resolved column index.
func (r *Reader) dictIndicesAt(ci int) []uint32 {
	if v := r.cachedIdx(ci); v != nil {
		return v // decoded on an earlier query; the blob is immutable
	}
	m := &r.cols[ci]
	data := r.blob[m.DataOff : m.DataOff+r.idxBytes(m)]
	idx := decodeIndices(data, r.Rows, m.Width)
	r.cacheIdx(ci, idx)
	return idx
}

// colIndex returns the index of the named column, or -1.
func (r *Reader) colIndex(name string) int {
	for i := range r.cols {
		if r.cols[i].Name == name {
			return i
		}
	}
	return -1
}

// DictDecodeSome decodes only the dict values whose id is marked in want,
// returning a DictLen-sized slice with just those filled. Blocks holding no
// wanted value are not decompressed. This is the big-result materialize path:
// a whole-record select over a fraction of a high-cardinality column decodes
// only the referenced values, not every distinct string.
func (r *Reader) DictDecodeSome(name string, want []bool) []string {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return nil
	}
	return dictSectionSome(r.dictSec(m), m.DictLen, want)
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
	if int(id) >= m.DictLen {
		return "", false
	}
	return dictSectionAt(r.dictSec(m), m.DictLen, int(id)), true
}

// DictID returns the index of value in the named column's dictionary, or
// -1 -- the equality filter's fast path: one comparison per row against
// this id over the decoded indices, no string compares.
func (r *Reader) DictID(name, value string) int {
	m := r.col(name)
	if m == nil || m.Type != ColDict {
		return -1
	}
	return dictSectionSearch(r.dictSec(m), m.DictLen, value)
}

// EqualityCount returns a dict value's id and its row count, read from
// the posting count table alone (bit-packed in v8, a prefix-sum difference
// in v7) -- so the query planner can choose the posting path for a rare
// value or the vectorized scan for a common one without decoding anything.
func (r *Reader) EqualityCount(name, value string) (id, count int, ok bool) {
	m := r.col(name)
	if m == nil || m.Type != ColDict || m.PostLen == 0 {
		return 0, 0, false
	}
	if !bloomMaybe(m.Bloom, value) {
		return -1, 0, true
	}
	id = dictSectionSearch(r.dictSec(m), m.DictLen, value)
	if id < 0 {
		return -1, 0, true
	}
	blob := r.blob[m.PostOff : m.PostOff+m.PostLen]
	return id, postCount(blob, id), true
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
	id := dictSectionSearch(r.dictSec(m), m.DictLen, value)
	if id < 0 {
		return nil, true
	}
	return postingRows(r.blob[m.PostOff:m.PostOff+m.PostLen], id), true
}

// ---- dict-value bloom, on the simd hash ----

// The bloom is sized to the column's cardinality (~bloomBits per value, k
// hashes) so it does not saturate on a high-cardinality column -- a fixed
// 2048-bit bloom filled with 128K values is always "maybe", which at scale
// forces a dict block-decompress on every group. A sized bloom rejects
// almost every non-matching group from the in-RAM footer, so a needle over
// tens of thousands of groups never touches their dict data.
const (
	bloomBits     = 10 // bits per value -> ~1% false-positive rate at k=7
	bloomK        = 7
	bloomMinWords = 4
)

func bloomWordsFor(n int) int {
	if n < 1 {
		return bloomMinWords
	}
	words := (n*bloomBits + 63) / 64
	if words < bloomMinWords {
		words = bloomMinWords
	}
	return words
}

func buildDictBloom(dict []string) []uint64 {
	bloom := make([]uint64, bloomWordsFor(len(dict)))
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
	nbits := uint64(len(bloom) * 64)
	h1, h2 := h, (h>>32)|1 // enhanced double hashing; h2 odd
	for i := 0; i < bloomK; i++ {
		b := h1 % nbits
		bloom[b>>6] |= 1 << (b & 63)
		h1 += h2
	}
}
func test(bloom []uint64, h uint64) bool {
	nbits := uint64(len(bloom) * 64)
	h1, h2 := h, (h>>32)|1
	for i := 0; i < bloomK; i++ {
		b := h1 % nbits
		if bloom[b>>6]&(1<<(b&63)) == 0 {
			return false
		}
		h1 += h2
	}
	return true
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

// ColumnFootprint is one column's on-disk byte breakdown by section.
type ColumnFootprint struct {
	Name                                  string
	Index, Postings, Dict, Bloom, TimeCol int
}

// ColumnBytes returns the per-column, per-section byte breakdown -- the
// footprint profile, to target the biggest chunk before compressing.
func (r *Reader) ColumnBytes() []ColumnFootprint {
	out := make([]ColumnFootprint, len(r.cols))
	for i := range r.cols {
		m := &r.cols[i]
		cf := ColumnFootprint{Name: m.Name, Bloom: len(m.Bloom) * 8}
		if m.Type == ColTimestamp {
			cf.TimeCol = m.DataLen
		} else {
			cf.Index = m.PostOff - m.DataOff // indices precede postings
			cf.Postings = m.PostLen
			cf.Dict = m.DictLen2
		}
		out[i] = cf
	}
	return out
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
// posting count table alone (bit-packed in v8, prefix-sum differences in
// v7) -- so no per-row index or row list is decoded.
func (r *Reader) ValueCounts(name string) []ValueCount {
	m := r.col(name)
	if m == nil || m.Type != ColDict || m.PostLen == 0 {
		return nil
	}
	blob := r.blob[m.PostOff : m.PostOff+m.PostLen]
	out := make([]ValueCount, 0, m.DictLen)
	// The whole dictionary in one pass: dictSectionAt decompresses the block a
	// value lives in on EVERY call, so asking it for each id in turn re-inflated
	// each block once per value it holds. That was 47% of a `top N by (host)`.
	vals := dictSectionAll(r.dictSec(m), m.DictLen)
	// Each count is then an O(1) read from the bit-packed table.
	if dictLen, cw, cs, _, _, ok := postV8Header(blob); ok {
		n := m.DictLen
		if dictLen < n {
			n = dictLen
		}
		if n > len(vals) {
			n = len(vals)
		}
		for id := 0; id < n; id++ {
			out = append(out, ValueCount{Value: vals[id], Count: extractCountBits(cs, id, cw)})
		}
		return out
	}
	no := int(le32(blob))
	for id := 0; id+1 < no && id < m.DictLen && id < len(vals); id++ {
		start := le32(blob[4+id*4:])
		end := le32(blob[4+(id+1)*4:])
		out = append(out, ValueCount{Value: vals[id], Count: int(end - start)})
	}
	return out
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
