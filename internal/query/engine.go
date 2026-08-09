package query

import (
	"regexp"
	"unsafe"

	"github.com/sebishogun/simd"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Query is a selective log query: a time window and a conjunction of
// field predicates. It is the subset the planner and LogsQL parser feed;
// AND across predicates, with equality and substring the two the storage
// footer can skip on.
type Query struct {
	From, To int64
	Preds    []Pred
	Limit    int
}

// PredKind selects the comparison.
type PredKind uint8

const (
	Eq       PredKind = iota // field := value  (dict-id equality)
	Contains                 // field ~ substr  (substring, bloom-skippable)
	Regexp                   // field ~ /re/    (RE2 on survivors only)
)

// Pred is one field predicate.
type Pred struct {
	Field string
	Kind  PredKind
	Value string
	re    *regexp.Regexp
}

// Field is one decoded key/value of a matched row.
type Field struct {
	Key   string
	Value string
}

// Row is a materialized result: the decoded field values of a match. Fields
// is an ordered slice, not a map -- a query returns a handful of fields per
// row and one small slice per row is far cheaper than a map allocation and
// its hashing, which the profile showed dominating the selective query.
type Row struct {
	Time   int64
	Fields []Field
}

// Store is the read surface the engine needs; storage.Store satisfies it.
type Store interface {
	Groups(from, to int64) []*storage.Reader
}

// Run executes q over the store and returns matching rows in time order,
// up to Limit. Groups outside the time window are skipped by the store;
// groups a predicate proves cannot match are skipped by the footer bloom;
// only survivors are decoded and scanned. This layered skip is where the
// orders of magnitude over a whole-block scan come from.
func Run(s Store, q *Query) []Row {
	groups := s.Groups(q.From, q.To)
	// Footer-prune first, then decide whether to fan out. A selective query
	// (a rare value) survives in one or two groups; spawning a worker pool
	// for that costs more than it saves, and it was the largest single cost
	// in the needle profile. Pruning is a cheap bloom + dict binary search,
	// no column decoded, so it is always worth doing before the fork.
	survivors := groups[:0]
	for _, g := range groups {
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	if len(survivors) >= parallelMinGroups && q.Limit == 0 {
		return runParallel(survivors, q)
	}
	var out []Row
	for _, g := range survivors {
		out = appendMatches(out, g, q)
		if q.Limit > 0 && len(out) >= q.Limit {
			return out[:q.Limit]
		}
	}
	return out
}

// groupCanMatch rejects a group whose footer proves a required equality
// value absent -- the bloom + dict scan, no row decode.
func groupCanMatch(g *storage.Reader, q *Query) bool {
	for i := range q.Preds {
		p := &q.Preds[i]
		if p.Kind == Eq && g.ColumnExists(p.Field) && !g.DictContains(p.Field, p.Value) {
			return false
		}
	}
	return true
}

// appendMatches decodes the survivor group ONCE per column, builds the
// match bitset with the vectorized predicates, and materializes only the
// selected rows. The earlier version decoded a column inside the per-row
// loop -- O(matches x column) -- which lost the head-to-head; each column
// is now decoded exactly once and indexed into.
func appendMatches(out []Row, g *storage.Reader, q *Query) []Row {
	n := g.Rows
	sel := NewBitset(n)
	sel.SetAll()

	// Time predicate. Skip it entirely when the whole group is inside the
	// window; otherwise the block-aware mask skips blocks whose [min,max]
	// miss the window and decodes only the boundary blocks -- never the whole
	// column, and never the per-row scalar loop the row path once ran.
	windowed := g.TimeMin < q.From || g.TimeMax >= q.To
	if windowed {
		mask := g.TimeRangeMaskInto("_time", q.From, q.To, nil)
		tb := NewBitset(n)
		packBools(tb, mask)
		sel.And(tb)
	}

	// Decode each predicate field's indices+dict once, reused for both the
	// filter and the materialize. Equality goes through eqPredBitset, which
	// takes the posting path for a rare value and decodes no column at all.
	type col struct {
		idx  []uint32
		dict []string
	}
	cols := make(map[string]col, len(q.Preds))
	for i := range q.Preds {
		p := &q.Preds[i]
		if p.Kind == Eq {
			sel.And(eqPredBitset(g, p, n))
			continue
		}
		idx, dict := g.DictIndices(p.Field)
		cols[p.Field] = col{idx: idx, dict: dict}
		sel.And(predBitsetCol(g, p, idx, dict, n))
	}

	cnt := sel.Count()
	if cnt == 0 {
		return out // no match in this group: never decode its timestamps
	}
	// Timestamps for the Time field. Restrict the decode to the window's
	// block span; when matches are sparse enough that the span decode would
	// cost more than point reads (the needle), read each match's time from
	// its checkpoint block instead.
	lo, hi := 0, n
	if windowed {
		lo, hi = g.TimeWindowSpan("_time", q.From, q.To)
		if lo >= hi {
			lo, hi = 0, n
		}
	}
	var ts []int64
	pointRead := cnt*tsBlockGuess < hi-lo
	if !pointRead {
		ts = g.TimestampsRange("_time", lo, hi)
	}
	sel.ForEach(func(i int) {
		var t int64
		if pointRead {
			t, _ = g.TimestampAt("_time", i)
		} else {
			t = ts[i-lo]
		}
		row := Row{Time: t, Fields: make([]Field, 0, len(q.Preds))}
		for _, p := range q.Preds {
			// Prefer a decoded column if we already have one; otherwise
			// (the posting path skipped the full decode) fetch just this
			// row's value -- O(1), keeping the selective query lazy.
			if c := cols[p.Field]; c.idx != nil {
				row.Fields = append(row.Fields, Field{p.Field, c.dict[c.idx[i]]})
			} else if v, ok := g.DictValueAt(p.Field, i); ok {
				row.Fields = append(row.Fields, Field{p.Field, v})
			}
		}
		out = append(out, row)
	})
	return out
}

// tsBlockGuess approximates the timestamp checkpoint stride for the
// materialize crossover: point-read when the matches are sparse relative to
// the span, span-decode otherwise. It need not equal the storage stride
// exactly -- it only picks the cheaper path.
const tsBlockGuess = 512

// predBitsetCol is predBitset over already-decoded indices/dict.
func predBitsetCol(g *storage.Reader, p *Pred, idx []uint32, dict []string, n int) *Bitset {
	b := NewBitset(n)
	if idx == nil {
		return b
	}
	switch p.Kind {
	case Eq:
		id := g.DictID(p.Field, p.Value)
		if id < 0 {
			return b
		}
		// Vectorized residual scan: one vpcmpeqd per lane over the encoded
		// indices (EqualScalarInto), then pack the bool mask to bits
		// (MaskBits) -- no per-row compare, no per-row Set. The design's
		// Task 3.3, replacing VictoriaLogs' scalar filter_exact pattern.
		eqMaskInto(b, idx, uint32(id))
	case Contains:
		hit := make([]bool, len(dict))
		for di, d := range dict {
			hit[di] = containsSubstr(d, p.Value)
		}
		for i, v := range idx {
			if hit[v] {
				b.Set(i)
			}
		}
	case Regexp:
		if p.re == nil {
			p.re = regexp.MustCompile(p.Value)
		}
		hit := make([]bool, len(dict))
		for di, d := range dict {
			hit[di] = p.re.MatchString(d)
		}
		for i, v := range idx {
			if hit[v] {
				b.Set(i)
			}
		}
	}
	return b
}

func containsSubstr(s, sub string) bool {
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Count returns how many rows match q, without materializing any -- the
// aggregation hot path. It is the design's best case: group-skip by
// footer, then popcount of the match bitset, no per-row map, no field
// decode beyond the predicate columns. This is where finer skip
// granularity turns into a real margin over a scan-and-count.
func Count(s Store, q *Query) int {
	groups := s.Groups(q.From, q.To)
	survivors := groups[:0]
	for _, g := range groups {
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	if len(survivors) >= parallelMinGroups {
		return countParallel(survivors, q)
	}
	total := 0
	for _, g := range survivors {
		total += matchBitset(g, q).Count()
	}
	return total
}

// Histogram buckets match counts by time at the given step (nanoseconds),
// the /select/logsql/hits shape -- again without materializing rows.
func Histogram(s Store, q *Query, step int64) map[int64]int {
	out := map[int64]int{}
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		sel := matchBitset(g, q)
		if sel.Count() == 0 {
			continue
		}
		// Bucket only the matched rows' times, and decode only the window's
		// block span for them, not the whole timestamp column.
		lo, hi := g.TimeWindowSpan("_time", q.From, q.To)
		if lo >= hi {
			lo, hi = 0, g.Rows
		}
		ts := g.TimestampsRange("_time", lo, hi)
		sel.ForEach(func(i int) {
			out[ts[i-lo]/step*step]++
		})
	}
	return out
}

// matchBitset builds the selection for a group: time window AND every
// predicate, each column decoded once. Shared by Count, Histogram and the
// row path.
func matchBitset(g *storage.Reader, q *Query) *Bitset {
	n := g.Rows
	sel := NewBitset(n)
	sel.SetAll()
	// Time: skip the per-row filter entirely when the whole group is inside
	// the window (no _time touched); otherwise the block-aware mask skips
	// blocks whose [min,max] miss the window and decodes only the boundary
	// blocks, never the whole column.
	if g.TimeMin < q.From || g.TimeMax >= q.To {
		mask := g.TimeRangeMaskInto("_time", q.From, q.To, nil)
		tb := NewBitset(n)
		packBools(tb, mask)
		sel.And(tb)
	}
	for i := range q.Preds {
		p := &q.Preds[i]
		if p.Kind == Eq {
			sel.And(eqPredBitset(g, p, n))
			continue
		}
		idx, dict := g.DictIndices(p.Field)
		sel.And(predBitsetCol(g, p, idx, dict, n))
	}
	return sel
}

// eqPredBitset chooses the equality path by selectivity: a rare value
// (few rows) reads its posting list directly, no column decode; a common
// value takes the vectorized residual scan (vpcmpeqd + pack) over the
// decoded indices, which beats iterating a huge posting list one Set at a
// time. The crossover is one eighth of the group -- below it the postings
// win, above it the scan does.
func eqPredBitset(g *storage.Reader, p *Pred, n int) *Bitset {
	b := NewBitset(n)
	id, count, ok := g.EqualityCount(p.Field, p.Value)
	if !ok {
		// No postings for this column: fall back to a decode + scan.
		idx, dict := g.DictIndices(p.Field)
		return predBitsetCol(g, p, idx, dict, n)
	}
	if id < 0 || count == 0 {
		return b // provably absent
	}
	if count <= n/8 {
		rows, _ := g.EqualityRows(p.Field, p.Value)
		for _, row := range rows {
			b.Set(int(row))
		}
		return b
	}
	idx, _ := g.DictIndices(p.Field)
	eqMaskInto(b, idx, uint32(id))
	return b
}

// eqMaskInto fills b with the rows where idx == want, using the simd
// compare and pack kernels: EqualScalarInto writes a bool per row with
// one vector compare per lane, MaskBits packs those bools to the bitset's
// words. Both are kernel paths; the Go loop the design set out to kill is
// gone.
func eqMaskInto(b *Bitset, idx []uint32, want uint32) {
	n := len(idx)
	if n == 0 {
		return
	}
	bools := make([]bool, n)
	simd.EqualScalarInto(bools, idx, want)
	packBools(b, bools)
}

// packBools ORs a bool mask into b's words via MaskBits (bit set where the
// bool byte is 1). b starts clear for a fresh predicate bitset.
func packBools(b *Bitset, bools []bool) {
	if len(bools) == 0 {
		return
	}
	bb := b.bytesForPack()
	boolBytes := boolsAsBytes(bools)
	simd.MaskBits(bb, boolBytes, 1)
}

func boolsAsBytes(s []bool) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s))
}
