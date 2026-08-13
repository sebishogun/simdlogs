package query

import (
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	"github.com/sebishogun/simd"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Query is a selective log query: a time window and a conjunction of
// field predicates. It is the subset the planner and LogsQL parser feed;
// AND across predicates, with equality and substring the two the storage
// footer can skip on.
type Query struct {
	From, To    int64
	Now         int64    // request time (nanos) for relative _time filters; 0 => fall back to To
	Preds       []Pred   // implicit-AND predicates (programmatic callers, ES planner)
	Filter      *Expr    // boolean filter tree from LogsQL; takes precedence when set
	Pipes       []Pipe   // LogsQL pipe chain (stats/sort/limit/fields), applied after the filter
	Materialize []string // extra fields to materialize for the pipes (beyond predicate fields)
	Limit       int
	// MaxRows bounds materialization without changing result semantics: the scan
	// stops once it has produced more than MaxRows, and the caller errors. Unlike
	// Limit (which must return the first N in time order, and so forces the serial
	// path) this only has to DETECT overflow, so it keeps the parallel scan.
	MaxRows int
	MatAll  bool // materialize every column (full-record output: bare selects, live tail)
}

// PredKind selects the comparison.
type PredKind uint8

const (
	Eq            PredKind = iota // field := value   (dict-id equality)
	Contains                      // field ~ substr   (substring, bloom-skippable)
	Regexp                        // field ~ /re/     (RE2 on survivors only)
	Lt                            // field < num      (numeric compare over the dict)
	Le                            // field <= num
	Gt                            // field > num
	Ge                            // field >= num
	In                            // field in (a,b,c) (set membership)
	Prefix                        // field = val*     (dict range on a prefix)
	RangeNum                      // range(lo,hi)     (numeric, inclusive both ends)
	LenRange                      // len_range(lo,hi) (value byte-length, inclusive)
	StringRange                   // string_range(a,b)(lexicographic a <= v < b)
	IContains                     // i(phrase)        (case-insensitive substring)
	Seq                           // seq(a,b,..)      (phrases occurring in order)
	IPv4Range                     // ipv4_range(lo,hi)(field IPv4 in [lo,hi])
	EqField                       // eq_field(f)      (this field == field f, per row)
	NeField                       // ne_field(f)      (this field != field f)
	LtField                       // lt_field(f)      (this field <  field f)
	LeField                       // le_field(f)      (this field <= field f)
	GtField                       // gt_field(f)      (this field >  field f)
	GeField                       // ge_field(f)      (this field >= field f)
	TimeRange                     // _time:[a,b]      (timestamp in [T1,T2), resolved absolute)
	TimeDayRange                  // _time:day_range  (minute-of-day in [T1,T2], UTC)
	TimeWeekRange                 // _time:week_range (weekday in T1 bitmask, UTC)
	StreamIDEq                    // _stream_id:<id>  (_stream value whose hash == Value)
)

// isFieldCmp reports whether the kind compares this field against another field
// (Field2) per row, rather than this field's values against a constant.
func isFieldCmp(k PredKind) bool { return k >= EqField && k <= GeField }

// regex compiles the predicate's pattern once, tolerating an invalid pattern
// (which then matches nothing) so a malformed user regex is never a panic --
// the parser also validates it up front for a clean 400, this is the guard for
// programmatic callers that set Value directly.
func (p *Pred) regex() *regexp.Regexp {
	if p.re == nil {
		re, err := regexp.Compile(p.Value)
		if err != nil {
			return nil
		}
		p.re = re
	}
	return p.re
}

// Pred is one field predicate. Fields ordered large-to-small (pointers and
// strings, then float64, then the byte-sized Kind last) to avoid interior
// padding.
type Pred struct {
	Field  string         // Eq/Contains/Regexp/Prefix/IContains key
	Field2 string         // *_field: the other field to compare against
	Value  string         // Eq/Contains/Regexp/Prefix/IContains value, StringRange lo
	Value2 string         // StringRange hi
	Values []string       // In, Seq (ordered phrases)
	re     *regexp.Regexp // compiled Regexp
	Num    float64        // Lt/Le/Gt/Ge bound, Range/LenRange/IPv4Range lo
	Num2   float64        // Range/LenRange/IPv4Range hi
	T1, T2 int64          // TimeRange bounds (nanos); day/week-range params; pre-resolve relative offsets
	Sub    *Query         // In: a subquery whose result values become the set, resolved at Run
	Rel    bool           // TimeRange: T1/T2 are offsets before Now, resolved at Run
	Kind   PredKind
}

// ExprOp is the node kind of a boolean filter tree.
type ExprOp uint8

const (
	OpLeaf ExprOp = iota // a single Pred
	OpAnd
	OpOr
	OpNot
)

// Expr is a boolean tree of predicates -- LogsQL's AND/OR/NOT/parentheses.
// A leaf carries one Pred; And/Or carry children; Not carries one child.
// Query.Filter holds it; when nil the engine falls back to Preds (implicit
// AND) so existing programmatic callers keep working.
type Expr struct {
	Pred  Pred    // OpLeaf
	Kids  []*Expr // OpAnd, OpOr
	Child *Expr   // OpNot
	Op    ExprOp
}

// smallResultRows is the match count below which appendMatches uses the direct
// per-row path: the arena and positional resolve do not amortize on a selective
// query, and setting them up measured as a needle regression.
const smallResultRows = 64

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
	resolveTimePreds(q)
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
		if q.MaxRows > 0 && len(out) > q.MaxRows {
			return out // over the cap: stop scanning, the caller errors
		}
	}
	return out
}

// groupCanMatch rejects a group whose footer proves a required equality
// value absent -- the bloom + dict scan, no row decode. For a filter tree it
// prunes on the AND-of-equality leaves only (an OR branch or a non-equality
// leaf could still match, so those never reject).
func groupCanMatch(g *storage.Reader, q *Query) bool {
	if q.Filter != nil {
		return exprCanMatch(g, q.Filter)
	}
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

	// Predicate bitset and the fields to materialize. A filter tree evaluates
	// recursively (its leaf fields are what we output); the flat Preds path
	// decodes each field once and reuses it for both filter and materialize.
	type col struct {
		idx  []uint32
		dict []string
	}
	cols := make(map[string]col, len(q.Preds))
	var matFields []string
	seenF := map[string]bool{}
	addField := func(f string) {
		if !seenF[f] {
			seenF[f] = true
			matFields = append(matFields, f)
		}
	}
	if q.Filter != nil {
		sel.And(evalExpr(g, q.Filter, n))
		fs := map[string]bool{}
		filterFields(q.Filter, fs)
		for f := range fs {
			addField(f)
		}
	} else {
		for i := range q.Preds {
			p := &q.Preds[i]
			if isTimePred(p.Kind) {
				sel.And(timePredBitset(g, p, n))
				continue // _time is the timestamp column, not a materialized field
			}
			addField(p.Field)
			if p.Field2 != "" {
				addField(p.Field2)
			}
			if p.Kind == Eq {
				sel.And(eqPredBitset(g, p, n))
				continue
			}
			idx, dict := g.DictIndices(p.Field)
			cols[p.Field] = col{idx: idx, dict: dict}
			sel.And(predBitsetCol(g, p, idx, dict, n))
		}
	}
	for _, f := range q.Materialize { // fields the pipe chain needs
		addField(f)
	}
	if q.MatAll { // full-record output: every column, not just the filtered ones
		for _, f := range g.ColumnNames() {
			if f != "_time" {
				addField(f)
			}
		}
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
	// Many matches: decode each materialize column once and index into it,
	// rather than a per-row DictValueAt point read (which decompresses a dict
	// block per field per row). Bulk-decoding every materialize column only
	// pays when the match set is a large fraction of the group; below n/8 the
	// point reads win (the needle and selective AND stay lazy).
	if cnt >= n/16 {
		for _, f := range matFields {
			if _, ok := cols[f]; ok {
				continue
			}
			// Decode only the dict values the matched rows reference, not every
			// distinct string: on a whole-record select over a fraction of a
			// high-cardinality column, the unreferenced 80% never become Go
			// strings (the profile's slicebytetostring + the GC it drove).
			idx := g.DictIndicesRaw(f)
			if idx == nil {
				continue
			}
			want := make([]bool, g.DictLen(f))
			sel.ForEach(func(i int) {
				if i < len(idx) {
					want[idx[i]] = true
				}
			})
			cols[f] = col{idx: idx, dict: g.DictDecodeSome(f, want)}
		}
	}
	// A selective query (the needle: one or a few matches) does not amortize the
	// arena and the positional column resolve below -- their setup costs more than
	// the per-row work they remove, which measured as a needle regression. Keep the
	// direct path for small result sets; the arena is for the big ones.
	if cnt <= smallResultRows {
		sel.ForEach(func(i int) {
			var t int64
			if pointRead {
				t, _ = g.TimestampAt("_time", i)
			} else {
				t = ts[i-lo]
			}
			row := Row{Time: t, Fields: make([]Field, 0, len(matFields))}
			for _, f := range matFields {
				if c := cols[f]; c.idx != nil {
					row.Fields = append(row.Fields, Field{f, c.dict[c.idx[i]]})
				} else if v, ok := g.DictValueAt(f, i); ok {
					row.Fields = append(row.Fields, Field{f, v})
				}
			}
			out = append(out, row)
		})
		return out
	}

	// Resolve each materialize field to its decoded column ONCE, positionally --
	// the inner loop ran a map lookup per field per row.
	type matCol struct {
		name string
		idx  []uint32
		dict []string
	}
	mats := make([]matCol, len(matFields))
	for k, f := range matFields {
		c := cols[f]
		mats[k] = matCol{name: f, idx: c.idx, dict: c.dict}
	}
	// One Field arena for the whole group instead of a make() per row: a big
	// result set was one allocation per matched row (75k allocs / 34MB on the
	// common-select bench, and the GC to match). Sized exactly and never grown,
	// so the sub-slices handed to rows stay valid.
	arena := make([]Field, cnt*len(mats))
	pos := 0
	// Reserve room for this group's matches, but keep append's amortized doubling:
	// growing to the exact size reallocated once PER GROUP, which cost a selective
	// query touching many groups more than the doubling it replaced (needle +47%).
	if need := len(out) + cnt; cap(out) < need {
		if grow := 2 * cap(out); grow > need {
			need = grow
		}
		grown := make([]Row, len(out), need)
		copy(grown, out)
		out = grown
	}
	sel.ForEach(func(i int) {
		var t int64
		if pointRead {
			t, _ = g.TimestampAt("_time", i)
		} else {
			t = ts[i-lo]
		}
		start := pos
		for k := range mats {
			// Prefer a decoded column if we already have one; otherwise
			// (the posting path skipped the full decode) fetch just this
			// row's value -- O(1), keeping the selective query lazy.
			m := &mats[k]
			if m.idx != nil {
				arena[pos] = Field{m.name, m.dict[m.idx[i]]}
				pos++
			} else if v, ok := g.DictValueAt(m.name, i); ok {
				arena[pos] = Field{m.name, v}
				pos++
			}
		}
		out = append(out, Row{Time: t, Fields: arena[start:pos:pos]})
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
	// Equality is the vectorized residual scan: one vpcmpeqd per lane over
	// the encoded indices (EqualScalarInto), then pack the bool mask to bits
	// (MaskBits) -- no per-row compare. The design's Task 3.3, replacing
	// VictoriaLogs' scalar filter_exact pattern.
	if p.Kind == Eq {
		id := g.DictID(p.Field, p.Value)
		if id < 0 {
			return b
		}
		eqMaskInto(b, idx, uint32(id))
		return b
	}
	// Field-vs-field compares two columns per row, so it cannot use the
	// per-dict-value hit table; decode the other column and compare row by row.
	if isFieldCmp(p.Kind) {
		idx2, dict2 := g.DictIndices(p.Field2)
		if idx2 == nil {
			return b
		}
		for i := 0; i < n && i < len(idx) && i < len(idx2); i++ {
			if fieldCmp(dict[idx[i]], dict2[idx2[i]], p.Kind) {
				b.Set(i)
			}
		}
		return b
	}
	// Every other kind marks which dict values match, then maps rows through
	// the indices. The test runs once per distinct value, not per row, so a
	// low-cardinality column is cheap regardless of predicate complexity.
	hit := make([]bool, len(dict))
	switch p.Kind {
	case Contains:
		for di, d := range dict {
			hit[di] = containsSubstr(d, p.Value)
		}
	case Regexp:
		re := p.regex()
		if re == nil {
			break // invalid pattern: matches nothing
		}
		for di, d := range dict {
			hit[di] = re.MatchString(d)
		}
	case Prefix:
		for di, d := range dict {
			hit[di] = strings.HasPrefix(d, p.Value)
		}
	case In:
		set := make(map[string]bool, len(p.Values))
		for _, v := range p.Values {
			set[v] = true
		}
		for di, d := range dict {
			hit[di] = set[d]
		}
	case Lt, Le, Gt, Ge:
		for di, d := range dict {
			if f, err := strconv.ParseFloat(d, 64); err == nil {
				hit[di] = cmpNum(f, p.Kind, p.Num)
			}
		}
	case RangeNum:
		for di, d := range dict {
			if f, err := strconv.ParseFloat(d, 64); err == nil {
				hit[di] = f >= p.Num && f <= p.Num2
			}
		}
	case LenRange:
		for di, d := range dict {
			l := float64(len(d))
			hit[di] = l >= p.Num && l <= p.Num2
		}
	case StringRange:
		for di, d := range dict {
			hit[di] = d >= p.Value && d < p.Value2 // VL: a <= v < b
		}
	case IContains:
		lc := strings.ToLower(p.Value)
		for di, d := range dict {
			hit[di] = containsSubstr(strings.ToLower(d), lc)
		}
	case Seq:
		for di, d := range dict {
			hit[di] = seqMatch(d, p.Values)
		}
	case IPv4Range:
		lo, hi := uint32(p.Num), uint32(p.Num2)
		for di, d := range dict {
			if v, ok := ipToU32(d); ok {
				hit[di] = v >= lo && v <= hi
			}
		}
	case StreamIDEq:
		for di, d := range dict {
			hit[di] = StreamID(d) == p.Value
		}
	}
	for i, v := range idx {
		if hit[v] {
			b.Set(i)
		}
	}
	return b
}

// cmpNum applies a numeric comparison predicate.
func cmpNum(f float64, kind PredKind, want float64) bool {
	switch kind {
	case Lt:
		return f < want
	case Le:
		return f <= want
	case Gt:
		return f > want
	case Ge:
		return f >= want
	}
	return false
}

// fieldCmp compares two field values for the *_field predicates: equality is a
// string compare; the ordered kinds compare numerically when both values parse
// as numbers, else lexicographically.
func fieldCmp(a, b string, kind PredKind) bool {
	switch kind {
	case EqField:
		return a == b
	case NeField:
		return a != b
	}
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	var less, eq bool
	if ea == nil && eb == nil {
		less, eq = fa < fb, fa == fb
	} else {
		less, eq = a < b, a == b
	}
	switch kind {
	case LtField:
		return less
	case LeField:
		return less || eq
	case GtField:
		return !less && !eq
	case GeField:
		return !less
	}
	return false
}

// seqMatch reports whether every phrase occurs in s in the given order.
func seqMatch(s string, phrases []string) bool {
	pos := 0
	for _, ph := range phrases {
		i := strings.Index(s[pos:], ph)
		if i < 0 {
			return false
		}
		pos += i + len(ph)
	}
	return true
}

// ipToU32 parses a dotted IPv4 string to its uint32, ok=false if malformed.
func ipToU32(s string) (uint32, bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return 0, false
	}
	var v uint32
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return 0, false
		}
		v = v<<8 | uint32(n)
	}
	return v, true
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
	groups := s.Groups(q.From, q.To)
	survivors := groups[:0]
	for _, g := range groups {
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	// Fan out when the window spans enough groups -- at scale a selective
	// window covers hundreds of groups, and serial bucketing over them was
	// the aggregation's loss to VictoriaLogs at a billion rows.
	if len(survivors) >= parallelMinGroups {
		return histogramParallel(survivors, q, step)
	}
	out := map[int64]int{}
	for _, g := range survivors {
		histoGroup(g, q, step, out)
	}
	return out
}

// histoGroup buckets one group's matched rows' times into out at the given
// step, decoding only the window's block span. Shared by the serial and
// parallel Histogram paths.
func histoGroup(g *storage.Reader, q *Query, step int64, out map[int64]int) {
	sel := matchBitset(g, q)
	if sel.Count() == 0 {
		return
	}
	lo, hi := g.TimeWindowSpan("_time", q.From, q.To)
	if lo >= hi {
		lo, hi = 0, g.Rows
	}
	ts := g.TimestampsRange("_time", lo, hi)
	sel.ForEach(func(i int) {
		out[ts[i-lo]/step*step]++
	})
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
	if q.Filter != nil {
		sel.And(evalExpr(g, q.Filter, n))
		return sel
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
	// Raw indices only: the mask compares dict ids, so decoding the whole dict
	// section into Go strings (what DictIndices also does) was pure waste.
	idx := g.DictIndicesRaw(p.Field)
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
