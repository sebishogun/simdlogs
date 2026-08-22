package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	// ToSet says the caller RESOLVED To, as against leaving it at its zero
	// value.
	//
	// Zero is a real instant -- the epoch -- and `To == 0` was the marker for
	// "no end" in two places at once: parseRequest, which turned it into
	// 1<<62, and resolveTimePreds, which lets a `_time:` filter widen past it.
	// So `?end=0`, or any spelling of 1970-01-01T00:00:00Z, meant the epoch
	// for a bare query and "no end" for one carrying an absolute `_time:`
	// filter -- one binary reading one parameter two ways, at HTTP 200:
	//
	//	/select/logsql/query?end=0&query=*                          0 rows
	//	/select/logsql/query?end=0&query=_time:[2026-06-01,...]     30 rows
	//
	// A caller that never sets To leaves this false and keeps the old
	// behaviour, which is what every programmatic constructor wants.
	ToSet bool
	Now   int64 // request time (nanos) for relative _time filters; see NowSet
	// NowSet separates "the caller set Now to zero" from "the caller never set
	// Now", which the zero value alone cannot.
	//
	// The same collision as ToSet, one field over. With Now unset the fallback
	// is To, and To's own unset form on some paths is the 1<<62 sentinel, so
	// `Now=0, To=1<<62` resolved `_time:5m` to
	// [4611685718427387904, 4611686018427387904] -- a five-minute window in the
	// year 2116, silently. Every handler sets Now today (parseRequest, both
	// stats entry points, alerts, logrules), so no route reaches it; it is one
	// programmatic caller away, which is what the window defect was before
	// somebody wrote that caller.
	//
	// Use SetNow.
	NowSet      bool
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
	// Deadline stops the scan when the wall clock passes it, and MaxBytes
	// stops it when the materialized rows exceed a budget. Both are checked
	// per group rather than per row: a group is the unit of work the scan
	// commits to, and a per-row check costs more than it saves.
	//
	// They exist here because the executor-level cancellation of plan task
	// 6.1 does not, and without them -search.maxDuration and
	// -search.maxQueryBytes were configuration nothing read: a query ran to
	// completion whatever the request context said, because Go does not
	// abort a handler when its context is cancelled.
	Deadline time.Time
	MaxBytes int64
	// Stopped reports that a limit ended the scan early, so a caller can
	// answer with an error instead of a short result presented as complete.
	// A pointer to an atomic, not to a bool: runParallel checks the budget
	// from every worker, so a plain *bool here is a data race the moment the
	// budget is ever reached with more than one group in flight.
	Stopped *atomic.Bool

	// LastN returns the n most RECENT matching rows, newest first -- the select
	// endpoint's `limit` argument, which is how a log viewer shows the tail of a
	// stream. Deliberately not Limit: the `| limit N` pipe returns the FIRST n,
	// and the reference draws the same distinction (measured against it: the
	// parameter answers m19,m18,m17 where the pipe answers m0,m1,m2).
	LastN  int
	MatAll bool // materialize every column (full-record output: bare selects, live tail)
	// MatCols asks the SCAN for every column without asking for full-record
	// OUTPUT. MatAll means both, and the API layer reads it as "synthesize the
	// _stream/_stream_id pair onto every row" (appendRowJSON's withStream) --
	// so setting MatAll to feed a pack-all pipe put those two fields onto stats
	// rows that had never carried them. Two meanings in one flag; this is the
	// half that is only about what the scan reads.
	MatCols bool

	// Cancellation and the reason a scan stopped.
	//
	// Unexported and set only by Executor.bindContext: a Query built by hand
	// has no context and behaves exactly as it did before, which is what
	// every internal caller and every existing test relies on.
	//
	// ctx is read at the checkpoint the scan already had -- exceeded() --
	// rather than at new ones, so every loop that already stopped for a byte
	// budget stops for a cancelled context too, and no call site has to
	// remember a second check.
	ctx context.Context
	// maxGroups bounds survivors after the prune; 0 is unbounded.
	maxGroups int
	// maxMemory bounds the working set; 0 is unbounded.
	maxMemory int64
	// maxGroupKeys bounds an aggregate's distinct `by` keys; 0 is unbounded.
	// Separate from maxGroups, which counts row groups on disk: a
	// `stats by (request_id)` reads few of those and explodes this one.
	maxGroupKeys int
	// maxPipeRows bounds the rows one pipe may produce; 0 is unbounded.
	maxPipeRows int
	// stopReason is why the scan stopped, recorded once by the FIRST stop.
	// A cancelled context and an exhausted budget can both become true while
	// the scan unwinds, and reporting the second tells the caller to fix the
	// wrong thing.
	//
	// A POINTER to the atomic, for the same reason Stopped is one: a Query is
	// copied by value in four places -- subqueries, introspection, the
	// executor -- and an embedded atomic makes the type uncopyable. Sharing it
	// across the copy is also the behaviour that is wanted: a cancelled parent
	// stops its subqueries, and a subquery that trips a budget is visible to
	// the parent that will report it.
	stopReason *atomic.Pointer[error]
}

// Bind attaches a context and a budget to a query run directly through Run or
// RunPipeline rather than through an Executor.
//
// It exists because the HTTP layer has eighteen call sites that build a Query
// and call the engine, and converting them all to Executor.Execute in one
// change would be a rewrite of every read endpoint for a benefit -- typed stop
// reasons -- that Bind delivers on its own. Executor is the API for new
// callers; this is how the existing ones get cancellation.
func (q *Query) Bind(ctx context.Context, lim Limits) { q.bindContext(ctx, lim) }

// StopErr reports why a bound query stopped early, or nil.
func (q *Query) StopErr() error { return q.stopErr() }

// bindContext attaches a context and the executor's ceilings.
func (q *Query) bindContext(ctx context.Context, lim Limits) {
	q.ctx = ctx
	q.maxGroups = lim.MaxGroups
	q.maxMemory = lim.MaxMemory
	q.maxGroupKeys = lim.MaxGroupKeys
	q.maxPipeRows = lim.MaxPipeRows
	q.stopReason = new(atomic.Pointer[error])
	if lim.MaxRows > 0 && (q.MaxRows == 0 || lim.MaxRows < q.MaxRows) {
		q.MaxRows = lim.MaxRows
	}
	if lim.MaxBytes > 0 && (q.MaxBytes == 0 || lim.MaxBytes < q.MaxBytes) {
		q.MaxBytes = lim.MaxBytes
	}
	if q.Stopped == nil {
		q.Stopped = new(atomic.Bool)
	}
}

// countsBytes reports whether the scan has to total up materialized row sizes.
//
// The walk is per row and per field, measured at +2.4% to +6.9% instructions
// depending on the corpus, so it runs only when something reads the answer.
// MaxMemory is in the gate as well as MaxBytes: with only the memory ceiling
// set, the total stayed at zero and the ceiling could never be reached -- a
// limit that is configuration nothing reads, which is the state the Deadline
// and MaxBytes fields were added to fix in the first place.
func (q *Query) countsBytes() bool { return q.MaxBytes > 0 || q.maxMemory > 0 }

// stopErr reports why the scan stopped, or nil.
//
// A row-limit overflow is one of the reasons. It used to be signalled only by
// the scan producing more rows than MaxRows, which made enforcement the
// caller's job -- and exactly one caller did it, so every piped query answered
// from a silently truncated input.
func (q *Query) stopErr() error {
	if q.stopReason == nil {
		return nil
	}
	if p := q.stopReason.Load(); p != nil {
		return *p
	}
	return nil
}

// stop records the first reason a scan ended early.
func (q *Query) stop(err error) {
	if q.Stopped != nil {
		q.Stopped.Store(true)
	}
	if q.stopReason == nil {
		return // no executor bound this query; the bool is the whole signal
	}
	// CompareAndSwap, not Store: two parallel workers can stop in the same
	// instant, and the second must not overwrite the first's reason.
	q.stopReason.CompareAndSwap(nil, &err)
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

// numInRange applies the predicate's bounds honouring their exclusivity.
func numInRange(f float64, p *Pred) bool {
	if p.ExLo {
		if f <= p.Num {
			return false
		}
	} else if f < p.Num {
		return false
	}
	if p.ExHi {
		return f < p.Num2
	}
	return f <= p.Num2
}

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
	// RangeNum/LenRange bound exclusivity. LogsQL uses math-interval notation:
	// range(a, b) excludes both ends, range[a, b] includes them. Measured against
	// VictoriaLogs: range(100, 500) does NOT match the row whose value is 100.
	ExLo, ExHi bool
	Sub        *Query // In: a subquery whose result values become the set, resolved at Run
	Rel        bool   // TimeRange: T1/T2 are offsets before Now, resolved at Run
	Kind       PredKind
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

// bulkDecodeRows is the match count above which materialization decodes each
// column once (DictDecodeSome) rather than point-reading per row. Point reads
// decompress a dict block per call, so their cost scales with rows*fields;
// the bulk decode's scales with the blocks the matches touch.
const bulkDecodeRows = 512

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
	// NoTime marks a row that has no timestamp of its own -- a stats/aggregation
	// result, or a projection that did not keep _time. VictoriaLogs treats _time
	// as an ordinary field subject to projection, so such rows must NOT carry one;
	// emitting a zero timestamp printed _time=1970-01-01 on every stats row.
	NoTime bool
}

// rowBytes is a row's materialized size: the timestamp plus every key and
// value it carries. It is what MaxBytes bounds, so it has to be the size that
// actually accumulates in memory rather than a per-row constant.
func rowBytes(r Row) int64 {
	n := int64(8)
	for _, f := range r.Fields {
		n += int64(len(f.Key) + len(f.Value))
	}
	return n
}

// exceeded reports whether a scan budget is spent, and records that it was.
// bytes is the materialized size so far.
//
// Both budgets were checked in exactly three places, all inside the group
// pre-filter, and all with a literal zero for bytes -- so MaxBytes could
// never fire at all, and the deadline could only fire if it had already
// passed before any work started. A 300,000-row scan with a 20 ms budget and
// a 1-byte byte budget ran to completion in 121 ms and returned 69 MB. The
// scan loop below is where they have to be checked, because the scan is what
// costs.
func (q *Query) exceeded(bytes int64) bool {
	// The context first, and cheaply: ctx.Err() is one atomic load on a live
	// context, which is what this is on every call but the last.
	if q.ctx != nil {
		if err := q.ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				q.stop(ErrDeadlineExceeded)
			} else {
				q.stop(ErrCanceled)
			}
			return true
		}
	}
	if !q.Deadline.IsZero() && time.Now().After(q.Deadline) {
		q.stop(ErrDeadlineExceeded)
		return true
	}
	if q.MaxBytes > 0 && bytes > q.MaxBytes {
		q.stop(ErrByteLimit)
		return true
	}
	if q.maxMemory > 0 && bytes > q.maxMemory {
		// Checked against the same running total for now: the scan's working
		// set and its cumulative output coincide while rows are accumulated
		// into one slice. Stated rather than implied, because when a
		// streaming sink lands (task 6.3) the two stop coinciding and this
		// has to become the live figure rather than the running one.
		q.stop(ErrMemoryLimit)
		return true
	}
	return false
}

// Store is the read surface the engine needs; storage.Store satisfies it.
//
// It hands out snapshots rather than raw readers because a reader is a window
// onto an mmap: retention, recompaction, cold demotion and store close all
// release mappings, and a query holding a bare *Reader across any of them
// reads freed memory. A snapshot holds its groups until Close.
type Store interface {
	Snapshot(from, to int64) (*storage.Snapshot, error)
}

// snapshotOf takes a snapshot for a caller that has no way to report an
// error. A closed store yields an empty snapshot, so the caller's loop runs
// zero times -- the same answer it gave before, when a closed store simply
// had no groups left in its index.
func snapshotOf(s Store, from, to int64) *storage.Snapshot {
	sn, err := s.Snapshot(from, to)
	if err != nil || sn == nil {
		return storage.EmptySnapshot()
	}
	return sn
}

// Run executes q over the store and returns matching rows in time order,
// up to Limit. Groups outside the time window are skipped by the store;
// groups a predicate proves cannot match are skipped by the footer bloom;
// only survivors are decoded and scanned. This layered skip is where the
// orders of magnitude over a whole-block scan come from.
func Run(s Store, q *Query) []Row {
	resolveTimePreds(q)
	sn1 := snapshotOf(s, q.From, q.To)
	defer sn1.Close()
	groups := sn1.Groups
	// Footer-prune first, then decide whether to fan out. A selective query
	// (a rare value) survives in one or two groups; spawning a worker pool
	// for that costs more than it saves, and it was the largest single cost
	// in the needle profile. Pruning is a cheap bloom + dict binary search,
	// no column decoded, so it is always worth doing before the fork.
	survivors := groups[:0]
	for _, g := range groups {
		if q.exceeded(0) {
			break
		}
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	// The group ceiling fires here: after the time window and the footer
	// prune, before a single column is decoded. Before either, every query on
	// a large store would trip it; after the scan, it would have cost what it
	// exists to avoid.
	if q.maxGroups > 0 && len(survivors) > q.maxGroups {
		q.stop(fmt.Errorf("%w: %d groups survived the prune, ceiling is %d",
			ErrTooManyGroups, len(survivors), q.maxGroups))
		return nil
	}
	if q.LastN > 0 {
		return runNewest(survivors, q)
	}
	if len(survivors) >= parallelMinGroups && q.Limit == 0 {
		return runParallel(survivors, q)
	}
	var out []Row
	var bytes int64
	for _, g := range survivors {
		before := len(out)
		out = appendMatches(out, g, q)
		// Only pay for the accounting when something reads it: a per-row,
		// per-field walk. An earlier version of this comment quoted +11.7%
		// against 890.9M instructions per op, which does not reproduce and
		// named no corpus or query to reproduce it from. Re-measured with
		// perf stat -e instructions:u, GOMAXPROCS=1 GOGC=off, slope between
		// 1x and 5x, MatAll over 3M rows: +2.4% on a four-column corpus
		// with a near-unique _msg, +6.9% when every value is short and
		// low-cardinality. The cost is real and it scales with field count,
		// not with a single number.
		//
		// Who reaches the fast path is narrower than it looks, and an
		// earlier version of this comment said the opposite:
		// config.DefaultLimits sets MaxQueryBytes to 256 MiB and
		// applyQueryBudget copies it onto every HTTP read, so a default
		// deployment pays the walk on every query. Only an internal caller
		// with no budget, or -search.maxQueryBytes=-1, skips it.
		if q.countsBytes() {
			for _, r := range out[before:] {
				bytes += rowBytes(r)
			}
		}
		if q.Limit > 0 && len(out) >= q.Limit {
			return out[:q.Limit]
		}
		if q.MaxRows > 0 && len(out) > q.MaxRows {
			// Recorded, not merely returned. The caller used to be trusted to
			// compare len(out) against MaxRows, and only ONE caller did: a
			// bare select. Every other shape -- `| sort`, `| offset`,
			// `| rename`, `| join`, `| union` -- got the cap set by the HTTP
			// layer, had its input silently cut here, and then answered from
			// the truncated set. A sort of the first N rows is not the first
			// N of the sort, and nothing said so.
			q.stop(fmt.Errorf("%w: more than %d rows matched", ErrRowLimit, q.MaxRows))
			return out
		}
		// The budget check that costs something: after a group has been
		// materialized, not only before the first one.
		if q.exceeded(bytes) {
			return out
		}
	}
	return out
}

// runNewest answers the endpoint's `limit`: the n most recent matching rows,
// newest first. It walks the groups from the newest backwards and keeps only
// each one's last n matches, so a bounded tail query never materializes a
// group's whole match set -- and stops as soon as no older group can hold a row
// newer than the oldest one kept.
func runNewest(survivors []*storage.Reader, q *Query) []Row {
	var out []Row
	var bytes int64
	for i := len(survivors) - 1; i >= 0; i-- {
		before := len(out)
		out = appendMatches(out, survivors[i], q)
		if q.countsBytes() {
			for _, r := range out[before:] {
				bytes += rowBytes(r)
			}
		}
		// The budgets apply here too: this is a scan, and the bounded-tail
		// shape is the one an interactive client uses most.
		if q.exceeded(bytes) {
			sortByTimeDesc(out)
			if len(out) > q.LastN {
				out = out[:q.LastN]
			}
			return out
		}
		if len(out) < q.LastN {
			continue
		}
		sortByTimeDesc(out)
		out = out[:q.LastN]
		// Groups are stored in ingest order, which is time order up to the
		// out-of-order arrivals every real log stream has -- so the cutoff is
		// checked against the next group's actual TimeMax, not assumed.
		if i == 0 || survivors[i-1].TimeMax < out[len(out)-1].Time {
			return out
		}
	}
	sortByTimeDesc(out)
	if len(out) > q.LastN {
		out = out[:q.LastN]
	}
	return out
}

func sortByTimeDesc(rows []Row) {
	sort.SliceStable(rows, func(a, b int) bool { return rows[a].Time > rows[b].Time })
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
	if q.MatAll || q.MatCols { // every column, not just the filtered ones
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
	// A bounded query stops inside the group, not after it. Trimming the bitset
	// here bounds every decode below -- without it `| limit 100` still decoded
	// every matching row of the first group and discarded all but a hundred.
	if q.LastN > 0 {
		// Newest-first: the rows that can survive are this group's LAST n.
		if cnt > q.LastN {
			sel.KeepLast(q.LastN)
			cnt = q.LastN
		}
	} else if q.Limit > 0 {
		remaining := q.Limit - len(out)
		if remaining <= 0 {
			return out
		}
		if cnt > remaining {
			sel.KeepFirst(remaining)
			cnt = remaining
		}
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
	// A bounded query kept a handful of rows; decoding the group's whole
	// timestamp column to read them was most of its cost -- 860us of a 956us
	// facets request, where the bound was a thousand rows out of 131072.
	if q.Limit > 0 || q.LastN > 0 {
		if first, last := sel.FirstLast(); first >= 0 {
			if first > lo {
				lo = first
			}
			if last+1 < hi {
				hi = last + 1
			}
		}
	}
	var ts []int64
	pointRead := cnt*tsBlockGuess < hi-lo
	if !pointRead {
		// Scratch: every time read below is copied into a Row's Time field
		// before this returns, so the buffer goes back to the pool here.
		var tp *[]int64
		tp, ts = groupTimestamps(g, lo, hi)
		defer releaseTs(tp)
	}
	// Many matches: decode each materialize column once and index into it,
	// rather than a per-row DictValueAt point read (which decompresses a dict
	// block per field per row). Bulk-decoding every materialize column only
	// pays when the match set is a large fraction of the group; for a HANDFUL
	// of rows the point reads win (the needle and selective AND stay lazy).
	//
	// The bound is absolute, not a fraction of the group: at 4k matched rows in
	// a 128K group the old n/16 guard chose point reads, and each one
	// decompresses the dict block its value lives in -- 37k block inflations to
	// materialize 7.5k rows, which is the same pathology docs/wrong.md entry 33
	// records for ValueCounts. Bulk DictDecodeSome inflates each needed block
	// once.
	if cnt >= n/16 || cnt >= bulkDecodeRows {
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
				} else if f == "_time" {
					// _time is stored once, in the timestamp column; a pipe that
					// asks for it by name gets it formatted from there rather
					// than from a second copy of every record's time on disk.
					row.Fields = append(row.Fields, Field{f, formatTime(t)})
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
			} else if m.name == "_time" {
				arena[pos] = Field{m.name, formatTime(t)}
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
				hit[di] = numInRange(f, p)
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
	sn2 := snapshotOf(s, q.From, q.To)
	defer sn2.Close()
	groups := sn2.Groups
	survivors := groups[:0]
	for _, g := range groups {
		if q.exceeded(0) {
			break
		}
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	if len(survivors) >= parallelMinGroups {
		return countParallel(survivors, q)
	}
	total := 0
	for _, g := range survivors {
		// The deadline, checked per group. These paths return counts and
		// facets rather than rows, so MaxBytes has nothing to measure --
		// but a scan of every group is exactly what the wall-clock budget
		// exists to bound, and until this went in twelve read routes ran
		// with no bound at all.
		if q.exceeded(0) {
			break
		}
		total += matchBitset(g, q).Count()
	}
	return total
}

// HitsSeries is the /select/logsql/hits answer for one group: the bucket start
// times covering the whole requested window and the count in each. Empty
// buckets are present with a zero count -- a graph needs the gap drawn, not
// skipped, and the reference returns them.
type HitsSeries struct {
	Fields     map[string]string
	Timestamps []int64 // bucket starts, nanoseconds, ascending
	Values     []int
	Total      int
}

// Hits buckets matching rows into step-sized buckets across [q.From,q.To),
// optionally split by the values of one field. Buckets are aligned to multiples
// of step, the alignment the reference uses, so two engines asked for the same
// window agree on the bucket boundaries.
func Hits(s Store, q *Query, step int64, by string) []HitsSeries {
	if step <= 0 {
		step = int64(time.Minute)
	}
	// The budget is consulted before any work, not only inside the scan
	// loops.
	//
	// Every other read path checks per group, which is enough while there is
	// work to do -- and reports nothing when there is not. A query whose
	// window holds no groups finished without ever asking, so an already-blown
	// deadline produced a cheerful 200. That is a read path that does not obey
	// the budget on exactly the inputs where obeying it is free.
	if q.exceeded(0) {
		return nil
	}
	// THE SCAN READS WHOLE BUCKETS; THE WALK STILL STOPS AT THE REQUESTED
	// `to`. A bucket is [k*step, (k+1)*step) whatever the request's start and
	// end fall on, so the first and last buckets count the instants a caller
	// did not name -- which is what the reference does on BOTH range surfaces,
	// asked rather than reasoned about. Measured, `internal/bench/victoria-logs`
	// against six rows at 00:15, 00:45, 01:15, 01:45, 02:15 and 02:45 on
	// 2026-06-01Z, `start=00:30Z&end=02:30Z&step=1h`:
	//
	//	                  VictoriaLogs                   simdlogs, before
	//	/hits             00:00,01:00,02:00 = 2,2,2      the same three = 1,2,1
	//	                  total 6                        total 4
	//
	// The scan window was the request's own [from,to), so the two edge buckets
	// were partial: the count under a floored label was not the count of the
	// bucket that label names. `/select/logsql/query` over the same window
	// still answers four rows -- the point query is not widened, and neither
	// is it in the reference.
	sq := *q
	sq.SetWindow(BucketSpan(q.From, q.To, step))
	if by == "" {
		return []HitsSeries{fillHits(Histogram(s, &sq, step), q, step, map[string]string{})}
	}
	// Split by a field: one series per distinct value. Each value is counted
	// with its own predicate so the per-bucket work stays in the bitset path.
	out := make([]HitsSeries, 0, 8)
	for _, vc := range StatsByField(s, &sq, by) {
		sub := sq
		sub.Preds = append(append([]Pred{}, q.Preds...), Pred{Kind: Eq, Field: by, Value: vc.Value})
		// The buckets come from the widened scan; the WALK comes from the
		// caller's window, which is why the two arguments are different
		// queries.
		walk := *q
		walk.Preds = sub.Preds
		hs := fillHits(Histogram(s, &sub, step), &walk, step, map[string]string{by: vc.Value})
		out = append(out, hs)
	}
	return out
}

// BucketSpan is the instants a floored bucket walk over [from,to) at `step`
// covers: `from` floored to a multiple of step, and the END of the last bucket
// the walk emits, which is past `to` whenever `to` is not itself a multiple.
//
// IT IS THE SCAN WINDOW, NOT THE WALK. The walk still runs `bs < to` and so
// emits the same buckets it always did; this is how far the data behind them
// reaches. Both range surfaces use it, which is what makes them agree with
// each other -- and with the reference, which widens both.
//
// A non-positive step or an empty window has no buckets and no widening.
func BucketSpan(from, to, step int64) (int64, int64) {
	if step <= 0 || to <= from {
		return from, to
	}
	lo := alignDown(from, step)
	// The last bucket the walk emits starts at the greatest multiple of step
	// strictly below `to`; to > from means to-1 does not underflow.
	last := alignDown(to-1, step)
	hi := last + step
	if hi < last {
		// The last bucket in the domain runs off the top: MaxInt64 is the
		// widest the scan can be asked for and the walk stops there anyway.
		hi = math.MaxInt64
	}
	if hi < to {
		// alignDown clamped at the bottom of the domain (see its note): the
		// bucket that holds `to-1` starts after it, so there is nothing to
		// widen and the caller's own bound is the honest one.
		hi = to
	}
	return lo, hi
}

// fillHits turns the sparse bucket map into the dense, ascending series the
// hits API returns.
//
// THE COUNT IS THE WINDOW'S EXACT WIDTH AND THE WALK STOPS AT `to`.
//
// It was `n := int((to - start + step - 1) / step)` with `if n < 0 || n >
// maxHitsBuckets { n = maxHitsBuckets }`, and both halves of that failed over a
// saturated window -- which `?end=9999-01-01` produces, because the far bound
// is outside the int64-nanosecond domain and saturates to MaxInt64 (entry
// 129/130). The addition then runs past MaxInt64 and comes back either
// negative or small-positive, and the two wraps gave two different wrong
// answers under HTTP 200, measured at `step=8760h` (one year, an ordinary
// dashboard step) on a two-row store:
//
//	?start=1970-01-01&end=9999-01-01    100000 buckets   the true count is 293
//	?start=1000-01-01&end=9999-01-01         0 buckets   ...and 0 rows totalled
//
// The first is the negative wrap being read as "no buckets" and replaced by
// THIS package's 100,000 -- a ceiling ten times the HTTP one, which is the one
// `internal/api`'s 413 enforces and the one the caller was told about. The
// second is the same addition wrapping back positive and small.
//
// `start + int64(i)*step` wrapped as well, so past bucket 292 the timestamps
// ran off MaxInt64 and came back at the far-past end: a series documented
// "dense, ascending and gap-free" that was none of the three. The walk carries
// `t` and stops when the next step would leave the window or leave the domain,
// which is the shape StatsQueryRange's bucket walk already uses for the same
// reason.
//
// RangeWidthNs is exact for any to >= from whatever the signs (two's
// complement), so the count no longer depends on the window fitting in an
// int64.
func fillHits(buckets map[int64]int, q *Query, step int64, fields map[string]string) HitsSeries {
	from, to := q.From, q.To
	if to <= from {
		return HitsSeries{Fields: fields}
	}
	// Aligned to a multiple of step, the same way histoGroup keys a row.
	//
	// AND THE SAME WAY THE OTHER RANGE SURFACE DOES IT. `StatsQueryRange`
	// below and `exactMatrix` in internal/api used to walk
	// `for bs := from; bs < to`, anchored on the request's own start, so one
	// window and one step gave a different bucket count, different labels and
	// different per-bucket values on the two routes. That was pinned as a
	// Prometheus convention without asking `internal/bench/victoria-logs`,
	// which is in this repository and floors BOTH. Both walks are this one
	// now; see the note on `BucketSpan` above and
	// TestTheTwoRangeSurfacesAgreeOnBuckets.
	//
	// The first bucket can still begin before the requested `start` -- that is
	// what flooring means, and `boundRangeBuckets` leaves an explicit
	// start/end alone so nothing re-aligns downstream.
	start := alignDown(from, step)
	n := 0
	if w := RangeWidthNs(start, to); w > 0 {
		ustep := uint64(step) // step > 0: Hits normalises a non-positive one
		nb := w / ustep
		if w%ustep != 0 {
			nb++
		}
		if nb > maxHitsBuckets {
			nb = maxHitsBuckets
		}
		n = int(nb)
	}
	hs := HitsSeries{
		Fields:     fields,
		Timestamps: make([]int64, 0, n),
		Values:     make([]int, 0, n),
	}
	for t := start; t < to && len(hs.Timestamps) < n; {
		c := buckets[t]
		hs.Timestamps = append(hs.Timestamps, t)
		hs.Values = append(hs.Values, c)
		hs.Total += c
		next := t + step
		if next <= t {
			// Past MaxInt64: that was the last bucket in the domain.
			//
			// KEEP THIS, AND NOT FOR THE REASON THE RECORD FIRST GAVE.
			// Entry 132 said reverting the walk to `start + int64(i)*step`
			// reddened nothing because "the multiply has no value left to
			// overflow on". It has: on `?start=1000-01-01&end=9999-01-01
			// &step=8760h` (start = -9208512000000000000, n = 585) the
			// multiply overflows for every i >= 293 -- `int64(293)*8760h` is
			// -9206696073709551616 against a true 9240048000000000000, 292 of
			// the 585 -- and the series is right anyway because Go's signed
			// arithmetic is MODULAR, so the second wrap in `start + i*step`
			// cancels the first whenever the true sum is representable, which
			// the exact `n` guarantees. That makes the old walk unkillable by
			// mutation, not correct by construction: it is one arithmetic
			// identity away from wrong. This costs one compare per bucket and
			// is the only thing between a future change to `n` and the wrapped
			// series entry 132 measured.
			break
		}
		t = next
	}
	return hs
}

// formatTime renders a row timestamp the way the wire format does, for the
// pipes that read _time as a value.
func formatTime(t int64) string {
	return time.Unix(0, t).UTC().Format(time.RFC3339Nano)
}

// maxHitsBuckets caps a hits response so a one-second step over a year cannot
// be asked to allocate 31 million buckets.
const maxHitsBuckets = 100_000

// Histogram buckets match counts by time at the given step (nanoseconds),
// the /select/logsql/hits shape -- again without materializing rows.
func Histogram(s Store, q *Query, step int64) map[int64]int {
	sn3 := snapshotOf(s, q.From, q.To)
	defer sn3.Close()
	groups := sn3.Groups
	survivors := groups[:0]
	for _, g := range groups {
		if q.exceeded(0) {
			break
		}
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
		// The deadline, checked per group. These paths return counts and
		// facets rather than rows, so MaxBytes has nothing to measure --
		// but a scan of every group is exactly what the wall-clock budget
		// exists to bound, and until this went in twelve read routes ran
		// with no bound at all.
		if q.exceeded(0) {
			break
		}
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
	// Scratch: each time is bucketed into out and nothing keeps the slice.
	tp, ts := groupTimestamps(g, lo, hi)
	defer releaseTs(tp)
	sel.ForEach(func(i int) {
		out[alignDown(ts[i-lo], step)]++
	})
}

// alignDown snaps an instant down to a multiple of step: the bucket it belongs
// to.
//
// IT FLOORS RATHER THAN TRUNCATING TOWARD ZERO, AND THE DIFFERENCE IS A LOST
// COUNT. This was `ts/step*step` here and `from - from%step` in fillHits, both
// of which truncate toward zero, so the bucket keyed 0 spanned (-step, +step)
// -- rows on BOTH sides of the epoch -- while every other bucket spanned
// [k*step, (k+1)*step). A window that ends before the epoch never reaches key
// 0, because the walk stops at `to`. Measured on two rows inside one
// pre-epoch window, both at HTTP 200:
//
//	/select/logsql/hits?query=*&start=1969-12-31T00:00:00Z
//	    &end=1969-12-31T23:59:00Z&step=1h    24 buckets, TOTAL 1
//	/select/logsql/query, the same window                  2 rows
//
// The row at -1800e9 keyed to bucket 0, the walk ran `t < to` with
// `to = -60e9`, and the count vanished from Timestamps, Values AND Total. Entry
// 132 recorded these two sites as truncating "the same way, so no count is
// lost to a mismatch"; they do truncate the same way and a count is lost
// anyway, because the mismatch is not between the two sites -- it is between
// the key and the window.
//
// Post-epoch instants are unchanged: for t >= 0 the floor and the truncation
// are the same value, which is every window a deployment actually queries.
//
// THE BOTTOM OF THE DOMAIN HAS NO ALIGNED BUCKET, and that corner is reachable
// from a default window: `defaultWindowFrom` is MinInt64, and MinInt64 is not
// a multiple of any ordinary step, so its floor is below the domain. Both
// callers clamp to the smallest multiple of step int64 can hold, so both agree
// on one key for the whole partial bucket at the bottom and no count is lost
// there either. A row that near 1677-09-21T00:12:43Z is keyed to a bucket that
// starts after it, which is the only alternative to keying it to a bucket that
// does not exist.
func alignDown(t, step int64) int64 {
	r := t % step
	if r < 0 {
		r += step
	}
	if t-r > t {
		// The subtraction fell below MinInt64 and wrapped. MinInt64/step
		// truncates toward zero, so this is the smallest representable
		// multiple of step.
		return math.MinInt64 / step * step
	}
	return t - r
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
	// ONE DISPATCH, `leafBitset`'s. This loop used to be a second copy of it,
	// and the copy was missing a case:
	//
	//	if isTimePred(p.Kind) { return timePredBitset(g, p, n) }
	//
	// `_time` is a ColTimestamp, not a ColDict, so `DictIndices` returns nil
	// for it and `predBitsetCol` matched nothing. `ParseLogsQL` puts a bare
	// `_time:` filter in `q.Preds` rather than `q.Filter`, so the copy without
	// the case is the one that ran, and every surface reading through this
	// answered EMPTY for a query whose filter matches every row:
	//
	//	/select/logsql/hits            values [0,0,0]   control [10,10,10]
	//	/select/logsql/field_values    []               3 values
	//	/select/logsql/field_names     []               the full list
	//	/select/logsql/facets          only _time       all fields
	//	/select/logsql/streams         []               1
	//	/select/logsql/stats_query     result: []       c=30
	//	| top 2 by (level)             empty            2 rows
	//	| uniq by (level)              empty            3 rows
	//
	// all at HTTP 200, and `query.Count` with them -- which is what an alert
	// rule evaluates, so a rule whose query carries a `_time:` filter never
	// fired. The same filter with no stats pipe returned all 30 rows, because
	// that path goes through `leafBitset`.
	//
	// Calling it rather than adding the missing case: a second copy of a
	// dispatch is a case waiting to be missed again.
	for i := range q.Preds {
		sel.And(leafBitset(g, &q.Preds[i], n))
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

// SetWindow resolves a query's window and MARKS it resolved.
//
// The three assignments belong together and were made separately in five
// places. Two of them missed `ToSet`, and the miss is not inert: with it false
// the narrowing in `resolveTimePreds` stops being narrowing-only and lets an
// absolute `_time:` filter widen the window past the end the caller asked for.
// A stats_query over [12:00:00, 12:00:10] with `_time:[12:00:00, 12:00:30]`
// answered 30 rows instead of 10, and a three-bucket range came back
// [30, 20, 10] where every bucket holds 10 -- buckets that are not buckets,
// at HTTP 200.
//
// So there is one way to set a window now. A caller that genuinely means "no
// end I know of" leaves the fields alone and gets the old behaviour, which is
// what a programmatic constructor with a `_time:` filter and no window wants.
func (q *Query) SetWindow(from, to int64) {
	q.From, q.To, q.ToSet = from, to, true
}

// SetNow records the request time relative _time filters resolve against.
//
// The setter exists so the flag cannot be forgotten, which is the whole
// failure mode NowSet is there to prevent -- and the reason SetWindow exists
// for the pair beside it.
func (q *Query) SetNow(now int64) {
	q.Now, q.NowSet = now, true
}
