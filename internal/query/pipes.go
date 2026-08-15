package query

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// The pipe language: after the filter selects rows, pipes transform them --
// LogsQL's `filter | stats ... | sort ... | limit N`. stats is the heavy one
// and aggregates during the group scan (never materializing the matched rows);
// the row pipes (sort, limit, fields) run on whatever row set precedes them.

// Pipe transforms a row set. Stats is a source pipe (it consumes the scan, not
// a prior row set) and is handled specially by RunPipeline.
type Pipe interface {
	apply(rows []Row) []Row
}

// AggKind is a stats aggregation function.
type AggKind uint8

const (
	AggCount AggKind = iota
	AggSum
	AggAvg
	AggMin
	AggMax
	AggUniq
	AggCountUniq
	AggQuantile
	AggValues     // values(field): JSON array of every non-empty value in the group
	AggUniqValues // uniq_values(field): sorted JSON array of the distinct values
	AggSumLen     // sum_len(field): sum of len(value) over the group
	AggCountEmpty // count_empty(field): rows where the field is empty or missing
	AggRowAny     // row_any(field): an arbitrary value of the field in the group
	AggHistogram  // histogram(field): VM-style bucketed distribution as JSON
	AggRowMin     // row_min(sort, out): out value of the row with minimal sort
	AggRowMax     // row_max(sort, out): out value of the row with maximal sort
	AggRate       // rate(): group count per second over the query window
	AggRateSum    // rate_sum(field): group sum per second over the query window
)

// Agg is one aggregation in a stats pipe.
type Agg struct {
	Field  string // "" for count(); the sort field for row_min/row_max
	Field2 string // the output field for row_min/row_max
	Alias  string
	If     *Expr   // optional `if (<filter>)`: skip a sample the row does not satisfy
	P      float64 // percentile for quantile() (0..1)
	Kind   AggKind
}

// StatsPipe is `stats by (fields) agg1, agg2, ...`.
type StatsPipe struct {
	By       []string
	Aggs     []Agg
	rangeSec float64 // query-window seconds, stamped at run for rate()/rate_sum()
	// q is the running query, stamped at run so the aggregate can refuse a key
	// space bigger than its ceiling. Not a plain int: the refusal has to be
	// RECORDED, and a pipe that returned a truncated map would be the silent
	// answer-change this exists to stop.
	q *Query
}

// apply aggregates a materialized row stream -- stats used mid-pipe (e.g. after
// collapse_nums or a filter). A leading stats instead runs during the scan via
// RunPipeline/runStats and never reaches here.
func (p *StatsPipe) apply(rows []Row) []Row {
	acc := map[string]*statEntry{}
	var order []string
	var key []byte
	for _, r := range rows {
		key = key[:0]
		for _, f := range p.By {
			key = append(key, rowField(r, f)...)
			key = append(key, 0)
		}
		e := acc[string(key)]
		if e == nil {
			by := make([]string, len(p.By))
			for j, f := range p.By {
				by[j] = rowField(r, f)
			}
			e = newStatEntry(by, p.Aggs)
			acc[string(key)] = e
			order = append(order, string(key))
			if tooManyKeys(p.q, len(order), "stats by") {
				return nil
			}
		}
		accSample(e, p.Aggs,
			func(j int) string { return rowField(r, p.Aggs[j].Field) },
			func(j int) string { return rowField(r, p.Aggs[j].Field2) },
			func(name string) string { return rowField(r, name) })
	}
	out := make([]Row, 0, len(order))
	for _, k := range order {
		out = append(out, statEntryRow(p, acc[k]))
	}
	return out
}

// newStatEntry allocates an entry with the sets the uniq aggregations need.
func newStatEntry(by []string, aggs []Agg) *statEntry {
	e := &statEntry{by: by, slots: make([]statSlot, len(aggs))}
	for j := range aggs {
		if aggs[j].Kind == AggUniq || aggs[j].Kind == AggCountUniq || aggs[j].Kind == AggUniqValues {
			e.slots[j].set = map[string]struct{}{}
		}
	}
	return e
}

// accSample folds one sample into an entry: valOf(j) is the value of
// aggregation j's field on this sample, valOf2(j) its second field (row_min/max
// output). Shared by the during-scan and mid-pipe stats so the two never drift.
func accSample(e *statEntry, aggs []Agg, valOf func(j int) string, valOf2 func(j int) string, getField func(name string) string) {
	e.rows++
	for j := range aggs {
		a := &aggs[j]
		sl := &e.slots[j]
		if a.If != nil && !exprMatchesRow(a.If, getField) {
			continue // conditional aggregate: this row does not qualify for agg j
		}
		if a.Kind == AggCount || a.Kind == AggRate {
			sl.n++
			continue
		}
		if a.Kind == AggRowMin || a.Kind == AggRowMax {
			f, err := strconv.ParseFloat(valOf(j), 64)
			if err != nil {
				continue
			}
			if !sl.bestSet || (a.Kind == AggRowMin && f < sl.bestF) || (a.Kind == AggRowMax && f > sl.bestF) {
				sl.bestF, sl.bestStr, sl.bestSet = f, valOf2(j), true
			}
			continue
		}
		if a.Field == "" {
			continue // count()
		}
		val := valOf(j)
		if a.Kind == AggUniq { // uniq returns the values, so it stays exact
			sl.set[val] = struct{}{}
			continue
		}
		if a.Kind == AggCountUniq {
			if sl.hll != nil {
				sl.hll.add(val)
				continue
			}
			sl.set[val] = struct{}{}
			if len(sl.set) > hllThreshold { // spill the exact set into a bounded sketch
				sl.hll = newHLL()
				for v := range sl.set {
					sl.hll.add(v)
				}
				sl.set = nil
			}
			continue
		}
		if a.Kind == AggCountEmpty {
			if val == "" {
				sl.sum++
			}
			continue
		}
		if a.Kind == AggValues {
			if val != "" && len(sl.strs) < valuesCap {
				sl.strs = append(sl.strs, val)
			}
			continue
		}
		if a.Kind == AggRowAny {
			if len(sl.strs) == 0 && val != "" {
				sl.strs = append(sl.strs, val)
			}
			continue
		}
		if a.Kind == AggUniqValues {
			if val != "" {
				sl.set[val] = struct{}{}
			}
			continue
		}
		if a.Kind == AggSumLen {
			sl.sum += float64(len(val))
			continue
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			continue
		}
		if a.Kind == AggQuantile || a.Kind == AggHistogram {
			sl.vals = append(sl.vals, f)
			if len(sl.vals) >= quantileCap*2 { // bound RAM: thin the sorted samples by half
				sort.Float64s(sl.vals)
				j := 0
				for i := 0; i < len(sl.vals); i += 2 {
					sl.vals[j] = sl.vals[i]
					j++
				}
				sl.vals = sl.vals[:j]
			}
		}
		if !sl.has || f < sl.min {
			sl.min = f
		}
		if !sl.has || f > sl.max {
			sl.max = f
		}
		sl.sum += f
		sl.cnt++
		sl.has = true
	}
}

// statEntryRow renders one group-by entry as an output row.
func statEntryRow(sp *StatsPipe, e *statEntry) Row {
	fields := make([]Field, 0, len(sp.By)+len(sp.Aggs))
	for j, f := range sp.By {
		fields = append(fields, Field{f, e.by[j]})
	}
	for j := range sp.Aggs {
		fields = append(fields, Field{sp.Aggs[j].Alias, formatAgg(&sp.Aggs[j], &e.slots[j], e.rows, sp.rangeSec)})
	}
	return Row{NoTime: true, Fields: fields}
}

// SortPipe is `sort by (fields) [desc] [limit N]`.
type SortPipe struct {
	By    []string
	Limit int
	Desc  bool
}

// LimitPipe is `limit N` / `head N`.
type LimitPipe struct{ N int }

// FieldsPipe is `fields a, b` -- keep only these, in that order.
type FieldsPipe struct{ Keep []string }

// UniqPipe is `uniq by (fields) [limit N]` -- distinct rows by those fields.
type UniqPipe struct {
	By    []string
	Limit int
	q     *Query // stamped at run; see StatsPipe.q
}

// TopPipe is `top N (fields)` -- the N most frequent field-tuples with counts.
type TopPipe struct {
	By []string
	N  int
	q  *Query // stamped at run; see StatsPipe.q
}

// TailPipe is `tail N` -- the last N rows.
type TailPipe struct{ N int }

// OffsetPipe is `offset N` -- skip the first N rows.
type OffsetPipe struct{ N int }

// RenamePipe is `rename a as b, c as d`.
type RenamePipe struct{ From, To []string }

// DeletePipe is `delete a, b` / `drop a, b` -- remove those fields.
type DeletePipe struct{ Drop []string }

// FilterPipe is `filter <expr>` -- a filter applied mid-pipe over the row
// stream (e.g. after stats: `... | stats by(x) count() c | filter c:>10`).
type FilterPipe struct{ Expr *Expr }

// statSlot accumulates one aggregation for one group-by key.
type statSlot struct {
	sum, min, max float64
	n             int64 // count() for this slot (per-agg, so `count() if (...)` works)
	cnt           int64 // numeric samples (avg denominator)
	set           map[string]struct{}
	hll           *hyperLogLog // count_uniq once the set exceeds hllThreshold (bounded RAM)
	vals          []float64    // numeric samples for quantile()/histogram() (exact/bounded)
	strs          []string     // collected values for values() (bounded by valuesCap)
	bestF         float64      // row_min/row_max: the extremal sort value
	bestStr       string       // row_min/row_max: the out value at the extremal row
	bestSet       bool         // row_min/row_max: whether a sample has been seen
	has           bool
}

// valuesCap bounds how many values a single values() slot collects, so one
// group-by key over a billion rows cannot OOM. uniq_values() is bounded by
// cardinality via the set, not this.
const valuesCap = 10000

// hllThreshold is where count_uniq switches from an exact set to HyperLogLog:
// below it the answer is exact; above it RAM is bounded at ~16KB regardless of
// cardinality (vs an unbounded set that OOMs at billion-row scale).
const hllThreshold = 8192

// quantileCap bounds a quantile aggregation's sample buffer: past 2x this the
// sorted samples are thinned by half (keep every other), which preserves the
// distribution's shape -- and thus the quantile -- while keeping RAM bounded no
// matter how many rows a group-by key spans. Exact below the cap.
const quantileCap = 65536

// statEntry is one group-by key's accumulators.
type statEntry struct {
	by    []string
	slots []statSlot
	rows  int64 // count()
}

// tooManyKeys reports whether an aggregate has outgrown its key ceiling, and
// records why if it has.
//
// Checked as keys are ADDED rather than on the finished map: the map is the
// memory the ceiling exists to bound, so noticing after it is built is
// noticing after the cost. A nil query means an internal caller with no
// budget, which is unbounded on purpose.
func tooManyKeys(q *Query, have int, what string) bool {
	if q == nil || q.maxGroupKeys <= 0 || have <= q.maxGroupKeys {
		return false
	}
	q.stop(fmt.Errorf("%w: %s produced more than %d distinct groups",
		ErrTooManyGroupKeys, what, q.maxGroupKeys))
	return true
}

// RunPipeline runs q's filter and pipes. A leading stats pipe aggregates
// during the scan; otherwise the filter's rows feed the pipe chain.
func RunPipeline(s Store, q *Query) []Row {
	resolveTimePreds(q)     // relative _time -> absolute before stats or Run see the window
	resolveSubqueries(s, q) // in(<subquery>) -> value set, before the filter evaluates
	rangeSec := float64(q.To-q.From) / 1e9
	for _, p := range q.Pipes {
		// The window, for rate()/rate_sum(); and the running query, so a pipe
		// that builds state proportional to CARDINALITY can stop when it
		// blows its ceiling instead of returning a map it silently truncated.
		switch pp := p.(type) {
		case *StatsPipe:
			pp.rangeSec = rangeSec
			pp.q = q
		case *UniqPipe:
			pp.q = q
		case *TopPipe:
			pp.q = q
		}
	}
	var rows []Row
	pipes := q.Pipes
	if len(pipes) > 0 {
		// Only a projecting pipe chain skips the full-record materialize. A chain
		// that rewrites or slices rows (delete/rename/limit/...) still returns whole
		// records, and clearing MatAll for those was dropping every field but _time.
		if PipesProject(pipes) {
			q.MatAll = false
		}
		switch p0 := pipes[0].(type) {
		case *StatsPipe:
			rows = runStats(s, q, p0)
			pipes = pipes[1:]
		case *TopPipe:
			if r, ok := runTopFast(s, q, p0); ok {
				rows, pipes = r, pipes[1:]
			}
		case *UniqPipe:
			if r, ok := runUniqFast(s, q, p0); ok {
				rows, pipes = r, pipes[1:]
			}
		case *LimitPipe:
			// Push the bound into the scan so it stops after N rows instead of
			// materializing the whole match set and slicing it. Limit (not
			// MaxRows) because the rows are kept, not just counted.
			if p0.N >= 0 && (q.Limit == 0 || p0.N < q.Limit) {
				q.Limit = p0.N
			}
		case *FieldValuesPipe:
			rows = runFieldValues(s, q, p0)
			pipes = pipes[1:]
		case *FieldNamesPipe:
			rows = runFieldNames(s, q)
			pipes = pipes[1:]
		case *FacetsPipe:
			rows = runFacets(s, q, p0)
			pipes = pipes[1:]
		case *BlocksCountPipe:
			rows = runBlocksCount(s, q)
			pipes = pipes[1:]
		case *BlockStatsPipe:
			rows = runBlockStats(s, q)
			pipes = pipes[1:]
		}
	}
	if rows == nil {
		q.Materialize = pipeFields(pipes) // so the row pipes see their fields
		rows = Run(s, q)
	}
	for _, p := range pipes {
		// Between pipes, not only during the scan. A sort or a join over a
		// large result is a phase that runs for as long as the result is big,
		// with no group boundary in it -- so a query cancelled during one used
		// to run to completion and then discover nobody was waiting.
		//
		// Between rather than inside: a pipe that stopped halfway would return
		// a partial result the executor then reports as complete, and the
		// point of the checkpoint is to end the query, not to truncate it.
		if q.exceeded(0) {
			return nil
		}
		switch pp := p.(type) {
		case *JoinPipe: // store-aware pipes run a subquery, so they cannot use apply(rows)
			rows = pp.run(s, q, rows)
		case *UnionPipe:
			rows = pp.run(s, q, rows)
		case *StreamContextPipe:
			rows = pp.run(s, q, rows)
		default:
			rows = p.apply(rows)
		}
		// After every pipe, because a pipe can GROW its input. A join whose
		// key is not unique on the right multiplies and a union appends, so a
		// result inside MaxRows on the way in is outside every budget on the
		// way out -- and MaxRows is checked in the scan, which has already
		// finished by here.
		if q.maxPipeRows > 0 && len(rows) > q.maxPipeRows {
			q.stop(fmt.Errorf("%w: %T produced %d rows, ceiling is %d",
				ErrPipeRowLimit, p, len(rows), q.maxPipeRows))
			return nil
		}
		// And the stop a pipe recorded itself -- cardinality, or a subquery
		// that blew its own budget. Returning the partial rows would be the
		// silent truncation this whole change is about.
		if q.stopErr() != nil {
			return nil
		}
	}
	return rows
}

// pipeFields is the set of fields the row pipes reference, so the filter's
// materialize step includes them (a `* | top (service)` needs service on the
// rows even though nothing filtered on it).
func pipeFields(pipes []Pipe) []string {
	seen := map[string]bool{}
	var out []string
	add := func(fs ...string) {
		for _, f := range fs {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	for _, p := range pipes {
		switch t := p.(type) {
		case *StatsPipe: // a mid-pipe stats needs its group-by and agg fields materialized
			add(t.By...)
			for _, a := range t.Aggs {
				if a.Field != "" {
					add(a.Field)
				}
				if a.Field2 != "" {
					add(a.Field2)
				}
				if a.If != nil {
					fs := map[string]bool{}
					filterFields(a.If, fs)
					for f := range fs {
						add(f)
					}
				}
			}
		case *SortPipe:
			add(t.By...)
		case *UniqPipe:
			add(t.By...)
		case *TopPipe:
			add(t.By...)
		case *RenamePipe:
			add(t.From...)
		case *FieldsPipe:
			add(t.Keep...)
		case *FilterPipe:
			fs := map[string]bool{}
			filterFields(t.Expr, fs)
			for f := range fs {
				add(f)
			}
		case *UnpackJSONPipe:
			add(orDefault(t.From, "_msg"))
		case *ExtractPipe:
			add(orDefault(t.From, "_msg"))
		case *FormatPipe:
			add(templateFields(t.Template)...)
		case *MathPipe:
			add(t.fields...)
		case *CollapseNumsPipe:
			add(orDefault(t.Field, "_msg"))
		case *RankPipe:
			add(orDefault(t.Field, "_msg"))
		case *UnpackLogfmtPipe:
			add(orDefault(t.From, "_msg"))
		case *ReplacePipe:
			add(orDefault(t.Field, "_msg"))
		case *CopyPipe:
			add(t.From...)
		case *LenPipe:
			add(t.Field)
		case *ExtractRegexpPipe:
			add(orDefault(t.From, "_msg"))
		case *DecolorizePipe:
			add(orDefault(t.Field, "_msg"))
		case *PackPipe:
			add(t.Fields...) // explicit fields from storage; pack-all uses whatever rows already carry
		case *UnrollPipe:
			add(t.Field)
		case *UnpackSyslogPipe:
			add(orDefault(t.From, "_msg"))
		case *JSONArrayLenPipe:
			add(t.Field)
		case *UnpackWordsPipe:
			add(orDefault(t.From, "_msg"))
		case *HashPipe:
			add(t.Field)
		case *JoinPipe:
			add(t.By...) // join keys must be on the outer rows
		}
	}
	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// templateFields returns the <name> placeholders referenced by a format
// template, so they are materialized before the pipe runs.
func templateFields(tpl string) []string {
	var fs []string
	for i := 0; i < len(tpl); {
		if tpl[i] == '<' {
			if j := strings.IndexByte(tpl[i:], '>'); j >= 0 {
				fs = append(fs, tpl[i+1:i+j])
				i += j + 1
				continue
			}
		}
		i++
	}
	return fs
}

// runStats aggregates matched rows by the group-by fields during the scan,
// accumulating each aggregation without building the matched rows.
func runStats(s Store, q *Query, sp *StatsPipe) []Row {
	// The ceiling applies to the scan-time path as much as to the mid-pipe
	// one. A leading `| stats by (...)` never reaches StatsPipe.apply, so a
	// bound written only there would cover the rarer of the two.
	sp.q = q
	// Fast path: `by (field) count()` is the footer posting counts --
	// StatsByField reads them from the offset table without a per-row scan
	// for whole-in-window groups (the 1078x trick). This is the common
	// top-N / group-by shape.
	if r, ok := runCountFast(s, q, sp); ok {
		return r
	}
	if len(sp.By) == 1 && len(sp.Aggs) == 1 && sp.Aggs[0].Kind == AggCount && sp.Aggs[0].If == nil {
		alias := sp.Aggs[0].Alias
		vcs := StatsByField(s, q, sp.By[0])
		if tooManyKeys(q, len(vcs), "stats by") {
			return nil
		}
		out := make([]Row, 0, len(vcs))
		for _, vc := range vcs {
			out = append(out, Row{NoTime: true, Fields: []Field{{sp.By[0], vc.Value}, {alias, strconv.Itoa(vc.Count)}}})
		}
		return out
	}
	// Fields referenced by any `if (<filter>)` on an aggregate, so they can be
	// decoded per group and read by the conditional check.
	ifFields := map[string]bool{}
	for i := range sp.Aggs {
		if sp.Aggs[i].If != nil {
			filterFields(sp.Aggs[i].If, ifFields)
		}
	}
	var overKeys bool // set by the cardinality ceiling inside sel.ForEach
	acc := map[string]*statEntry{}
	var key []byte
	sn1 := snapshotOf(s, q.From, q.To)
	defer sn1.Close()
	for _, g := range sn1.Groups {
		// The deadline, checked per group. These paths return counts and
		// facets rather than rows, so MaxBytes has nothing to measure --
		// but a scan of every group is exactly what the wall-clock budget
		// exists to bound, and until this went in twelve read routes ran
		// with no bound at all.
		if q.exceeded(0) {
			break
		}
		if !groupCanMatch(g, q) {
			continue
		}
		sel := matchBitset(g, q)
		if sel.Count() == 0 {
			continue
		}
		ifCol := map[string][]uint32{}
		ifDict := map[string][]string{}
		for f := range ifFields {
			ifCol[f], ifDict[f] = dictOrTime(g, f)
		}
		byCol := make([][]uint32, len(sp.By))
		byDict := make([][]string, len(sp.By))
		for j, f := range sp.By {
			byCol[j], byDict[j] = dictOrTime(g, f)
		}
		aggCol := make([][]uint32, len(sp.Aggs))
		aggDict := make([][]string, len(sp.Aggs))
		aggCol2 := make([][]uint32, len(sp.Aggs))
		aggDict2 := make([][]string, len(sp.Aggs))
		for j := range sp.Aggs {
			if sp.Aggs[j].Field != "" {
				aggCol[j], aggDict[j] = dictOrTime(g, sp.Aggs[j].Field)
			}
			if sp.Aggs[j].Field2 != "" {
				aggCol2[j], aggDict2[j] = dictOrTime(g, sp.Aggs[j].Field2)
			}
		}
		sel.ForEach(func(i int) {
			// ForEach takes a func with no return, so the ceiling sets a flag
			// the group loop reads rather than returning from inside it.
			if overKeys {
				return
			}
			key = key[:0]
			for j := range sp.By {
				if byCol[j] != nil {
					key = append(key, byDict[j][byCol[j][i]]...)
				}
				key = append(key, 0)
			}
			e := acc[string(key)]
			if e == nil {
				by := make([]string, len(sp.By))
				for j := range sp.By {
					if byCol[j] != nil {
						by[j] = byDict[j][byCol[j][i]]
					}
				}
				e = newStatEntry(by, sp.Aggs)
				acc[string(key)] = e
				if tooManyKeys(q, len(acc), "stats by") {
					overKeys = true
					return
				}
			}
			accSample(e, sp.Aggs, func(j int) string {
				if aggCol[j] != nil {
					return aggDict[j][aggCol[j][i]]
				}
				return ""
			}, func(j int) string {
				if aggCol2[j] != nil {
					return aggDict2[j][aggCol2[j][i]]
				}
				return ""
			}, func(name string) string {
				if c := ifCol[name]; c != nil {
					return ifDict[name][c[i]]
				}
				return ""
			})
		})
		if overKeys {
			return nil
		}
	}
	out := make([]Row, 0, len(acc))
	for _, e := range acc {
		out = append(out, statEntryRow(sp, e))
	}
	return out
}

func formatAgg(a *Agg, sl *statSlot, rows int64, rangeSec float64) string {
	switch a.Kind {
	case AggCount:
		return strconv.FormatInt(sl.n, 10)
	case AggRate:
		if rangeSec <= 0 {
			return "0"
		}
		return trimFloat(float64(sl.n) / rangeSec)
	case AggRateSum:
		if rangeSec <= 0 {
			return "0"
		}
		return trimFloat(sl.sum / rangeSec)
	case AggSum:
		return trimFloat(sl.sum)
	case AggAvg:
		if sl.cnt == 0 {
			return "0"
		}
		return trimFloat(sl.sum / float64(sl.cnt))
	case AggMin:
		return trimFloat(sl.min)
	case AggMax:
		return trimFloat(sl.max)
	case AggUniq:
		return strconv.Itoa(len(sl.set))
	case AggCountUniq:
		if sl.hll != nil {
			return strconv.Itoa(sl.hll.count())
		}
		return strconv.Itoa(len(sl.set))
	case AggQuantile:
		return trimFloat(quantileOf(sl.vals, a.P))
	case AggValues:
		return jsonStrArray(sl.strs)
	case AggUniqValues:
		keys := make([]string, 0, len(sl.set))
		for k := range sl.set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return jsonStrArray(keys)
	case AggSumLen:
		return trimFloat(sl.sum)
	case AggCountEmpty:
		return trimFloat(sl.sum)
	case AggRowAny:
		if len(sl.strs) > 0 {
			return sl.strs[0]
		}
		return ""
	case AggRowMin, AggRowMax:
		return sl.bestStr
	case AggHistogram:
		return histogramJSON(sl.vals)
	}
	return ""
}

// VictoriaMetrics standard histogram buckets: 18 per decade from 1e-9 to 1e18.
const (
	histBucketsPerDecimal = 18
	histE10Min            = -9
	histE10Max            = 18
	histBucketsCount      = (histE10Max - histE10Min) * histBucketsPerDecimal
)

var (
	histBucketMultiplier = math.Pow(10, 1.0/histBucketsPerDecimal)
	histRanges           = buildHistRanges()
)

func buildHistRanges() []string {
	r := make([]string, histBucketsCount)
	v := math.Pow(10, histE10Min)
	start := fmt.Sprintf("%.3e", v)
	for i := 0; i < histBucketsCount; i++ {
		v *= histBucketMultiplier
		end := fmt.Sprintf("%.3e", v)
		r[i] = start + "..." + end
		start = end
	}
	return r
}

// histBucketIdx returns the VM bucket index for v (>0), or -1.
func histBucketIdx(v float64) int {
	if v <= 0 {
		return -1
	}
	bf := (math.Log10(v) - histE10Min) * histBucketsPerDecimal
	idx := int(bf)
	if bf == float64(idx) { // a lower boundary belongs to the previous bucket
		idx--
	}
	if idx < 0 {
		idx = 0
	}
	if idx >= histBucketsCount {
		idx = histBucketsCount - 1
	}
	return idx
}

// histogramJSON buckets vals into the VM ranges and renders the non-empty ones
// in ascending order as [{"vmrange":"lo...hi","hits":n}], matching VL.
func histogramJSON(vals []float64) string {
	counts := map[int]int{}
	for _, v := range vals {
		if idx := histBucketIdx(v); idx >= 0 {
			counts[idx]++
		}
	}
	idxs := make([]int, 0, len(counts))
	for i := range counts {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	var b strings.Builder
	b.WriteByte('[')
	for k, i := range idxs {
		if k > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"vmrange":%q,"hits":%d}`, histRanges[i], counts[i])
	}
	b.WriteByte(']')
	return b.String()
}

// jsonStrArray renders values() / uniq_values() output as a JSON array string,
// matching VictoriaLogs. An empty slot is "[]", never "null".
func jsonStrArray(vals []string) string {
	if len(vals) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(vals)
	return string(b)
}

func trimFloat(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// quantileOf returns the p-quantile (0..1) of vals by linear interpolation
// between the two nearest ranks -- exact, not sketched. It sorts in place; the
// slice is the aggregation's own sample buffer, discarded after.
func quantileOf(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	if p <= 0 {
		return vals[0]
	}
	if p >= 1 {
		return vals[len(vals)-1]
	}
	// Nearest-rank, NOT linear interpolation: VictoriaLogs' quantile returns a
	// value that is actually in the data (measured against it -- interpolation
	// gave p50=290.5 where VL gives 291).
	idx := int(p * float64(len(vals)))
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return vals[idx]
}

// rowField returns a row's value for key.
func rowField(r Row, key string) string {
	for _, f := range r.Fields {
		if f.Key == key {
			return f.Value
		}
	}
	return ""
}

func lessVal(a, b string) bool {
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	if ea == nil && eb == nil {
		return fa < fb
	}
	return a < b
}

func (p *SortPipe) apply(rows []Row) []Row {
	sort.SliceStable(rows, func(a, b int) bool {
		for _, f := range p.By {
			va, vb := rowField(rows[a], f), rowField(rows[b], f)
			if va == vb {
				continue
			}
			less := lessVal(va, vb)
			if p.Desc {
				return !less
			}
			return less
		}
		return false
	})
	if p.Limit > 0 && len(rows) > p.Limit {
		rows = rows[:p.Limit]
	}
	return rows
}

func (p *LimitPipe) apply(rows []Row) []Row {
	if p.N >= 0 && len(rows) > p.N {
		return rows[:p.N]
	}
	return rows
}

func (p *FieldsPipe) apply(rows []Row) []Row {
	keep := make(map[string]bool, len(p.Keep))
	for _, k := range p.Keep {
		keep[k] = true
	}
	// _time is an ordinary field for projection: `fields a, b` drops it unless
	// asked for, which is what VictoriaLogs does.
	dropTime := !keep["_time"]
	for i := range rows {
		if dropTime {
			rows[i].NoTime = true
		}
		nf := rows[i].Fields[:0]
		for _, f := range rows[i].Fields {
			if keep[f.Key] {
				nf = append(nf, f)
			}
		}
		rows[i].Fields = nf
	}
	return rows
}

func rowKey(r Row, by []string) string {
	var b strings.Builder
	for _, f := range by {
		b.WriteString(rowField(r, f))
		b.WriteByte(0)
	}
	return b.String()
}

func (p *UniqPipe) apply(rows []Row) []Row {
	seen := make(map[string]bool, len(rows))
	out := rows[:0]
	for _, r := range rows {
		k := rowKey(r, p.By)
		if seen[k] {
			continue
		}
		seen[k] = true
		if tooManyKeys(p.q, len(seen), "uniq by") {
			return nil
		}
		if len(p.By) == 0 {
			out = append(out, r)
			continue
		}
		// VictoriaLogs emits just the distinct combination of the `by` fields --
		// no timestamp and none of the row's other fields.
		f := make([]Field, 0, len(p.By))
		for _, name := range p.By {
			f = append(f, Field{name, rowField(r, name)})
		}
		out = append(out, Row{NoTime: true, Fields: f})
	}
	if p.Limit > 0 && len(out) > p.Limit {
		out = out[:p.Limit]
	}
	return out
}

func (p *TopPipe) apply(rows []Row) []Row {
	cnt := map[string]int{}
	vals := map[string][]string{}
	for _, r := range rows {
		k := rowKey(r, p.By)
		if cnt[k] == 0 {
			v := make([]string, len(p.By))
			for i, f := range p.By {
				v[i] = rowField(r, f)
			}
			vals[k] = v
			if tooManyKeys(p.q, len(vals), "top by") {
				return nil
			}
		}
		cnt[k]++
	}
	out := make([]Row, 0, len(cnt))
	for k, c := range cnt {
		fields := make([]Field, 0, len(p.By)+1)
		for i, f := range p.By {
			fields = append(fields, Field{f, vals[k][i]})
		}
		// VictoriaLogs names the column `hits`, and breaks count ties by the
		// grouped value ascending (measured against it).
		fields = append(fields, Field{"hits", strconv.Itoa(c)})
		out = append(out, Row{NoTime: true, Fields: fields})
	}
	sort.SliceStable(out, func(a, b int) bool {
		ca, _ := strconv.Atoi(rowField(out[a], "hits"))
		cb, _ := strconv.Atoi(rowField(out[b], "hits"))
		if ca != cb {
			return ca > cb
		}
		for _, f := range p.By {
			va, vb := rowField(out[a], f), rowField(out[b], f)
			if va != vb {
				return va < vb
			}
		}
		return false
	})
	if p.N > 0 && len(out) > p.N {
		out = out[:p.N]
	}
	return out
}

func (p *TailPipe) apply(rows []Row) []Row {
	if p.N >= 0 && len(rows) > p.N {
		return rows[len(rows)-p.N:]
	}
	return rows
}

func (p *OffsetPipe) apply(rows []Row) []Row {
	if p.N >= len(rows) {
		return rows[:0]
	}
	if p.N > 0 {
		return rows[p.N:]
	}
	return rows
}

func (p *RenamePipe) apply(rows []Row) []Row {
	m := make(map[string]string, len(p.From))
	for i := range p.From {
		m[p.From[i]] = p.To[i]
	}
	for ri := range rows {
		for fi := range rows[ri].Fields {
			if nn, ok := m[rows[ri].Fields[fi].Key]; ok {
				rows[ri].Fields[fi].Key = nn
			}
		}
	}
	return rows
}

func (p *FilterPipe) apply(rows []Row) []Row {
	out := rows[:0]
	for _, r := range rows {
		if matchRow(r, p.Expr) {
			out = append(out, r)
		}
	}
	return out
}

// CollapseNumsPipe is `collapse_nums [at field]` (digits -> <N>, VictoriaLogs'
// semantics) or `pattern [at field]` (Full: also hex/uuid tokens -> <ID>, a
// drain-style templater). Either way `stats by (_msg) count()` after it mines
// the top log patterns.
type CollapseNumsPipe struct {
	Field string
	Full  bool
}

func (p *CollapseNumsPipe) apply(rows []Row) []Row {
	f := p.Field
	if f == "" {
		f = "_msg"
	}
	for ri := range rows {
		setRowField(&rows[ri], f, templatize(rowField(rows[ri], f), p.Full))
	}
	return rows
}

// templatize collapses variable tokens to placeholders. Full also collapses
// hex/uuid-ish identifiers (before the digit pass, so their digits do not leak).
func templatize(s string, full bool) string {
	if full {
		s = collapseHexTokens(s)
	}
	return collapseNums(s)
}

// collapseNums replaces every maximal run of ASCII digits with "<N>".
func collapseNums(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	inNum := false
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			if !inNum {
				sb.WriteString("<N>")
				inNum = true
			}
			continue
		}
		inNum = false
		sb.WriteByte(s[i])
	}
	return sb.String()
}

// collapseHexTokens replaces alphanumeric tokens that look like hex identifiers
// (>=8 chars, all hex, mixing a letter and a digit -- request ids, hashes, uuid
// segments) with <ID>. Pure-digit tokens are left for collapseNums.
func collapseHexTokens(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		if !isAlnum(s[i]) {
			sb.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for j < len(s) && isAlnum(s[j]) {
			j++
		}
		tok := s[i:j]
		if isHexID(tok) {
			sb.WriteString("<ID>")
		} else {
			sb.WriteString(tok)
		}
		i = j
	}
	return sb.String()
}

func isAlnum(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isHexID(tok string) bool {
	if len(tok) < 8 {
		return false
	}
	hasDigit, hasLetter := false, false
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F':
			hasLetter = true
		default:
			return false // non-hex char -> not an id
		}
	}
	return hasDigit && hasLetter
}

// RankPipe is `rank "term1 term2" [at field]`: score each row by how often the
// terms appear in the field (default _msg), write _score, and sort by it
// descending -- relevance ranking of full-text matches, which VictoriaLogs
// (time-ordered only) does not offer.
type RankPipe struct {
	Terms []string
	Field string
}

func (p *RankPipe) apply(rows []Row) []Row {
	f := p.Field
	if f == "" {
		f = "_msg"
	}
	for ri := range rows {
		v := rowField(rows[ri], f)
		score := 0
		for _, t := range p.Terms {
			score += strings.Count(v, t)
		}
		setRowField(&rows[ri], "_score", strconv.Itoa(score))
	}
	sort.SliceStable(rows, func(a, b int) bool {
		sa, _ := strconv.Atoi(rowField(rows[a], "_score"))
		sb, _ := strconv.Atoi(rowField(rows[b], "_score"))
		return sa > sb
	})
	return rows
}

// setRowField updates key in place, or appends it -- the mutation the
// transform pipes (unpack_json/format/extract) share.
func setRowField(r *Row, key, val string) {
	for i := range r.Fields {
		if r.Fields[i].Key == key {
			r.Fields[i].Value = val
			return
		}
	}
	r.Fields = append(r.Fields, Field{key, val})
}

// UnpackJSONPipe is `unpack_json [from field] [prefix p]`: parse the (JSON
// object) source field and add each of its keys as a field. Default source is
// _msg.
type UnpackJSONPipe struct {
	From   string
	Prefix string
}

func (p *UnpackJSONPipe) apply(rows []Row) []Row {
	from := p.From
	if from == "" {
		from = "_msg"
	}
	for ri := range rows {
		var m map[string]json.RawMessage
		if json.Unmarshal([]byte(rowField(rows[ri], from)), &m) != nil {
			continue // not a JSON object; leave the row unchanged
		}
		for k, raw := range m {
			setRowField(&rows[ri], p.Prefix+k, jsonRawScalar(raw))
		}
	}
	return rows
}

// jsonRawScalar renders a JSON value as a plain string (a string unquoted,
// anything else as its source bytes).
func jsonRawScalar(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// FormatPipe is `format "template" as field`: build a field from a template
// whose <name> placeholders are replaced by field values.
type FormatPipe struct {
	Template string
	As       string
}

func (p *FormatPipe) apply(rows []Row) []Row {
	as := p.As
	if as == "" {
		as = "_msg"
	}
	for ri := range rows {
		setRowField(&rows[ri], as, expandTemplate(p.Template, rows[ri]))
	}
	return rows
}

// expandTemplate replaces each <name> in tpl with rowField(name); literal text
// is copied through. An unclosed < is copied literally.
func expandTemplate(tpl string, r Row) string {
	var sb strings.Builder
	for i := 0; i < len(tpl); {
		if tpl[i] == '<' {
			if j := strings.IndexByte(tpl[i:], '>'); j >= 0 {
				sb.WriteString(rowField(r, tpl[i+1:i+j]))
				i += j + 1
				continue
			}
		}
		sb.WriteByte(tpl[i])
		i++
	}
	return sb.String()
}

// ExtractPipe is `extract "pattern" [from field]`: pull fields out of the
// source (default _msg) with a pattern of literal text and <name> captures,
// each capture running up to the next literal.
type ExtractPipe struct {
	From    string
	Pattern string
}

// extractTok is one pattern token: a literal (cap == "") or a capture.
type extractTok struct {
	lit string
	cap string
}

func parseExtractPattern(pat string) []extractTok {
	var toks []extractTok
	for i := 0; i < len(pat); {
		if pat[i] == '<' {
			e := strings.IndexByte(pat[i:], '>')
			if e < 0 {
				toks = append(toks, extractTok{lit: pat[i:]})
				break
			}
			toks = append(toks, extractTok{cap: pat[i+1 : i+e]})
			i += e + 1
			continue
		}
		nb := strings.IndexByte(pat[i:], '<')
		if nb < 0 {
			toks = append(toks, extractTok{lit: pat[i:]})
			break
		}
		toks = append(toks, extractTok{lit: pat[i : i+nb]})
		i += nb
	}
	return toks
}

func (p *ExtractPipe) apply(rows []Row) []Row {
	from := p.From
	if from == "" {
		from = "_msg"
	}
	toks := parseExtractPattern(p.Pattern)
	for ri := range rows {
		src := rowField(rows[ri], from)
		for ti := 0; ti < len(toks); ti++ {
			t := toks[ti]
			if t.cap == "" { // literal: skip past it, or give up on this row
				idx := strings.Index(src, t.lit)
				if idx < 0 {
					break
				}
				src = src[idx+len(t.lit):]
				continue
			}
			// capture: up to the next literal, or to the end
			var next string
			if ti+1 < len(toks) {
				next = toks[ti+1].lit
			}
			if next == "" {
				setRowField(&rows[ri], t.cap, src)
				src = ""
				continue
			}
			end := strings.Index(src, next)
			if end < 0 {
				setRowField(&rows[ri], t.cap, src)
				break
			}
			setRowField(&rows[ri], t.cap, src[:end])
			src = src[end:]
		}
	}
	return rows
}

// matchRow evaluates a filter tree against one materialized row (string
// fields) -- the mid-pipe filter, distinct from evalExpr which runs over a
// group's columns.
func matchRow(r Row, e *Expr) bool {
	switch e.Op {
	case OpAnd:
		for _, k := range e.Kids {
			if !matchRow(r, k) {
				return false
			}
		}
		return true
	case OpOr:
		for _, k := range e.Kids {
			if matchRow(r, k) {
				return true
			}
		}
		return false
	case OpNot:
		return !matchRow(r, e.Child)
	default: // OpLeaf
		return matchPredRow(r, &e.Pred)
	}
}

func matchPredRow(r Row, p *Pred) bool {
	v := rowField(r, p.Field)
	switch p.Kind {
	case Eq:
		return v == p.Value
	case Contains:
		return strings.Contains(v, p.Value)
	case Prefix:
		return strings.HasPrefix(v, p.Value)
	case In:
		for _, x := range p.Values {
			if x == v {
				return true
			}
		}
		return false
	case Regexp:
		re := p.regex()
		return re != nil && re.MatchString(v)
	case Lt, Le, Gt, Ge:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
		return cmpNum(f, p.Kind, p.Num)
	}
	return false
}

func (p *DeletePipe) apply(rows []Row) []Row {
	drop := make(map[string]bool, len(p.Drop))
	for _, d := range p.Drop {
		drop[d] = true
	}
	dropTime := drop["_time"] // _time is deletable like any other field
	for ri := range rows {
		if dropTime {
			rows[ri].NoTime = true
		}
		nf := rows[ri].Fields[:0]
		for _, f := range rows[ri].Fields {
			if !drop[f.Key] {
				nf = append(nf, f)
			}
		}
		rows[ri].Fields = nf
	}
	return rows
}

// PipesProject reports whether a pipe chain narrows a row's field set. A chain
// that only slices or reorders rows (limit/head/tail/sort/offset) still returns
// whole records, so the engine must materialize every column for it -- matching
// VictoriaLogs, where `* | limit 5` returns five full records, not five
// timestamps.
func PipesProject(pipes []Pipe) bool {
	for _, p := range pipes {
		switch p.(type) {
		case *FieldsPipe, *StatsPipe, *UniqPipe, *TopPipe,
			*FieldValuesPipe, *FieldNamesPipe, *FacetsPipe,
			*BlocksCountPipe, *BlockStatsPipe:
			return true // narrows to a chosen field set or its own aggregate rows
		}
	}
	// Everything else -- limit/head/tail/sort/offset, and the row REWRITERS
	// (delete/rename/copy/format/math/extract/unpack_*/filter/replace) -- still
	// emits whole records, so the engine must materialize every column for them.
	return false
}
