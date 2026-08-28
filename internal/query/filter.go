package query

import (
	"strconv"
	"strings"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// evalExpr builds the bitset of rows in group g that match the boolean filter
// tree: AND intersects, OR unions, NOT complements against the group's rows.
// Each leaf is one predicate, evaluated by leafBitset. The bitset kernels
// (And/Or/AndNot over the packed words) do the set algebra, so a complex
// filter is still vectorized composition, not a per-row interpreter.
func evalExpr(g *storage.Reader, e *Expr, n int) *Bitset {
	switch e.Op {
	case OpLeaf:
		return leafBitset(g, &e.Pred, n)
	case OpAnd:
		b := NewBitset(n)
		b.SetAll()
		for _, k := range e.Kids {
			b.And(evalExpr(g, k, n))
		}
		return b
	case OpOr:
		b := NewBitset(n)
		for _, k := range e.Kids {
			b.Or(evalExpr(g, k, n))
		}
		return b
	case OpNot:
		b := NewBitset(n)
		b.SetAll()
		b.AndNot(evalExpr(g, e.Child, n))
		return b
	}
	return NewBitset(n)
}

// leafBitset evaluates one predicate. Equality takes the posting/scan chooser
// (rare value -> postings, common -> vectorized scan); every other kind scans
// the decoded dict indices against a per-dict-value hit mask.
func leafBitset(g *storage.Reader, p *Pred, n int) *Bitset {
	if isTimePred(p.Kind) {
		return timePredBitset(g, p, n)
	}
	if p.Kind == Eq {
		return eqPredBitset(g, p, n)
	}
	idx, dict := g.DictIndices(p.Field)
	return predBitsetCol(g, p, idx, dict, n)
}

// exprCanMatch is the footer-only prune for a filter tree: it returns false
// only when the group provably holds no matching row. That is decidable for
// an AND of equality leaves (any required value absent -> impossible); an OR
// branch or a non-equality leaf could still match, so those never reject.
func exprCanMatch(g *storage.Reader, e *Expr) bool {
	switch e.Op {
	case OpAnd:
		for _, k := range e.Kids {
			if !exprCanMatch(g, k) {
				return false
			}
		}
		return true
	case OpLeaf:
		p := &e.Pred
		if p.Kind == Eq && g.ColumnExists(p.Field) && !g.DictContains(p.Field, p.Value) {
			return false
		}
		return true
	default: // OpOr, OpNot -- cannot cheaply prove impossible
		return true
	}
}

// rowSource is where a per-row predicate test reads from.
//
// There are exactly two, and they are not interchangeable: the pipe evaluators
// hold a decoded Row -- fields AND a timestamp -- while the columnar stats scan
// holds neither, reading each field out of its dictionary column at the current
// index. That is why the field lookup is a function here and the Row is not
// simply passed everywhere.
//
// ONE STRUCT because there was ONE dispatch written TWICE, and each copy was
// missing what the other had. `predMatchesRow` (this file) had no time case, so
// `count() if (_time:...)` could not have worked; `matchPredRow` (pipes.go) had
// both time cases and was missing RangeNum, LenRange, StringRange, IContains,
// Seq, IPv4Range, StreamIDEq and the field comparisons -- so `| filter` answered
// ZERO rows at HTTP 200 for five predicate kinds the identical predicate answers
// correctly at the top level. Measured on two rows:
//
//	                                       top level   | filter
//	n:range(1, 10)                                 1         0
//	_msg:len_range(1, 5)                           2         0
//	ip:ipv4_range(10.0.0.0, 10.0.0.255)            1         0
//	_msg:seq("al", "ha")                           1         0
//	_msg:string_range(a, b)                        1         0
//	_msg:alpha              (control)              1         1
//	n:>10                   (control)              1         1
//
// docs/wrong.md entries 113, 122 and 127 each record a copy of one dispatch
// drifting from the other; this is the same shape with both copies wrong.
//
// Passed by value: no closure is built for the Row path, so the pipe evaluator
// allocates nothing per row.
type rowSource struct {
	row Row
	// get reads a field when there is no Row. Nil when hasRow is set.
	get    func(string) string
	hasRow bool
}

func rowOf(r Row) rowSource                    { return rowSource{row: r, hasRow: true} }
func lookupOf(g func(string) string) rowSource { return rowSource{get: g} }

func (s rowSource) field(name string) string {
	if s.hasRow {
		return rowField(s.row, name)
	}
	return s.get(name)
}

// stamp is the row's timestamp, and false when this source has none: a stats
// result, a projection that dropped `_time`, or the columnar scan, which reads
// dictionary columns and `_time` is not one. False rather than zero, because
// zero is 1970 and would make every such row match a range containing it.
func (s rowSource) stamp() (int64, bool) {
	if s.hasRow && !s.row.NoTime {
		return s.row.Time, true
	}
	return 0, false
}

// exprMatchesRow evaluates a filter tree against a single row. It is the scalar
// per-row form used by conditional aggregates (`... if (<filter>)`) and by
// `| filter`, where the group bitset is not available.
func exprMatchesRow(e *Expr, src rowSource) bool {
	if e == nil {
		return true
	}
	switch e.Op {
	case OpLeaf:
		return predMatchesRow(&e.Pred, src)
	case OpAnd:
		for _, k := range e.Kids {
			if !exprMatchesRow(k, src) {
				return false
			}
		}
		return true
	case OpOr:
		for _, k := range e.Kids {
			if exprMatchesRow(k, src) {
				return true
			}
		}
		return false
	case OpNot:
		return !exprMatchesRow(e.Child, src)
	}
	return false
}

// predMatchesRow tests one predicate against a single row.
func predMatchesRow(p *Pred, src rowSource) bool {
	// TIME IS NOT A FIELD ON A ROW. `_time` lives in Row.Time, not in
	// Row.Fields, so a field lookup returns "" and every comparison below
	// fails. Handled before the lookup, because the lookup is what cannot
	// answer it.
	switch p.Kind {
	case TimeRange:
		ts, ok := src.stamp()
		if !ok {
			return false
		}
		// MinInt64 IS A BOUND, NOT A SENTINEL FOR "THE EPOCH".
		//
		// This read `if from == math.MinInt64 { from = 0 }`, and MinInt64 is
		// exactly what a far-past bound saturates to: `_time:>=1000-01-01`
		// and the open lower end of `_time:<X` both produce it. So the one
		// value the saturation exists to produce was the one value that got
		// turned back into 1970, and rows between 1677-09-21 and the epoch --
		// which this store accepts and stores -- were invisible to the LogsQL
		// filter while `?start=1000-01-01` reached them. Measured on a store
		// holding 1900, 1969 and 2026, all under `?start=1000-01-01&end=9999-01-01`:
		//
		//	*                                3 of 3   OK
		//	_time:[1000-01-01, 2100-01-01]   1        want 3
		//	_time:<2100-01-01                1        want 3
		//	_time:>1000-01-01                1        want 3
		//	* | filter _time:<2100-01-01     1        want 3
		//	_time:[1900-01-01, 2100-01-01]   3        OK  (1900 is representable:
		//	_time:[1677-09-22, 2100-01-01]   3        OK   no saturation, no clamp)
		//
		// `ts >= math.MinInt64` is true for every int64, which is exactly what
		// an absent lower bound means, so nothing replaces the clamp.
		return ts >= p.T1 && ts < p.T2
	case TimeDayRange, TimeWeekRange:
		ts, ok := src.stamp()
		if !ok {
			return false
		}
		return matchDayWeek(time.Unix(0, ts).UTC(), p)
	}
	if isFieldCmp(p.Kind) {
		return fieldCmp(src.field(p.Field), src.field(p.Field2), p.Kind)
	}
	v := src.field(p.Field)
	switch p.Kind {
	case Eq:
		return v == p.Value
	case Contains:
		return containsSubstr(v, p.Value)
	case Prefix:
		return strings.HasPrefix(v, p.Value)
	case Regexp:
		re := p.regex()
		return re != nil && re.MatchString(v)
	case Lt, Le, Gt, Ge:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return cmpNum(f, p.Kind, p.Num)
		}
		return false
	case In:
		for _, x := range p.Values {
			if v == x {
				return true
			}
		}
		return false
	case RangeNum:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return numInRange(f, p)
		}
		return false
	case LenRange:
		l := float64(len(v))
		return l >= p.Num && l <= p.Num2
	case StringRange:
		return v >= p.Value && v < p.Value2
	case IContains:
		return containsSubstr(strings.ToLower(v), strings.ToLower(p.Value))
	case Seq:
		return seqMatch(v, p.Values)
	case IPv4Range:
		if u, ok := ipToU32(v); ok {
			return u >= uint32(p.Num) && u <= uint32(p.Num2)
		}
		return false
	case StreamIDEq:
		return StreamID(v) == p.Value
	}
	return false
}

// filterFields collects the distinct fields a filter references, so the row
// path knows which columns to materialize.
func filterFields(e *Expr, out map[string]bool) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		// A time predicate names no column to materialize: `_time` lives in
		// the timestamp column and lands in Row.Time, where the group scan
		// and both row evaluators read it. The flat-Preds path skips these
		// with the comment "_time is the timestamp column, not a materialized
		// field"; this copy did not, so a tree-filtered query materialized a
		// formatted `_time` field onto every row -- observable as a `_source`
		// key on the ES surface that appeared and disappeared with the
		// SPELLING of the same time filter (lifted window vs `must_not`).
		if isTimePred(e.Pred.Kind) {
			return
		}
		out[e.Pred.Field] = true
		if e.Pred.Field2 != "" {
			out[e.Pred.Field2] = true
		}
	case OpNot:
		filterFields(e.Child, out)
	default:
		for _, k := range e.Kids {
			filterFields(k, out)
		}
	}
}
