package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// StreamContextPipe is `stream_context before N after N` -- around each matched
// row, also return the N rows before and after it FROM THE SAME STREAM.
//
// # Why the stream matters
//
// Context is a debugging tool: you found the error and you want the lines
// around it. "Around it" means around it in the log that produced it. Scoped
// to the query window instead, the neighbours of an error on one host are
// whatever other hosts happened to write at the same moment -- on a busy
// server, `before 5 after 5` returns ten lines from ten unrelated processes
// and none of the ten from the process that failed. That is not a smaller
// answer, it is a different one, and it looks exactly like a correct one.
//
// So neighbours are taken within a row's own `_stream` label set. A row with
// no stream fields configured is in EmptyStream, which is one stream -- so an
// unconfigured deployment gets the window-scoped behaviour it had, because
// there genuinely is only one stream to scope to.
//
// # Why identity is a tuple and not a timestamp
//
// The matches arriving here were located in the ORIGINAL scan; the pipe has to
// find them again in the context scan. Matching them by timestamp is what the
// pipe used to do, and timestamps collide constantly: an unrelated row written
// in the same millisecond was treated as a match and got its own ten lines of
// context. Rows are identified by their content -- timestamp and every field
// -- which is exact, and two rows identical in all of those are the same row
// for any purpose a caller has.
//
// Ordering within the context scan uses the (time, group id, row index) tuple
// from order.go, so equal timestamps have a defined neighbour order rather
// than whichever the scan happened to produce.
type StreamContextPipe struct{ Before, After int }

// streamContextCap bounds the context scan's materialized rows.
//
// It used to be a Limit, so a window with more rows than this silently
// returned context computed over the first two million and nothing said so:
// a neighbour that exists gets dropped because a row far away in the window
// used up the budget, and the answer is wrong in a way the caller cannot see.
// It is a ceiling now -- the query errors instead.
const streamContextCap = 2_000_000

func (p *StreamContextPipe) apply(rows []Row) []Row { return rows }

func (p *StreamContextPipe) run(s Store, parent *Query, matches []Row) []Row {
	if len(matches) == 0 || (p.Before == 0 && p.After == 0) {
		return matches
	}
	budget := streamContextCap
	if parent.maxPipeRows > 0 && parent.maxPipeRows < budget {
		budget = parent.maxPipeRows
	}
	// The whole window, unfiltered: a neighbour is by definition a row the
	// query did NOT match, so the context scan cannot carry the parent's
	// filter.
	ctxQ := &Query{From: parent.From, To: parent.To, ToSet: parent.ToSet, Now: parent.Now, NowSet: parent.NowSet,
		Filter: &Expr{Op: OpAnd}, MatAll: true}
	applyBudget(ctxQ, parent)
	// Its OWN stop reason, then translated. applyBudget shares the parent's so
	// the first stop anywhere in the query tree is the one reported -- right
	// everywhere else, and wrong here: this scan's row ceiling is not the
	// caller's row budget, it is "the window does not fit in memory", and
	// reporting ErrRowLimit would send the caller to raise -search.maxRows,
	// which is not the knob.
	ctxQ.stopReason = new(atomic.Pointer[error])
	// ScanPage rather than Run: it returns the (time, group, row) tuple with
	// each row, which is what gives equal timestamps a defined neighbour
	// order. One page of budget+1 -- More means the window overflowed.
	page, err := ScanPage(s, ctxQ, nil, Oldest, budget)
	if err != nil {
		if !errors.Is(err, ErrRowLimit) {
			parent.stop(err) // cancellation, deadline, bytes: the caller's, unchanged
			return nil
		}
		page = &Page{More: true}
	}
	if e := ctxQ.stopErr(); e != nil && !errors.Is(e, ErrRowLimit) {
		parent.stop(e)
		return nil
	}
	if page.More {
		parent.stop(fmt.Errorf(
			"%w: stream_context needs the whole window in time order and it holds more "+
				"than %d rows; narrow the window", ErrPipeRowLimit, budget))
		return nil
	}

	// Matches by content, not by timestamp. Real strings here because the set
	// is built once and is the size of the match set; the lookups below use
	// map[string(buf)], which the compiler does without allocating.
	wanted := make(map[string]bool, len(matches))
	var buf []byte
	for _, m := range matches {
		buf = appendRowIdentity(buf[:0], m)
		wanted[string(buf)] = true
	}

	// Positions within each stream, in the total order ScanPage returned. The
	// slice per stream is what makes "the N before" mean the N before IN THIS
	// STREAM rather than the N before in the window.
	byStream := map[string][]int{}
	for i, r := range page.Rows {
		st := rowField(r, "_stream")
		byStream[st] = append(byStream[st], i)
	}

	include := make([]bool, len(page.Rows))
	for _, idxs := range byStream {
		for pos, i := range idxs {
			buf = appendRowIdentity(buf[:0], page.Rows[i])
			if !wanted[string(buf)] {
				continue
			}
			lo, hi := pos-p.Before, pos+p.After
			if lo < 0 {
				lo = 0
			}
			if hi >= len(idxs) {
				hi = len(idxs) - 1
			}
			// Overlapping context ranges collapse here: two matches three
			// rows apart with `before 5` share most of their neighbours, and
			// a bool per row is the dedup.
			for k := lo; k <= hi; k++ {
				include[idxs[k]] = true
			}
		}
	}

	out := make([]Row, 0, len(matches))
	for i := range page.Rows {
		if include[i] {
			out = append(out, page.Rows[i])
		}
	}
	if parent.maxPipeRows > 0 && len(out) > parent.maxPipeRows {
		parent.stop(fmt.Errorf("%w: stream_context produced %d rows, ceiling is %d",
			ErrPipeRowLimit, len(out), parent.maxPipeRows))
		return nil
	}
	return out
}

// appendRowIdentity writes a row's identity -- its timestamp and every field --
// into dst.
//
// Appended into a caller-supplied buffer rather than built with Sprintf or a
// strings.Builder, because this runs once per row of the context window and
// the window is bounded at two million. Field values are length-prefixed so a
// row with fields {"ab","c"} cannot collide with one holding {"a","bc"}.
func appendRowIdentity(dst []byte, r Row) []byte {
	dst = strconv.AppendInt(dst, r.Time, 10)
	for _, f := range r.Fields {
		dst = append(dst, 0)
		dst = strconv.AppendInt(dst, int64(len(f.Key)), 10)
		dst = append(dst, ':')
		dst = append(dst, f.Key...)
		dst = append(dst, 0)
		dst = strconv.AppendInt(dst, int64(len(f.Value)), 10)
		dst = append(dst, ':')
		dst = append(dst, f.Value...)
	}
	return dst
}

// Subquery pipes: join and union run a nested LogsQL query and combine it with
// the outer rows; in(<subquery>) resolves a nested query's values into a set.
// The sub-query inherits the outer time window.

// JoinPipe is `join by (fields) (<subquery>) [prefix p]` -- a left join: each
// outer row gains the matched sub-row's other fields (Prefix-prefixed); an
// unmatched outer row passes through unchanged.
type JoinPipe struct {
	By     []string
	Prefix string
	Sub    *Query
}

func (p *JoinPipe) run(s Store, parent *Query, rows []Row) []Row {
	index := map[string][]Row{}
	for _, sr := range runSub(s, parent, p.Sub) {
		k := joinKey(sr, p.By)
		index[k] = append(index[k], sr)
	}
	var out []Row
	for _, r := range rows {
		matches := index[joinKey(r, p.By)]
		if len(matches) == 0 {
			out = append(out, r)
			continue
		}
		// The fanout, bounded as it is produced. A left join whose key is not
		// unique on the right multiplies -- 10k outer rows against a right
		// side averaging 100 matches is a million-row answer built from two
		// results that each passed every other budget. Checked inside the
		// loop, because by the time the slice is returned the memory is
		// already spent.
		if parent.maxPipeRows > 0 && len(out)+len(matches) > parent.maxPipeRows {
			parent.stop(fmt.Errorf("%w: join fanout passed %d rows, ceiling is %d",
				ErrPipeRowLimit, parent.maxPipeRows, parent.maxPipeRows))
			return nil
		}
		for _, m := range matches {
			nr := cloneRow(r)
			for _, f := range m.Fields {
				if inList(p.By, f.Key) {
					continue // join keys are already on the outer row
				}
				setRowField(&nr, p.Prefix+f.Key, f.Value)
			}
			out = append(out, nr)
		}
	}
	return out
}

// apply satisfies Pipe; RunPipeline dispatches join via run (it needs the store).
func (p *JoinPipe) apply(rows []Row) []Row { return rows }

// UnionPipe is `union (<subquery>)` -- append the subquery's rows.
type UnionPipe struct{ Sub *Query }

func (p *UnionPipe) apply(rows []Row) []Row { return rows }

func (p *UnionPipe) run(s Store, parent *Query, rows []Row) []Row {
	sub := runSub(s, parent, p.Sub)
	if parent.maxPipeRows > 0 && len(rows)+len(sub) > parent.maxPipeRows {
		parent.stop(fmt.Errorf("%w: union would produce %d rows, ceiling is %d",
			ErrPipeRowLimit, len(rows)+len(sub), parent.maxPipeRows))
		return nil
	}
	return append(rows, sub...)
}

// runSub executes a subquery over the parent's time window and under the
// parent's budget.
//
// The budget half was missing: only From/To/Now were copied, so `| union (*)`
// appended to any query ran a full unbounded scan while the outer query's
// deadline was already spent. The route still answered 504 -- the outer scan
// sets Stopped -- so the unbounded work was invisible from outside.
func runSub(s Store, parent *Query, sub *Query) []Row {
	// CloneResolvable, not `*sub`. The comment here said "copy so the parsed
	// template is not mutated by run", and a `*sub` does not do that: Preds is
	// a slice and Filter is a tree of pointers, so both are shared, and
	// resolveTimePred writes T1/T2/Rel through them. The same shallow copy on
	// the tail path was a stream that delivered nothing (entry 118).
	//
	// Not reachable today -- every path re-parses per execution, so there is no
	// template that outlives one run -- which is exactly what was true of the
	// tail's copy until something re-ran the query.
	q := *sub.CloneResolvable()
	q.SetWindow(parent.From, parent.To)
	q.ToSet, q.Now, q.NowSet = parent.ToSet, parent.Now, parent.NowSet
	// Whole records, by the same rule the top-level select uses: a chain that
	// does not PROJECT still emits whole rows, so the scan has to materialize
	// every column for it.
	//
	// It was not applied here, and a subquery is where it matters most. The
	// sub rows came back carrying _time and nothing else, so `join by (f)
	// (<sub>)` computed its key from an absent field: every key was the empty
	// one, nothing ever matched, and the join returned the outer rows
	// unchanged. A left join that never joins looks exactly like a left join
	// with no matches, which is why it survived -- the answer is a valid
	// answer to a different question.
	if !PipesProject(q.Pipes) {
		q.MatAll = true
	}
	applyBudget(&q, parent)
	return RunPipeline(s, &q)
}

func joinKey(r Row, by []string) string {
	var b strings.Builder
	for _, f := range by {
		b.WriteString(rowField(r, f))
		b.WriteByte(0)
	}
	return b.String()
}

func inList(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// resolveSubqueries runs any in(<subquery>) filter once, replacing it with the
// value set (In) of the subquery's matching field. Idempotent (Sub cleared).
func resolveSubqueries(s Store, q *Query) {
	resolveSubExpr(s, q, q.Filter)
	for i := range q.Preds {
		resolveSubPred(s, q, &q.Preds[i])
	}
}

func resolveSubExpr(s Store, parent *Query, e *Expr) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		resolveSubPred(s, parent, &e.Pred)
	case OpNot:
		resolveSubExpr(s, parent, e.Child)
	default:
		for _, k := range e.Kids {
			resolveSubExpr(s, parent, k)
		}
	}
}

func resolveSubPred(s Store, parent *Query, p *Pred) {
	if p.Kind != In || p.Sub == nil {
		return
	}
	seen := map[string]bool{}
	for _, r := range runSub(s, parent, p.Sub) {
		v := subValue(r, p.Field)
		if !seen[v] {
			seen[v] = true
			p.Values = append(p.Values, v)
		}
	}
	p.Sub = nil
}

// subValue picks the value to match against the outer field: the same-named
// field if the subquery emitted it, else the row's last field.
func subValue(r Row, field string) string {
	for _, f := range r.Fields {
		if f.Key == field {
			return f.Value
		}
	}
	if len(r.Fields) > 0 {
		return r.Fields[len(r.Fields)-1].Value
	}
	return ""
}
