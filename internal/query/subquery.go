package query

import (
	"sort"
	"strings"
)

// StreamContextPipe is `stream_context before N after N` -- around each matched
// row, also return the N rows before and after it in time order. Without a
// first-class stream model it scopes context to the query window, not a single
// stream; matches are located by timestamp.
type StreamContextPipe struct{ Before, After int }

// streamContextCap bounds the context scan's materialized rows.
const streamContextCap = 2_000_000

func (p *StreamContextPipe) apply(rows []Row) []Row { return rows }

func (p *StreamContextPipe) run(s Store, parent *Query, matches []Row) []Row {
	if len(matches) == 0 || (p.Before == 0 && p.After == 0) {
		return matches
	}
	// The N neighbors sit next to each match in time, so the context scan needs
	// the window's rows in time order. Cap the materialize at streamContextCap
	// rows so a huge window degrades (context over the first cap rows) instead
	// of OOMing; real stream-context use is over a bounded slice.
	ctxQ := &Query{From: parent.From, To: parent.To, Now: parent.Now,
		Filter: &Expr{Op: OpAnd}, MatAll: true, Limit: streamContextCap}
	applyBudget(ctxQ, parent)
	all := Run(s, ctxQ)
	sort.SliceStable(all, func(i, j int) bool { return all[i].Time < all[j].Time })
	matchTime := map[int64]bool{}
	for _, m := range matches {
		matchTime[m.Time] = true
	}
	include := make([]bool, len(all))
	for i := range all {
		if !matchTime[all[i].Time] {
			continue
		}
		lo, hi := i-p.Before, i+p.After
		if lo < 0 {
			lo = 0
		}
		if hi >= len(all) {
			hi = len(all) - 1
		}
		for k := lo; k <= hi; k++ {
			include[k] = true
		}
	}
	out := make([]Row, 0, len(matches))
	for i := range all {
		if include[i] {
			out = append(out, all[i])
		}
	}
	return out
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
	return append(rows, runSub(s, parent, p.Sub)...)
}

// runSub executes a subquery over the parent's time window and under the
// parent's budget.
//
// The budget half was missing: only From/To/Now were copied, so `| union (*)`
// appended to any query ran a full unbounded scan while the outer query's
// deadline was already spent. The route still answered 504 -- the outer scan
// sets Stopped -- so the unbounded work was invisible from outside.
func runSub(s Store, parent *Query, sub *Query) []Row {
	q := *sub // copy so the parsed template is not mutated by run
	q.From, q.To, q.Now = parent.From, parent.To, parent.Now
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
