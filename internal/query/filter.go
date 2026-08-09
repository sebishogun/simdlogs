package query

import "github.com/sebishogun/simdlogs/internal/storage"

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

// filterFields collects the distinct fields a filter references, so the row
// path knows which columns to materialize.
func filterFields(e *Expr, out map[string]bool) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		out[e.Pred.Field] = true
	case OpNot:
		filterFields(e.Child, out)
	default:
		for _, k := range e.Kids {
			filterFields(k, out)
		}
	}
}
