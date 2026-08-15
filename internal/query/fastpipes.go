package query

import (
	"sort"
	"strconv"
)

// Fast paths for the pipes whose answer is already in the footers. Each one
// produces exactly what the generic apply() over materialized rows produces --
// the generic path stays as the reference, and the differential tests compare
// the two -- but reads posting counts and dictionaries instead of decoding
// every matching row.
//
// The shapes covered here are the ones a dashboard issues constantly:
// `stats count()`, `top N by (f)`, `uniq by (f)` and a bare `limit N`.

// runCountFast answers a whole-window `stats count()` with a popcount of the
// match bitset -- no per-row key building, no accumulator map.
func runCountFast(s Store, q *Query, sp *StatsPipe) ([]Row, bool) {
	if len(sp.By) != 0 || len(sp.Aggs) != 1 {
		return nil, false
	}
	a := &sp.Aggs[0]
	if a.Kind != AggCount || a.If != nil || a.Field != "" {
		return nil, false
	}
	n := Count(s, q)
	if n == 0 {
		// No matching rows is no group, so it is no row -- the same answer the
		// accumulator gives. Returning a count-of-zero row instead put a phantom
		// 0 into every empty bucket of a stats_query_range.
		return []Row{}, true
	}
	return []Row{{NoTime: true, Fields: []Field{{a.Alias, strconv.Itoa(n)}}}}, true
}

// runTopFast answers `top N by (field)` from the same posting counts
// `stats by (field) count()` reads, then sorts and truncates. Only the
// single-field form: a multi-field tuple is not a single dictionary.
func runTopFast(s Store, q *Query, p *TopPipe) ([]Row, bool) {
	if len(p.By) != 1 {
		return nil, false
	}
	vcs := StatsByField(s, q, p.By[0])
	// The ceiling applies here as much as on the generic path. These fast
	// paths read the footer's posting counts, so they build the key space
	// without ever building the accumulator map -- cheaper, and exactly as
	// unbounded. A ceiling written only on the map would have covered the
	// path that is NOT taken for the common single-field shape.
	if tooManyKeys(q, len(vcs), "top by") {
		return nil, true
	}
	// StatsByField sorts by count only; top's tie-break is the grouped value
	// ascending, which VictoriaLogs applies and the generic path reproduces.
	sort.SliceStable(vcs, func(a, b int) bool {
		if vcs[a].Count != vcs[b].Count {
			return vcs[a].Count > vcs[b].Count
		}
		return vcs[a].Value < vcs[b].Value
	})
	if p.N > 0 && len(vcs) > p.N {
		vcs = vcs[:p.N]
	}
	out := make([]Row, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, Row{NoTime: true, Fields: []Field{
			{p.By[0], vc.Value}, {"hits", strconv.Itoa(vc.Count)},
		}})
	}
	return out, true
}

// runUniqFast answers `uniq by (field)` from the per-value counts: a value with
// a non-zero count is a distinct value, which is the whole question.
//
// The generic path emits distinct values in first-seen (time) order; this one
// emits them in the count-descending order StatsByField returns. `uniq` defines
// no order, but a bare `limit` on top of it would otherwise pick a different
// subset, so the rows are sorted by value to make the answer deterministic.
func runUniqFast(s Store, q *Query, p *UniqPipe) ([]Row, bool) {
	if len(p.By) != 1 {
		return nil, false
	}
	vcs := StatsByField(s, q, p.By[0])
	if tooManyKeys(q, len(vcs), "uniq by") {
		return nil, true
	}
	sort.Slice(vcs, func(a, b int) bool { return vcs[a].Value < vcs[b].Value })
	if p.Limit > 0 && len(vcs) > p.Limit {
		vcs = vcs[:p.Limit]
	}
	out := make([]Row, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, Row{NoTime: true, Fields: []Field{{p.By[0], vc.Value}}})
	}
	return out, true
}
