package query

import (
	"sort"
	"strconv"
)

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// SortValueCounts orders a value list the way every values endpoint returns it:
// most hits first, ties broken by value ascending so the order is stable.
func SortValueCounts(vcs []ValueCount) { sortValueCounts(vcs) }

func sortValueCounts(vcs []ValueCount) {
	sort.Slice(vcs, func(i, j int) bool {
		if vcs[i].Count != vcs[j].Count {
			return vcs[i].Count > vcs[j].Count
		}
		return vcs[i].Value < vcs[j].Value
	})
}

// Introspection pipe forms of the /select/logsql endpoints: field_names,
// field_values, facets. As a query's leading pipe they run as source pipes in
// RunPipeline against the footer counts (no row materialize) -- the fast path.
// Used mid-pipe, apply() aggregates the incoming row stream instead.

// FieldValuesPipe is `field_values <field> [limit N]` -- value -> hit count.
type FieldValuesPipe struct {
	Field string
	Limit int
}

func (p *FieldValuesPipe) apply(rows []Row) []Row {
	counts := map[string]int{}
	for _, r := range rows {
		counts[rowField(r, p.Field)]++
	}
	// Sorted BEFORE the limit truncates, which it was not.
	//
	// This built the list in first-seen order and handed it to valueCountRows,
	// which truncates whatever it is given. `| field_values svc limit 2` over a
	// log stream therefore returned the two values that appeared EARLIEST, and
	// in a stream those are typically the rarest: the reviewer got the two least
	// frequent values presented as the top two, HTTP 200, from an endpoint whose
	// whole purpose is "which values are there, most first". StatsByField and
	// every /select values endpoint sort by hits, and this pipe shares their
	// output shape, so nothing in the response said its ordering rule differed.
	return valueCountRows(mapToValueCounts(counts), p.Limit)
}

// FieldNamesPipe is `field_names` -- each distinct field name and its hit count.
type FieldNamesPipe struct{}

func (p *FieldNamesPipe) apply(rows []Row) []Row {
	counts := map[string]int{}
	var order []string
	for _, r := range rows {
		for _, f := range r.Fields {
			if _, ok := counts[f.Key]; !ok {
				order = append(order, f.Key)
			}
			counts[f.Key]++
		}
	}
	out := make([]Row, 0, len(order))
	for _, name := range order {
		out = append(out, Row{NoTime: true, Fields: []Field{{"name", name}, {"hits", strconv.Itoa(counts[name])}}})
	}
	return out
}

// FacetsPipe is `facets [N]` -- the top N values of each field.
type FacetsPipe struct{ N int }

func (p *FacetsPipe) apply(rows []Row) []Row {
	byField := map[string]map[string]int{}
	var order []string
	for _, r := range rows {
		for _, f := range r.Fields {
			m := byField[f.Key]
			if m == nil {
				m = map[string]int{}
				byField[f.Key] = m
				order = append(order, f.Key)
			}
			m[f.Value]++
		}
	}
	var out []Row
	for _, name := range order {
		out = append(out, facetRows(name, mapToValueCounts(byField[name]), p.N)...)
	}
	return out
}

// BlocksCountPipe is `blocks_count` -- how many storage blocks (row-groups) the
// query scans. One row.
type BlocksCountPipe struct{}

func (p *BlocksCountPipe) apply(rows []Row) []Row {
	return []Row{{Fields: []Field{{"blocks_count", strconv.Itoa(len(rows))}}}}
}

// BlockStatsPipe is `block_stats` -- per-block rows, bytes and column count.
type BlockStatsPipe struct{}

func (p *BlockStatsPipe) apply(rows []Row) []Row { return rows }

// ---- source-pipe (leading) fast paths, over the footer counts ----

func runBlocksCount(s Store, q *Query) []Row {
	n := 0
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
		if groupCanMatch(g, q) {
			n++
		}
	}
	return []Row{{Fields: []Field{{"blocks_count", strconv.Itoa(n)}}}}
}

func runBlockStats(s Store, q *Query) []Row {
	var out []Row
	sn2 := snapshotOf(s, q.From, q.To)
	defer sn2.Close()
	for _, g := range sn2.Groups {
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
		bytes := 0
		for _, c := range g.ColumnBytes() {
			bytes += c.Index + c.Postings + c.Dict + c.Bloom + c.TimeCol
		}
		out = append(out, Row{NoTime: true, Fields: []Field{
			{"rows", strconv.Itoa(g.Rows)},
			{"bytes", strconv.Itoa(bytes)},
			{"columns", strconv.Itoa(len(g.ColumnNames()))},
		}})
	}
	return out
}

func runFieldValues(s Store, q *Query, p *FieldValuesPipe) []Row {
	return valueCountRows(StatsByField(s, q, p.Field), p.Limit)
}

func runFieldNames(s Store, q *Query) []Row {
	names := FieldNames(s, q.From, q.To)
	out := make([]Row, 0, len(names))
	for _, name := range names {
		if name == "_time" {
			continue
		}
		hits := 0
		for _, vc := range StatsByField(s, q, name) {
			hits += vc.Count
		}
		out = append(out, Row{NoTime: true, Fields: []Field{{"name", name}, {"hits", strconv.Itoa(hits)}}})
	}
	return out
}

func runFacets(s Store, q *Query, p *FacetsPipe) []Row {
	var out []Row
	for _, name := range FieldNames(s, q.From, q.To) {
		if name == "_time" {
			continue
		}
		out = append(out, facetRows(name, StatsByField(s, q, name), p.N)...)
	}
	return out
}

// ---- shared rendering ----

func valueCountRows(vcs []ValueCount, limit int) []Row {
	if limit > 0 && len(vcs) > limit {
		vcs = vcs[:limit]
	}
	out := make([]Row, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, Row{NoTime: true, Fields: []Field{{"value", vc.Value}, {"hits", strconv.Itoa(vc.Count)}}})
	}
	return out
}

func facetRows(field string, vcs []ValueCount, n int) []Row {
	if n <= 0 {
		n = 10
	}
	if len(vcs) > n {
		vcs = vcs[:n]
	}
	out := make([]Row, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, Row{NoTime: true, Fields: []Field{{"field", field}, {"value", vc.Value}, {"hits", strconv.Itoa(vc.Count)}}})
	}
	return out
}

func mapToValueCounts(m map[string]int) []ValueCount {
	vcs := make([]ValueCount, 0, len(m))
	for v, c := range m {
		vcs = append(vcs, ValueCount{Value: v, Count: c})
	}
	sortValueCounts(vcs)
	return vcs
}
