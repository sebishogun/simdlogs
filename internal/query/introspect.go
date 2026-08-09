package query

import (
	"sort"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// FieldNames returns the distinct column names across groups overlapping
// the window -- the /select/logsql/field_names shape. Read from footers,
// no column decoded.
func FieldNames(s Store, from, to int64) []string {
	seen := map[string]struct{}{}
	for _, g := range s.Groups(from, to) {
		for _, name := range g.ColumnNames() {
			seen[name] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// FieldValues returns the distinct values of one field across the window,
// with per-value counts -- /select/logsql/field_values and the building
// block of facets. Dict columns answer from the dictionary and postings
// without decoding per-row indices.
func FieldValues(s Store, field string, from, to int64) []ValueCount {
	counts := map[string]int{}
	for _, g := range s.Groups(from, to) {
		for _, vc := range g.ValueCounts(field) {
			counts[vc.Value] += vc.Count
		}
	}
	out := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		out = append(out, ValueCount{Value: v, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Value < out[j].Value
	})
	return out
}

// ValueCount is a value and how many rows hold it.
type ValueCount struct {
	Value string `json:"value"`
	Count int    `json:"hits"`
}

// Facets returns the top-k values per field across the window -- the
// /select/logsql/facets shape for dashboards.
func Facets(s Store, from, to int64, topk int) map[string][]ValueCount {
	out := map[string][]ValueCount{}
	for _, name := range FieldNames(s, from, to) {
		if name == "_time" {
			continue
		}
		vc := FieldValues(s, name, from, to)
		if len(vc) > topk {
			vc = vc[:topk]
		}
		out[name] = vc
	}
	return out
}

// StatsByField groups matching rows by a field and counts each group --
// `stats by (field) count()`. The dict IS the grouping, so this is the
// posting counts filtered by the query's predicates.
func StatsByField(s Store, q *Query, field string) []ValueCount {
	counts := map[string]int{}
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		// Fast path: with no predicate and the whole group inside the window,
		// `stats by (field) count()` is exactly the posting offset table's
		// per-value counts -- a footer read, no column decode, no per-row
		// loop. Only a predicate or a boundary group needs the scan.
		if len(q.Preds) == 0 && g.TimeMin >= q.From && g.TimeMax < q.To {
			for _, vc := range g.ValueCounts(field) {
				counts[vc.Value] += vc.Count
			}
			continue
		}
		sel := matchBitset(g, q)
		if sel.Count() == 0 {
			continue
		}
		idx, dict := g.DictIndices(field)
		if idx == nil {
			continue
		}
		per := make([]int, len(dict))
		sel.ForEach(func(i int) { per[idx[i]]++ })
		for id, c := range per {
			if c > 0 {
				counts[dict[id]] += c
			}
		}
	}
	out := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		out = append(out, ValueCount{Value: v, Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ensure storage import used
var _ = storage.ColDict
