package query

import (
	"sort"
	"time"

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

// FieldNameCounts pairs each field name with how many matching rows carry it --
// the /select/logsql/field_names shape. The count is what lets a UI rank the
// fields, so a bare list of names is not a substitute.
func FieldNameCounts(s Store, q *Query) []ValueCount {
	// Per group: how many rows match, once -- then every column present in that
	// group gets that count, minus the rows whose value is empty. Summing a
	// field's VALUE counts instead would build a map over every distinct value
	// of every field, which on a corpus with a 200k-value trace_id column took
	// nine seconds to answer a question about column names.
	counts := map[string]int{}
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		whole := len(q.Preds) == 0 && q.Filter == nil && g.TimeMin >= q.From && g.TimeMax < q.To
		var sel *Bitset
		rows := g.Rows
		if !whole {
			sel = matchBitset(g, q)
			rows = sel.Count()
		}
		if rows == 0 {
			continue
		}
		// A group can carry two columns of the same name -- _time is stored both
		// as the timestamp column and, when the record spelled it out, as a
		// value column. Counting both reported twice the rows for that field.
		seen := make(map[string]bool, len(g.ColumnNames()))
		for _, name := range g.ColumnNames() {
			if seen[name] {
				continue
			}
			seen[name] = true
			counts[name] += rows - emptyValued(g, sel, name)
		}
	}
	out := make([]ValueCount, 0, len(counts))
	for name, hits := range counts {
		if hits > 0 {
			out = append(out, ValueCount{Value: name, Count: hits})
		}
	}
	// _stream and _stream_id are synthesized onto every returned record, so they
	// are fields the client can see and filter on -- listing only the stored
	// columns hid two fields that queries plainly work against.
	hasStream := false
	for _, vc := range out {
		if vc.Value == "_stream" {
			hasStream = true
			break
		}
	}
	if n := Count(s, q); n > 0 {
		if !hasStream {
			out = append(out, ValueCount{Value: "_stream", Count: n})
		}
		out = append(out, ValueCount{Value: "_stream_id", Count: n})
	}
	sortValueCounts(out)
	return out
}

// emptyValued returns how many of the selected rows hold the empty value for a
// column -- rows where the field is declared by the group but not set on that
// record. sel is nil when every row of the group is selected.
//
// The empty value's row list comes from the postings, so a dense column costs
// one lookup that finds nothing.
func emptyValued(g *storage.Reader, sel *Bitset, name string) int {
	rows, has := g.EqualityRows(name, "")
	if !has || len(rows) == 0 {
		return 0
	}
	if sel == nil {
		return len(rows)
	}
	n := 0
	for _, i := range rows {
		if sel.Test(int(i)) {
			n++
		}
	}
	return n
}

// FieldFacet is one field's top values -- the element type of the
// /select/logsql/facets array.
type FieldFacet struct {
	FieldName string       `json:"field_name"`
	Values    []FacetValue `json:"values"`
}

// FacetValue is a value of a faceted field and its hit count. It carries
// different JSON names from ValueCount because the reference spells them
// field_value/hits inside facets and value/hits everywhere else.
type FacetValue struct {
	FieldValue string `json:"field_value"`
	Hits       int    `json:"hits"`
}

// FacetList returns the top-`limit` values per field for the rows matching q,
// ordered by field name -- the /select/logsql/facets shape for dashboards.
//
// Two fields are left out, matching the reference, because neither tells a
// dashboard anything: one whose distinct values exceed maxPerField (a trace id
// is not a facet), and one with a single value across the whole result unless
// keepConst asks for it.
func FacetList(s Store, q *Query, limit, maxPerField int, keepConst bool) []FieldFacet {
	names := FieldNames(s, q.From, q.To)
	groups := s.Groups(q.From, q.To)
	out := make([]FieldFacet, 0, len(names))
	for _, name := range names {
		if name == "_time" {
			if f, ok := timeFacet(s, q, limit, maxPerField, keepConst); ok {
				out = append(out, f)
			}
			continue
		}
		// A field has at least as many distinct values as the largest single
		// group's dictionary holds, so a high-cardinality field is rejected from
		// the footers -- without building the map over its values that made
		// field_names take nine seconds.
		if maxPerField > 0 {
			lower := 0
			for _, g := range groups {
				if n := g.DictLen(name); n > lower {
					lower = n
				}
			}
			if lower > maxPerField {
				continue
			}
		}
		vc := StatsByField(s, q, name)
		if !facetKeep(len(vc), maxPerField, keepConst) {
			continue
		}
		sortValueCounts(vc)
		if limit > 0 && len(vc) > limit {
			vc = vc[:limit]
		}
		vals := make([]FacetValue, 0, len(vc))
		for _, v := range vc {
			vals = append(vals, FacetValue{FieldValue: v.Value, Hits: v.Count})
		}
		out = append(out, FieldFacet{FieldName: name, Values: vals})
	}
	return out
}

func facetKeep(distinct, maxPerField int, keepConst bool) bool {
	if distinct == 0 || (maxPerField > 0 && distinct > maxPerField) {
		return false
	}
	return distinct > 1 || keepConst
}

// timeFacet facets _time, which has no dictionary to read: the values are the
// timestamps themselves. Only worth materializing while the result is small
// enough for the field to survive the cardinality rule anyway, so the scan is
// bounded to maxPerField+1 rows and abandoned past it.
func timeFacet(s Store, q *Query, limit, maxPerField int, keepConst bool) (FieldFacet, bool) {
	bound := maxPerField
	if bound <= 0 {
		bound = defaultFacetMaxValues
	}
	sub := *q
	sub.Limit = bound + 1
	sub.MatAll = false
	sub.Materialize = nil
	rows := Run(s, &sub)
	if len(rows) > bound {
		return FieldFacet{}, false
	}
	counts := map[string]int{}
	for _, r := range rows {
		counts[time.Unix(0, r.Time).UTC().Format(time.RFC3339Nano)]++
	}
	if !facetKeep(len(counts), maxPerField, keepConst) {
		return FieldFacet{}, false
	}
	vc := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		vc = append(vc, ValueCount{Value: v, Count: c})
	}
	sortValueCounts(vc)
	if limit > 0 && len(vc) > limit {
		vc = vc[:limit]
	}
	vals := make([]FacetValue, 0, len(vc))
	for _, v := range vc {
		vals = append(vals, FacetValue{FieldValue: v.Value, Hits: v.Count})
	}
	return FieldFacet{FieldName: "_time", Values: vals}, true
}

// The reference's facet defaults: ten values shown per field, and a field with
// more than a thousand distinct values is not a facet.
const (
	DefaultFacetLimit     = 10
	defaultFacetMaxValues = 1000
	DefaultFacetMaxValues = defaultFacetMaxValues
)

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
