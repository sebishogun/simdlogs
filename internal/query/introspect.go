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
	sn1 := snapshotOf(s, from, to)
	defer sn1.Close()
	for _, g := range sn1.Groups {
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
	sn2 := snapshotOf(s, from, to)
	defer sn2.Close()
	// One buffer for every group's counts: the values are read into the map
	// before the next group overwrites it, so the per-group result slice never
	// needs to be allocated.
	var vcBuf []storage.ValueCount
	for _, g := range sn2.Groups {
		vcBuf = g.ValueCountsInto(vcBuf, field)
		for _, vc := range vcBuf {
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
	total := 0
	var names []string // reused across groups; column sets are small and repeat
	sn3 := snapshotOf(s, q.From, q.To)
	defer sn3.Close()
	for _, g := range sn3.Groups {
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
		total += rows
		// A group can carry two columns of the same name -- _time is stored both
		// as the timestamp column and, when the record spelled it out, as a
		// value column. Counting both reported twice the rows for that field.
		// Deduplicated by scanning the names already seen: a group holds a
		// handful of columns, and a map per group allocated more than the scan
		// costs.
		names = names[:0]
		for _, name := range g.ColumnNames() {
			dup := false
			for _, prev := range names {
				if prev == name {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			names = append(names, name)
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
	// BOTH names, not just `_stream`.
	//
	// `_stream_id` was appended unconditionally while `_stream` was guarded,
	// so a store whose rows carry a client-supplied `_stream_id` column listed
	// it TWICE on a node -- and a router, which sums the shards' counts by
	// name, answered twice the number of rows there are. Measured, six rows
	// each carrying `_stream_id`, one shard:
	//
	//	node   [… {"value":"_stream_id","hits":6},{"value":"_stream_id","hits":6} …]
	//	router [{"value":"_stream_id","hits":12}, …]
	//
	// Both at HTTP 200, and the facets endpoint over the same store was
	// already correct -- this is the same defect one endpoint over, which is
	// why the guard now covers both names in one pass instead of one name in
	// a loop that can be copied wrong again.
	hasStream, hasStreamID := false, false
	for _, vc := range out {
		switch vc.Value {
		case "_stream":
			hasStream = true
		case "_stream_id":
			hasStreamID = true
		}
	}
	// `total` is the matching row count, already summed above -- calling Count
	// here instead ran a second full pass over every group to learn a number
	// the first pass had.
	if n := total; n > 0 {
		if !hasStream {
			out = append(out, ValueCount{Value: "_stream", Count: n})
		}
		if !hasStreamID {
			out = append(out, ValueCount{Value: "_stream_id", Count: n})
		}
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
	if sel == nil {
		// Whole group selected: the COUNT is enough, and the group memoizes it --
		// the lookup behind it probes a compressed dictionary, and field_names
		// asks it for every column of every group on every request.
		return g.EmptyValueCount(name)
	}
	rows, has := g.EqualityRows(name, "")
	if !has || len(rows) == 0 {
		return 0
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
	sn4 := snapshotOf(s, q.From, q.To)
	defer sn4.Close()
	groups := sn4.Groups
	out := make([]FieldFacet, 0, len(names))
	for _, name := range names {
		if name == "_time" {
			if f, ok := timeFacet(s, q, limit, maxPerField, keepConst); ok {
				out = append(out, f)
			}
			continue
		}
		// `_stream` and `_stream_id` are emitted ONCE, below, from Streams and
		// StreamIDs.
		//
		// They are synthesized onto every record, and they are ALSO stored
		// columns once `_stream_fields` is configured -- so a store with
		// streams had them faceted twice, once from the column here and once
		// from the tail. A single node answered with two `_stream` blocks of
		// 10/10/10, and a router summed the pair into one block of 20/20/20:
		//
		//	30 rows, 3 streams of 10
		//	  node   "_stream" appears TWICE, 10/10/10 in each
		//	  router "_stream" once, 20/20/20
		//
		// Both HTTP 200, and the truth is 10. The duplicate on the node is
		// merely odd; the router's sum is a wrong number a dashboard draws.
		if name == "_stream" || name == "_stream_id" {
			continue
		}
		// A field has at least as many distinct values as the largest single
		// group's dictionary holds, so a high-cardinality field is rejected from
		// the footers -- without building the map over its values that made
		// field_names take nine seconds.
		// Outside the maxPerField branch, not inside it. Inside, a request
		// with max_values_per_field=0 -- which intParam accepts -- switched
		// the budget check off entirely and kept calling StatsByField once
		// per field past the deadline.
		//
		// And before the cardinality loop, not inside it: inside, an early
		// break left `lower` smaller than the truth, so `lower > maxPerField`
		// went false and the high-cardinality field was NOT skipped -- a
		// budget check that both changed the answer and increased the work.
		if q.exceeded(0) {
			break
		}
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
	// _stream and _stream_id are synthesized onto every record, so they are
	// facetable like any other field. With no stream fields configured they hold
	// one value for every row -- constant, and therefore only shown when
	// keep_const_fields asks, which is exactly what the reference does.
	for _, sf := range []struct {
		name string
		vals []ValueCount
	}{
		{"_stream", Streams(s, q)},
		{"_stream_id", StreamIDs(s, q)},
	} {
		if !facetKeep(len(sf.vals), maxPerField, keepConst) {
			continue
		}
		sortValueCounts(sf.vals)
		if limit > 0 && len(sf.vals) > limit {
			sf.vals = sf.vals[:limit]
		}
		vals := make([]FacetValue, 0, len(sf.vals))
		for _, v := range sf.vals {
			vals = append(vals, FacetValue{FieldValue: v.Value, Hits: v.Count})
		}
		out = append(out, FieldFacet{FieldName: sf.name, Values: vals})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FieldName < out[j].FieldName })
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
//
// maxPerField <= 0 is UNLIMITED here, as it is in facetKeep's own
// `maxPerField > 0 &&` guard. It used to mean defaultFacetMaxValues, so `0`
// meant two different things in the two functions that read it -- and a
// coordinator sending the shards `max_values_per_field=0` to get their whole
// distribution got _time bounded to 1000 ROWS (not distinct values) per shard
// and dropped. Measured: 1100 rows per shard over two distinct timestamps, a
// caller asking for 5000, and the cluster answered every field except _time
// while a single node answered _time with 2 values.
//
// Unlimited here is a real scan of every matching row, which is why the
// caller has to ask for it: it is the same bargain `limit=0` makes.
func timeFacet(s Store, q *Query, limit, maxPerField int, keepConst bool) (FieldFacet, bool) {
	// An explicit 0 does NOT mean an unbounded scan here.
	//
	// facetKeep reads 0 as "no cardinality cap", which is free for a field
	// read from a dictionary. _time has no dictionary -- its values are the
	// timestamps, roughly one distinct per row -- so removing the bound made
	// this materialize every matching row in the window. Measured at 85.8
	// bytes of response per row, linear, on the default cluster dashboard
	// path, failing outright above ~3.1M rows after allocating gigabytes.
	//
	// So the field is DROPPED past the ceiling, which is what it always did
	// past 1000, and the ceiling for an explicit "unlimited" is a large
	// constant rather than infinity.
	bound := maxPerField
	if bound <= 0 {
		bound = maxTimeFacetRows
	}
	sub := *q
	sub.Limit = bound + 1
	// LastN is the endpoint's `limit`, which shapes the ROWS a select
	// returns. Inherited here it made the _time facet count only the newest N
	// rows while every other field counted all of them -- one response with a
	// _time distribution summing to 25 and an svc distribution summing to 30.
	sub.LastN = 0
	// BOTH halves. MatAll was one flag and is now two: MatAll is full-record
	// output, MatCols is "the scan reads every column". Clearing only MatAll
	// left an inherited MatCols to make this timestamps-only scan read every
	// column of every matching row -- and since sub shares the parent's
	// stopReason, blowing the 256 MiB budget here would fail the whole request.
	// Not reachable today (runFacets skips _time, and the one FacetList caller
	// does not come through RunPipeline), which is exactly why it would be
	// found late.
	sub.MatAll = false
	sub.MatCols = false
	sub.Materialize = nil
	rows := Run(s, &sub)
	if len(rows) > bound {
		return FieldFacet{}, false
	}
	// Counted as integers, and formatted only for the values that survive the
	// limit: rendering a thousand RFC3339 strings to rank them and keep ten was
	// most of what a facets request cost.
	counts := make(map[int64]int, len(rows))
	for _, r := range rows {
		counts[r.Time]++
	}
	if !facetKeep(len(counts), maxPerField, keepConst) {
		return FieldFacet{}, false
	}
	type timeCount struct {
		t int64
		n int
	}
	all := make([]timeCount, 0, len(counts))
	for t, n := range counts {
		all = append(all, timeCount{t, n})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].n != all[j].n {
			return all[i].n > all[j].n
		}
		return all[i].t < all[j].t
	})
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	vals := make([]FacetValue, 0, len(all))
	for _, e := range all {
		vals = append(vals, FacetValue{FieldValue: formatTime(e.t), Hits: e.n})
	}
	return FieldFacet{FieldName: "_time", Values: vals}, true
}

// The reference's facet defaults: ten values shown per field, and a field with
// more than a thousand distinct values is not a facet.
const (
	DefaultFacetLimit = 10
	// maxTimeFacetRows is the ceiling on an explicitly-unlimited _time facet.
	// _time has one distinct value per row in the worst case, so "no cap" has
	// to mean "a large cap" or the scan is the whole window.
	maxTimeFacetRows      = 100000
	defaultFacetMaxValues = 1000
	DefaultFacetMaxValues = defaultFacetMaxValues
)

// StatsByField groups matching rows by a field and counts each group --
// `stats by (field) count()`. The dict IS the grouping, so this is the
// posting counts filtered by the query's predicates.
func StatsByField(s Store, q *Query, field string) []ValueCount {
	counts := map[string]int{}
	sn5 := snapshotOf(s, q.From, q.To)
	defer sn5.Close()
	// One buffer for every group's counts, refilled per group -- see
	// FieldValues. The loop below reads it into the map before moving on.
	var vcBuf []storage.ValueCount
	for _, g := range sn5.Groups {
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
		// Fast path: with no predicate and the whole group inside the window,
		// `stats by (field) count()` is exactly the posting offset table's
		// per-value counts -- a footer read, no column decode, no per-row
		// loop. Only a predicate or a boundary group needs the scan.
		// _time has no dictionary to read counts from -- it is stored as
		// timestamps -- so it takes the scan below, which synthesizes one.
		if len(q.Preds) == 0 && field != "_time" && g.TimeMin >= q.From && g.TimeMax < q.To {
			vcBuf = g.ValueCountsInto(vcBuf, field)
			for _, vc := range vcBuf {
				counts[vc.Value] += vc.Count
			}
			continue
		}
		sel := matchBitset(g, q)
		if sel.Count() == 0 {
			continue
		}
		idx, dict := dictOrTime(g, field)
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
	// sortValueCounts, not a bare count comparison.
	//
	// This sorted by count with NO tie-break, over a slice built by ranging a Go
	// map -- so equal counts came out in whatever order the map handed them
	// over, which Go deliberately randomizes per run. Five identical requests
	// for `stats by (svc) | limit 3` returned five different sets of three
	// values, and an operator comparing two dashboard loads saw data change that
	// had not. sortValueCounts and runTopFast both break the tie by value, and
	// both say why; this was the one place that did not.
	sortValueCounts(out)
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

// dictOrTime returns a column's dict indices and values, synthesizing them for
// _time from the timestamp column. _time is stored once, as timestamps, so a
// query that GROUPS or aggregates by it finds no dictionary there; this builds
// the one it would have had, and only for the group being read.
func dictOrTime(g *storage.Reader, field string) ([]uint32, []string) {
	idx, dict := g.DictIndices(field)
	if idx != nil || field != "_time" {
		return idx, dict
	}
	ts := g.TimestampsRange("_time", 0, g.Rows)
	if len(ts) == 0 {
		return nil, nil
	}
	idx = make([]uint32, len(ts))
	dict = make([]string, len(ts))
	for i, t := range ts {
		idx[i] = uint32(i)
		dict[i] = formatTime(t)
	}
	return idx, dict
}
