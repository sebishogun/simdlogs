package query

import "testing"

// BenchmarkFieldNameCounts drives the shape /select/logsql/field_names meets in
// practice: many groups, a handful of columns each, one of them high
// cardinality. The endpoint answers a question about column NAMES, so its cost
// must not scale with the number of distinct values.
func BenchmarkFieldNameCounts(b *testing.B) {
	s, lo, hi := hostStore(b, 200_000, 8192, 100_000)
	q := &Query{From: lo, To: hi + 1}
	b.ResetTimer()
	for b.Loop() {
		sinkVC = FieldNameCounts(s, q)
	}
}

// BenchmarkFieldNameCountsWindowed is the same question asked over a WINDOW,
// which is what a dashboard sends: the whole-group shortcut does not apply and
// every group needs its match bitset.
func BenchmarkFieldNameCountsWindowed(b *testing.B) {
	s, lo, hi := hostStore(b, 200_000, 8192, 100_000)
	q := &Query{From: lo + 1, To: hi - 1} // both ends inside the data
	b.ResetTimer()
	for b.Loop() {
		sinkVC = FieldNameCounts(s, q)
	}
}
