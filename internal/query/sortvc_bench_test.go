package query

import (
	"fmt"
	"math/rand"
	"testing"
)

// sortValueCounts on a high-cardinality field.
//
// A review measured StatsByField at 1.31 ms -> 4.40 ms (+235% wall) over 20,000
// distinct values when the deterministic tie-break went in. The old comparator
// compared only counts, so on data where every count was equal every comparison
// was false and pdqsort took its already-sorted fast path -- fast, and returning
// a different order every run, which is the defect the tie-break fixed.
//
// This measures the sort itself, apart from the scan, so the choice of sort
// implementation can be judged on its own.
func benchVCs(n int, equalCounts bool) []ValueCount {
	r := rand.New(rand.NewSource(9))
	out := make([]ValueCount, n)
	for i := range out {
		c := 3
		if !equalCounts {
			c = r.Intn(n)
		}
		out[i] = ValueCount{Value: fmt.Sprintf("value-%06d", r.Intn(n)), Count: c}
	}
	return out
}

func benchSort(b *testing.B, n int, equal bool) {
	src := benchVCs(n, equal)
	buf := make([]ValueCount, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(buf, src)
		sortValueCounts(buf)
	}
}

func BenchmarkSortValueCountsEqual20k(b *testing.B)  { benchSort(b, 20000, true) }
func BenchmarkSortValueCountsSpread20k(b *testing.B) { benchSort(b, 20000, false) }
func BenchmarkSortValueCountsEqual1k(b *testing.B)   { benchSort(b, 1000, true) }
