package storage

import (
	"fmt"
	"testing"
)

var sinkVC []ValueCount

// BenchmarkValueCounts isolates the count-table read across cardinalities: a
// low-card column (level-like) takes the scalar tail, a high-card one
// (trace-like) engages the SIMD bulk decode. One 128K-row group.
func BenchmarkValueCounts(b *testing.B) {
	const rows = 128 * 1024
	for _, card := range []int{8, 1000, rows} {
		b.Run(fmt.Sprintf("card=%d", card), func(b *testing.B) {
			vals := make([]string, rows)
			for i := range vals {
				vals[i] = fmt.Sprintf("v%d", i%card)
			}
			d := BuildDict(vals)
			g := &Group{Rows: rows, Columns: []Column{
				{Name: "_time", Type: ColTimestamp, Ts: make([]int64, rows)},
				{Name: "c", Type: ColDict, Dict: &d},
			}}
			r, err := ReadGroup(g.Marshal())
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for b.Loop() {
				sinkVC = r.ValueCounts("c")
			}
		})
	}
}
