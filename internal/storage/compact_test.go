package storage

import (
	"fmt"
	"testing"
)

// TestCompactDict verifies compact mode round-trips identically to the default
// and reports the footprint delta on realistic-shaped high-cardinality data.
func TestCompactDict(t *testing.T) {
	t.Parallel()
	const n = 20000
	msg := make([]string, n)
	trace := make([]string, n)
	for i := 0; i < n; i++ {
		msg[i] = fmt.Sprintf("request %d completed in %dms for user session", i%500, i%1000)
		trace[i] = fmt.Sprintf("%016x%016x", uint64(i)*0x9e3779b97f4a7c15, uint64(i))
	}
	build := func(compact bool) []byte {
		md := BuildDict(msg)
		td := BuildDict(trace)
		g := &Group{Rows: n, Compact: compact, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: makeTs(n)},
			{Name: "_msg", Type: ColDict, Dict: &md},
			{Name: "trace_id", Type: ColDict, Dict: &td},
		}}
		return g.Marshal()
	}
	normal := build(false)
	compact := build(true)

	// Round-trip: compact must return the exact same values as default.
	rd, err := ReadGroup(compact)
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []struct {
		name string
		want []string
	}{{"_msg", msg}, {"trace_id", trace}} {
		vals, _ := rd.DictIndices(col.name)
		idx, dict := vals, rd.dictOf(col.name)
		for i := 0; i < n; i++ {
			if got := dict[idx[i]]; got != col.want[i] {
				t.Fatalf("%s row %d = %q want %q", col.name, i, got, col.want[i])
			}
		}
		// Equality lookup (the needle path) must still find a value.
		if id := rd.DictID(col.name, col.want[n/2]); id < 0 {
			t.Fatalf("compact %s: DictID lost value", col.name)
		}
	}
	t.Logf("footprint: default %d KB, compact %d KB (%.2fx smaller)",
		len(normal)/1024, len(compact)/1024, float64(len(normal))/float64(len(compact)))
}

func makeTs(n int) []int64 {
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i)
	}
	return ts
}

// dictOf returns a column's full dictionary (test helper).
func (r *Reader) dictOf(name string) []string {
	_, dict := r.DictIndices(name)
	return dict
}
