package query

import (
	"strconv"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// hostStore mirrors the shape `top N by (host)` meets in practice: a
// thousand-value dictionary repeated across many groups, which is where the
// per-group dictionary decode shows up.
func hostStore(b testing.TB, total, gsize, cardinality int) (*storage.Store, int64, int64) {
	s, _ := storage.OpenStore(b.TempDir())
	var ts []int64
	var hv []string
	var lo, hi int64
	flush := func() {
		if len(ts) == 0 {
			return
		}
		hd := storage.BuildDict(hv)
		s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
			{Name: "host", Type: storage.ColDict, Dict: &hd},
		}})
		ts, hv = nil, nil
	}
	t := int64(1_700_000_000_000_000_000)
	for i := 0; i < total; i++ {
		t += 400_000
		if lo == 0 {
			lo = t
		}
		hi = t
		ts = append(ts, t)
		hv = append(hv, "node-"+strconv.Itoa(i%cardinality))
		if len(ts) >= gsize {
			flush()
		}
	}
	flush()
	return s, lo, hi
}

func BenchmarkTopKHighCard(b *testing.B) {
	s, lo, hi := hostStore(b, 200_000, 8192, 1024)
	q := &Query{From: lo, To: hi + 1}
	b.ResetTimer()
	for b.Loop() {
		sinkRows, _ = runTopFast(s, q, &TopPipe{By: []string{"host"}, N: 10})
	}
}
