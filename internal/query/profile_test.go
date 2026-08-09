package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

func benchStore(b testing.TB, total, gsize int) (*storage.Store, int64, int64) {
	s, _ := storage.OpenStore(b.(*testing.B).TempDir())
	var ts []int64
	var sv []string
	var lo, hi int64
	flush := func() {
		if len(ts) == 0 {
			return
		}
		sd := storage.BuildDict(sv)
		s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
			{Name: "service", Type: storage.ColDict, Dict: &sd},
		}})
		ts, sv = nil, nil
	}
	corpus.Gen(9, total, func(r corpus.Record) {
		t := r.Time.UnixNano()
		if lo == 0 {
			lo = t
		}
		hi = t
		ts = append(ts, t)
		sv = append(sv, r.Service)
		if len(ts) >= gsize {
			flush()
		}
	})
	flush()
	return s, lo, hi
}

func BenchmarkEngineWindowed(b *testing.B) {
	s, lo, hi := benchStore(b, 3_000_000, 128*1024)
	from := lo + (hi-lo)/2
	to := from + (hi-lo)/50
	q := &Query{From: from, To: to, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	b.ResetTimer()
	for b.Loop() {
		sinkRows = Run(s, q)
	}
}

func BenchmarkEngineCount(b *testing.B) {
	s, lo, hi := benchStore(b, 3_000_000, 128*1024)
	from := lo + (hi-lo)/2
	to := from + (hi-lo)/50
	q := &Query{From: from, To: to, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	b.ResetTimer()
	for b.Loop() {
		sinkI = Count(s, q)
	}
}

var sinkI int
var sinkH map[int64]int

func BenchmarkEngineHistogram(b *testing.B) {
	s, lo, hi := benchStore(b, 3_000_000, 128*1024)
	from := lo + (hi-lo)/2
	to := from + (hi-lo)/50
	q := &Query{From: from, To: to, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	step := (to - from) / 20
	b.ResetTimer()
	for b.Loop() {
		sinkH = Histogram(s, q, step)
	}
}

func BenchmarkEngineFullScanCount(b *testing.B) {
	s, lo, hi := benchStore(b, 3_000_000, 128*1024) // ~23 groups
	q := &Query{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	b.ResetTimer()
	for b.Loop() {
		sinkI = Count(s, q)
	}
}

func BenchmarkEngineNeedle(b *testing.B) {
	// 3M rows, ~23 groups; a unique value planted in one group, queried
	// over the full span -- the selective needle, pure engine (no HTTP).
	s, _ := storage.OpenStore(b.TempDir())
	var ts []int64
	var tr []string
	i := 0
	corpus.Gen(9, 3_000_000, func(r corpus.Record) {
		ts = append(ts, r.Time.UnixNano())
		v := r.TraceID
		if i == 2_900_000 {
			v = "NEEDLEc0ffee42"
		}
		tr = append(tr, v)
		i++
		if len(ts) >= 128*1024 {
			sd := storage.BuildDict(tr)
			s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
				{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
				{Name: "trace", Type: storage.ColDict, Dict: &sd},
			}})
			ts, tr = nil, nil
		}
	})
	if len(ts) > 0 {
		sd := storage.BuildDict(tr)
		s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
			{Name: "trace", Type: storage.ColDict, Dict: &sd},
		}})
	}
	q := &Query{From: 0, To: int64(1) << 62, Preds: []Pred{{Field: "trace", Kind: Eq, Value: "NEEDLEc0ffee42"}}}
	b.ResetTimer()
	for b.Loop() {
		sinkRows = Run(s, q)
	}
}
