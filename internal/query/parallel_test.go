package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Parallel output must equal the serial path exactly, rows in the same
// order, for a query spanning many groups.
func TestParallelEqualsSerial(t *testing.T) {
	t.Parallel()
	s, _ := storage.OpenStore(t.TempDir())
	var ts []int64
	var sv []string
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
	corpus.Gen(5, 500_000, func(r corpus.Record) {
		ts = append(ts, r.Time.UnixNano())
		sv = append(sv, r.Service)
		if len(ts) >= 50_000 {
			flush()
		}
	})
	flush() // 10 groups
	q := &Query{From: 0, To: int64(1) << 62, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}

	par := Run(s, q) // parallel (>= 4 groups)
	groups := s.Groups(q.From, q.To)
	var ser []Row
	for _, g := range groups {
		if groupCanMatch(g, q) {
			ser = appendMatches(ser, g, q)
		}
	}
	if len(par) != len(ser) {
		t.Fatalf("parallel %d rows, serial %d", len(par), len(ser))
	}
	for i := range par {
		if par[i].Time != ser[i].Time {
			t.Fatalf("row %d order differs: %d vs %d", i, par[i].Time, ser[i].Time)
		}
	}
	if Count(s, q) != len(ser) {
		t.Fatalf("Count %d != %d", Count(s, q), len(ser))
	}
}
