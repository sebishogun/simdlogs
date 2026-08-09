package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// A rare value must be found with its field materialized correctly via the
// posting + single-row-decode path.
func TestNeedleCorrectness(t *testing.T) {
	dir := t.TempDir()
	s, _ := storage.OpenStore(dir)
	var ts []int64
	var tr []string
	i := 0
	corpus.Gen(3, 300_000, func(r corpus.Record) {
		ts = append(ts, r.Time.UnixNano())
		v := r.TraceID
		if i == 250_000 {
			v = "NEEDLE-XYZ"
		}
		tr = append(tr, v)
		i++
	})
	// one group
	td := storage.BuildDict(tr)
	g := &storage.Group{Rows: len(ts), Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "trace", Type: storage.ColDict, Dict: &td},
	}}
	s.AppendGroup(g)

	rows := Run(s, &Query{From: 0, To: int64(1) << 62,
		Preds: []Pred{{Field: "trace", Kind: Eq, Value: "NEEDLE-XYZ"}}})
	if len(rows) != 1 {
		t.Fatalf("needle matched %d rows, want 1", len(rows))
	}
	if rows[0].Fields["trace"] != "NEEDLE-XYZ" {
		t.Fatalf("needle field not materialized: %q", rows[0].Fields["trace"])
	}
	// Count path agrees.
	if c := Count(s, &Query{From: 0, To: int64(1) << 62,
		Preds: []Pred{{Field: "trace", Kind: Eq, Value: "NEEDLE-XYZ"}}}); c != 1 {
		t.Fatalf("Count needle = %d, want 1", c)
	}
}
