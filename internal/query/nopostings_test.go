package query

import (
	"fmt"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// buildStore makes a one-group store from the same data, with or without the
// per-column inverted index -- the two tiers the recompaction pass produces.
func buildTieredStore(t *testing.T, rows int, noPostings bool) *storage.Store {
	t.Helper()
	ts := make([]int64, rows)
	level := make([]string, rows)
	svc := make([]string, rows)
	msg := make([]string, rows)
	levels := []string{"info", "warn", "error", "debug"}
	for i := 0; i < rows; i++ {
		ts[i] = int64(1_700_000_000_000_000_000) + int64(i)*1_000_000
		level[i] = levels[i%len(levels)]
		svc[i] = fmt.Sprintf("service-%d", i%13)
		msg[i] = fmt.Sprintf("handler %d timed out after %dms", i%97, i%400)
	}
	ld, sd, md := storage.BuildDict(level), storage.BuildDict(svc), storage.BuildDict(msg)
	g := &storage.Group{Rows: rows, NoPostings: noPostings, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "level", Type: storage.ColDict, Dict: &ld},
		{Name: "service", Type: storage.ColDict, Dict: &sd},
		{Name: "_msg", Type: storage.ColDict, Dict: &md},
	}}
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestNoPostingsSameResults is the guard on the tiering pass: dropping the
// inverted index is a size/speed trade, never a correctness one. Every query
// shape must return byte-identical rows from both tiers.
func TestNoPostingsSameResults(t *testing.T) {
	t.Parallel()
	const rows = 5000
	withP := buildTieredStore(t, rows, false)
	noP := buildTieredStore(t, rows, true)

	queries := map[string]*Query{
		"eq-common":   {From: 0, To: 1 << 62, MatAll: true, Preds: []Pred{{Field: "level", Value: "error", Kind: Eq}}},
		"eq-rare":     {From: 0, To: 1 << 62, MatAll: true, Preds: []Pred{{Field: "service", Value: "service-7", Kind: Eq}}},
		"eq-absent":   {From: 0, To: 1 << 62, MatAll: true, Preds: []Pred{{Field: "level", Value: "nope", Kind: Eq}}},
		"substring":   {From: 0, To: 1 << 62, MatAll: true, Preds: []Pred{{Field: "_msg", Value: "timed out", Kind: Contains}}},
		"two-preds":   {From: 0, To: 1 << 62, MatAll: true, Preds: []Pred{{Field: "level", Value: "warn", Kind: Eq}, {Field: "service", Value: "service-3", Kind: Eq}}},
		"time-window": {From: int64(1_700_000_000_000_000_000), To: int64(1_700_000_000_000_000_000) + 1000*1_000_000, MatAll: true, Preds: []Pred{{Field: "level", Value: "info", Kind: Eq}}},
	}
	for name, q := range queries {
		a := Run(withP, q)
		b := Run(noP, q)
		if len(a) != len(b) {
			t.Errorf("%s: %d rows with postings vs %d without", name, len(a), len(b))
			continue
		}
		for i := range a {
			if a[i].Time != b[i].Time || len(a[i].Fields) != len(b[i].Fields) {
				t.Errorf("%s: row %d differs", name, i)
				break
			}
			for j := range a[i].Fields {
				if a[i].Fields[j] != b[i].Fields[j] {
					t.Errorf("%s: row %d field %d: %+v vs %+v", name, i, j, a[i].Fields[j], b[i].Fields[j])
					break
				}
			}
		}
		if len(a) == 0 && name != "eq-absent" {
			t.Errorf("%s matched nothing -- the test is not exercising the path", name)
		}
	}
}
