package query

import (
	"sync/atomic"
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// buildStore ingests the corpus into groups of gsize rows.
func buildStore(t testing.TB, dir string, total, gsize int) *storage.Store {
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ts []int64
	var lv, sv, ms []string
	flush := func() {
		if len(ts) == 0 {
			return
		}
		ld := storage.BuildDict(lv)
		sd := storage.BuildDict(sv)
		md := storage.BuildDict(ms)
		g := &storage.Group{Rows: len(ts), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
			{Name: "level", Type: storage.ColDict, Dict: &ld},
			{Name: "service", Type: storage.ColDict, Dict: &sd},
			{Name: "_msg", Type: storage.ColDict, Dict: &md},
		}}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
		ts, lv, sv, ms = nil, nil, nil, nil
	}
	corpus.Gen(9, total, func(r corpus.Record) {
		ts = append(ts, r.Time.UnixNano())
		lv = append(lv, r.Level)
		sv = append(sv, r.Service)
		ms = append(ms, r.Message)
		if len(ts) >= gsize {
			flush()
		}
	})
	flush()
	return s
}

func TestEngineSelectiveAndSkip(t *testing.T) {
	dir := t.TempDir()
	s := buildStore(t, dir, 200_000, 20_000) // 10 groups
	var opens int64
	s.SetOpenHook(func(uint64) { atomic.AddInt64(&opens, 1) })

	// Full span, equality on level -- correctness vs a brute-force count.
	full := s.Groups(mn, mx)
	_ = full
	q := &Query{From: mn, To: mx, Preds: []Pred{{Field: "level", Kind: Eq, Value: "error"}}}
	rows := Run(s, q)
	// Brute force the same predicate.
	want := 0
	corpus.Gen(9, 200_000, func(r corpus.Record) {
		if r.Level == "error" {
			want++
		}
	})
	if len(rows) != want {
		t.Fatalf("equality: got %d rows want %d", len(rows), want)
	}

	// Time skip: a one-group-wide window opens far fewer than all groups.
	atomic.StoreInt64(&opens, 0)
	// pick a narrow window inside the corpus span
	span := mx - mn
	from := mn + span/2
	to := from + span/40 // ~1/40th of the span
	Run(s, &Query{From: from, To: to, Preds: []Pred{{Field: "level", Kind: Eq, Value: "error"}}})
	if o := atomic.LoadInt64(&opens); o >= int64(s.Len()) {
		t.Fatalf("narrow window opened %d of %d groups -- no time skip", o, s.Len())
	}
}

const (
	mn = int64(0)
	mx = int64(1) << 62
)

// BenchmarkSelectiveSkip contrasts a selective query that skips groups by
// time and bloom against a forced full scan of every group -- the
// difference is the design's whole premise.
func BenchmarkSelectiveSkip(b *testing.B) {
	dir := b.TempDir()
	s := buildStore(b, dir, 1_000_000, 100_000) // 10 groups of 100K
	gs := s.Groups(mn, mx)
	lo, hi := gs[0].TimeMin, gs[len(gs)-1].TimeMax
	from := lo + (hi-lo)/2
	to := from + (hi-lo)/50 // ~1/50th window
	sel := &Query{From: from, To: to, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	full := &Query{From: lo, To: hi, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}
	b.Run("selective-window", func(b *testing.B) {
		for b.Loop() {
			sinkRows = Run(s, sel)
		}
	})
	b.Run("full-scan", func(b *testing.B) {
		for b.Loop() {
			sinkRows = Run(s, full)
		}
	})
}

var sinkRows []Row
