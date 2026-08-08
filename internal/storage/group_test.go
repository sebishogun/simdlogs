package storage

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
)

func buildGroup(n int) (*Group, []corpus.Record) {
	recs := make([]corpus.Record, 0, n)
	corpus.Gen(7, n, func(r corpus.Record) { recs = append(recs, r) })
	ts := make([]int64, n)
	levels := make([]string, n)
	svcs := make([]string, n)
	msgs := make([]string, n)
	for i, r := range recs {
		ts[i] = r.Time.UnixNano()
		levels[i] = r.Level
		svcs[i] = r.Service
		msgs[i] = r.Message
	}
	ld := BuildDict(levels)
	sd := BuildDict(svcs)
	md := BuildDict(msgs)
	g := &Group{Rows: n, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "level", Type: ColDict, Dict: &ld},
		{Name: "service", Type: ColDict, Dict: &sd},
		{Name: "_msg", Type: ColDict, Dict: &md},
	}}
	return g, recs
}

func TestGroupRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 1000, 100_000} {
		g, recs := buildGroup(n)
		r, err := ReadGroup(g.Marshal())
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if r.Rows != n {
			t.Fatalf("n=%d rows %d", n, r.Rows)
		}
		if n == 0 {
			continue
		}
		// Timestamps round-trip.
		got := r.Timestamps("_time", nil, nil)
		for i := range recs {
			if got[i] != recs[i].Time.UnixNano() {
				t.Fatalf("n=%d ts[%d]", n, i)
			}
		}
		// Dict values round-trip through indices + dict.
		idx, dict := r.DictIndices("level")
		for i := range recs {
			if dict[idx[i]] != recs[i].Level {
				t.Fatalf("n=%d level[%d]=%q want %q", n, i, dict[idx[i]], recs[i].Level)
			}
		}
	}
}

func TestSkipQueries(t *testing.T) {
	g, _ := buildGroup(50_000)
	r, _ := ReadGroup(g.Marshal())

	// Time skip: a window entirely before the group is skipped.
	if r.TimeRangeMatches(0, r.TimeMin) {
		t.Fatal("matched a window ending before timeMin")
	}
	if !r.TimeRangeMatches(r.TimeMin, r.TimeMax+1) {
		t.Fatal("failed to match its own span")
	}
	// Bloom skip: a value never present is rejected without a decode; a
	// present one is accepted.
	if r.DictContains("level", "nonexistent-level-zzz") {
		t.Fatal("bloom false positive on an impossible value (or dict scan wrong)")
	}
	if !r.DictContains("level", "error") {
		t.Fatal("failed to find a present value")
	}
	if !r.ColumnExists("service") || r.ColumnExists("nope") {
		t.Fatal("columnExists wrong")
	}
	// DictID equality fast path.
	if r.DictID("level", "error") < 0 {
		t.Fatal("dictID missing for present value")
	}
	if r.DictID("level", "zzz") != -1 {
		t.Fatal("dictID nonneg for absent value")
	}
}
