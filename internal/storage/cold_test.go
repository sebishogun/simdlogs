package storage

import (
	"path/filepath"
	"testing"
)

func TestColdTiering(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(v string, ts int64) *Group {
		d := BuildDict([]string{v})
		return &Group{Rows: 1, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: []int64{ts}},
			{Name: "service", Type: ColDict, Dict: &d},
		}}
	}
	if _, err := s.AppendGroup(mk("old", 1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendGroup(mk("new", 9_000_000_000)); err != nil {
		t.Fatal(err)
	}

	cold := LocalCold{Dir: filepath.Join(dir, "cold")}
	moved, err := s.Demote(2000, cold) // demote the old group
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 || s.Len() != 1 {
		t.Fatalf("demote moved %d, store len %d, want 1 and 1", moved, s.Len())
	}
	names, _ := cold.List()
	if len(names) != 1 {
		t.Fatalf("cold has %d objects, want 1", len(names))
	}

	// Promote it back and confirm it is queryable again.
	if err := s.Promote(names[0], cold); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if s.Len() != 2 {
		t.Fatalf("after promote store len %d, want 2", s.Len())
	}
}
