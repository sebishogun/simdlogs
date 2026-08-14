package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// A snapshot taken before a recompaction keeps reading the old mapping, and
// that mapping is released when the snapshot closes -- not on a timer.
//
// The old code retired the replaced mapping and unmapped it five minutes
// later, on the assumption that no request lives that long. Nothing bounded
// query duration, so the assumption was unfounded. It also mutated the group
// entry in place, which left a snapshot's reference guarding the NEW mapping
// while the old one it was actually reading went unowned.
func TestRecompactDefersUnmapToOpenSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A group with enough repeated values that flate beats LZ4.
	vals := make([]string, 4000)
	ts := make([]int64, len(vals))
	for i := range vals {
		vals[i] = "a fairly long repeated value that compresses well " + string(rune('a'+i%5))
		ts[i] = int64(i + 1)
	}
	d := BuildDict(vals)
	g := &Group{Rows: len(vals), Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "msg", Type: ColDict, Dict: &d},
	}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}

	var unmapped int
	var mu sync.Mutex
	s.mu.Lock()
	old := s.groups[0]
	inner := old.unmap
	old.unmap = func() error {
		mu.Lock()
		unmapped++
		mu.Unlock()
		return inner()
	}
	s.mu.Unlock()

	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	oldReader := snap.Groups[0]

	if _, _, _, err := s.Recompact(1<<62, false); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	n := unmapped
	mu.Unlock()
	if n != 0 {
		t.Fatal("recompaction unmapped a group an open snapshot was holding")
	}
	// The held reader must still be readable.
	if len(oldReader.blob) == 0 || oldReader.Rows != len(vals) {
		t.Fatal("the held reader lost its mapping across recompaction")
	}

	snap.Close()
	mu.Lock()
	n = unmapped
	mu.Unlock()
	if n != 1 {
		t.Fatalf("old mapping unmapped %d times after the last reader released, want 1", n)
	}
}

// Recompaction must not resurrect a group retention removed while the rewrite
// was in flight. Recompaction writes the file back through writeGroupFile, so
// without a check the removed group reappears on disk and in the index.
func TestRecompactDoesNotResurrectRemovedGroup(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	vals := make([]string, 2000)
	ts := make([]int64, len(vals))
	for i := range vals {
		vals[i] = "repeated repeated repeated value " + string(rune('a'+i%4))
		ts[i] = int64(i + 1)
	}
	d := BuildDict(vals)
	g := &Group{Rows: len(vals), Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "msg", Type: ColDict, Dict: &d},
	}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}

	s.mu.Lock()
	ge := s.groups[0]
	path := ge.path
	s.mu.Unlock()

	// Remove it exactly as retention does, then run a recompaction whose
	// candidate list still holds the stale entry.
	dropped := s.DropGroupsBefore(1 << 62)
	if dropped != 1 {
		t.Fatalf("dropped %d groups, want 1", dropped)
	}
	if err := writeGroupFile(path, g.Marshal()); err != nil {
		t.Fatal(err) // put the file back, as a mid-flight rewrite would
	}

	// Drive the swap directly with the stale entry: this is the state the
	// candidate list would be in.
	s.mu.Lock()
	stale := ge.retired.Load()
	inIndex := false
	for _, cur := range s.groups {
		if cur == ge {
			inIndex = true
		}
	}
	s.mu.Unlock()
	if !stale || inIndex {
		t.Fatalf("after retention the entry should be retired and out of the index (retired=%v inIndex=%v)", stale, inIndex)
	}

	if _, _, _, err := s.Recompact(1<<62, false); err != nil {
		t.Fatal(err)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("%d groups after recompaction, want 0 -- a removed group came back", got)
	}
}

// Cold demotion follows the same order as retention: the cold copy lands, the
// removal commits, the version retires, then the file is unlinked. A held
// snapshot keeps reading throughout.
func TestDemoteDefersUnmapAndCommitsRemoval(t *testing.T) {
	dir := t.TempDir()
	coldDir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{10}}}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}

	var unmapped int
	s.mu.Lock()
	e := s.groups[0]
	inner := e.unmap
	e.unmap = func() error { unmapped++; return inner() }
	s.mu.Unlock()

	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}

	cold := LocalCold{Dir: coldDir}
	moved, err := s.Demote(100, cold)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("demoted %d groups, want 1", moved)
	}
	if unmapped != 0 {
		t.Fatal("demotion unmapped a group an open snapshot was holding")
	}
	if len(snap.Groups[0].blob) == 0 {
		t.Fatal("the held reader lost its mapping across demotion")
	}
	snap.Close()
	if unmapped != 1 {
		t.Fatalf("unmap ran %d times after the last reader released, want 1", unmapped)
	}

	// The removal is committed, so a restart does not bring it back.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 0 {
		t.Fatalf("%d groups after restart, want 0 -- a demoted group came back", got)
	}
	if _, err := os.Stat(filepath.Join(coldDir, "group-0.bin")); err != nil {
		t.Fatalf("cold copy missing: %v", err)
	}
}

// Promotion is idempotent and commits before the group becomes visible.
func TestPromoteIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	coldDir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{10}}}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	cold := LocalCold{Dir: coldDir}
	if _, err := s.Demote(100, cold); err != nil {
		t.Fatal(err)
	}
	if err := s.Promote("group-0.bin", cold); err != nil {
		t.Fatal(err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("%d groups after promote, want 1", got)
	}
	if err := s.Promote("group-0.bin", cold); err != nil {
		t.Fatal(err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("%d groups after a second promote, want 1 -- promotion is not idempotent", got)
	}
}

// Retention, recompaction, demotion and snapshots running together must not
// race or double-unmap. Run under -race.
func TestTieringOperationsConcurrent(t *testing.T) {
	dir := t.TempDir()
	coldDir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 12; i++ {
		vals := make([]string, 200)
		ts := make([]int64, len(vals))
		for j := range vals {
			vals[j] = "value value value " + string(rune('a'+j%3))
			ts[j] = int64(i*1000 + j + 1)
		}
		d := BuildDict(vals)
		g := &Group{Rows: len(vals), Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: ts},
			{Name: "msg", Type: ColDict, Dict: &d},
		}}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				snap, err := s.Snapshot(0, 1<<62)
				if err != nil {
					return
				}
				for _, r := range snap.Groups {
					if len(r.blob) == 0 {
						t.Error("snapshot handed out an unmapped reader")
					}
				}
				snap.Close()
			}
		}()
	}
	wg.Add(3)
	go func() { defer wg.Done(); s.Recompact(1<<62, false) }()
	go func() { defer wg.Done(); s.DropGroupsBefore(3000) }()
	go func() { defer wg.Done(); s.Demote(6000, LocalCold{Dir: coldDir}) }()
	wg.Wait()
}
