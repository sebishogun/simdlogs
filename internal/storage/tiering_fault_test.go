package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

// Retention must serialize behind a recompaction rewriting the same path.
// dropGroups took only s.mu while Recompact's rewrite -- atomic rename, then
// mmap -- runs outside any lock, so a removal could unlink the path between
// the two: a spurious Recompact error (mmap of a missing file) or a removed
// group's file recreated on disk. The rewrite is held in that window below
// and retention is let loose at it; the file must not leave the store's
// hands until the rewrite is done.
func TestRetentionSerializedAgainstRecompactRewrite(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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
	s.mu.Lock()
	path := s.groups[0].path
	s.mu.Unlock()

	// Hold the rewrite between its rename and its mmap: the window in which
	// an unlink of the same path breaks it.
	rewrote := make(chan struct{}, 1)
	release := make(chan struct{})
	s.beforeMmap = func(p string) {
		if p == path {
			rewrote <- struct{}{}
			<-release
		}
	}

	done := make(chan error, 1)
	go func() { _, _, _, err := s.Recompact(1<<62, false); done <- err }()

	select {
	case <-rewrote:
	case <-time.After(5 * time.Second):
		close(release)
		<-done
		t.Fatal("recompaction did not reach its rewrite window")
	}

	// Retention starts now, with the new blob already renamed over the path.
	// It must wait for the rewrite to finish; reaching its unlink here is the
	// bug. The waits below are progress guards, not timing assertions: with
	// the lock missing, retention's removal is ~milliseconds of local work,
	// and with it in place retention cannot pass structMu at all.
	removed := make(chan int, 1)
	go func() { removed <- s.DropGroupsBefore(1 << 62) }()
	select {
	case n := <-removed:
		close(release)
		<-done
		t.Fatalf("retention removed %d group(s) while a recompaction was mid-rewrite: dropGroups must take structMu", n)
	case <-time.After(2 * time.Second):
		// Blocked behind the rewrite, as it must be.
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("recompaction errored: %v", err)
	}
	if n := <-removed; n != 1 {
		t.Fatalf("dropped %d groups, want 1", n)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("%d groups after the race, want 0 -- the removed group is still visible", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("group file still exists after retention removed it: %v", err)
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
