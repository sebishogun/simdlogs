package storage

import (
	"errors"
	"sync"
	"testing"
)

func storeWithGroups(t *testing.T, n int) *Store {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		d := BuildDict([]string{"a", "b"})
		g := &Group{Rows: 2, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i)*100 + 1, int64(i)*100 + 2}},
			{Name: "k", Type: ColDict, Dict: &d},
		}}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// A snapshot keeps its mappings alive across a store close. Closing the store
// used to unmap every group immediately, so an in-flight query walking a
// reader read freed memory -- and shutdown is exactly when in-flight queries
// are most likely to still be running.
func TestSnapshotSurvivesStoreClose(t *testing.T) {
	s := storeWithGroups(t, 3)
	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Groups) != 3 {
		t.Fatalf("snapshot holds %d groups, want 3", len(snap.Groups))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The readers must still be usable: touching blob is what a column decode
	// does, and it is the access that would fault on an unmapped region.
	for i, r := range snap.Groups {
		if len(r.blob) == 0 {
			t.Fatalf("group %d lost its mapping while a snapshot held it", i)
		}
		if r.Rows != 2 {
			t.Fatalf("group %d reads %d rows after store close", i, r.Rows)
		}
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	// After the last holder releases, a new snapshot is refused rather than
	// handing out anything.
	if _, err := s.Snapshot(0, 1<<62); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Snapshot after close: %v, want ErrStoreClosed", err)
	}
}

// Retiring a version while a snapshot holds it defers the unmap; the unmap
// happens on the last release, exactly once.
func TestRetireDefersUnmapToLastReader(t *testing.T) {
	s := storeWithGroups(t, 1)
	defer s.Close()

	var unmapped int
	var mu sync.Mutex
	s.mu.Lock()
	e := s.groups[0]
	inner := e.unmap
	e.unmap = func() error {
		mu.Lock()
		unmapped++
		mu.Unlock()
		return inner()
	}
	s.mu.Unlock()

	a, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.retire(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := unmapped
	mu.Unlock()
	if got != 0 {
		t.Fatalf("retire unmapped while %d snapshots held the group", 2)
	}

	a.Close()
	mu.Lock()
	got = unmapped
	mu.Unlock()
	if got != 0 {
		t.Fatal("unmapped while one snapshot still held the group")
	}

	b.Close()
	mu.Lock()
	got = unmapped
	mu.Unlock()
	if got != 1 {
		t.Fatalf("unmap ran %d times after the last release, want exactly 1", got)
	}

	// Double Close must not release twice.
	b.Close()
	a.Close()
	mu.Lock()
	got = unmapped
	mu.Unlock()
	if got != 1 {
		t.Fatalf("unmap ran %d times after redundant closes, want 1", got)
	}
}

// A group retired before a snapshot is taken never appears in it.
func TestSnapshotSkipsRetiredGroups(t *testing.T) {
	s := storeWithGroups(t, 2)
	defer s.Close()

	s.mu.Lock()
	e := s.groups[0]
	s.mu.Unlock()
	if err := e.retire(); err != nil {
		t.Fatal(err)
	}

	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	for _, r := range snap.Groups {
		if r == e.reader {
			t.Fatal("a retired and released group was handed out by a snapshot")
		}
	}
}

// Concurrent snapshots and retirement must not double-unmap or hand out a
// released version. Run under -race.
func TestSnapshotConcurrentWithRetire(t *testing.T) {
	s := storeWithGroups(t, 8)
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.mu.Lock()
		entries := append([]*groupEntry(nil), s.groups...)
		s.mu.Unlock()
		for _, e := range entries {
			e.retire()
		}
	}()
	wg.Wait()
}

// EmptySnapshot is usable and closing it is a no-op, so callers that cannot
// return an error have something safe to fall back to.
func TestEmptySnapshot(t *testing.T) {
	snap := EmptySnapshot()
	if len(snap.Groups) != 0 {
		t.Fatal("EmptySnapshot has groups")
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	var nilSnap *Snapshot
	if err := nilSnap.Close(); err != nil {
		t.Fatal(err)
	}
}
