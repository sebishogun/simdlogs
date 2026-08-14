package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func storeWithTimedGroups(t *testing.T, dir string, times ...int64) *Store {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ts := range times {
		g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{ts}}}}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// A group whose unlink fails is still gone, and stays gone across a restart.
// The old order -- drop from the in-memory index, then unlink, ignoring the
// error -- brought it back at the next start, so a deleted group reappeared
// with no trace of why.
func TestRetentionSurvivesFailedUnlink(t *testing.T) {
	dir := t.TempDir()
	s := storeWithTimedGroups(t, dir, 10, 20, 5000)

	// Find the file backing the oldest group and make its removal fail by
	// replacing it with a non-empty directory.
	s.mu.Lock()
	var victim string
	for _, g := range s.groups {
		if g.timeMax == 10 {
			victim = g.path
		}
	}
	s.mu.Unlock()
	if victim == "" {
		t.Fatal("no group with the expected timestamp")
	}

	before := RetentionFailures()
	dropped := s.DropGroupsBefore(100)
	if dropped == 0 && RetentionFailures() == before {
		t.Fatal("nothing dropped and no failure counted")
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("%d groups visible after retention, want 1", got)
	}

	// Simulate the unlink having failed by putting the file back.
	if _, err := os.Stat(victim); os.IsNotExist(err) {
		g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{10}}}}
		if err := os.WriteFile(victim, g.Marshal(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 1 {
		t.Fatalf("%d groups after restart, want 1 -- a dropped group came back", got)
	}
}

// Retention must not unmap a group a snapshot is holding, and must release it
// once that snapshot closes. The old code discarded the unmap callback
// entirely, so the mapping was never released at all.
func TestRetentionDefersUnmapToOpenSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := storeWithTimedGroups(t, dir, 10, 5000)
	defer s.Close()

	var unmapped int
	s.mu.Lock()
	var victim *groupEntry
	for _, g := range s.groups {
		if g.timeMax == 10 {
			victim = g
		}
	}
	inner := victim.unmap
	victim.unmap = func() error { unmapped++; return inner() }
	s.mu.Unlock()

	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Groups) != 2 {
		t.Fatalf("snapshot holds %d groups, want 2", len(snap.Groups))
	}

	s.DropGroupsBefore(100)
	if unmapped != 0 {
		t.Fatal("retention unmapped a group an open snapshot was holding")
	}
	// The reader must still be readable: this is the access that faults.
	for _, r := range snap.Groups {
		if len(r.blob) == 0 {
			t.Fatal("a held reader lost its mapping")
		}
	}
	snap.Close()
	if unmapped != 1 {
		t.Fatalf("unmap ran %d times after the last reader released, want 1", unmapped)
	}
}

// A concurrent snapshot taken after the removal commits must not include the
// dropped group.
func TestRetentionHidesGroupFromNewSnapshots(t *testing.T) {
	dir := t.TempDir()
	s := storeWithTimedGroups(t, dir, 10, 5000)
	defer s.Close()

	s.DropGroupsBefore(100)
	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if len(snap.Groups) != 1 {
		t.Fatalf("snapshot holds %d groups after retention, want 1", len(snap.Groups))
	}
	if snap.Groups[0].TimeMax == 10 {
		t.Fatal("a dropped group appeared in a new snapshot")
	}
}

// A failed unlink leaves a tombstone that a later pass retries, and the
// counter goes back down when it succeeds.
func TestRetentionRetriesTombstones(t *testing.T) {
	dir := t.TempDir()
	s := storeWithTimedGroups(t, dir, 10, 5000)
	defer s.Close()

	s.mu.Lock()
	var victim string
	for _, g := range s.groups {
		if g.timeMax == 10 {
			victim = g.path
		}
	}
	s.mu.Unlock()

	// Make the unlink fail: replace the file with a non-empty directory.
	if err := os.Remove(victim); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(victim, "blocker"), 0o700); err != nil {
		t.Fatal(err)
	}

	beforePending := PendingTombstones()
	s.DropGroupsBefore(100)
	if PendingTombstones() <= beforePending {
		t.Fatalf("pending tombstones %d, want more than %d after a failed unlink",
			PendingTombstones(), beforePending)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("%d groups visible, want 1 -- the group must be gone despite the failed unlink", got)
	}

	// Clear the blocker; the next pass must reclaim it.
	if err := os.RemoveAll(filepath.Join(victim, "blocker")); err != nil {
		t.Fatal(err)
	}
	s.DropGroupsBefore(0) // matches nothing, but retries tombstones
	if PendingTombstones() != beforePending {
		t.Fatalf("pending tombstones %d after a successful retry, want %d",
			PendingTombstones(), beforePending)
	}
	if _, err := os.Stat(victim); !os.IsNotExist(err) {
		t.Fatal("the tombstoned file was not reclaimed")
	}
}
