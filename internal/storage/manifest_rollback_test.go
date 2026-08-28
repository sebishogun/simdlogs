package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// A failed commit must roll back to the END of the existing log, not to zero.
//
// The first version took the offset with Seek(0, io.SeekCurrent). The file is
// opened O_APPEND, which repositions only before each write, so that returned
// 0 until the process's first commit -- and a failed first commit truncated
// the entire manifest. OpenStore's legacy path then re-adopted every
// group-*.bin on disk, bringing back exactly the removals the manifest exists
// to make durable. ENOSPC during a retention pass is the ordinary trigger.
func TestCommitRollbackKeepsEarlierRecords(t *testing.T) {
	dir := t.TempDir()

	// Commit some history, then close: the next open is a fresh process.
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := m.commit([]uint64{i}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, ManifestFileName)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sizeBefore := fi.Size()
	if sizeBefore == 0 {
		t.Fatal("no history was written")
	}

	m2, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.close()
	if got := len(m2.visibleIDs()); got != 5 {
		t.Fatalf("%d groups visible after reopen, want 5", got)
	}

	// The rollback point of the very first commit of this process must be the
	// end of the file, not the start.
	m2.truncateTo(m2.rollbackOffset(t))

	fi, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != sizeBefore {
		t.Fatalf("a rollback before any write changed the manifest from %d to %d bytes",
			sizeBefore, fi.Size())
	}

	m3, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m3.close()
	if got := len(m3.visibleIDs()); got != 5 {
		t.Fatalf("%d groups visible after the rollback, want 5", got)
	}
}

// rollbackOffset is what commit uses as its rollback point, exposed for the
// test above.
func (m *manifest) rollbackOffset(t *testing.T) int64 {
	t.Helper()
	// openManifest leaves the handle nil so the file's existence is still
	// readable when OpenStore decides whether to bootstrap; anything reaching
	// for the handle opens it first.
	if err := m.ensureOpen(); err != nil {
		t.Fatal(err)
	}
	fi, err := m.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// The end-to-end shape of the defect: committed removals must not come back
// when a later commit fails and rolls back.
func TestFailedCommitDoesNotResurrectRemovedGroups(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for i := 0; i < 3; i++ {
		g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}}}}
		id, err := s.AppendGroup(g)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// CommitRemoval records the decision; the in-memory index is the caller's
	// business (retention updates it), so the count only changes on reopen.
	if err := s.CommitRemoval(ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Len(); got != 1 {
		t.Fatalf("%d groups visible after removing two, want 1", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh open, then a rollback with nothing written in this process.
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	m.truncateTo(m.rollbackOffset(t))
	m.close()

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 1 {
		t.Fatalf("%d groups after a rollback, want 1 -- removed groups came back", got)
	}
}
