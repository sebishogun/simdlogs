package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// A group file that was written but never committed is an orphan: the
// manifest does not name it, so nothing will ever read it, and nothing will
// ever delete it either.
//
// The window is every step of AppendGroup after writeFileAtomic returns --
// the mmap, the group re-read, and the manifest commit. Each of those can
// fail for exactly the reason the append is being retried (a full disk), so
// the retry loop that is supposed to recover leaves one full-size file behind
// per attempt. That is not a leak that drains: it consumes the disk faster
// than the caller can free it.
func TestFailedAppendLeavesNoOrphanFile(t *testing.T) {
	groupFiles := func(dir string) []string {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir: %v", err)
		}
		var out []string
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "group-") {
				out = append(out, e.Name())
			}
		}
		return out
	}

	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// One good append, so the store has a committed group to compare against.
	if _, err := s.AppendGroup(testGroup(t, 4)); err != nil {
		t.Fatalf("append: %v", err)
	}
	before := groupFiles(dir)
	if len(before) != 1 {
		t.Fatalf("after one append: %v", before)
	}

	// Now fail the manifest commit, which happens AFTER the group file is
	// durable on disk. This is the disk-full shape: the group's own bytes fit,
	// the manifest record does not.
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultManifestWrite {
			return os.ErrInvalid
		}
		return nil
	})
	_, err = s.AppendGroup(testGroup(t, 4))
	restore()
	if err == nil {
		t.Fatal("append with a failing manifest commit returned no error")
	}

	after := groupFiles(dir)
	if len(after) != len(before) {
		t.Fatalf("a failed append left %d group files behind, was %d: %v",
			len(after)-len(before), len(before), after)
	}
}

// testGroup builds a small valid group.
func testGroup(t *testing.T, rows int) *Group {
	t.Helper()
	ts := make([]int64, rows)
	vals := make([]string, rows)
	for i := range ts {
		ts[i] = int64(i + 1)
		vals[i] = "line"
	}
	d := BuildDict(vals)
	return &Group{
		Rows: rows,
		Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: ts},
			{Name: "_msg", Type: ColDict, Dict: &d},
		},
	}
}

// A commit that reached its sync is not an orphan, whatever the error says.
//
// discardUncommitted removes a group file when the manifest does not name the
// id. faultManifestSync fires AFTER the record is durable and after m.apply
// has run, so the commit returns an error with the id VISIBLE -- and deleting
// there leaves a committed group with no bytes, which is a store that will not
// open. The isVisible gate is what keeps that point crash-only.
//
// The record said this seam did not exist. SetFaultHookForTest plus
// faultManifestSync is the seam.
func TestPostSyncCommitFailureKeepsTheGroupFile(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	restore := setFaultHook(func(p faultPoint) error {
		if p == faultManifestSync {
			return errors.New("injected after the record is durable")
		}
		return nil
	})
	_, aerr := s.AppendGroup(testGroup(t, 4))
	restore()
	if aerr == nil {
		t.Fatal("the injected post-sync fault produced no error")
	}

	// The manifest names it, so the file must still be there.
	s.mu.Lock()
	visible := s.man.isVisible(0)
	s.mu.Unlock()
	if !visible {
		t.Skip("the id is not visible; this fault point did not reach past the sync")
	}
	if _, serr := os.Stat(filepath.Join(dir, "group-0.bin")); serr != nil {
		t.Fatalf("a committed group's file was deleted: %v", serr)
	}
	s.Close()

	// And the store still opens, which is the consequence that matters.
	s2, oerr := OpenStore(dir)
	if oerr != nil {
		t.Fatalf("the store will not open: %v", oerr)
	}
	s2.Close()
}

// A commit that could not be rolled back keeps its errno reachable.
//
// joinRollback wrapped the commit error with %v, so errors.Is could not see
// through to the errno: a rollback-failed ENOSPC classified as unrecognised
// and was answered "retry in a second" instead of "someone has to free space".
// The wrapping exists precisely so a caller can tell those apart.
func TestJoinRollbackKeepsEveryCause(t *testing.T) {
	commitErr := fmt.Errorf("write: %w", syscall.ENOSPC)
	rollbackErr := fmt.Errorf("truncate: %w", syscall.EIO)

	err := joinRollback(commitErr, rollbackErr)
	for _, want := range []error{ErrRollbackFailed, syscall.ENOSPC, syscall.EIO} {
		if !errors.Is(err, want) {
			t.Fatalf("errors.Is could not reach %v through %v", want, err)
		}
	}
	// And a successful rollback returns the commit error unchanged, so the
	// ErrRollbackFailed gate above it does not fire on the ordinary path.
	if got := joinRollback(commitErr, nil); got != commitErr {
		t.Fatalf("a successful rollback returned %v, want the commit error itself", got)
	}
	if errors.Is(joinRollback(commitErr, nil), ErrRollbackFailed) {
		t.Fatal("a successful rollback was reported as a failed one")
	}
}
