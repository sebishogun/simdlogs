//go:build !windows

package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A lock is worth nothing if the file it holds is not the file at the path.
//
// lockDir opens the lock file and then flocks it, which is two syscalls, and a
// staged restore replaces the whole destination directory in one. Land in
// between and the descriptor names an inode that has left the path: unlinked
// with the directory it lived in, contended by nobody, so the flock always
// succeeds. Two processes then each believe they hold the directory, and the
// one holding the corpse goes on writing group files BY PATH into the store
// that replaced it.
//
// Found by tracing a 60-second hammer, not by reasoning: 24 writers against a
// restore loop produced a store opened on lock inode 113001480 six directory
// generations after that inode had been released and deleted, appending
// group-0.bin into the live directory whose lock was 113001501. 13 committed
// groups vanished and 27 of the archive's groups were overwritten by writers
// holding a dead lock; the same harness with Restore never called lost
// nothing, so the swap was the cause and not the harness.
//
// The window is a nanosecond wide, so this test does not race for it: the
// fault point inside lockDir puts it exactly there.
func TestLockDirRefusesAFileThatLeftThePath(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "store")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A lock file to find. The first lockDir creates it and release leaves it,
	// which is what makes the second call an OPEN of an existing inode rather
	// than a create -- the shape the window needs.
	first, err := lockDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.release(); err != nil {
		t.Fatal(err)
	}

	// Stand in the window and do what a staged restore does: take the whole
	// directory away and put a different one, with a different lock file, at
	// the same path.
	swaps := 0
	defer setFaultHook(func(p faultPoint) error {
		if p != faultLockOpened || swaps > 0 {
			return nil
		}
		swaps++
		if err := os.Rename(dir, filepath.Join(base, "displaced")); err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(dir, LockFileName))
		if err != nil {
			return err
		}
		return f.Close()
	})()

	held, err := lockDir(dir)
	if err != nil {
		t.Fatalf("lockDir after one swap: %v", err)
	}
	defer held.release()
	if swaps != 1 {
		t.Fatalf("the directory was swapped %d times; this test asserted nothing", swaps)
	}

	// The assertion that matters is not which inode came back, it is what the
	// lock DOES. Before the check, lockDir returned the unlinked inode from the
	// displaced directory and this second call flocked the live one without
	// contention: one path, two holders, which is the state that loses data.
	if second, err := lockDir(dir); err == nil {
		second.release()
		t.Fatal("a second lockDir succeeded while the first held the directory")
	} else if !errors.Is(err, ErrLocked) {
		t.Fatalf("second lockDir: %v, want ErrLocked", err)
	}

	// And the descriptor is the file at the path, which is the reason it does.
	fi, err := held.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	at, err := os.Stat(filepath.Join(dir, LockFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(fi, at) {
		t.Fatal("the returned lock holds a file that is not the one at the path")
	}
}

// A destination being replaced in a loop has no lock to give, and lockDir says
// so with ErrLocked rather than spinning. Without a bound the caller hangs,
// which is worse than an answer it can retry.
func TestLockDirGivesUpOnAnEndlesslySwappedDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "store")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	first, err := lockDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.release(); err != nil {
		t.Fatal(err)
	}

	swaps := 0
	defer setFaultHook(func(p faultPoint) error {
		if p != faultLockOpened {
			return nil
		}
		swaps++
		gone := filepath.Join(base, "displaced")
		if err := os.RemoveAll(gone); err != nil {
			return err
		}
		if err := os.Rename(dir, gone); err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(dir, LockFileName))
		if err != nil {
			return err
		}
		return f.Close()
	})()

	l, err := lockDir(dir)
	if err == nil {
		l.release()
		t.Fatal("lockDir returned a lock on a directory replaced under every attempt")
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("lockDir: %v, want ErrLocked", err)
	}
	// Bounded, and the bound is the one the constant names: an off-by-one here
	// is a test that would pass against an unbounded loop that happened to be
	// interrupted.
	if swaps != lockDirRetries+1 {
		t.Fatalf("lockDir made %d attempts, want %d", swaps, lockDirRetries+1)
	}
}
