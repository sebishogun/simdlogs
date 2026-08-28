//go:build !windows

package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The four guards a sign-off review found unguarded, in five test functions
// and six cases: the
// rename that displaces the old destination, the marker outliving the
// directories it vouches for (two paths, two subtests), clearStaging's arm for
// the displaced directory, the marker rule applied to that directory, and the
// rmdir on the failure path that could only ever remove a competitor's
// directory.
//
// Each of those guards was deletable with the whole suite staying green, which
// is the same thing as not having been tested.

// The old destination is RENAMED out of the way, not removed in place.
//
// Asserted where the difference is observable rather than by its outcome: the
// two are indistinguishable once the swap is done, which is why reverting the
// rename to os.RemoveAll(dst) failed nothing. Between the two renames the
// rename version has moved the directory to a sibling with this call's own
// lock inode still inside it, and the removal version has destroyed it. The
// fault point puts a test exactly there.
func TestTheOldDestinationIsRenamedAsideNotRemoved(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "store")
	if err := os.Mkdir(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	// A lock file in the destination, so the inode this test follows exists
	// before the restore starts and can be compared afterwards.
	seed, err := lockDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	seedIno := inodeOfFile(t, seed.f)
	if err := seed.release(); err != nil {
		t.Fatal(err)
	}

	archive := backupOf(t, 2)
	displaced := dst + ".restoring.old"

	var sawDisplaced, sawDstGone bool
	var displacedIno uint64
	defer setFaultHook(func(p faultPoint) error {
		if p != faultRestoreRemoved {
			return nil
		}
		// Here the destination has left the namespace and the archive has not
		// arrived yet.
		if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
			sawDstGone = true
		}
		if fi, err := os.Stat(filepath.Join(displaced, LockFileName)); err == nil {
			sawDisplaced = true
			displacedIno = inodeOfFileInfo(t, fi)
		}
		return nil
	})()

	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !sawDstGone {
		t.Error("the destination still existed between the two renames")
	}
	if !sawDisplaced {
		t.Fatal("no displaced directory existed between the two renames: the destination was " +
			"removed in place, which leaves it present and lockless for the length of the walk")
	}
	if displacedIno != seedIno {
		t.Errorf("the displaced lock is inode %d, want the destination's own %d -- the "+
			"directory was rebuilt rather than moved", displacedIno, seedIno)
	}
	// And nothing is left behind once the call returns.
	assertNoResidue(t, dir, dst)
}

// The marker outlives every directory it vouches for, on BOTH paths.
//
// This is an ORDERING, and the end state is the same either way: everything is
// gone when the call returns. The version that removed the marker inside the
// displaced cleanup left a staging directory with no marker for six to seven
// per cent of an unwind, and clearStaging refuses that forever. So the
// assertion has to be made from inside the cleanup, which is what the fault
// point is for.
//
// Both paths, because they are not the same shape and the defect was on the
// second. On success the staging directory has already been renamed into
// place, so there is none left to be orphaned and only the marker's own
// ordering is observable. On an abort between the two renames the staging
// directory is still on disk when the cleanup runs -- which is exactly the
// state a kill would freeze -- and the marker has to still be there with it.
// The first version of this test asserted only the success path, where its
// staging check could never fire.
func TestTheMarkerOutlivesTheDirectoriesItVouchesFor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		abort bool
	}{
		{"success", false},
		{"abort between the renames", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "store")
			archive := backupOf(t, 2)
			staging := dst + ".restoring"
			displaced := dst + ".restoring.old"
			marker := dst + restoringMarkerSuffix

			fired := 0
			var stagingLeft, displacedLeft, markerGone bool
			defer setFaultHook(func(p faultPoint) error {
				if p == faultRestoreRemoved && tc.abort {
					return errors.New("a fault the test injected between the renames")
				}
				if p != faultRestoreCleanup {
					return nil
				}
				fired++
				if _, err := os.Stat(staging); err == nil {
					stagingLeft = true
				}
				if _, err := os.Stat(displaced); err == nil {
					displacedLeft = true
				}
				if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
					markerGone = true
				}
				return nil
			})()

			_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
			if tc.abort && err == nil {
				t.Fatal("the restore returned nil after the injected abort")
			}
			if !tc.abort && err != nil {
				t.Fatalf("restore: %v", err)
			}
			if fired == 0 {
				t.Fatal("the cleanup fault point never fired; this test asserted nothing")
			}
			if displacedLeft {
				t.Error("the displaced directory outlived the point it is removed at")
			}
			// The invariant, and the only one that holds on both paths: the
			// marker is still there while the directories are being removed.
			if markerGone {
				t.Error("the marker was already gone before the directories it vouches for: " +
					"a kill there leaves residue clearStaging refuses forever")
			}
			if !tc.abort && stagingLeft {
				t.Error("a staging directory was still on disk on the success path, " +
					"where it should already have been renamed into place")
			}
			if tc.abort && !stagingLeft {
				t.Error("no staging directory was on disk when the cleanup ran on the abort " +
					"path; this subtest is not reaching the state it exists for")
			}
			assertNoResidue(t, dir, dst)
		})
	}
}

// A crashed restore's residue is retryable, and that includes the displaced
// directory -- which clearStaging did not clear until it was told to.
func TestCrashResidueIncludingTheDisplacedDirectoryIsCleared(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "store")
	staging := dst + ".restoring"
	displaced := dst + ".restoring.old"
	marker := dst + restoringMarkerSuffix

	// What a kill between the two renames leaves: both directories and the
	// marker that says they are a restore's.
	for _, d := range []string{staging, displaced} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "group-0.bin"), []byte("residue"), DataFileMode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(marker, nil, DataFileMode); err != nil {
		t.Fatal(err)
	}

	archive := backupOf(t, 2)
	man, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	if err != nil {
		t.Fatalf("a retry over a crashed restore's residue: %v", err)
	}
	if man == nil || len(man.Groups) == 0 {
		t.Fatal("the retry restored nothing")
	}
	assertNoResidue(t, dir, dst)
	// And the restored store is the archive's, not the residue's.
	if b, err := os.ReadFile(filepath.Join(dst, "group-0.bin")); err == nil && bytes.Equal(b, []byte("residue")) {
		t.Fatal("the residue's group survived into the restored store")
	}
}

// A displaced directory with no marker is somebody else's and is refused, the
// same rule the staging directory has. `-dst /srv/logs` derives
// `/srv/logs.restoring.old`, which is a path an operator can stumble into.
func TestAMarkerlessDisplacedDirectoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "store")
	displaced := dst + ".restoring.old"
	if err := os.MkdirAll(displaced, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(displaced, "somebody-elses.bin"), []byte("x"), DataFileMode); err != nil {
		t.Fatal(err)
	}
	archive := backupOf(t, 2)
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	if !errors.Is(err, ErrDestinationNotEmpty) {
		t.Fatalf("%v, want ErrDestinationNotEmpty", err)
	}
	// Untouched: it was never this call's to remove.
	if _, err := os.Stat(filepath.Join(displaced, "somebody-elses.bin")); err != nil {
		t.Fatalf("the refused directory's contents were removed anyway: %v", err)
	}
}

// A restore that aborts leaves the destination a competitor created alone.
//
// The rmdir this replaced could not remove its own residue -- lockDir has put
// a LOCK there and rmdir refuses a non-empty directory -- and could remove a
// competitor's, which is the only thing it ever did.
func TestAnAbortedRestoreDoesNotRemoveAForeignDestination(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "store")
	archive := backupOf(t, 2)

	// Abort between the two renames, and create the destination there, exactly
	// as a server starting on that path would.
	created := false
	defer setFaultHook(func(p faultPoint) error {
		if p != faultRestoreRemoved || created {
			return nil
		}
		created = true
		if err := os.Mkdir(dst, 0o755); err != nil {
			return err
		}
		return errors.New("a fault the test injected between the renames")
	})()

	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err == nil {
		t.Fatal("the restore returned nil after aborting")
	}
	if !created {
		t.Fatal("the fault point never fired; this test asserted nothing")
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("the aborted restore removed a directory it did not create: %v", err)
	}
}

// assertNoResidue checks the parent holds nothing but the destination.
func assertNoResidue(t *testing.T, parent, dst string) {
	t.Helper()
	ents, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if filepath.Join(parent, e.Name()) == dst {
			continue
		}
		t.Errorf("residue left in the parent: %s", e.Name())
	}
}

func inodeOfFile(t *testing.T, f *os.File) uint64 {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return inodeOfFileInfo(t, fi)
}

// inodeOfFileInfo is how this test tells "the directory moved" from "a new one
// was built at the same path": the identity of a lock file is its inode, and
// os.SameFile is the same comparison with the numbers hidden.
func inodeOfFileInfo(t *testing.T, fi os.FileInfo) uint64 {
	t.Helper()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("this platform does not expose an inode")
	}
	return st.Ino
}
