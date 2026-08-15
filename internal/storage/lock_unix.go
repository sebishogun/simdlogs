//go:build !windows

package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// LockFileName is the advisory lock a writer holds for the whole life of an
// open store.
const LockFileName = "LOCK"

// dirLock is a held exclusive lock on a store directory.
type dirLock struct {
	f    *os.File
	path string
}

// lockDir takes an exclusive, non-blocking lock on dir.
//
// Two processes opening the same directory both allocate group IDs from their
// own in-memory nextID, so both write group-7.bin and one silently destroys
// the other's data. Nothing detected that: OpenStore globbed the directory
// and took what it found. flock is advisory but process-scoped and released
// by the kernel on exit, so a crashed writer does not leave the directory
// permanently locked the way a pidfile would.
//
// # The flock has to be checked against the path afterwards
//
// open() and flock() are two syscalls, and a staged restore replaces the whole
// directory in between. The fd then names an inode that is no longer at
// dst/LOCK -- unlinked with the directory it lived in, contended by nobody,
// so the flock ALWAYS succeeds. The caller gets a lock that excludes no one
// and goes on writing group files BY PATH into the store that replaced it,
// beside a second process holding the real lock. Traced, twenty-four writers
// against a restore loop: a store opened on lock inode 113001480, six
// directory generations after that inode was released and deleted, appending
// group-0.bin into the live directory whose lock was 113001501.
//
// So the inode is checked against the path once the flock is held, and a
// mismatch is retried rather than returned: the loser of one swap is not
// wrong to want the lock, it is holding the wrong file. Retries are bounded
// because a destination being replaced in a loop has no lock to give, and an
// unbounded wait there is a hang rather than an answer.
//
// The check is not itself a race. A process that replaces the directory must
// hold this lock to get that far, so once a caller holds the inode that IS at
// the path, no swap can start until it lets go.
const lockDirRetries = 8

func lockDir(dir string) (*dirLock, error) {
	path := filepath.Join(dir, LockFileName)
	for attempt := 0; ; attempt++ {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, DataFileMode)
		if err != nil {
			return nil, err
		}
		// Here is the window. A test stands in it and swaps the directory.
		if ferr := fault(faultLockOpened); ferr != nil {
			f.Close()
			return nil, ferr
		}
		if err := flockExclusive(f); err != nil {
			f.Close()
			if err == errLockHeld {
				return nil, fmt.Errorf("storage: %s is locked by another process: %w", dir, ErrLocked)
			}
			return nil, err
		}
		if current, err := lockedFileIsAtPath(f, path); err == nil && current {
			return &dirLock{f: f, path: path}, nil
		}
		f.Close()
		if attempt >= lockDirRetries {
			return nil, fmt.Errorf(
				"storage: %s was replaced under the lock %d times running: %w",
				dir, attempt+1, ErrLocked)
		}
	}
}

// lockedFileIsAtPath reports whether the file this descriptor holds is still
// the file that path names.
//
// os.SameFile is exactly a device-and-inode comparison on unix
// (os/types_unix.go), so it is spelling rather than strength -- an earlier
// version of this comment claimed it was stronger than comparing the numbers,
// and it is not.
//
// What makes the comparison sound is the descriptor: the file it names cannot
// be reused while this process holds it open, so the identity on the left of
// the comparison is pinned for the length of it. Only the path can change
// underneath, which is the thing being checked.
func lockedFileIsAtPath(f *os.File, path string) (bool, error) {
	held, err := f.Stat()
	if err != nil {
		return false, err
	}
	at, err := os.Stat(path)
	if err != nil {
		// ENOENT is the answer, not a failure to get one: the directory was
		// taken away between the flock and here, which is exactly the state
		// this check exists to catch.
		return false, nil
	}
	return os.SameFile(held, at), nil
}

func (l *dirLock) unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := flockRelease(l.f)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// movedTo tells the lock its file has been renamed to a new directory, which a
// staged restore does: the lock file it created in the staging directory
// becomes the destination's LOCK when the staging directory is renamed into
// place.
//
// On unix this records a fact nothing here reads. `release` is `unlock`, which
// works on the descriptor and never touches `path`, so a lock whose path went
// stale still releases correctly. It is Windows that needs it, where `release`
// removes the file and a stale path would leave a LOCK that O_EXCL then
// refuses forever -- and the method is defined once, for both. An earlier
// version of this comment gave the Windows consequence as if it were unix's.
func (l *dirLock) movedTo(dir string) {
	if l != nil {
		l.path = filepath.Join(dir, LockFileName)
	}
}

// release drops the lock WITHOUT unlinking its file.
//
// Unlinking a lock file cannot be made safe by ordering, which is what two
// rounds of this got wrong. Unlink-then-unlock leaves a competitor that opened
// the fd before the unlink free to flock the now-unlinked inode after the
// unlock, while the next competitor creates a fresh inode at the path and
// flocks that: one lock, two holders. Unlock-then-unlink has the mirror gap.
// Measured under contention, 30 seconds: 268 and 211 double-holds with the
// unlink in either order, and zero without it.
//
// So the file stays, exactly as unlock and Store.Close already leave it. A
// stale LOCK is not a problem this package has: lockDir opens an existing one
// and flocks it, and the restore's destination check ignores it precisely
// because it outlives the process that made it.
//
// Windows is the other way round and defines release for itself: there the
// lock IS the open handle, so the file cannot be left behind -- O_EXCL would
// refuse the directory forever -- and unlock removes it after closing.
func (l *dirLock) release() error { return l.unlock() }

// releaseBeforeRemoval is what a staged restore does to its destination lock
// immediately before removing the destination -- and on unix that is NOTHING.
//
// os.RemoveAll unlinks a file whose descriptor is open without complaint, and
// the descriptor stays valid, so the lock protects the destination right up to
// the removal that ends it. Releasing early does the opposite of protecting:
// since the lock file is never unlinked, an early release leaves `dst/LOCK`
// present and UNHELD, a server flocks the file that is already there and opens
// the store, `os.RemoveAll` then deletes that live store, and the rename
// SUCCEEDS -- the winner never had to create the directory, so there is no
// EEXIST to abort on. Measured over thirty seconds of contention: a restore
// returning nil with the archive's group-0.bin overwritten by the ghost
// writer, and 3739 displaced holders against 192 with this a no-op.
//
// Windows defines it as a real release, because there os.RemoveAll cannot
// delete a directory holding an open handle at all.
func (l *dirLock) releaseBeforeRemoval() error { return nil }
