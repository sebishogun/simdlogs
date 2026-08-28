//go:build windows

package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// LockFileName is the advisory lock a writer holds for the whole life of an
// open store.
const LockFileName = "LOCK"

type dirLock struct {
	path string
	f    *os.File
}

// lockDir takes an exclusive lock on dir.
//
// Windows has no flock. It has something better for this purpose: a file
// opened without FILE_SHARE_DELETE cannot be deleted while open, and Go's
// os.OpenFile on Windows opens without sharing delete, so holding the handle
// is itself the lock. O_EXCL rejects a second opener whose predecessor is
// still running.
//
// The weakness is a crash: the lock file survives, and the next start refuses
// to open a directory nobody holds. That is stated in docs/lld/storage.md
// rather than papered over, because the alternative -- deleting a lock file
// whose owner might still be alive -- is the failure this lock exists to
// prevent.
func lockDir(dir string) (*dirLock, error) {
	path := filepath.Join(dir, LockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, DataFileMode)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("storage: %s is locked (remove %s if no process holds it): %w", dir, path, ErrLocked)
		}
		return nil, err
	}
	return &dirLock{path: path, f: f}, nil
}

func (l *dirLock) unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	if rerr := os.Remove(l.path); err == nil && !os.IsNotExist(rerr) {
		err = rerr
	}
	return err
}

// movedTo tells the lock its file has been renamed to a new directory. On
// Windows this is what makes release able to remove it at all: unlock deletes
// l.path, and after a staged restore's rename the lock file is at the
// destination, not where it was created.
func (l *dirLock) movedTo(dir string) {
	if l != nil {
		l.path = filepath.Join(dir, LockFileName)
	}
}

// release drops the lock and removes its file.
//
// The order is the opposite of unix's and has to be: here the lock IS the open
// handle -- a file opened without FILE_SHARE_DELETE cannot be deleted while
// open -- so removing first fails and leaves the file behind. Since lockDir
// uses O_EXCL, a leftover LOCK refuses every later open of that directory, so
// a restore that removed it in the unix order produced a store nobody could
// open.
func (l *dirLock) release() error { return l.unlock() }

// releaseBeforeRemoval closes the destination's lock before the directory
// holding it is removed.
//
// Required here and only here: the lock IS the open handle, opened without
// FILE_SHARE_DELETE, so os.RemoveAll of the directory containing it fails with
// a sharing violation. Every restore would fail after staging and validating
// the whole archive, leaving a LOCK that O_EXCL then refuses forever.
//
// On unix this is a no-op, and must be: there os.RemoveAll unlinks a held file
// happily, and releasing early hands the destination to a second writer.
func (l *dirLock) releaseBeforeRemoval() error { return l.release() }
