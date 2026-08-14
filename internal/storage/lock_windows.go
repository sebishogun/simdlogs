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
