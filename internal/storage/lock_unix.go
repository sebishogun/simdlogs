//go:build !windows

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockFileName is the advisory lock a writer holds for the whole life of an
// open store.
const LockFileName = "LOCK"

// dirLock is a held exclusive lock on a store directory.
type dirLock struct {
	f *os.File
}

// lockDir takes an exclusive, non-blocking lock on dir.
//
// Two processes opening the same directory both allocate group IDs from their
// own in-memory nextID, so both write group-7.bin and one silently destroys
// the other's data. Nothing detected that: OpenStore globbed the directory
// and took what it found. flock is advisory but process-scoped and released
// by the kernel on exit, so a crashed writer does not leave the directory
// permanently locked the way a pidfile would.
func lockDir(dir string) (*dirLock, error) {
	path := filepath.Join(dir, LockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, DataFileMode)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("storage: %s is locked by another process: %w", dir, ErrLocked)
		}
		return nil, err
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
