//go:build plan9 || solaris || aix || js || wasip1

package storage

import (
	"errors"
	"os"
)

// No flock on this platform.
//
// The store REFUSES to open rather than opening unlocked. The lock is what
// stops two processes writing one store's manifest, and a store opened without
// it is not a store with a missing feature -- it is a store whose next
// concurrent write corrupts it. A build that cannot take the lock is a build
// that must not pretend to.
//
// So the package compiles here and OpenStore fails with a reason, which is the
// difference between "this platform is unsupported" and "this platform
// silently loses data". A real implementation would be fcntl(F_SETLK); it is
// not written because nothing in CI can run it, and a lock nobody has seen
// work is the protection this comment is about.
var errLockHeld = errors.New("storage: the store lock is held")

var errNoFlock = errors.New(
	"storage: file locking is not implemented on this platform, and a store opened " +
		"without its lock is corrupted by the next concurrent write")

func flockExclusive(f *os.File) error { return errNoFlock }

func flockRelease(f *os.File) error { return nil }
