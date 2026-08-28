//go:build !windows && !plan9 && !solaris && !aix && !js && !wasip1

package storage

import (
	"os"
	"syscall"
)

// flock, where flock exists.
//
// Split out from lock_unix.go behind its own tag because the LOCK LOGIC --
// open, lock, verify the descriptor still names the path -- is portable and
// the syscall is not. syscall.Flock is absent on solaris, aix and plan9, so a
// `!windows` file calling it claimed a portability the package did not have
// and broke the build on all three. illumos has it; solaris does not, which is
// the kind of distinction a copied build tag never survives.
var errLockHeld = syscall.EWOULDBLOCK

func flockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func flockRelease(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
