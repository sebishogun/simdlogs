//go:build windows

package storage

// syncDir is a no-op on Windows, which has no directory handle that can be
// flushed the way fsync(2) flushes a POSIX directory. MoveFileEx-backed
// renames are atomic there, but the durability guarantee this package states
// for a rename is weaker on Windows, and docs/lld/storage.md says so rather
// than implying the same fsync policy holds everywhere.
//
// The fault points are still honoured so the failure-path tests exercise the
// same call sites on every platform.
func syncDir(dir string) error {
	if err := fault(faultDirOpen); err != nil {
		return err
	}
	if err := fault(faultDirSync); err != nil {
		return err
	}
	return nil
}
