package storage

import (
	"io"
	"os"
	"path/filepath"
)

// restoreTarForTest unpacks a backup into dir with NO staging.
//
// This was `RestoreTar`, exported, and Task 5.2 replaced it with the staged
// Restore -- its own doc said so. It had no production caller, and the reason
// it survived is that eight tests use it as the harness for readBackup's
// entry-by-entry validation: size, checksum, ReadGroup parse, ordering, the
// terminator. That is worth keeping and is not worth reaching through a staged
// restore, which would also test the staging.
//
// So it is here, in a _test file, where production cannot call it. The two
// callers outside this package moved to Restore, which is what a real restore
// does.
func restoreTarForTest(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	_, _, err := readBackup(r, backupReadLimits{}, nil, func(name string, data []byte) error {
		return writeFileAtomic(filepath.Join(dir, name), data, DataFileMode)
	})
	return err
}
