//go:build !windows

package storage

import "os"

// syncDir fsyncs a directory so a rename or unlink within it is durable.
//
// Opening a directory read-only and calling Sync is the portable POSIX way
// to do this; the kernel flushes the directory's own metadata, which is what
// carries the name-to-inode mapping the rename changed.
func syncDir(dir string) error {
	if err := fault(faultDirOpen); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := fault(faultDirSync); err != nil {
		return err
	}
	return d.Sync()
}
