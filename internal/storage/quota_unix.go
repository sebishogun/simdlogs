//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package storage

import "syscall"

// statfsUsage reports the filesystem holding dir.
//
// Bavail, not Bfree: Bfree counts blocks a non-root process cannot touch, and
// a reserve computed against it is a reserve that is already spent when the
// writes start failing. On a filesystem with the usual 5% root reserve the two
// differ by exactly the amount that matters.
func statfsUsage(dir string) (DiskUsage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return DiskUsage{}, err
	}
	bs := int64(st.Bsize)
	return DiskUsage{
		Total: int64(st.Blocks) * bs,
		Free:  int64(st.Bavail) * bs,
	}, nil
}
