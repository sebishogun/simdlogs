//go:build linux || darwin || freebsd || dragonfly

package api

import "syscall"

// freeDiskBytes reports the free space on the storage filesystem. An operator
// watches this to know when ingest will start failing, so it is read from the
// filesystem rather than estimated.
func freeDiskBytes(dir string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}
