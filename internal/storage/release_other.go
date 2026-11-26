//go:build !linux

package storage

// release leaves the pages mapped: only Linux is measured, and on platforms
// without mmap the group was read into the heap.
func release([]byte) {}
