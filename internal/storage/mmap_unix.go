//go:build unix

package storage

import (
	"os"
	"syscall"
)

// mmapFile maps a group file read-only. The returned bytes are backed by the
// file: the OS pages them in on access and evicts under memory pressure, so a
// store of many groups keeps only its working set resident rather than every
// blob on the heap. unmap releases the mapping.
func mmapFile(path string) (data []byte, unmap func() error, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	n := int(fi.Size())
	if n == 0 {
		return nil, func() error { return nil }, nil
	}
	b, err := syscall.Mmap(int(f.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return b, func() error { return syscall.Munmap(b) }, nil
}
