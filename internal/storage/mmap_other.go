//go:build !unix

package storage

import "os"

// mmapFile falls back to reading the whole file on platforms without mmap.
func mmapFile(path string) (data []byte, unmap func() error, err error) {
	b, err := os.ReadFile(path)
	return b, func() error { return nil }, err
}
