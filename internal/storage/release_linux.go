package storage

import "syscall"

// release drops a mapping's pages from this process's page tables. The pages
// stay in the page cache, and a later read of the mapping maps them again;
// MADV_DONTNEED on a shared file mapping never changes what the mapping reads.
func release(b []byte) {
	if len(b) > 0 {
		syscall.Madvise(b, syscall.MADV_DONTNEED)
	}
}
