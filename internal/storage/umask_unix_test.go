//go:build !windows

package storage

import "syscall"

// setUmask sets the process umask and returns the previous one.
//
// The umask is process-global, so this is only safe because `go test` runs the
// serial tests of a package one at a time and defers every t.Parallel one to
// after that pass -- the package does have parallel tests, and a caller of
// this must not be one of them.
func setUmask(mask int) int { return syscall.Umask(mask) }

const umaskSupported = true
