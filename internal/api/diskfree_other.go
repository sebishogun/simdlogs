//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package api

// freeDiskBytes has no portable spelling; -1 means the metric is not reported,
// which is honest where a fabricated number would not be.
func freeDiskBytes(string) int64 { return -1 }
