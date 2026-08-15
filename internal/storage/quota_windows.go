//go:build windows

package storage

import "errors"

var errNoStatfs = errors.New("storage: free space is not measured on this platform")

// statfsUsage has no Windows implementation yet.
//
// It reports zero rather than an error on purpose: QuotaState treats an
// unmeasurable filesystem as "do not refuse writes", so a Windows build
// enforces the tenant byte quota and not the disk reserve. Stated here so the
// gap is a known one; the alternative -- a plausible-looking GetDiskFreeSpaceEx
// call nothing in CI can run -- is a protection nobody has ever seen work.
func statfsUsage(dir string) (DiskUsage, error) {
	return DiskUsage{}, errNoStatfs
}
