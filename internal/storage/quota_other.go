//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package storage

import "errors"

var errNoStatfs = errors.New("storage: free space is not measured on this platform")

// statfsUsage has no implementation on this platform.
//
// The tag is the narrow one on purpose. It used to be `windows` against a
// `!windows` unix file, which claimed every non-Windows platform has
// syscall.Statfs_t -- illumos does not, and the build broke there. The unix
// file now names the platforms the syscall exists on and this one takes
// everything else, which is the shape internal/api/diskfree_unix.go already
// used.
//
// The reserve is not enforced here. The tenant byte quota IS: it is measured
// from the store's own groups and needs no filesystem call, which is why
// QuotaState evaluates it whether or not the statfs succeeded. An earlier
// version returned before reaching it, so this file's own comment claiming
// the tenant quota still applied was false on every platform it covers.
//
// The alternative -- a plausible-looking GetDiskFreeSpaceEx call nothing in
// CI can run -- is a protection nobody has ever seen work.
func statfsUsage(dir string) (DiskUsage, error) {
	return DiskUsage{}, errNoStatfs
}
