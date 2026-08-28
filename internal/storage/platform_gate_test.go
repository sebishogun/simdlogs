package storage

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// The platforms this package compiles for, gated.
//
// Two build tags shipped broken because a copied one is a claim nobody
// compiles: `quota_unix.go` took `internal/api/diskfree_unix.go`'s list, which
// named netbsd (no `syscall.Statfs` at all) and openbsd (whose `Statfs_t`
// spells the fields `F_bsize`/`F_blocks`/`F_bavail`), and `lock_unix.go` was
// `!windows` against a `syscall.Flock` that solaris, aix and plan9 do not have.
// Nothing caught either: CI's cross job is GOOS=linux with five GOARCHes.
//
// This is the gate. It cross-COMPILES, which is what the defect was about --
// running the tests on these platforms is a different and much larger problem.
func TestThePackageCompilesForEveryClaimedPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-compiling every platform is slow")
	}
	// A cross-compile needs a toolchain, and a sandbox without one would make
	// this vacuously green rather than red -- which is the failure mode of
	// every gate in this file's history.
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain")
	}
	for _, tc := range []struct{ goos, goarch string }{
		{"linux", "amd64"}, {"linux", "arm64"},
		{"darwin", "amd64"}, {"darwin", "arm64"},
		{"freebsd", "amd64"}, {"dragonfly", "amd64"},
		// The two the copied statfs tag broke.
		{"netbsd", "amd64"}, {"openbsd", "amd64"},
		{"windows", "amd64"},
		// The three the !windows flock tag broke.
		{"illumos", "amd64"}, {"solaris", "amd64"}, {"aix", "ppc64"},
		// The flockless list is hand-authored, so every member of it is here:
		// it omitted wasip1 when first written.
		{"js", "wasm"}, {"wasip1", "wasm"},
	} {
		t.Run(tc.goos+"/"+tc.goarch, func(t *testing.T) {
			cmd := exec.Command("go", "build", "./...")
			cmd.Env = append(cmd.Environ(),
				"GOOS="+tc.goos, "GOARCH="+tc.goarch, "CGO_ENABLED=0")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("GOOS=%s GOARCH=%s does not build:\n%s",
					tc.goos, tc.goarch, out)
			}
			_ = runtime.GOOS
			_ = strings.TrimSpace(string(out))
		})
	}
}

// A statfs failure keeps the last good sample rather than discarding it.
//
// Stamping the cache and storing nil turned one failed statfs into a
// two-second window with the reject reserve switched off on a full disk --
// QuotaState treats an unmeasurable filesystem as "do not refuse writes", so
// dropping the sample disables the reserve for the whole interval. Measured at
// 2.0 s against 0 s before the caching was added.
func TestAFailedStatfsKeepsTheLastGoodSample(t *testing.T) {
	var fail bool
	restore := SetDiskUsageForTest(func(string) (DiskUsage, error) {
		if fail {
			return DiskUsage{}, errUnmeasured
		}
		return DiskUsage{Total: 1 << 30, Free: 0}, nil
	})
	defer restore()

	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	// A good sample first: the disk is full, so writes are refused.
	if err := s.CheckWrite(); err == nil {
		t.Fatal("a full disk accepted a write")
	}

	// Now statfs starts failing, and the cache is expired the way time expires
	// it. The reserve must keep enforcing against the last reading.
	fail = true
	s.usageAt.Store(0)
	if err := s.CheckWrite(); err == nil {
		t.Fatal("one failed statfs switched the reject reserve off; " +
			"on a full disk that is an interval of unbounded writes")
	}
}

// A store with no measurable filesystem and no prior sample does not refuse
// writes: turning a statfs failure into a write outage is the protection
// causing the harm it exists to prevent.
func TestAStoreThatNeverMeasuredDoesNotRefuse(t *testing.T) {
	defer SetDiskUsageForTest(func(string) (DiskUsage, error) {
		return DiskUsage{}, errUnmeasured
	})()
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	if err := s.CheckWrite(); err != nil {
		t.Fatalf("%v, want writes to continue when free space has never been read", err)
	}
}
