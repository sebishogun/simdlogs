// Package soak runs the server under continuous mixed load and asserts that
// what should be bounded stays bounded.
//
// # Why this is opt-in, and hard about it
//
// A soak is a test that deliberately does not finish quickly. Left reachable
// from `go test ./...` it would run in every CI job, every pre-commit gate and
// every editor save-hook, and the failure mode of that is not a slow build: it
// is a machine with hundreds of live server processes on it. This package
// therefore does nothing at all unless SIMDLOGS_SOAK is set, and it bounds its
// own duration whatever it is told.
//
// # What a soak can show that nothing else does
//
// Every leak here is invisible in a short test by construction. A goroutine
// that is not joined, a mapping that is not unmapped, a manifest record that is
// never compacted, a temp file that is never removed -- each is fine once and
// fatal after a hundred thousand times. The assertions are all of the same
// shape: measure after warm-up, run, measure again, and require the second
// measurement to be within a bound of the first.
//
// # What it cannot show
//
// It runs one process on one machine for as long as it is given. It is not a
// substitute for production, and a clean one-hour run does not prove a
// twenty-four hour run is clean -- which is exactly why there are two modes and
// why the release gate is the long one.
package soak

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// soakEnabled reports whether the soak may run at all.
//
// An environment variable rather than a build tag, so the package still
// compiles and vets in every ordinary build -- a soak behind a build tag is a
// soak that silently stops compiling.
func soakEnabled(t *testing.T) bool {
	t.Helper()
	if !soakRequested(os.Getenv("SIMDLOGS_SOAK")) {
		t.Skip("soak is opt-in: set SIMDLOGS_SOAK=1 (see scripts/soak.sh). " +
			"It runs continuous load for minutes to hours and must never be " +
			"reachable from an ordinary `go test ./...`")
		return false
	}
	return true
}

// soakRequested is the policy, separated from the testing.T that acts on it.
//
// Separated because the guard has to be testable, and a guard expressed only
// as `t.Skip` cannot be: a test that calls it either skips -- proving nothing
// -- or does not, which is the failure it was meant to catch. The pure
// function can be asserted in both directions.
func soakRequested(env string) bool { return env != "" }

// soakBound is the ceiling on how long a soak may be told to run.
//
// Not paranoia about the value: a soak invoked from a script with a typo'd
// duration is a machine under load until someone notices. 24h is the release
// mode and nothing legitimate needs more.
const soakBound = 24 * time.Hour

// parseSoakDuration is the duration policy, likewise separated so it can be
// asserted without running anything.
func parseSoakDuration(spec string) (time.Duration, error) {
	if spec == "" {
		spec = "1h" // developer mode
	}
	d, err := time.ParseDuration(spec)
	if err != nil {
		return 0, fmt.Errorf("SIMDLOGS_SOAK_DURATION=%q: %w", spec, err)
	}
	if d > soakBound {
		return 0, fmt.Errorf("SIMDLOGS_SOAK_DURATION=%s exceeds the %s ceiling", d, soakBound)
	}
	if d < time.Second {
		return 0, fmt.Errorf("SIMDLOGS_SOAK_DURATION=%s is too short to reach steady state", d)
	}
	return d, nil
}

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	d, err := parseSoakDuration(os.Getenv("SIMDLOGS_SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// sample is one observation of everything that must stay bounded.
//
// Taken from /proc where the kernel knows and from the runtime where it does
// not. Each field is here because something can leak it, and the names say
// which: a goroutine that is never joined, a mapping that is never unmapped, a
// file that is never removed.
type sample struct {
	At time.Time

	Goroutines int
	// Mappings is the line count of /proc/self/maps. An mmap that is not
	// unmapped shows up here long before RSS notices, because a mapping of a
	// file that is not being read costs address space and no resident pages.
	Mappings int
	// VmSizeKB is address space; RSSKB is resident. They leak differently and
	// the difference is diagnostic: mappings that are never released grow the
	// first and not the second.
	VmSizeKB int64
	RSSKB    int64

	// Files and DiskKB are the store directory. ManifestKB is called out
	// separately because a manifest that never compacts is the leak that makes
	// startup slower every day and is invisible in total disk use.
	Files int
	// GroupFiles is group-*.bin only. The mapping bound is about groups: a
	// manifest and a lock file are not mapped, so counting them would put a
	// constant into a ratio that is supposed to be about growth.
	GroupFiles int
	DiskKB     int64
	// ManifestBytes is in BYTES, not kilobytes. In KB it read as 0 for the
	// whole of a 45-second run -- every manifest was under a kilobyte -- and
	// the bound built on it was skipped silently on every sample.
	ManifestBytes int64

	// QueryNanos is the latency of one fixed query, so a scan that degrades as
	// the store grows is visible as a number rather than as a feeling.
	QueryNanos int64
}

func (s sample) String() string {
	return fmt.Sprintf(
		"goroutines=%d mappings=%d vm=%dMB rss=%dMB files=%d groups=%d disk=%dMB manifest=%dB query=%.1fms",
		s.Goroutines, s.Mappings, s.VmSizeKB>>10, s.RSSKB>>10,
		s.Files, s.GroupFiles, s.DiskKB>>10, s.ManifestBytes, float64(s.QueryNanos)/1e6)
}

// procInt reads one /proc/self/status field in kB.
func procStatusKB(field string) int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, field+":") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			return 0
		}
		n, _ := strconv.ParseInt(f[1], 10, 64)
		return n
	}
	return 0
}

// mappingCount is the number of entries in /proc/self/maps.
func mappingCount() int {
	b, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return 0 // not Linux: the assertion below is skipped rather than faked
	}
	return strings.Count(string(b), "\n")
}

// growthBound is what must not grow without bound.
//
// # The bounds are RATIOS, not absolute growth, and that is the whole design
//
// The first version compared each raw number against its warm-up value. Every
// one of them failed on the first soak that actually generated load -- mappings
// went 1060 to 6585 against a bound of 2184 -- and they were right to, because
// the store had gone from 1028 group files to 7602. A store holding more data
// maps more groups, uses more memory and occupies more disk. That is the system
// working.
//
// So an absolute bound on those numbers can only pass if nothing happened. It
// did pass, once: the run where every request was refused 400 and the soak
// reported 172,037 "writes" against a flat resource profile. A bound that
// passes exactly when the test is broken is worse than no bound.
//
// What isolates a leak is the RELATIONSHIP. One mapping per group is correct;
// a mapping that outlived its group is not, and it shows up as the gap between
// mappings and files widening. The same for memory against data, and manifest
// against groups.
type growthBound struct {
	name string
	// get derives the quantity that must stay bounded. It returns 0 when this
	// platform cannot measure it, and the bound is then skipped rather than
	// passing vacuously.
	get     func(sample) int64
	maxMul  float64 // multiple of the warm-up value
	maxPlus int64   // absolute slack, for quantities whose warm-up value is small
	why     string
}

func bounds() []growthBound {
	return []growthBound{
		{"goroutines", func(s sample) int64 { return int64(s.Goroutines) }, 1.5, 32,
			"a goroutine per request that is never joined is the classic leak, and " +
				"unlike everything else here it does not grow with the data: the " +
				"number of goroutines a server needs does not depend on how much it " +
				"has stored"},

		{"mappings per 100 groups", func(s sample) int64 {
			if s.GroupFiles < 50 {
				return 0 // too few to take a ratio from
			}
			// A RATIO, not a difference. The first version subtracted file
			// count from mapping count, which went negative -- not every file
			// is mapped, and the binary has mappings of its own -- so the bound
			// clamped to zero and was skipped on every sample. Roughly one
			// mapping per group is correct; what must not grow is the number of
			// mappings PER group, which is what a mapping that outlived its
			// group looks like.
			return int64(s.Mappings) * 100 / int64(s.GroupFiles)
		}, 2.0, 100,
			"a mapping that outlived its group costs address space and no resident " +
				"pages, so nothing notices until the process dies of it"},

		{"KB resident per MB stored", func(s sample) int64 {
			if s.DiskKB < 1<<10 { // under a megabyte: no ratio worth taking
				return 0
			}
			return s.RSSKB * 1024 / s.DiskKB
		}, 2.0, 64 << 10,
			"memory grows with the data being served, and should not grow FASTER " +
				"than it: a per-group structure that is built and never released " +
				"widens this ratio while the raw number looks like ordinary growth"},

		{"manifest bytes per group", func(s sample) int64 {
			if s.GroupFiles < 50 {
				return 0
			}
			return s.ManifestBytes / int64(s.GroupFiles)
		}, 3.0, 512,
			"a manifest that never compacts makes every startup slower and is " +
				"invisible in total disk use, because the groups dwarf it. This is " +
				"the bound most likely to fire on a long run: retention writes a " +
				"record per removal, so the manifest grows with ACTIVITY while the " +
				"group count it is divided by does not -- measured at 36 bytes per " +
				"group after a minute and 77 after retention had run for half of it"},
	}
}

// The guard on the guard.
//
// These run in every ordinary `go test ./...`. Without them, a change that made
// the soak default-on would be found by whoever's machine fell over -- and a
// guard written as `t.Skip` cannot check itself, which is why the policy is a
// pure function.
func TestTheSoakDoesNotRunWithoutBeingAsked(t *testing.T) {
	if soakRequested("") {
		t.Fatal("an unset SIMDLOGS_SOAK enables the soak; every `go test ./...` " +
			"would start continuous load")
	}
	if !soakRequested("1") {
		t.Fatal("SIMDLOGS_SOAK=1 does not enable the soak, so it cannot be run at all")
	}
}

func TestTheSoakDurationIsBounded(t *testing.T) {
	for _, tc := range []struct {
		spec string
		want string // "" means accepted
	}{
		{"", ""},    // the developer default
		{"1h", ""},  //
		{"24h", ""}, // exactly the ceiling
		{"25h", "ceiling"},
		{"1000h", "ceiling"},
		{"500ms", "too short"},
		{"banana", "SIMDLOGS_SOAK_DURATION"},
		{"-1h", "too short"},
	} {
		t.Run(tc.spec, func(t *testing.T) {
			d, err := parseSoakDuration(tc.spec)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("%q was refused: %v", tc.spec, err)
				}
				if d <= 0 || d > soakBound {
					t.Fatalf("%q accepted as %s", tc.spec, d)
				}
				return
			}
			if err == nil {
				t.Fatalf("%q accepted as %s; a typo'd duration is a machine under "+
					"load until someone notices", tc.spec, d)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not say why (%q): %v", tc.want, err)
			}
		})
	}
}

func TestSoakRuntimeIsLinuxAware(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the /proc samples are Linux-only; on %s the soak runs without "+
			"the mapping and RSS bounds", runtime.GOOS)
	}
	if mappingCount() == 0 {
		t.Fatal("/proc/self/maps read as empty on Linux; the mapping bound would " +
			"pass vacuously")
	}
	if procStatusKB("VmRSS") == 0 {
		t.Fatal("VmRSS read as 0 on Linux; the RSS bound would pass vacuously")
	}
}
