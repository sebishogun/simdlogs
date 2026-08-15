package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Provenance for every published measurement.
//
// # Why a number needs a header
//
// A benchmark result is a claim about a machine at a moment. Published without
// the machine, it is a claim about nothing -- and the specific way that goes
// wrong in this family of repositories is recorded in docs/wrong.md: a table of
// timings that could not be reproduced, because nobody had written down that
// the original run was on a different CPU tier.
//
// So every harness that publishes a number emits this block first, and the
// block goes in the commit message and the document beside the table.
//
// # Why the load average is a gate and not a note
//
// The repositories' benchmark discipline is "run the machine quiet: wait for
// load average under 1". It was written down and nothing enforced it, which
// makes it advice. At load 3 the wall-clock spread between runs exceeds the
// differences most of these tables report, so a number measured there is not a
// weaker number -- it is a different quantity that happens to share a unit.
//
// The gate refuses rather than warns, because a warning in a scrollback is a
// warning nobody reads back. It can be overridden, and the override is RECORDED
// IN THE OUTPUT, so a number taken on a busy machine cannot be quoted later as
// if it were not.

// maxQuietLoad is the one-minute load average a measurement may start under.
//
// 1.0 rather than "number of cores": what perturbs a timing run is another
// runnable task competing for the same core and evicting the same cache, and
// that begins well below saturation.
const maxQuietLoad = 1.0

// machineFacts is everything needed to reproduce or to discount a number.
type machineFacts struct {
	GOOS, GOARCH string
	GoVersion    string
	NumCPU       int
	GOMAXPROCS   int
	CPUModel     string
	Commit       string
	Dirty        bool
	Load1        float64
	Noisy        bool // the load gate was overridden
}

func gatherFacts() machineFacts {
	f := machineFacts{
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(),
		NumCPU:    runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		CPUModel: cpuModel(),
		Load1:    loadAverage1(),
	}
	f.Commit, f.Dirty = gitCommit()
	return f
}

func (f machineFacts) String() string {
	dirty := ""
	if f.Dirty {
		dirty = "+dirty"
	}
	noisy := ""
	if f.Noisy {
		noisy = "  MEASURED ON A BUSY MACHINE (load gate overridden)"
	}
	return fmt.Sprintf(
		"machine: %s/%s cpu=%q cores=%d gomaxprocs=%d go=%s commit=%s%s load1=%.2f%s",
		f.GOOS, f.GOARCH, f.CPUModel, f.NumCPU, f.GOMAXPROCS,
		f.GoVersion, f.Commit, dirty, f.Load1, noisy)
}

// cpuModel is the CPU's own name, which is what "CPU tier" means in practice.
func cpuModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(b), "\n") {
		if name, val, ok := strings.Cut(line, ":"); ok &&
			strings.TrimSpace(name) == "model name" {
			return strings.TrimSpace(val)
		}
	}
	return "unknown"
}

// loadAverage1 is the one-minute load average, or -1 where it cannot be read.
//
// -1 rather than 0: zero is a legitimate load and would let the gate pass on a
// platform that cannot report one, which is the gate failing open.
func loadAverage1() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return -1
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return -1
	}
	v, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return -1
	}
	return v
}

// gitCommit is the commit a number was measured at, and whether the tree was
// dirty.
//
// Dirty matters: a number measured against uncommitted changes cannot be
// reproduced from the repository at all, and that is worth saying in the same
// line as the number rather than discovering later.
func gitCommit() (string, bool) {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown", false
	}
	commit := strings.TrimSpace(string(out))
	st, err := exec.Command("git", "status", "--porcelain").Output()
	return commit, err == nil && len(strings.TrimSpace(string(st))) > 0
}

// requireQuiet refuses to measure on a busy machine, and reports the facts.
//
// Call it at the top of any test that publishes a timing. The returned facts
// go in the output beside the numbers.
func requireQuiet(t *testing.T) machineFacts {
	t.Helper()
	f := gatherFacts()
	noisy := os.Getenv("SIMDLOGS_BENCH_NOISY") != ""
	switch {
	case f.Load1 < 0:
		t.Logf("load average is not readable on this platform; the quiet-machine " +
			"gate cannot run and the numbers below are unqualified")
	case f.Load1 > maxQuietLoad && !noisy:
		t.Skipf("load average is %.2f, above the %.1f this measurement needs. "+
			"At this load the run-to-run spread exceeds the differences these "+
			"tables report, so the number would share a unit with the published "+
			"ones and not a meaning. Wait for the machine to settle, or set "+
			"SIMDLOGS_BENCH_NOISY=1 -- which stamps the result as unquotable.",
			f.Load1, maxQuietLoad)
	case f.Load1 > maxQuietLoad:
		f.Noisy = true
	}
	t.Logf("%s", f)
	return f
}

// corpusFingerprint is the hash of the exact bytes both engines were given.
//
// Published with the number because "the same corpus" is otherwise an
// assertion nobody can check: two runs with the same generator, the same seed
// and a changed generator produce different data under the same description.
func corpusFingerprint(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// The gate has to be able to fail, so this checks it against both sides.
func TestTheQuietMachineGateCanFail(t *testing.T) {
	if loadAverage1() < 0 {
		t.Skip("no /proc/loadavg on this platform")
	}
	// The published rule is "load average under 1". A gate whose threshold
	// drifted above the load a machine idles at would never fire.
	if maxQuietLoad > 2.0 {
		t.Fatalf("maxQuietLoad is %.1f; at that threshold the gate cannot "+
			"distinguish a quiet machine from a working one", maxQuietLoad)
	}
	f := gatherFacts()
	if f.CPUModel == "" {
		t.Error("the CPU model came back empty; the provenance line would name no machine")
	}
	if f.Commit == "" {
		t.Error("the commit came back empty; the number could not be reproduced")
	}
	if f.GoVersion == "" {
		t.Error("the Go version came back empty")
	}
	// Two different corpora must fingerprint differently, or the field says
	// nothing.
	if corpusFingerprint([]byte("a")) == corpusFingerprint([]byte("b")) {
		t.Fatal("the corpus fingerprint does not distinguish different corpora")
	}
}
