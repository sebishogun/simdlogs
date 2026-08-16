package bench

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"

	"net/http/httptest"
)

// Engine lifecycle for the head-to-heads. Both engines need the same thing
// from a repaired harness: a way to be put back to empty between ingest
// samples, so sample k measures ingest into an empty store rather than into
// one k-1 samples large.

// vlProc is a VictoriaLogs subprocess with its own storage directory.
type vlProc struct {
	bin  string
	dir  string
	addr string // host:port
	url  string // http://host:port
	// extra flags beyond the three every caller needs. The scale harnesses set
	// -memory.allowedPercent from the environment.
	extra []string
	cmd   *exec.Cmd
}

// newVL prepares a VL process on addr. It does not start it. Returns nil when
// the binary is not staged, which callers report as a loud skip rather than a
// silent one.
func newVL(t *testing.T, addr string) *vlProc {
	return newVLAt(t, addr, t.TempDir())
}

// newVLAt is newVL with the storage directory and extra flags chosen by the
// caller.
//
// The scale harnesses put a multi-gigabyte store wherever the operator pointed
// SIMDLOGS_BENCH_DIR, which t.TempDir cannot express, and they own the removal
// of that directory. Everything else about the process -- including that it is
// killed by its own PID and then REAPED -- is the same.
func newVLAt(t *testing.T, addr, dir string, extra ...string) *vlProc {
	t.Helper()
	bin, err := filepath.Abs("victoria-logs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bin); err != nil {
		return nil
	}
	return &vlProc{bin: bin, dir: dir, addr: addr, url: "http://" + addr, extra: extra}
}

func (p *vlProc) start() error { return p.startWithin(60 * time.Second) }

// startWithin is start with the readiness limit as a parameter, so a test can
// force the failure path without waiting a minute for it.
func (p *vlProc) startWithin(limit time.Duration) error {
	args := append([]string{
		"-httpListenAddr=" + p.addr,
		"-storageDataPath=" + p.dir,
		"-retentionPeriod=10y",
	}, p.extra...)
	cmd := exec.Command(p.bin, args...)
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start victoria-logs: %w", err)
	}
	p.cmd = cmd
	// STOPPED BY start ITSELF when readiness fails.
	//
	// cmd.Start() has already succeeded here: the child is running. Every call
	// site registers its t.Cleanup(p.stop) or defer p.stop() on the line AFTER
	// the t.Fatalf that fires on this error, so nothing was ever registered and
	// the child outlived the test binary -- reparented to init, holding its
	// port, serving from a directory the framework had already deleted. Proven
	// by forcing the wait to time out: `pid 1560623 ppid 1 victoria-logs`.
	//
	// f3fc6e2 closed the zombie leak and left this one, which is the worse of
	// the two: a zombie is a few bytes of kernel bookkeeping, this is a
	// multi-gigabyte process with no parent left to kill it.
	if err := p.waitReady(limit); err != nil {
		p.stop()
		return err
	}
	return nil
}

// stop kills this process by its own PID and reaps it. Never a pattern kill:
// a pattern would take the operator's other victoria-logs down with it, and
// this package has run more than one VL at a time.
func (p *vlProc) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait() // reap before the port or the dir is reused
	p.cmd = nil
}

// reset returns the engine to empty: stop, wipe the storage directory,
// start again. The port is reused, so start's readiness wait is also what
// keeps the next sample from racing the old listener.
func (p *vlProc) reset() error {
	p.stop()
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("wipe VL storage: %w", err)
	}
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return fmt.Errorf("recreate VL storage: %w", err)
	}
	return p.start()
}

func (p *vlProc) waitReady(limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		resp, err := http.Get(p.url + "/insert/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond) // bench:untimed -- readiness poll before any measurement
	}
	return fmt.Errorf("victoria-logs on %s not ready inside %v", p.addr, limit)
}

// slProc is the simdlogs side of the same interface: an in-process server
// behind httptest, resettable to an empty store.
type slProc struct {
	t   *testing.T
	dir string
	srv *api.Server
	ts  *httptest.Server
	url string
}

func newSL(t *testing.T) *slProc {
	t.Helper()
	p := &slProc{t: t, dir: t.TempDir()}
	if err := p.reset(); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *slProc) stop() {
	if p.ts != nil {
		p.ts.Close()
		p.ts = nil
	}
	if p.srv != nil {
		p.srv.Close()
		p.srv = nil
	}
}

// reset returns this side to empty: stop, wipe the storage directory, start
// again -- the same shape as vlProc.reset, and for the same reason.
//
// It used to take a FRESH t.TempDir() per call, which does empty the store but
// leaves the previous one on disk: t.TempDir cleans up when the TEST ends, not
// when the caller stops using it. TestPerOperation resets between every
// operation, so it held nine full 200k-row stores at once and the disk figure
// it was measuring included eight stores nothing was reading.
func (p *slProc) reset() error {
	p.stop()
	if p.dir == "" {
		p.dir = p.t.TempDir()
	}
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("wipe simdlogs storage: %w", err)
	}
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return fmt.Errorf("recreate simdlogs storage: %w", err)
	}
	srv, err := api.NewServer(p.dir)
	if err != nil {
		return err
	}
	p.srv = srv
	p.ts = httptest.NewServer(srv.Handler())
	p.url = p.ts.URL
	return nil
}

// TestResettingTheSimdlogsSideReusesOneDirectory pins both halves: the store
// moves back to empty, and the one it replaced is GONE rather than merely
// unreferenced.
//
// Checking only that the store is empty would pass with a fresh directory per
// reset, which is exactly the shape being fixed. Checking only that the path is
// stable would pass if reset stopped wiping.
func TestResettingTheSimdlogsSideReusesOneDirectory(t *testing.T) {
	p := newSL(t)
	defer p.stop()
	first := p.dir

	const from, to = 1714521600, 1714522200
	row := "{\"_time\":\"2024-05-01T00:00:10Z\",\"_msg\":\"reset probe\",\"level\":\"info\"}\n"

	for i := 0; i < 3; i++ {
		postNDJSON(t, p.url+"/insert/jsonline", []byte(row))
		if !waitUntil(func() bool { n, err := rowCount(p.url, from, to); return err == nil && n >= 1 }, 10*time.Second) {
			t.Fatalf("cycle %d: the row never became queryable", i)
		}
		if err := p.reset(); err != nil {
			t.Fatalf("cycle %d reset: %v", i, err)
		}
		if p.dir != first {
			t.Fatalf("cycle %d moved the store to %s (was %s) -- a fresh "+
				"directory per reset leaves its predecessor on disk until the "+
				"whole test ends, which is how nine 200k-row stores were live "+
				"at once", i, p.dir, first)
		}
		// readyAtLeast, not rowCount == 0: an empty store answers the stats
		// query with no rows at all, which rowCount reports as an error rather
		// than a zero. "Not even one row is queryable" is the package's own way
		// of saying empty, and the ingest at the top of the NEXT cycle is what
		// proves the fresh server works rather than merely being unreachable.
		if readyAtLeast(p.url, from, to, 1)() {
			t.Fatalf("cycle %d: after reset the store still answers with rows "+
				"-- reset exists so sample k measures ingest into an EMPTY "+
				"store, not one k-1 samples large", i)
		}
	}
}

// waitUntil polls cond until it holds or the limit passes.
func waitUntil(cond func() bool, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond) // bench:untimed -- readiness poll
	}
	return cond()
}

// startVL starts a VictoriaLogs on addr, registers its stop with t.Cleanup, and
// skips with `why` when the binary is not staged.
//
// Seven call sites each ran `exec.Command(...).Start()` and then
// `defer cmd.Process.Kill()`. Kill without Wait does not reap: the child
// becomes a zombie held by this process until the test binary itself exits, and
// its three stdio pipes stay open with it. Measured over eight ingest cycles:
// +8 zombies and +7 file descriptors, growing without bound in a package whose
// whole purpose is running the same engine over and over.
//
// vlProc.stop already did it correctly -- kill by this process's own PID, never
// a pattern, then Wait to reap before the port or the directory is reused --
// and one call site used it. This is that one path, for all of them.
func startVL(t *testing.T, addr, why string) *vlProc {
	t.Helper()
	p := newVL(t, addr)
	if p == nil {
		skipNoVL(t, why)
	}
	if err := p.start(); err != nil {
		t.Fatalf("start victoria-logs on %s: %v", addr, err)
	}
	t.Cleanup(p.stop)
	return p
}

// zombieChildren returns the pids of this process's children in state Z.
//
// /proc/<pid>/stat is `pid (comm) state ppid ...` and comm can contain spaces
// and parentheses, so the fields are taken after the LAST ')'.
func zombieChildren(t *testing.T) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read /proc: %v", err)
	}
	me := os.Getpid()
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // exited between the readdir and the read
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 || i+2 >= len(s) {
			continue
		}
		f := strings.Fields(s[i+2:])
		if len(f) < 2 {
			continue
		}
		if f[0] == "Z" && f[1] == strconv.Itoa(me) {
			out = append(out, pid)
		}
	}
	return out
}

func openFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(ents)
}

// TestStoppingVictoriaLogsReapsItAndClosesItsPipes is the gate on the leak.
//
// It runs the start/stop cycle the head-to-heads run and asserts the two things
// `defer cmd.Process.Kill()` gets wrong: the child must be reaped, and its
// pipes must be closed. Both are unbounded -- a bench package that resets the
// engine between samples pays them once per sample.
func TestStoppingVictoriaLogsReapsItAndClosesItsPipes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc is where the child state is readable")
	}
	if newVL(t, "127.0.0.1:19495") == nil {
		skipNoVL(t, "the process-reaping gate")
	}
	// One warm cycle first, so the baseline includes whatever a single start
	// allocates once (the HTTP client's idle connection, the DNS cache).
	warm := newVL(t, "127.0.0.1:19495")
	if err := warm.start(); err != nil {
		t.Fatalf("warm start: %v", err)
	}
	warm.stop()

	beforeZ := len(zombieChildren(t))
	beforeFD := openFDs(t)

	const cycles = 3
	for i := 0; i < cycles; i++ {
		p := newVL(t, "127.0.0.1:19495")
		if err := p.start(); err != nil {
			t.Fatalf("cycle %d start: %v", i, err)
		}
		p.stop()
	}

	if z := zombieChildren(t); len(z) > beforeZ {
		t.Errorf("%d cycles left %d zombie children (was %d): pids %v -- "+
			"Kill without Wait does not reap, and the child is held until this "+
			"test binary exits", cycles, len(z), beforeZ, z)
	}
	// Three stdio pipes per unreaped child is what the measurement showed; the
	// bound is generous because the HTTP client may legitimately hold one idle
	// connection, and tight enough that per-cycle growth cannot hide under it.
	if fd := openFDs(t); fd > beforeFD+2 {
		t.Errorf("%d cycles left %d open file descriptors (was %d) -- that is "+
			"per-cycle growth, which is the unclosed pipes of a child that was "+
			"killed but never waited for", cycles, fd, beforeFD)
	}
}

// A child that starts but never becomes ready is killed by start itself.
//
// This is the leak f3fc6e2 did not close. cmd.Start() succeeding and the
// readiness wait failing are different things, and between them sits a running
// process that no call site had yet registered a stop for -- because every one
// of them registers it on the line after the t.Fatalf that fires on the error.
//
// Forced by pointing the readiness poll at a port nothing answers on, with a
// short limit, so the wait fails while the child is alive.
func TestAChildThatNeverBecomesReadyIsStillKilled(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc is where the child state is readable")
	}
	p := newVL(t, "127.0.0.1:19497")
	if p == nil {
		skipNoVL(t, "the unready-child gate")
	}
	// Point the readiness poll somewhere nothing will ever answer, so the wait
	// fails with the child running.
	p.url = "http://127.0.0.1:1"

	before := childPIDs(t)
	err := p.startWithin(1500 * time.Millisecond)
	if err == nil {
		t.Fatal("the readiness wait did not fail, so this test proves nothing")
	}
	if p.cmd != nil {
		t.Errorf("start returned an error and left p.cmd set: %v", p.cmd.Process)
	}
	// Nothing new of ours is still alive.
	for pid := range childPIDs(t) {
		if !before[pid] {
			t.Errorf("pid %d is still a live child after start failed. It has "+
				"no parent left to kill it once this binary exits", pid)
		}
	}
}

// childPIDs is this process's live (non-zombie) children.
func childPIDs(t *testing.T) map[int]bool {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read /proc: %v", err)
	}
	me := strconv.Itoa(os.Getpid())
	out := map[int]bool{}
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 || i+2 >= len(s) {
			continue
		}
		f := strings.Fields(s[i+2:])
		if len(f) >= 2 && f[0] != "Z" && f[1] == me {
			out[pid] = true
		}
	}
	return out
}
