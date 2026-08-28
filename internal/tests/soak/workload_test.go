package soak

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
	"github.com/sebishogun/simdlogs/internal/config"
)

// The soak workload.
//
// Every generator below exists because something it exercises can leak, and the
// leak is invisible at short scale:
//
//	ingest      -- a writer buffer, a flush batch, a goroutine per request
//	query       -- a snapshot lease; a group mapping released late is address
//	               space that never comes back
//	tenant churn-- a tenant created and evicted has a store, a writer and a
//	               background loop to take down with it
//	retention   -- unmapping and removing groups while readers hold them
//	backup      -- a snapshot held for the whole stream, pinning every group
//	restart     -- draining and re-opening over the same directory
//
// They run CONCURRENTLY on purpose. Each in isolation is already covered by an
// ordinary test; what a soak adds is that they overlap, which is where a
// snapshot outlives the retention pass that was waiting for it.

// errThrottled is backpressure, not failure and not success.
//
// It has to be a distinct value because it is neither: a soak that treated a
// 429 as failure would refuse to run at the rates it exists to sustain, and one
// that treated it as success counted throttled requests as writes -- which is
// how a run in which every request was refused reported thousands of them
// against an empty store.
var errThrottled = errors.New("throttled")

// errRestarting is a request that raced this soak's own listener swap.
//
// The restart generator closes the listener and opens a new one, so a request
// already in flight to the old address gets "connection refused". That is the
// soak restarting, not the server failing -- and it must be told apart from a
// connection refused to the CURRENT address, which is the server having died
// and is exactly what a soak is for.
var errRestarting = errors.New("raced a listener restart")

// tenantsInPlay is the steady set of tenants under continuous load.
const tenantsInPlay = 8

// churnRing is how many DISTINCT tenants the churn generator cycles through.
//
// Bounded, and the bound is the whole point. The first version drew a fresh
// tenant id from a 2^20 space every iteration, which in twenty seconds created
// 92,589 tenants, 92,601 files and 92,798 mappings -- and the mapping bound
// duly failed. That was not a leak: it was a load generator asking for
// unbounded tenants and getting them. Unbounded creation is not a leak test,
// it is a memory-exhaustion test with no assertion in it.
//
// A ring exercises what tenant churn is actually about: a tenant goes idle,
// gets evicted, and comes back later needing its store reopened. That needs
// tenants to RECUR, which a fresh id every time guarantees they never do.
const churnRing = 64

// churnPace and ingestPace keep the soak a soak.
//
// Unpaced, the generators ran at benchmark speed: 92k ingest requests and
// 624 MB of store in twenty seconds, which extrapolates to terabytes over the
// 24-hour release mode. A soak is about duration, not throughput -- the
// failures it looks for need time to accumulate, not bytes.
const (
	ingestPace = 20 * time.Millisecond
	churnPace  = 200 * time.Millisecond
)

func TestSoak(t *testing.T) {
	if !soakEnabled(t) {
		return
	}
	total := soakDuration(t)

	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	c.Limits = config.TestLimits()

	srv, err := api.NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()
	// The listener is swappable, because the restart generator cycles it and
	// every other generator has to follow it to the new address.
	var listenerMu sync.Mutex
	ts := httptest.NewServer(srv.Handler())
	baseURL := func() string {
		listenerMu.Lock()
		defer listenerMu.Unlock()
		return ts.URL
	}
	swapListener := func() string {
		listenerMu.Lock()
		defer listenerMu.Unlock()
		if ctx.Err() != nil {
			return ""
		}
		prev := ts.URL
		// Close DRAINS: it waits for every in-flight handler before returning.
		// That wait is the property this generator exists to exercise.
		ts.Close()
		ts = httptest.NewServer(srv.Handler())
		return prev
	}

	// Retention, running INSIDE the soak.
	//
	// This is the overlap the soak most wants: a retention pass unmapping and
	// removing groups while queries hold snapshots of them and backups hold
	// snapshots of everything. Without it the run exercises growth and never
	// exercises removal -- and removal is where a mapping outlives its group.
	//
	// A short window and a fast interval, because a soak's clock is the run
	// and not the deployment: with the default hour-scale retention no pass
	// would fire inside a developer soak at all.
	stopRetention := srv.StartRetention(30*time.Second, 5*time.Second)
	defer func() {
		stopRetention()
		listenerMu.Lock()
		ts.Close()
		listenerMu.Unlock()
		srv.Close()
	}()

	var (
		writes, reads, churns, backups, restarts atomic.Int64
		throttled, failures                      atomic.Int64
	)
	fail := func(what string, err error) {
		failures.Add(1)
		t.Errorf("%s: %v", what, err)
	}

	// Warm-up before the baseline sample, so the baseline is steady state and
	// not an empty process. Comparing against an empty process would make every
	// bound pass: everything grows from nothing on the first request.
	warmup := total / 10
	if warmup < 5*time.Second {
		warmup = 5 * time.Second
	}
	if warmup > time.Minute {
		warmup = time.Minute
	}

	var wg sync.WaitGroup
	// defer, not a bare statement: a t.Fatal or a panic in the assertions below
	// would otherwise skip the wait and leave these goroutines writing into a
	// server the deferred Close has already taken down.
	defer wg.Wait()
	defer cancel()

	client := &http.Client{Timeout: 30 * time.Second}

	spawn := func(name string, fn func(rnd *rand.Rand) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Seeded per generator, deterministically: a soak that cannot be
			// re-run with the same shape is a soak whose failures cannot be
			// investigated.
			rnd := rand.New(rand.NewSource(int64(len(name))))
			for ctx.Err() == nil {
				err := fn(rnd)
				if errors.Is(err, errThrottled) || errors.Is(err, errRestarting) {
					// Backpressure, already counted; or this soak's own restart.
					// Neither is the server failing, and a connection refused to
					// the CURRENT address still falls through to fail() below.
					continue
				}
				if err != nil && ctx.Err() == nil {
					fail(name, err)
					return
				}
			}
		}()
	}

	// raced reports whether a transport error was this soak's own listener
	// swap: the address the request went to is no longer the current one.
	raced := func(used string) bool { return used != baseURL() }

	post := func(path, ctype, body, tenant string) error {
		used := baseURL()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, used+path,
			strings.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", ctype)
		if tenant != "" {
			req.Header.Set("AccountID", tenant)
		}
		resp, err := client.Do(req)
		if err != nil {
			if raced(used) {
				return errRestarting
			}
			return err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		// Any non-2xx is a failure, not just a 5xx.
		//
		// Counting only 5xx meant a soak whose every write was refused 401 or
		// 400 still reported hundreds of thousands of "writes" and a flat
		// resource profile -- because nothing was happening. A load generator
		// that cannot tell refusal from success measures nothing, and the
		// bounds it feeds pass vacuously.
		// 429 is the concurrency limiter doing its job under load, not a
		// defect -- a soak that treated backpressure as failure would refuse to
		// run at the very rates it exists to sustain.
		//
		// But it is NOT a write either, and returning nil here made it one:
		// the caller increments `writes` before calling, so a run in which
		// every request was throttled reported thousands of writes against an
		// empty store. Probed: with admission refusing everything 429, the soak
		// passed with writes=1682, groups=0, disk=0 and three of four bounds
		// skipped. That is entry 42's defect exactly, one status code over.
		//
		// So it is counted separately and the caller learns nothing landed.
		if resp.StatusCode == http.StatusTooManyRequests {
			throttled.Add(1)
			time.Sleep(50 * time.Millisecond)
			return errThrottled
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("%s: HTTP %d: %.200s", path, resp.StatusCode, b)
		}
		return nil
	}

	get := func(path, tenant string) error {
		used := baseURL()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, used+path, nil)
		if err != nil {
			return err
		}
		if tenant != "" {
			req.Header.Set("AccountID", tenant)
		}
		resp, err := client.Do(req)
		if err != nil {
			if raced(used) {
				return errRestarting
			}
			return err
		}
		defer resp.Body.Close()
		// Drained, always: an undrained body holds the connection and the
		// server-side goroutine, which would show up as the very leak this is
		// looking for and be the test's own fault.
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusTooManyRequests {
			throttled.Add(1)
			time.Sleep(50 * time.Millisecond)
			return errThrottled
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
		}
		return nil
	}

	// Ingest, across tenants.
	for i := 0; i < 4; i++ {
		spawn(fmt.Sprintf("ingest-%d", i), func(rnd *rand.Rand) error {
			defer time.Sleep(ingestPace)
			tenant := strconv.Itoa(rnd.Intn(tenantsInPlay))
			var b strings.Builder
			for k := 0; k < 200; k++ {
				fmt.Fprintf(&b, `{"_msg":"soak line %d","level":"%s","host":"h%d","n":"%d"}`+"\n",
					k, []string{"error", "warn", "info"}[rnd.Intn(3)], rnd.Intn(16), rnd.Int63())
			}
			// Counted AFTER acceptance. Incrementing first counts requests
			// offered, which is not the same number and is the one that stays
			// high when nothing is landing.
			if err := post("/insert/jsonline", "application/x-ndjson", b.String(), tenant); err != nil {
				return err
			}
			writes.Add(1)
			return nil
		})
	}

	// Queries, including the shapes that hold a snapshot longest.
	for i := 0; i < 3; i++ {
		spawn(fmt.Sprintf("query-%d", i), func(rnd *rand.Rand) error {
			defer time.Sleep(ingestPace)
			tenant := strconv.Itoa(rnd.Intn(tenantsInPlay))
			for _, q := range []string{
				"/select/logsql/query?query=%2A&limit=100",
				"/select/logsql/query?query=level%3A%3Derror%20%7C%20stats%20count%28%29%20n",
				"/select/logsql/facets?query=%2A",
				"/select/logsql/field_values?query=%2A&field=level",
				"/select/logsql/hits?query=%2A&step=1m",
			} {
				if err := get(q, tenant); err != nil {
					return err
				}
				reads.Add(1)
			}
			return nil
		})
	}

	// Tenant churn: new tenants arriving and old ones going idle.
	spawn("tenant-churn", func(rnd *rand.Rand) error {
		defer time.Sleep(churnPace)
		// A bounded ring, above the steady set so the two do not collide. The
		// ring is walked rather than sampled, so every tenant in it goes idle
		// for a full lap -- which is what makes eviction and reopen happen.
		tenant := strconv.FormatInt(1000+churns.Load()%churnRing, 10)
		if err := post("/insert/jsonline", "application/x-ndjson",
			`{"_msg":"churn"}`+"\n", tenant); err != nil {
			return err
		}
		if err := get("/select/logsql/query?query=%2A&limit=1", tenant); err != nil {
			return err
		}
		churns.Add(1)
		return nil
	})

	// Backups: each holds a snapshot for its whole stream, pinning every group
	// it captured against unmapping.
	spawn("backup", func(rnd *rand.Rand) error {
		backups.Add(1)
		defer time.Sleep(2 * time.Second) // not a tight loop; a backup is expensive
		return get("/admin/backup", strconv.Itoa(rnd.Intn(tenantsInPlay)))
	})

	// Rules and observability surfaces, which run their own loops.
	spawn("ops", func(rnd *rand.Rand) error {
		defer time.Sleep(time.Second)
		for _, p := range []string{"/metrics", "/alerts", "/-/ready", "/health"} {
			if err := get(p, ""); err != nil {
				return err
			}
		}
		return nil
	})

	// Graceful restarts of the HTTP front, which drains in-flight requests.
	//
	// This generator's body used to be `get("/-/ready")` -- it restarted
	// nothing, while its name, its counter and its comment all said it did, and
	// the run printed `restarts=6` for six readiness probes. The drain is named
	// in this package's header as one of the six things a soak adds and was
	// exercised nowhere.
	//
	// A real listener cycle: Close() waits for in-flight handlers, which is the
	// drain, and a new listener takes the same store. The URL changes, so the
	// generators read it through a mutex rather than closing over it.
	spawn("restart", func(rnd *rand.Rand) error {
		defer time.Sleep(10 * time.Second)
		old := swapListener()
		if old == "" {
			return nil // shutting down
		}
		restarts.Add(1)
		// The new listener must actually serve, or a "restart" that left the
		// server dead would look like a successful one to everything above.
		return get("/-/ready", "")
	})

	t.Logf("warming up for %s", warmup)
	select {
	case <-time.After(warmup):
	case <-ctx.Done():
		t.Fatal("the soak ended during warm-up; the duration is too short to measure anything")
	}
	base := takeSample(t, dir, baseURL(), client)
	t.Logf("baseline  %s", base)

	// Sample periodically so a run that fails late still leaves a trace of how
	// it got there.
	ticker := time.NewTicker(maxDur(total/10, 15*time.Second))
	defer ticker.Stop()
	var last sample
	for done := false; !done; {
		select {
		case <-ticker.C:
			last = takeSample(t, dir, baseURL(), client)
			t.Logf("t+%-8s %s", time.Since(base.At).Truncate(time.Second), last)
		case <-ctx.Done():
			done = true
		}
	}
	cancel()
	wg.Wait()

	final := takeSample(t, dir, baseURL(), client)
	t.Logf("final     %s", final)
	if os.Getenv("SIMDLOGS_SOAK_DEBUG") != "" {
		filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
			if err == nil {
				t.Logf("  DEBUG %s dir=%v size=%d", p, fi.IsDir(), fi.Size())
			}
			return nil
		})
	}
	t.Logf("writes=%d reads=%d churns=%d backups=%d restarts=%d throttled=%d failures=%d",
		writes.Load(), reads.Load(), churns.Load(), backups.Load(),
		restarts.Load(), throttled.Load(), failures.Load())

	// The load must have REACHED THE STORE, not merely been offered.
	//
	// Counting requests is what let a run in which every one was refused report
	// six figures of them, and counting only accepted requests is still not
	// enough: a write can be accepted and buffered and never flushed. The
	// bounds below are ratios over group files and stored bytes, so those are
	// what has to be non-zero for any of them to mean anything.
	switch {
	case writes.Load() == 0 || reads.Load() == 0:
		t.Fatalf("no load was accepted (writes=%d reads=%d throttled=%d); every "+
			"bound below would pass vacuously",
			writes.Load(), reads.Load(), throttled.Load())
	case final.GroupFiles == 0:
		t.Fatalf("%d writes were accepted and the store holds no group files; "+
			"nothing reached disk and three of the four bounds are ratios over "+
			"group count", writes.Load())
	case final.DiskKB == 0:
		t.Fatalf("%d writes were accepted and the store occupies no measurable "+
			"disk", writes.Load())
	case final.GroupFiles <= base.GroupFiles:
		t.Fatalf("the store went from %d group files to %d: the run stored "+
			"nothing after the baseline, so the bounds compare a sample against "+
			"itself", base.GroupFiles, final.GroupFiles)
	}
	// Every bound reports, including the ones that did not run. A bound
	// skipped in silence reads as a bound that passed -- and two of these were
	// being skipped on every sample (a manifest measured in KB that rounded to
	// zero, and a mapping count expressed as a difference that went negative)
	// while the summary said the soak was clean.
	for _, b := range bounds() {
		from, to := b.get(base), b.get(final)
		if from == 0 {
			t.Logf("SKIPPED %s: not measurable in this run (baseline reads 0)", b.name)
			continue
		}
		limit := int64(float64(from)*b.maxMul) + b.maxPlus
		if to > limit {
			t.Errorf("%s grew %d -> %d, past the bound of %d (x%.1f + %d): %s",
				b.name, from, to, limit, b.maxMul, b.maxPlus, b.why)
		}
	}
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// takeSample measures everything the bounds are about.
func takeSample(t *testing.T, dir, url string, client *http.Client) sample {
	t.Helper()
	runtime.GC() // so RSS reflects what is retained, not what is merely uncollected

	s := sample{
		At:         time.Now(),
		Goroutines: runtime.NumGoroutine(),
		Mappings:   mappingCount(),
		VmSizeKB:   procStatusKB("VmSize"),
		RSSKB:      procStatusKB("VmRSS"),
	}
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		s.Files++
		s.DiskKB += fi.Size() >> 10
		base := filepath.Base(p)
		if strings.HasPrefix(base, "group-") {
			s.GroupFiles++
		}
		if strings.EqualFold(base, "MANIFEST") || strings.Contains(base, "manifest") {
			s.ManifestBytes += fi.Size()
		}
		return nil
	})

	start := time.Now()
	req, _ := http.NewRequest(http.MethodGet, url+"/select/logsql/query?query=%2A&limit=100", nil)
	req.Header.Set("AccountID", "0")
	if resp, err := client.Do(req); err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	s.QueryNanos = time.Since(start).Nanoseconds()
	return s
}
