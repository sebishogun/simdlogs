package bench

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Timing primitives shared by every head-to-head in this package.
//
// Two defects these exist to make unrepresentable, both of which had shipped
// numbers behind them:
//
//   - A readiness wait counted as ingest. TestHeadToHead stamped VictoriaLogs'
//     ingest duration after a hardcoded `time.Sleep(3 * time.Second)` that let
//     VL flush, so every VL ingest figure this harness ever produced carried
//     three seconds VL did not spend. timeIngest stamps accept the instant the
//     POST returns and measures readiness separately.
//
//   - Ingest samples accumulating in one store. timeIt warms up once and then
//     takes seven samples, so timing an ingest with it wrote the corpus eight
//     times; TestPerOperation did that for two formats and then reported read
//     latencies over 3.2M rows under a heading that said 200000. sampleIngest
//     demands a fresh store per sample.
//
// benchHTTP is the client every request in this package goes through.
//
// http.DefaultClient has NO timeout, so a hung server hangs the test until go
// test's own -timeout kills the binary -- which skips deferred cleanups and
// orphans any VictoriaLogs subprocess the test started, holding its port. A
// bounded client turns that into a failed request the harness can report.
//
// The timeout is generous because these requests are real work: a 200k-row
// ingest at scale can legitimately take minutes. It bounds a hang, not a slow
// engine.
var benchHTTP = &http.Client{Timeout: 30 * time.Minute}

// errIngestTimeout is returned when an engine never becomes queryable inside
// the limit. It is an error rather than a Fatal so callers can report which
// engine stalled.
var errIngestTimeout = errors.New("ingest never became queryable inside the limit")

// ingestTiming separates the two latencies an ingest path has. Reporting one
// number for both is only correct for a synchronous engine; for an
// asynchronous one it either understates the engine (accept alone, ignoring
// that the data is not yet readable) or slanders it (a fixed sleep folded into
// accept). Both are reported so the reader picks.
type ingestTiming struct {
	accept    time.Duration // what the client waits for: the POST returning
	queryable time.Duration // from the same origin: when the rows read back
}

// timeIngest runs post, stamps accept the instant it returns, then polls ready
// until it reports true and stamps queryable from the same origin. accept can
// never contain poll time -- the property TestTimeIngestExcludesReadinessWait
// pins -- because the poll loop starts after the stamp.
//
// limit bounds the READINESS POLL ONLY. It cannot bound post: post has already
// been called when the deadline is computed, and the callers hand in bare
// http.Post on http.DefaultClient, which has no timeout. A hung POST is stopped
// by nothing here -- not the limit, not the poll loop -- and go test's own
// -timeout kills the binary without running deferred cleanups, orphaning any
// subprocess the test started. Callers that need the POST bounded must give
// their client a Timeout; benchHTTP below is that client.
//
// A ready that is already true on its first call yields queryable == the first
// poll, which is the correct answer for a synchronous engine: it was readable
// as soon as it was accepted.
func timeIngest(post func(), ready func() bool, poll, limit time.Duration) (ingestTiming, error) {
	origin := time.Now()
	post()
	t := ingestTiming{accept: time.Since(origin)}

	deadline := origin.Add(limit)
	for {
		if ready() {
			t.queryable = time.Since(origin)
			return t, nil
		}
		if time.Now().After(deadline) {
			t.queryable = time.Since(origin)
			return t, errIngestTimeout
		}
		time.Sleep(poll) // bench:untimed -- readiness poll, outside the accept stamp
	}
}

// sampleIngest takes n independent ingest samples and returns the minimum of
// each latency. fresh is called before every sample, including the warmup, and
// must leave the target engine holding no rows from a previous sample --
// otherwise sample k measures ingest into a store k-1 samples large, and the
// corpus the read half then times is n+1 times the size the report claims.
//
// The minimum, never a mean: the least-perturbed run is the one that says most
// about the engine rather than about the machine.
func sampleIngest(n int, fresh func() error, post func(), ready func() bool, poll, limit time.Duration) (ingestTiming, error) {
	if n < 1 {
		return ingestTiming{}, fmt.Errorf("sampleIngest: n=%d, want >= 1", n)
	}
	// ONE sample is returned, not the two minima taken independently.
	// Minimising each field separately produces a pair that never happened:
	// the accept from one run beside the queryable from another, with no
	// guarantee that queryable >= accept -- the invariant timeIngest holds and
	// TestTimeIngestSynchronousEngineIsQueryableAtAccept pins. The sample with
	// the smallest accept is the least-perturbed ingest, and its own queryable
	// is the one that belongs next to it.
	best := ingestTiming{accept: time.Duration(1<<62 - 1)}
	// n+1 passes: the first is warmup and is discarded, so a cold page cache
	// or a lazily-built index does not become the reported number.
	for i := 0; i <= n; i++ {
		if err := fresh(); err != nil {
			return ingestTiming{}, fmt.Errorf("sample %d: fresh: %w", i, err)
		}
		t, err := timeIngest(post, ready, poll, limit)
		if err != nil {
			return t, fmt.Errorf("sample %d: %w", i, err)
		}
		if i == 0 {
			continue
		}
		if t.accept < best.accept {
			best = t
		}
	}
	return best, nil
}

// rowCount asks an engine how many rows it holds in a window, through the
// stats endpoint both engines serve. Used to verify a corpus is the size the
// report says it is before any read is timed -- the check that would have
// caught the corpus multiplication directly, rather than by reading the
// harness: 8x per format, and 16x per engine once both formats had run.
//
// from and to are Unix seconds, the form both engines accept.
func rowCount(base string, from, to int64) (int, error) {
	v := url.Values{
		"query": {"* | stats count() n"},
		"start": {strconv.FormatInt(from, 10)},
		"end":   {strconv.FormatInt(to, 10)},
	}
	resp, err := benchHTTP.Get(base + "/select/logsql/query?" + v.Encode())
	if err != nil {
		return 0, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("stats count: %s: %s", resp.Status, first(body, 160))
	}
	// One NDJSON object, {"n":"<count>"}. The count arrives as a string from
	// both engines; a number is accepted too rather than failing the harness
	// on a representation change.
	for _, ln := range strings.Split(string(body), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			return 0, fmt.Errorf("stats count: not JSON: %s", first(ln, 160))
		}
		raw, ok := m["n"]
		if !ok {
			return 0, fmt.Errorf("stats count: no field n in %s", first(ln, 160))
		}
		switch x := raw.(type) {
		case string:
			n, err := strconv.Atoi(x)
			if err != nil {
				return 0, fmt.Errorf("stats count: n=%q is not an integer", x)
			}
			return n, nil
		case float64:
			return int(x), nil
		default:
			return 0, fmt.Errorf("stats count: n has type %T", raw)
		}
	}
	return 0, fmt.Errorf("stats count: empty answer")
}

// The compatibility corpus's shape, shared by every differential that uses
// compatCorpus. Its records start at 2024-05-01T00:00:00Z and run one per
// second for compatRows seconds; the window has slack on both ends so a
// boundary record cannot be missed by the count.
const (
	compatRows = 500
	compatFrom = 1714521600 - 10
	compatTo   = 1714521600 + compatRows + 10
)

// requireRows fails the run unless an engine holds exactly want rows in the
// window, before anything is timed against it.
//
// This is step 3 of the harness repair, and it is the check that catches the
// class of defect by measurement rather than by reading: TestPerOperation
// timed its ingest with a helper that warmed up once and sampled seven times,
// wrote two formats, and so held 3.2M rows while reporting read latencies
// under a heading that said 200000. No assertion existed that could notice.
func requireRows(t testingT, engine, base string, from, to int64, want int) {
	t.Helper()
	got, err := rowCount(base, from, to)
	if err != nil {
		t.Fatalf("%s: counting rows before timing: %v", engine, err)
	}
	if got != want {
		t.Fatalf("%s holds %d rows, the report says %d -- the timed corpus is not the one being described",
			engine, got, want)
	}
}

// waitFor polls cond until it holds, failing with msg at the limit. Used
// outside every timed interval, to bring an asynchronous engine to a known
// state before a measurement starts rather than during one.
func waitFor(t interface {
	Helper()
	Fatalf(format string, args ...any)
}, cond func() bool, limit time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s (waited %v)", msg, limit)
		}
		time.Sleep(50 * time.Millisecond) // bench:untimed -- state poll before a measurement
	}
}

// testingT is the slice of *testing.T requireRows uses, so the helper can be
// exercised without a real failure ending the test that checks it.
type testingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// readyAtLeast builds the ready predicate an asynchronous engine needs: the
// rows are queryable once the engine reports at least want of them. "At least"
// rather than "exactly" so a shared window that also catches a probe record
// does not deadlock the poll; the exact count is asserted separately by
// requireRows, where a mismatch is a report defect rather than a timing one.
func readyAtLeast(base string, from, to int64, want int) func() bool {
	return func() bool {
		n, err := rowCount(base, from, to)
		return err == nil && n >= want
	}
}

// skipNoVL is the ONE way this package declines to run a differential, and it
// says why in a form nobody mistakes for a pass. A bare t.Skip("not staged")
// scrolls past identically to a test that ran: SIMDLOGS_COMPAT=1 was set, the
// operator believes the differential ran, and it did not.
//
// The absent binary is also the only acceptable reason. Every other skip in a
// differential -- a route that 404s, an answer that is empty, a version that
// disagrees -- is the finding, not a reason to stop looking.
func skipNoVL(t *testing.T, what string) {
	t.Helper()
	t.Skipf("SKIPPED, NOT PASSED: %s did not run. internal/bench/victoria-logs "+
		"is not staged, so there was nothing to compare against. Build it with "+
		"`go build -o internal/bench/victoria-logs ./app/victoria-logs` in the "+
		"VictoriaLogs checkout. Nothing about compatibility was verified by this run.",
		what)
}
