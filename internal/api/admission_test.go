package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/query"
)

// admissionServer returns a server whose per-tenant read limit is n, plus the
// Server itself -- the test needs to take slots directly.
//
// Taking the slot through srv.admission rather than by racing two real queries
// is what makes this deterministic. A query against a small store finishes in
// microseconds, so a test that tried to catch two of them overlapping would
// pass or fail on scheduling, and a test that made the store big enough to be
// slow would be measuring the machine. What is under test is the WIRING --
// that the read path consults admission under the resolved tenant key and maps
// the refusal to 429 -- and that is exactly what an occupied slot exercises.
func admissionServer(t *testing.T, n int, wait time.Duration) (*Server, *httptest.Server) {
	t.Helper()
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	c.Limits.MaxQueriesPerTenant = n
	c.Limits.QueryQueueWait = wait
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// asTenant issues a request as a specific tenant. With no -auth.config the
// resolved key follows the headers, so the tenant key is known to the test.
func asTenant(t *testing.T, ts *httptest.Server, acc, proj, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("AccountID", acc)
	req.Header.Set("ProjectID", proj)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// A tenant at its read limit is refused with 429, and the tenant next door is
// unaffected. The second half is the point: a process-wide limit alone cannot
// express this, and a per-tenant limit that leaked across tenants would be the
// same outage wearing a different number.
func TestPerTenantAdmissionRefusesOnlyTheTenantAtItsLimit(t *testing.T) {
	srv, ts := admissionServer(t, 1, 0)

	release, err := srv.admission.Acquire(context.Background(), "7:3")
	if err != nil {
		t.Fatal(err)
	}

	code, body := asTenant(t, ts, "7", "3", "/select/logsql/query?query=*")
	if code != http.StatusTooManyRequests {
		t.Fatalf("a query for a tenant at its limit returned %d (%s), want 429", code, body)
	}
	if !strings.Contains(body, "7:3") {
		t.Errorf("the refusal does not name the tenant or its limit: %q", body)
	}

	if code, body := asTenant(t, ts, "8", "0", "/select/logsql/query?query=*"); code != 200 {
		t.Fatalf("a different tenant was refused too: %d (%s)", code, body)
	}

	release()
	if code, body := asTenant(t, ts, "7", "3", "/select/logsql/query?query=*"); code != 200 {
		t.Fatalf("the slot was not returned: %d (%s)", code, body)
	}
}

// Writes are not read admission. The limit exists because a scan holds memory
// and CPU for as long as it runs; an ingest request does not, and a full slot
// pool refusing ingest would drop data an agent cannot re-send.
func TestAdmissionDoesNotRefuseWrites(t *testing.T) {
	srv, ts := admissionServer(t, 1, 0)
	release, err := srv.admission.Acquire(context.Background(), "0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(`{"_time":1,"_msg":"x"}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		t.Fatalf("ingest was refused by read admission: %d (%s)", resp.StatusCode, b)
	}
}

// The counters an operator alerts on. A rejection that is invisible in
// /metrics looks identical to a slow client from the outside.
func TestAdmissionAndWorkerCountersAreExposed(t *testing.T) {
	srv, ts := admissionServer(t, 1, 0)
	release, err := srv.admission.Acquire(context.Background(), "7:3")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if code, _ := asTenant(t, ts, "7", "3", "/select/logsql/query?query=*"); code != 429 {
		t.Fatalf("expected a rejection to count, got %d", code)
	}

	code, body := get(t, ts, "/metrics")
	if code != 200 {
		t.Fatalf("/metrics returned %d", code)
	}
	for _, want := range []string{
		"simdlogs_scan_workers_total",
		"simdlogs_scan_workers_in_use",
		"simdlogs_query_admission_in_flight",
		"simdlogs_query_admission_queued",
		"simdlogs_query_admission_rejected_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not expose %s:\n%s", want, body)
		}
	}
}

// Nothing configured admits everything, so a deployment that has not decided
// behaves as it did before this existed.
func TestNoAdmissionConfiguredAdmitsEverything(t *testing.T) {
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if srv.admission != nil {
		t.Fatal("an admission controller was built with no limit configured")
	}
	// The worker budget IS installed unconditionally: the default it replaces
	// -- every query taking GOMAXPROCS of its own -- is the pathological one.
	if srv.workers == nil || srv.workers.Total() < 1 {
		t.Fatalf("no scan worker budget was installed: %+v", srv.workers)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	if code, body := asTenant(t, ts, "7", "3", "/select/logsql/query?query=*"); code != 200 {
		t.Fatalf("%d (%s)", code, body)
	}
}

// A queue wait that elapses is 429 too, not 504: the server never started the
// query, so nothing timed out except the wait for permission. 504 would tell a
// client its query was too slow and send it to shorten a time range that was
// never scanned.
func TestQueueTimeoutIsARejection(t *testing.T) {
	if got := query.HTTPStatus(query.ErrQueueTimeout); got != http.StatusTooManyRequests {
		t.Fatalf("a queue timeout maps to %d, want 429", got)
	}
	if got := query.HTTPStatus(query.ErrDeadlineExceeded); got != http.StatusGatewayTimeout {
		t.Fatalf("an execution deadline maps to %d, want 504", got)
	}
}
