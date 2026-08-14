package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

// newQuarantineServer opens dir with the quarantine policy set in the
// configuration, which is the only place it can take effect for tenants that
// already exist on disk.
func newQuarantineServer(t *testing.T, dir string) (*Server, error) {
	t.Helper()
	c := config.Default()
	c.Dir = dir
	c.CorruptionPolicy = "quarantine"
	return NewServerConfig(c)
}

// Readiness reflects storage health, and liveness does not.
//
// They gave the same unconditional answer, which made readiness useless for
// the one thing it exists for: taking a replica that cannot serve correctly
// out of rotation. A degraded store still WORKS -- it opens, it serves, its
// queries return -- and every query touching a quarantined group comes back
// with fewer rows and nothing in the response saying so. That is the case a
// load balancer must route around.

// corruptOneGroupOfTheTenant writes a row through the server, closes it, and
// corrupts the resulting group on disk.
func corruptOneGroupOfTheTenant(t *testing.T, dir string) {
	t.Helper()
	var groups []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "group-") && strings.HasSuffix(d.Name(), ".bin") {
			groups = append(groups, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) == 0 {
		t.Fatal("the tenant wrote no group file to corrupt")
	}
	b, err := os.ReadFile(groups[0])
	if err != nil {
		t.Fatal(err)
	}
	for i := len(b) / 4; i < len(b)/2; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(groups[0], b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, ts *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestReadinessReflectsStorageHealth(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}

	corruptOneGroupOfTheTenant(t, dir)

	// Reopen under the quarantine policy. It goes through the CONFIG, not a
	// setter: NewServerConfig opens every tenant already on disk, so a policy
	// applied after construction arrives too late for exactly the tenants that
	// need it.
	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	// Liveness stays up: the process is fine, and restarting it fixes nothing
	// a quarantined group suffers from.
	for _, p := range []string{"/health", "/-/healthy"} {
		if code, _ := get(t, ts2, p); code != 200 {
			t.Errorf("%s = %d, want 200: liveness must not follow storage health", p, code)
		}
	}

	// The QUERY readiness probe is out of rotation, and says which tenant and
	// why. /insert/ready is not: see TestInsertReadinessIgnoresStorageHealth.
	code, body := get(t, ts2, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d, want 503 with a degraded tenant", code)
	}
	if !strings.Contains(body, "degraded") {
		t.Errorf("/-/ready body %q does not say what is wrong", body)
	}

	// Acknowledgement puts it back.
	n, err := srv2.AcknowledgeDegraded()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("acknowledged %d tenants, want 1", n)
	}
	if code, body := get(t, ts2, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d after acknowledgement, want 200 (%s)", code, body)
	}
	// A second call has nothing left to acknowledge.
	if n, err := srv2.AcknowledgeDegraded(); err != nil || n != 0 {
		t.Errorf("re-acknowledged %d tenants (err %v), want 0", n, err)
	}
}

// A healthy server is ready, which is the negative case: a readiness probe
// that always failed would be as useless as one that always passed.
func TestReadinessIsGreenWhenHealthy(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")

	for _, p := range []string{"/-/ready", "/insert/ready", "/health", "/-/healthy"} {
		if code, body := get(t, ts, p); code != 200 {
			t.Errorf("%s = %d on a healthy server, want 200 (%s)", p, code, body)
		}
	}
}

// The storage health metrics are emitted and carry the right numbers.
func TestStorageHealthMetrics(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	body := metricsBody(t, ts2)
	for _, want := range []string{
		"simdlogs_storage_corrupt_groups 1",
		"simdlogs_storage_quarantined_groups 1",
		"simdlogs_storage_degraded_tenants 1",
		"simdlogs_storage_degraded_unacknowledged_tenants 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}

	// Acknowledgement moves one number and leaves the rest: the tenant is
	// still degraded, it is just a known state now.
	if _, err := srv2.AcknowledgeDegraded(); err != nil {
		t.Fatal(err)
	}
	body = metricsBody(t, ts2)
	if !strings.Contains(body, "simdlogs_storage_degraded_unacknowledged_tenants 0") {
		t.Error("the unacknowledged count did not drop after acknowledgement")
	}
	if !strings.Contains(body, "simdlogs_storage_degraded_tenants 1") {
		t.Error("acknowledgement cleared the degraded count; it records a decision, not a repair")
	}
}

// metricsBody fetches /metrics. It goes through the same handler the server
// registers, so a role gate on it would show up here rather than in
// production.
func metricsBody(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, body := get(t, ts, "/metrics")
	if code != 200 {
		t.Fatalf("/metrics = %d: %s", code, body)
	}
	return body
}

// The SERVER's default policy is fail, and nothing but configuration changes
// it. The storage layer's default is tested separately; this pins that the API
// layer does not quietly pick something else on the way down.
//
// Reviewer mutation M9 flipped this default to quarantine and the whole suite
// stayed green: a server that silently quarantines is a server that serves
// short answers to every query touching the lost range, which is the outcome
// the fail default exists to prevent.
func TestServerDefaultPolicyRefusesACorruptTenant(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	// NewServer opens the default tenant, so a corrupt default tenant is a
	// startup failure under the default policy.
	srv2, err := NewServer(dir)
	if err == nil {
		h := srv2.Handler()
		_ = h
		srv2.Close()
		t.Fatal("the server started with a corrupt default tenant under the default policy; " +
			"it would answer every query touching that range with fewer rows")
	}
	if !strings.Contains(err.Error(), "unreadable") {
		t.Errorf("error %q does not say the group is unreadable", err)
	}

	// The same directory opens once the policy asks for it.
	srv3, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatalf("the quarantine policy did not open it: %v", err)
	}
	srv3.Close()
}

// Readiness must survive EVICTION. forEachTenant walks open tenants only, so
// evicting an idle degraded tenant turned a 503 into a 200 while the data was
// still missing.
func TestReadinessSurvivesTenantEviction(t *testing.T) {
	dir := t.TempDir()
	// Build a degraded tenant on disk under a non-default key.
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	c2.Limits.MaxOpenTenants = 2
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()

	// Open tenant 7:0, which quarantines and degrades.
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	if code, _ := get(t, ts2, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d with a degraded tenant open, want 503", code)
	}

	// Push it out: MaxOpenTenants is 2 and the default tenant holds one slot.
	postTenant(t, ts2, "8", "0", `{"_time":3,"service":"c"}`+"\n")
	postTenant(t, ts2, "9", "0", `{"_time":4,"service":"d"}`+"\n")

	code, body := get(t, ts2, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d after the degraded tenant was evicted, want 503 (%s). "+
			"An evicted tenant is still degraded and its data is still missing.", code, body)
	}
	if !strings.Contains(body, "7:0") {
		t.Errorf("readiness body %q does not name the evicted degraded tenant", body)
	}

	// Acknowledging must work for a tenant that is not open.
	n, err := srv2.AcknowledgeDegraded()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("acknowledged %d, want 1", n)
	}
	if code, body := get(t, ts2, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d after acknowledging an evicted tenant, want 200 (%s)", code, body)
	}
}

// The admin endpoint is the operator-facing acknowledgement. Without it the
// only way to clear a 503 was a restart.
func TestAcknowledgeDegradedEndpoint(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	if code, _ := get(t, ts2, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503", code)
	}
	// A GET is refused: this silences a readiness failure, so it is not
	// something a link preview or a crawler can trigger.
	if code, _ := get(t, ts2, "/admin/acknowledge-degraded"); code != http.StatusMethodNotAllowed {
		t.Errorf("GET /admin/acknowledge-degraded = %d, want 405", code)
	}
	resp, err := http.Post(ts2.URL+"/admin/acknowledge-degraded", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("POST = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "acknowledged 1") {
		t.Errorf("response %q does not say what it accepted", body)
	}
	if code, body := get(t, ts2, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d after the endpoint acknowledged, want 200 (%s)", code, body)
	}
}

// The ingest readiness probe stays 200 on a degraded store. The degradation is
// read-side; the store takes writes normally, and failing this probe converts
// a read-side loss into an ingest outage.
func TestInsertReadinessIgnoresStorageHealth(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	if code, _ := get(t, ts2, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}
	if code, body := get(t, ts2, "/insert/ready"); code != 200 {
		t.Errorf("/insert/ready = %d on a degraded store, want 200 (%s). "+
			"Writes still work; failing this takes the node out of the ingest Service.", code, body)
	}
	// And the writes really do land.
	postBody(t, ts2, `{"_time":5,"service":"e"}`+"\n")
}

// postTenant writes one line into a named tenant, which is how a degraded
// non-default tenant gets built.
func postTenant(t *testing.T, ts *httptest.Server, acc, proj, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("AccountID", acc)
	req.Header.Set("ProjectID", proj)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("post to %s:%s = %d: %s", acc, proj, resp.StatusCode, b)
	}
}

// corruptGroupOfTenant corrupts one group of a named tenant directory.
func corruptGroupOfTenant(t *testing.T, dir, tenantDir string) {
	t.Helper()
	corruptOneGroupOfTheTenant(t, filepath.Join(dir, tenantDir))
}

// A degraded tenant that no request has touched must show at the FIRST probe.
//
// A tenant is marked degraded when its store opens, and NewServerConfig opens
// only the default one — so a replica restarted onto a disk with a degraded
// tenant reported ready until some request happened to open it. A probe whose
// job is keeping traffic off went green until traffic arrived.
func TestReadinessSeesADegradedTenantBeforeAnyRequest(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	// Quarantine it in a separate process-equivalent open, so the directory
	// carries the evidence without this server having opened the tenant.
	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	// A FRESH server that has opened nothing but the default tenant.
	c3 := config.Default()
	c3.Dir = dir
	c3.CorruptionPolicy = "quarantine"
	srv3, err := NewServerConfig(c3)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()

	code, body := get(t, ts3, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d at startup with a degraded tenant on disk, want 503 (%s). "+
			"No request has opened it, which is exactly when the probe matters.", code, body)
	}
	if !strings.Contains(body, "7:0") {
		t.Errorf("readiness body %q does not name the untouched degraded tenant", body)
	}
}

// Acknowledging twice must not report two acknowledgements. It was idempotent
// for open tenants and not for evicted ones.
func TestAcknowledgeDegradedIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	first, err := srv2.AcknowledgeDegraded()
	if err != nil {
		t.Fatal(err)
	}
	second, err := srv2.AcknowledgeDegraded()
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 0 {
		t.Errorf("acknowledged %d then %d, want 1 then 0", first, second)
	}
}

// Concurrent acknowledgement must not FAIL. writeFileAtomic uses a fixed
// `path + ".tmp"`, so two writers raced on one temp name and the loser's
// rename found nothing: 9 to 25 of 100 concurrent POSTs returned 500, on the
// one endpoint whose job is clearing a readiness failure.
//
// The assertion is "no 500", not "every response is 200". The first version
// fired 50 requests and required all 200, which measured the QUERY BUDGET
// rather than the fix: adminSpec goes through the query semaphore, the default
// budget is 32, and the surplus got 429. It failed one run in three when
// internal/storage ran alongside, and it would have passed on a machine where
// the requests never collided whether or not the serialization existed. The
// endpoint is exempt from the budget now, so a 429 should not appear either --
// but the assertion is on the failure mode the fix is about.
func TestConcurrentAcknowledgementDoesNotFail(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	const n = 50
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Post(ts2.URL+"/admin/acknowledge-degraded", "", nil)
			if err != nil {
				codes[i] = -1
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			codes[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	var failed, throttled int
	for _, c := range codes {
		switch {
		case c == http.StatusTooManyRequests:
			throttled++
		case c != 200:
			failed++
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d concurrent acknowledgements FAILED (not throttled)", failed, n)
	}
	if throttled > 0 {
		t.Errorf("%d of %d were throttled; the endpoint that clears a readiness "+
			"failure must not be charged the query budget", throttled, n)
	}
	if code, body := get(t, ts2, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d after acknowledgement, want 200 (%s)", code, body)
	}
}

// The acknowledge endpoint must answer while the query budget is exhausted.
// 32 in-flight queries is enough to make it 429, and the replica it would have
// restored is the one under that load.
func TestAcknowledgeEndpointIsNotChargedTheQueryBudget(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptOneGroupOfTheTenant(t, dir)

	c := config.Default()
	c.Dir = dir
	c.CorruptionPolicy = "quarantine"
	// A budget of one, and hold it.
	c.Limits.MaxConcurrentQuery = 1
	srv2, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")

	// Occupy the single query slot for the duration of the POST.
	release := make(chan struct{})
	occupied := make(chan struct{})
	go func() {
		req, _ := http.NewRequest(http.MethodGet, ts2.URL+"/select/logsql/query?query=*", nil)
		close(occupied)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		<-release
	}()
	<-occupied

	resp, err := http.Post(ts2.URL+"/admin/acknowledge-degraded", "", nil)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Errorf("the acknowledge endpoint answered 429 under query load: %s. "+
			"It is the button that ends the load, and it was charged the budget.", body)
	}
	if resp.StatusCode != 200 {
		t.Errorf("POST = %d: %s", resp.StatusCode, body)
	}
}

// A DELETED tenant directory is evidence gone. Deleting a tenant is an
// ordinary operator action, and treating "not a store" as "keep the recorded
// answer" left the probe reporting a quarantined group for a directory that
// does not exist.
func TestDeletedTenantDirectoryClearsTheDegradation(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	// The throttle is tested separately; here the question is whether the
	// re-read clears the record at all.
	srv3.SetDirRereadInterval(0)
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}

	// The operator deletes the tenant.
	if err := os.RemoveAll(filepath.Join(dir, "tenant-7-0")); err != nil {
		t.Fatal(err)
	}

	code, body := get(t, ts3, "/-/ready")
	if code != 200 {
		t.Errorf("/-/ready = %d after the tenant directory was deleted, want 200 (%s)", code, body)
	}
	m := metricsBody(t, ts3)
	if !strings.Contains(m, "simdlogs_storage_quarantined_groups 0") {
		t.Error("the quarantined gauge counts a directory that does not exist")
	}
}

// Readiness was moved onto the server's own record so an evicted or untouched
// degraded tenant still counts; /metrics was left walking the open tenants.
// The result was a server that pulled itself out of rotation while every
// storage-health gauge read 0 — and docs/lld/storage.md names one of those
// gauges as the one to alert on, so an operator following it was blind to the
// failure the scan exists to surface.
func TestMetricsAgreeWithReadinessAboutAnUntouchedTenant(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	// Quarantine it, then close, so the directory carries the evidence.
	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	// A fresh server that has opened nothing but the default tenant.
	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	// The throttle is tested separately; here the question is whether the
	// re-read clears the record at all.
	srv3.SetDirRereadInterval(0)
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()

	code, _ := get(t, ts3, "/-/ready")
	notReady := code == http.StatusServiceUnavailable
	body := metricsBody(t, ts3)
	metricSaysDegraded := strings.Contains(body, "simdlogs_storage_degraded_tenants 1")
	metricSaysUnacked := strings.Contains(body, "simdlogs_storage_degraded_unacknowledged_tenants 1")

	if !notReady {
		t.Errorf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}
	if !metricSaysDegraded || !metricSaysUnacked {
		t.Errorf("/-/ready says not ready and the metrics say degraded=%v unacknowledged=%v. "+
			"Two endpoints on one server disagree about one tenant, and the alert never fires.",
			metricSaysDegraded, metricSaysUnacked)
	}
	if !strings.Contains(body, "simdlogs_storage_quarantined_groups 1") {
		t.Error("the quarantined count is 0 for a tenant with a quarantined group")
	}

	// After acknowledgement the two must still agree.
	if _, err := srv3.AcknowledgeDegraded(); err != nil {
		t.Fatal(err)
	}
	if code, _ := get(t, ts3, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d after acknowledgement, want 200", code)
	}
	body = metricsBody(t, ts3)
	if !strings.Contains(body, "simdlogs_storage_degraded_unacknowledged_tenants 0") {
		t.Error("the unacknowledged gauge did not follow the acknowledgement")
	}
	if !strings.Contains(body, "simdlogs_storage_degraded_tenants 1") {
		t.Error("the degraded gauge cleared on acknowledgement; the data is still gone")
	}
}

// The documented remediation must work: emptying the quarantine directory is
// an operator deciding the evidence has been dealt with, and the replica must
// come back.
//
// It did not. The server's degraded record survives without an open store (so
// eviction and restart cannot hide a degraded tenant), nothing re-read the
// directory, and `AcknowledgeDegradedDir` treated "nothing quarantined" as a
// skip rather than a clearance. So `/-/ready` stayed 503 and the alert metric
// stayed 1 for an EMPTY directory, with no escape but a process restart —
// three individually-correct fixes interacting.
func TestEmptyingTheQuarantineDirectoryClearsTheDegradation(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	// A fresh server: the tenant is degraded on disk and not open here.
	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	// The throttle is tested separately; here the question is whether the
	// re-read clears the record at all.
	srv3.SetDirRereadInterval(0)
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}

	// The operator deals with the evidence.
	qdir := filepath.Join(dir, "tenant-7-0", "quarantine")
	ents, err := os.ReadDir(qdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if err := os.Remove(filepath.Join(qdir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}

	code, body := get(t, ts3, "/-/ready")
	if code != 200 {
		t.Errorf("/-/ready = %d after the quarantine directory was emptied, want 200 (%s). "+
			"The remediation docs/lld/storage.md documents leaves the replica stranded.", code, body)
	}
	m := metricsBody(t, ts3)
	for _, want := range []string{
		"simdlogs_storage_degraded_tenants 0",
		"simdlogs_storage_degraded_unacknowledged_tenants 0",
		"simdlogs_storage_quarantined_groups 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics do not contain %q after the evidence was cleared", want)
		}
	}
}

// Readiness and the metrics derive from ONE snapshot, so they cannot disagree
// about which tenants exist. Two implementations of one snapshot differed in
// their population; the difference was inert and was the next drift.
func TestReadinessAndMetricsShareOneSnapshot(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	c.CorruptionPolicy = "quarantine"
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	// One tenant that will be degraded, one that stays healthy.
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	postTenant(t, ts, "9", "0", `{"_time":1,"service":"b"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	srv2, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	// Open the degraded one so it quarantines, and the healthy one.
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"c"}`+"\n")
	postTenant(t, ts2, "9", "0", `{"_time":2,"service":"d"}`+"\n")

	code, body := get(t, ts2, "/-/ready")
	m := metricsBody(t, ts2)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503", code)
	}
	if strings.Count(body, "\n") != 2 { // the header plus one tenant
		t.Errorf("readiness names %d lines, want the header and one tenant:\n%s",
			strings.Count(body, "\n"), body)
	}
	if !strings.Contains(m, "simdlogs_storage_degraded_tenants 1") {
		t.Error("the metrics count a different number of degraded tenants than readiness names")
	}
	if strings.Contains(body, "9:0") {
		t.Error("readiness names the healthy tenant")
	}
}

// An UNREADABLE data directory must not be read as "the tenant was deleted".
//
// dirGone used to be `err == nil && fi.IsDir()`, so EACCES answered "gone" and
// the degraded record was DELETED. The server reported ready, and restoring
// the permissions did not bring the signal back — only a restart rebuilds the
// record. Misreporting is recoverable; destroying the record is not.
func TestUnreadableDataDirectoryDoesNotDeleteTheRecord(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; a mode of 000 does not stop a read")
	}
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	srv3.SetDirRereadInterval(0)
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}

	// The DATA directory, not the tenant directory: stat on a mode-000
	// directory still succeeds (you need +x on the parent to stat it, not read
	// on the thing itself), so removing the tenant's own permissions does not
	// make os.Stat fail and does not exercise this at all. Removing the
	// parent's does.
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Skipf("cannot remove permissions: %v", err)
	}
	defer os.Chmod(dir, 0o755)

	if code, body := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d with the tenant directory unreadable, want 503 (%s). "+
			"An unreadable directory is a problem to report, never an absence.", code, body)
	}

	// And restoring the permissions must leave the signal intact, not require
	// a restart.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if code, body := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d after the permissions were restored, want 503 (%s). "+
			"The record was destroyed while the directory was unreadable.", code, body)
	}
}

// The throttle throttles, and the default window is what the constant says.
// Tested on its own, so the tests that assert the remediation takes effect can
// set the window to zero and measure the semantics rather than the clock.
func TestDirectoryRereadIsThrottled(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()

	// A long window, so the change is provably not seen.
	srv3.SetDirRereadInterval(time.Hour)
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503", code)
	}
	qdir := filepath.Join(dir, "tenant-7-0", "quarantine")
	ents, err := os.ReadDir(qdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if err := os.Remove(filepath.Join(qdir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready = %d inside a one-hour window, want the throttled 503", code)
	}

	// Zero: the very next probe re-reads.
	srv3.SetDirRereadInterval(0)
	if code, body := get(t, ts3, "/-/ready"); code != 200 {
		t.Errorf("/-/ready = %d with the throttle off, want 200 (%s)", code, body)
	}

	if DefaultDirRereadEvery != 250*time.Millisecond {
		t.Errorf("the default window is %v; the comment argues for 250ms against a "+
			"probe interval of one to ten seconds", DefaultDirRereadEvery)
	}
}

// The answer must not depend on which side of the throttle window a probe
// lands on.
//
// The fresh directory read fed the response and was never written back into
// the server's record, so the record stayed at whatever startup found. A
// re-reading probe answered 200 and a throttled one 503 for the SAME on-disk
// state, and `/-/ready` and `/metrics` disagreed with each other — the
// invariant the one-snapshot change exists to hold.
//
// The trigger is a marker written by something other than this process, which
// is the ordinary case where the record and the disk diverge: a second replica
// on the same volume, a restore, or an operator following the documented file
// format. The acknowledge endpoint is the only thing that updates the record.
func TestThrottledProbeDoesNotRevertToTheStartupRecord(t *testing.T) {
	dir := t.TempDir()
	c := config.Default()
	c.Dir = dir
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postTenant(t, ts, "7", "0", `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()
	corruptGroupOfTenant(t, dir, "tenant-7-0")

	c2 := config.Default()
	c2.Dir = dir
	c2.CorruptionPolicy = "quarantine"
	srv2, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(srv2.Handler())
	postTenant(t, ts2, "7", "0", `{"_time":2,"service":"b"}`+"\n")
	ts2.Close()
	srv2.Close()

	srv3, err := NewServerConfig(c2)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	ts3 := httptest.NewServer(srv3.Handler())
	defer ts3.Close()

	srv3.SetDirRereadInterval(0)
	if code, _ := get(t, ts3, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready = %d, want 503: the fixture is not degraded", code)
	}

	// Something other than this process acknowledges: the marker appears with
	// a count matching what is quarantined.
	qdir := filepath.Join(dir, "tenant-7-0", "quarantine")
	n := 0
	ents, err := os.ReadDir(qdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".bin") {
			n++
		}
	}
	if err := os.WriteFile(filepath.Join(qdir, "ACKNOWLEDGED"),
		[]byte(itoa(n)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// One re-reading probe sees it.
	code, body := get(t, ts3, "/-/ready")
	if code != 200 {
		t.Fatalf("/-/ready = %d after the marker appeared, want 200 (%s)", code, body)
	}

	// Now a THROTTLED probe must give the same answer, and the metrics must
	// agree with it.
	srv3.SetDirRereadInterval(time.Hour)
	code2, body2 := get(t, ts3, "/-/ready")
	if code2 != code {
		t.Errorf("a throttled probe answered %d where the re-reading one answered %d (%s). "+
			"The fresh read was never written back, so the record reverted.", code2, code, body2)
	}
	m := metricsBody(t, ts3)
	if !strings.Contains(m, "simdlogs_storage_degraded_unacknowledged_tenants 0") {
		t.Errorf("/-/ready says ready and the metrics say unacknowledged; the two disagree")
	}
}

// The readiness re-read interval is reachable from configuration, not only
// from a setter the tests call. An exported API reachable from nothing but
// tests is the shape this task has already produced twice.
func TestDirRereadIntervalComesFromConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  time.Duration
		want time.Duration
	}{
		{"unset means the default", 0, DefaultDirRereadEvery},
		{"explicit", 5 * time.Second, 5 * time.Second},
		{"negative means every probe", -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Default()
			c.Dir = t.TempDir()
			c.DirRereadInterval = tc.set
			srv, err := NewServerConfig(c)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			srv.mu.Lock()
			got := srv.dirRereadEvery
			srv.mu.Unlock()
			if got != tc.want {
				t.Errorf("dirRereadEvery = %v, want %v", got, tc.want)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
