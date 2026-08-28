package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// The health-state matrix.
//
// Liveness and readiness are used for OPPOSITE actions: liveness failing means
// kill the process, readiness failing means stop sending it traffic.
// Conflating them turns a full disk into a crash loop -- probe red, process
// killed, restarts onto the same full disk, and the restart destroys the rows
// a graceful drain would have flushed. Every test below is about keeping that
// separation true.

func healthJSON(t *testing.T, ts *httptest.Server, path string) (int, HealthReport, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + path + "?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var rep HealthReport
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("%s is not JSON: %s", path, b)
	}
	return resp.StatusCode, rep, string(b)
}

// A healthy server: both probes green, no conditions.
func TestAHealthyServerIsLiveAndReady(t *testing.T) {
	ts := quotaServer(t, config.Storage{})
	for _, path := range []string{"/-/ready", "/-/healthy", "/health"} {
		code, body := get(t, ts, path)
		if code != 200 || body != "OK" {
			t.Errorf("%s = %d %q, want 200 OK", path, code, body)
		}
	}
	code, rep, raw := healthJSON(t, ts, "/-/ready")
	if code != 200 || rep.State != StateReady || !rep.Ready || !rep.Live {
		t.Fatalf("%d %+v: %s", code, rep, raw)
	}
	if len(rep.Conditions) != 0 {
		t.Errorf("a healthy server reported conditions: %+v", rep.Conditions)
	}
	if rep.UptimeSecs < 0 {
		t.Errorf("uptime %v", rep.UptimeSecs)
	}
}

// A full disk takes the server out of ROTATION, not out of existence. This is
// the crash-loop case: liveness must stay green so the process is not killed
// and restarted onto the same full disk.
func TestAFullDiskFailsReadinessAndNotLiveness(t *testing.T) {
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	postLine(t, ts, `{"_time":2,"_msg":"x"}`)

	code, rep, raw := healthJSON(t, ts, "/-/ready")
	if code != http.StatusServiceUnavailable || rep.Ready {
		t.Fatalf("readiness %d %+v: %s", code, rep, raw)
	}
	if rep.State != StateDiskFull {
		t.Fatalf("state = %q, want %q", rep.State, StateDiskFull)
	}
	if !rep.Live {
		t.Fatal("a full disk reported the PROCESS as dead; the orchestrator kills it " +
			"and it restarts onto the same full disk")
	}
	for _, path := range []string{"/-/healthy", "/health"} {
		if code, _ := get(t, ts, path); code != 200 {
			t.Errorf("liveness at %s = %d on a full disk, want 200", path, code)
		}
	}
	if len(rep.Conditions) == 0 || !strings.Contains(rep.Conditions[0].Detail, "reject reserve") {
		t.Errorf("the condition does not name the reserve: %+v", rep.Conditions)
	}
}

// The warn reserve makes readiness red BEFORE any write fails. That interval
// is the entire reason there are two thresholds; a probe that only went red
// once writes failed would report the outage rather than prevent it.
func TestTheWarnReserveMakesReadinessRedBeforeWritesFail(t *testing.T) {
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 500}, nil
	})()
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	// Writes still succeed at the warn level.
	if code, body := postLine(t, ts, `{"_time":2,"_msg":"x"}`); code/100 != 2 {
		t.Fatalf("a write at the warn level returned %d (%s)", code, body)
	}
	code, rep, raw := healthJSON(t, ts, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readiness %d at the warn level, want 503: %s", code, raw)
	}
	if rep.State != StateDiskLow {
		t.Fatalf("state = %q, want %q", rep.State, StateDiskLow)
	}
	if !rep.Live {
		t.Error("the warn reserve killed liveness")
	}
}

// A draining server is NOT live: it is going to exit, and an orchestrator that
// keeps probing it green keeps routing to it until the connection fails.
func TestADrainingServerIsNotLive(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if code, _ := get(t, ts, "/-/healthy"); code != 200 {
		t.Fatal("not live before shutdown")
	}
	srv.Close()

	code, rep, raw := healthJSON(t, ts, "/-/healthy")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("liveness = %d while draining, want 503: %s", code, raw)
	}
	if rep.Live || rep.Ready {
		t.Fatalf("a draining server reports live=%v ready=%v", rep.Live, rep.Ready)
	}
	if rep.State != StateShuttingDown {
		t.Fatalf("state = %q, want %q", rep.State, StateShuttingDown)
	}
}

// The plain-text body keeps the shape an existing probe parses. A health
// endpoint whose body changes shape breaks the monitoring that was watching
// for exactly this -- the one time you least want the monitoring to break.
func TestTheTextBodyKeepsItsShape(t *testing.T) {
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	postLine(t, ts, `{"_time":2,"_msg":"x"}`)

	code, body := get(t, ts, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("%d: %s", code, body)
	}
	if !strings.Contains(body, "NOT READY: 1 tenant(s) under storage pressure") {
		t.Fatalf("the count line changed shape:\n%s", body)
	}
	if !strings.HasPrefix(body, "NOT READY") {
		t.Errorf("the body does not start with NOT READY:\n%s", body)
	}
}

// The detailed report is authorized. It names tenants and byte counts, which
// is an internal inventory, and a probe endpoint is the one place a server
// answers an unauthenticated caller by design.
func TestTheDetailedReportIsAuthorized(t *testing.T) {
	_, ts := authedServer(t)

	// Unauthenticated: the state, and nothing that names a tenant. The probe
	// itself still answers -- a liveness or readiness probe carries no
	// credential, and 401 on it takes the server out of rotation for having
	// authentication configured.
	resp := do(t, ts, http.MethodGet, "/-/ready?format=json", "", nil, "")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("the probe itself needs a credential: %s", b)
	}
	var bare map[string]any
	if err := json.Unmarshal(b, &bare); err != nil {
		t.Fatalf("not JSON: %s", b)
	}
	if _, ok := bare["state"]; !ok {
		t.Errorf("an unauthenticated caller gets no state: %s", b)
	}
	for _, leaked := range []string{"conditions", "uptime_seconds"} {
		if _, ok := bare[leaked]; ok {
			t.Errorf("an unauthenticated caller got %q: %s", leaked, b)
		}
	}

	// The metrics role sees the whole report -- the same class of information
	// as /metrics, which that role already scrapes.
	resp = do(t, ts, http.MethodGet, "/-/ready?format=json", tokAdmin, nil, "")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var full map[string]any
	if err := json.Unmarshal(b, &full); err != nil {
		t.Fatalf("not JSON: %s", b)
	}
	if _, ok := full["uptime_seconds"]; !ok {
		t.Errorf("an authorized caller does not get the full report: %s", b)
	}
}

// Every state that can occur is reachable, and no state that cannot occur is
// defined. A state that never fires is the dead readiness arm this campaign
// has already removed twice.
func TestEverySeverityIsDistinct(t *testing.T) {
	states := []HealthState{
		StateReady, StateDiskLow, StateClusterIncomplete,
		StateStorageDegraded, StateDiskFull, StateShuttingDown,
	}
	seen := map[int]HealthState{}
	for _, st := range states {
		sev := st.severity()
		if other, dup := seen[sev]; dup {
			t.Errorf("%q and %q share severity %d, so which one is reported is arbitrary",
				st, other, sev)
		}
		seen[sev] = st
	}
	// And the ordering is the one an operator acts on.
	if StateDiskFull.severity() <= StateDiskLow.severity() {
		t.Error("disk_full does not outrank disk_low")
	}
	if StateShuttingDown.severity() <= StateDiskFull.severity() {
		t.Error("shutting_down does not outrank every store condition")
	}
}
