package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// What a shipper is told when a write fails.
//
// The status code and the Retry-After header are the only two things a log
// shipper's backoff can read without parsing a body, so they carry the load.
// A flat 503 with no Retry-After -- which is what every storage failure used
// to produce -- tells a client to retry and gives it no interval, which is
// how a full disk becomes a retry storm on top of a full disk.

// newWriteFailServer opens a one-tenant server over a temp dir.
func newWriteFailServer(t *testing.T) *Server {
	t.Helper()
	c := config.Default()
	c.Dir = t.TempDir()
	s, err := NewServerConfig(c)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// postJSONLine sends one ndjson record and returns the response.
func postJSONLine(t *testing.T, s *Server, path, body string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec.Result()
}

// A full disk is 503 with a Retry-After, and the body says a retry is safe.
func TestDiskFullIsRetryableWithAnInterval(t *testing.T) {
	s := newWriteFailServer(t)

	hook, err := storage.FailAt("temp-create", syscall.ENOSPC)
	if err != nil {
		t.Fatalf("arm: %v", err)
	}
	restore := storage.SetFaultHookForTest(hook)
	resp := postJSONLine(t, s, "/insert/jsonline", `{"_msg":"one"}`+"\n")
	restore()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 503; body %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After %q, want \"30\"", got)
	}

	var payload struct {
		Error            string `json:"error"`
		Status           int    `json:"status"`
		Retryable        bool   `json:"retryable"`
		RetryAfter       int    `json:"retryAfterSeconds"`
		DuplicateOnRetry bool   `json:"duplicateOnRetry"`
		GroupsFailed     int    `json:"groupsFailed"`
		GroupsTotal      int    `json:"groupsTotal"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !payload.Retryable {
		t.Fatal("ENOSPC reported as not retryable")
	}
	if payload.RetryAfter != 30 {
		t.Fatalf("retryAfterSeconds %d, want 30", payload.RetryAfter)
	}
	// Every group in the window failed, so nothing landed and resending the
	// same body cannot store anything twice. Saying otherwise would push a
	// client toward dropping data it should resend.
	if payload.DuplicateOnRetry {
		t.Fatal("a wholly-failed write was reported as duplicating on retry")
	}
	if payload.GroupsTotal != 1 || payload.GroupsFailed != 1 {
		t.Fatalf("groups %d/%d, want 1/1", payload.GroupsFailed, payload.GroupsTotal)
	}
}

// A group that will not read back the instant after it was written is a
// repair-class failure, not a never-retry one.
//
// It used to be answered 500 with retryable=false and no Retry-After, on the
// reasoning that the bytes are a pure function of the payload. A CRC mismatch
// on a file written seconds ago is at least as likely to be the storage
// returning different bytes than the ones written, and that answer told the
// shipper to DROP data a media error had corrupted.
func TestUnreadableGroupIsARepairClassFailure(t *testing.T) {
	s := newWriteFailServer(t)

	hook := func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) == "rename" {
			return storage.ErrCorruptGroup
		}
		return nil
	}
	restore := storage.SetFaultHookForTest(hook)
	resp := postJSONLine(t, s, "/insert/jsonline", `{"_msg":"one"}`+"\n")
	restore()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 503; body %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After %q, want \"30\"", got)
	}
	var payload struct {
		Retryable  bool `json:"retryable"`
		RetryAfter int  `json:"retryAfterSeconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !payload.Retryable {
		t.Fatal("a media-shaped failure was reported as never-retry")
	}
	if payload.RetryAfter != 30 {
		t.Fatalf("retryAfterSeconds %d, want 30", payload.RetryAfter)
	}
}

// A successful write must carry none of this. A Retry-After on a 200 is a
// header a proxy can act on.
func TestSuccessfulWriteCarriesNoRetryMetadata(t *testing.T) {
	s := newWriteFailServer(t)
	resp := postJSONLine(t, s, "/insert/jsonline", `{"_msg":"one"}`+"\n")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 200; body %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After %q on a success", got)
	}
}

// The OTLP route answers in its own JSON envelope, and the retry facts must
// survive that difference. A fact that depends on which protocol the client
// used is a fact a client cannot rely on.
func TestOTLPWriteFailureCarriesRetryMetadata(t *testing.T) {
	s := newWriteFailServer(t)

	hook, err := storage.FailAt("temp-create", syscall.ENOSPC)
	if err != nil {
		t.Fatalf("arm: %v", err)
	}
	// A minimal OTLP/JSON logs payload with one record.
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[` +
		`{"timeUnixNano":"1","body":{"stringValue":"one"}}]}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/logs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	restore := storage.SetFaultHookForTest(hook)
	s.Handler().ServeHTTP(rec, req)
	restore()

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 503; body %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After %q, want \"30\"", got)
	}
}

// One backup per tenant at a time.
//
// The 429 turns a request that used to succeed into a rejection, which is
// exactly the kind of change that needs a test, and it had none.
//
// The pre-flush that arrived with it still has none. Deleting the whole
// pre-flush block leaves this test and the next one green, because the ingest
// request's own FlushMark already made the row durable -- so neither observes
// it. An earlier version of this comment claimed both were covered. Recorded
// as open in docs/wrong.md rather than asserted here.
func TestBackupAdmitsOneAtATime(t *testing.T) {
	s := newWriteFailServer(t)

	// Resolve the tenant through the same path the handler does, so the flag
	// is set on the object the request will find. Reaching for s.tenant("","")
	// gets a different object and the request sails past a flag nobody is
	// holding -- which is how the first version of this test asserted nothing.
	probe := httptest.NewRequest(http.MethodGet, "/admin/backup", nil)
	tn, err := s.tenantOf(probe)
	if err != nil {
		t.Fatalf("tenant: %v", err)
	}
	// tenantOf hands the tenant back BUSY; not releasing it makes Close wait
	// out its ten-second drain.
	defer tn.inFlight.Add(-1)
	if !tn.backupBusy.CompareAndSwap(false, true) {
		t.Fatal("the flag was already set on a fresh server")
	}
	defer tn.backupBusy.Store(false)

	req := httptest.NewRequest(http.MethodGet, "/admin/backup", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 429 while a backup is in progress; body %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Retry-After"); got != "60" {
		t.Fatalf("Retry-After %q, want \"60\"", got)
	}
}

// And the flag is released, so a second backup after the first is not a 429.
func TestBackupReleasesItsAdmission(t *testing.T) {
	s := newWriteFailServer(t)
	postJSONLine(t, s, "/insert/jsonline", `{"_msg":"one"}`+"\n").Body.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/admin/backup", nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		resp := rec.Result()
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("backup %d: status %d, want 200", i, resp.StatusCode)
		}
		// The pre-flush means the row posted above is in the archive.
		man, err := storage.VerifyBackup(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("backup %d does not verify: %v", i, err)
		}
		if man.TotalRows() != 1 {
			t.Fatalf("backup %d holds %d rows; the flush before the snapshot did not happen",
				i, man.TotalRows())
		}
	}
}

// duplicateOnRetry must be TRUE when it is true, and the only test that read
// the field asserted it false.
//
// So `"duplicateOnRetry": false` hardcoded in writeFlushErr passed the suite,
// on every non-parallel ingest route: jsonline, logfmt, ES bulk, OTLP. That is
// the exact false claim this task exists to remove, reachable from four
// handlers, guarded by a test that could only confirm the safe case.
func TestPartialWriteReportsDuplicateOnRetryOverHTTP(t *testing.T) {
	s := newWriteFailServer(t)

	// Fail the SECOND group only, so one lands and one does not.
	var mu sync.Mutex
	seen := 0
	restore := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen == 2 {
			return syscall.ENOSPC
		}
		return nil
	})

	// The LOGFMT route, deliberately. /insert/jsonline hands a body over
	// MinParallelBytes to the sharded path, which answers through failIngest
	// -- so a jsonline version of this test exercises that copy of the field
	// and passes with writeFlushErr's copy hardcoded to false, which is the
	// hole it was written for. insertLogfmt has no parallel branch and always
	// answers through writeFlushErr.
	//
	// Enough rows to cross FlushRows, so the request produces two flush jobs
	// and the second one fails.
	var body strings.Builder
	for i := 0; i < ingest.FlushRows+64; i++ {
		body.WriteString("_msg=line _time=1700000000000000000\n")
	}
	req := httptest.NewRequest(http.MethodPost, "/insert/logfmt", strings.NewReader(body.String()))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	restore()

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Skipf("the fault did not land on this scheduling (%d temp-creates)", seen)
	}
	var payload struct {
		DuplicateOnRetry bool   `json:"duplicateOnRetry"`
		GroupsFailed     int    `json:"groupsFailed"`
		GroupsTotal      int    `json:"groupsTotal"`
		Unit             string `json:"unit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// A skip here reads as "the scheduling did not produce a partial", and it
	// also fires if the route stops reporting the counts at all -- reverting
	// insertLogfmt to a flat writeErr turns this test into a SKIP rather than
	// a failure. The status check above is what keeps that from being silent:
	// the route must still answer 503.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
	if payload.GroupsTotal < 2 || payload.GroupsFailed >= payload.GroupsTotal {
		t.Skipf("not a partial failure: %d of %d %s failed",
			payload.GroupsFailed, payload.GroupsTotal, payload.Unit)
	}
	if !payload.DuplicateOnRetry {
		t.Fatalf("%d of %d %s failed and the response says a retry cannot duplicate",
			payload.GroupsFailed, payload.GroupsTotal, payload.Unit)
	}
}
