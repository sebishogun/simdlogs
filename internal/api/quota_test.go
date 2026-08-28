package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/storage"
)

func quotaServer(t *testing.T, st config.Storage) *httptest.Server {
	t.Helper()
	c := config.Config{Dir: t.TempDir(), Storage: st}
	c.Limits = config.DefaultLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// postLine posts one NDJSON line and returns the status and body.
func postLine(t *testing.T, ts *httptest.Server, line string) (int, string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(line+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// A full disk refuses writes with 507 and keeps queries, readiness and
// /metrics answering. 507 rather than 503, because an agent that retries a 503
// forever against a full disk is a busy loop and the condition is about
// storage rather than the server being down.
func TestFullDiskRefusesWritesAndKeepsReading(t *testing.T) {
	// The filesystem is full before the server takes its first sample.
	//
	// Not "write with room, then flip to full and write again": the free-space
	// sample is cached for a couple of seconds so a burst of small writes does
	// not become a burst of statfs calls, and that staleness is deliberate --
	// it is the reason the threshold is a RESERVE, with room to be wrong by
	// one interval's worth of writes. A test that flipped the disk and
	// expected the very next request to see it would be asserting the cache
	// away. The transition is covered at the storage layer, where the sample
	// can be expired the way time expires it.
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})

	code, body := postLine(t, ts, `{"_time":2,"_msg":"after"}`)
	if code != http.StatusInsufficientStorage {
		t.Fatalf("a write on a full disk returned %d (%s), want 507", code, body)
	}
	if !strings.Contains(body, "reserve") {
		t.Errorf("the refusal does not say why: %q", body)
	}

	// Reads keep working: this is the state an operator has to diagnose from.
	if code, _ := get(t, ts, "/select/logsql/query?query=*"); code != 200 {
		t.Errorf("a query on a full disk returned %d", code)
	}
	if code, _ := get(t, ts, "/metrics"); code != 200 {
		t.Errorf("/metrics on a full disk returned %d", code)
	}
	// And readiness has gone red, which is what an operator is paged on.
	if code, body := get(t, ts, "/-/ready"); code != http.StatusServiceUnavailable {
		t.Errorf("/-/ready returned %d (%s), want 503", code, body)
	}
}

func TestARejectedWriteIDIsNotCommitted(t *testing.T) {
	post := func(t *testing.T, s *Server, id, body string) *http.Response {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set(HdrWriteID, id)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec.Result()
	}

	t.Run("storage refusal", func(t *testing.T) {
		restore := storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
			return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
		})
		defer restore()

		c := config.Config{
			Dir:     t.TempDir(),
			Limits:  config.DefaultLimits(),
			Storage: config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100},
		}
		s, err := NewServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		const id = "aaaabbbbccccdddd"
		resp := post(t, s, id, `{"_msg":"refused"}`+"\n")
		resp.Body.Close()
		if resp.StatusCode != http.StatusInsufficientStorage {
			t.Fatalf("status %d, want 507", resp.StatusCode)
		}
		if s.def.store.CommittedWrite(storage.WriteID(id)) {
			t.Fatal("a storage-refused write id was committed")
		}
	})

	t.Run("parse failure", func(t *testing.T) {
		s := newWriteFailServer(t)
		const id = "1111222233334444"
		req := httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/logs",
			strings.NewReader("{"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(HdrWriteID, id)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		resp := rec.Result()
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", resp.StatusCode)
		}
		if s.def.store.CommittedWrite(storage.WriteID(id)) {
			t.Fatal("a rejected payload's write id was committed")
		}
	})
}

// Readiness degrades at the WARN level, while writes are still accepted. A
// probe that only went red once writes failed would report the outage rather
// than give anyone a chance to prevent it.
func TestReadinessDegradesBeforeWritesFail(t *testing.T) {
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 500}, nil // between the two
	})()

	if code, body := postLine(t, ts, `{"_time":1,"_msg":"still accepted"}`); code >= 300 {
		t.Fatalf("a write between the thresholds returned %d (%s); it must still be accepted",
			code, body)
	}
	code, body := get(t, ts, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready returned %d (%s) at the warn level, want 503", code, body)
	}
	if !strings.Contains(body, "storage pressure") {
		t.Errorf("the readiness body does not name the cause: %q", body)
	}
}

// The metrics an operator alerts on exist and move.
func TestQuotaMetricsAreExposed(t *testing.T) {
	ts := quotaServer(t, config.Storage{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	// One refused write, so the counter has something to show.
	if code, _ := postLine(t, ts, `{"_time":1,"_msg":"x"}`); code != http.StatusInsufficientStorage {
		t.Fatalf("the write returned %d, want 507", code)
	}
	_, body := get(t, ts, "/metrics")
	for _, want := range []string{
		"simdlogs_storage_capacity_bytes",
		"simdlogs_storage_warn_tenants",
		"simdlogs_storage_reject_tenants",
		"simdlogs_storage_over_quota_tenants",
		"simdlogs_writes_rejected_disk_total",
		"simdlogs_writes_rejected_quota_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not expose %s", want)
		}
	}
	if !strings.Contains(body, "simdlogs_storage_reject_tenants 1") {
		t.Errorf("the reject gauge does not show the tenant under pressure:\n%s", body)
	}
}

// A budget whose reject level is not below its warn level refuses to start the
// server, rather than failing the first tenant to arrive an hour later.
func TestAnInvalidBudgetRefusesToStart(t *testing.T) {
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	c.Storage = config.Storage{ReserveWarnBytes: 10, ReserveRejectBytes: 100}
	if _, err := NewServerConfig(c); err == nil {
		t.Fatal("a reject reserve above the warn reserve started")
	}
}

// One tenant under pressure is ONE tenant, however many budgets it trips.
//
// storagePressureForTest returns one string per finding and readiness prints
// `len(pressure)` as "N tenant(s) under storage pressure". Splitting the
// per-tenant switch into three independent ifs -- to fix a dead arm that could
// never print -- made one tenant tripping the reject reserve, the warn reserve
// and its quota report as three tenants. The count is the number an operator
// is paged on.
func TestReadinessCountsTenantsNotFindings(t *testing.T) {
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	ts := quotaServer(t, config.Storage{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100, MaxTenantBytes: 1,
	})
	// A write to open the default tenant and put bytes in it.
	postLine(t, ts, `{"_time":2,"_msg":"x"}`)

	code, body := get(t, ts, "/-/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/-/ready returned %d (%s), want 503", code, body)
	}
	if !strings.Contains(body, "1 tenant(s) under storage pressure") {
		t.Fatalf("the tenant count is wrong:\n%s", body)
	}
	// The line names the budget that refused. Both causes on one line is
	// covered below, where the samples can be expired the way time expires
	// them -- rows are buffered in the writer until a flush, so the tenant's
	// own size is legitimately 0 here and asserting the quota cause at this
	// layer would be asserting the flush schedule rather than the reporting.
	if !strings.Contains(body, "reject reserve") {
		t.Errorf("the line does not name the budget:\n%s", body)
	}
	if n := strings.Count(body, "0:0:"); n != 1 {
		t.Errorf("%d lines for one tenant:\n%s", n, body)
	}
}
