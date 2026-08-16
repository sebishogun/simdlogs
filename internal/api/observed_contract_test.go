package api

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// What a refusal is allowed to say, and to whom.
//
// Each test here is a claim a previous commit made and did not gate. A review
// found five of seven such claims unguarded, and the reason they were wrong in
// the first place is that nothing asserted them.

// A refused syslog message is counted by the storage rejection counters and by
// NOTHING else.
//
// It used to also call countRows(0, 1, n), which adds n to
// vl_bytes_ingested_total ("Bytes of log data ingested") for bytes that were
// never ingested and 1 to vl_rows_dropped_total ("Log entries rejected as
// malformed") for a well-formed message, and to bump nHTTPErrs, charging a UDP
// datagram to vl_http_errors_total. The HTTP path counts none of those for the
// identical event, so the transports disagreed about it.
func TestARefusedSyslogMessageIsCountedOnceAndCorrectly(t *testing.T) {
	srv, tcpAddr, _ := budgetedSyslogServer(t, SyslogConfig{FlushLines: 1})

	before := metricsSnapshot(t, srv)
	disk0, _ := storage.RejectedWrites()

	c, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte(msg5424 + "\n")); err != nil {
		t.Fatal(err)
	}
	c.(*net.TCPConn).CloseWrite()
	buf := make([]byte, 1)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	c.Read(buf)
	c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d, _ := storage.RejectedWrites(); d > disk0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if d, _ := storage.RejectedWrites(); d == disk0 {
		t.Fatal("the refusal was not counted at all")
	}

	after := metricsSnapshot(t, srv)
	for _, name := range []string{
		"vl_bytes_ingested_total",
		"vl_rows_dropped_total",
		"vl_http_errors_total",
	} {
		if after[name] != before[name] {
			t.Errorf("%s moved %d -> %d for a refused syslog message; the HTTP path "+
				"moves none of these for the same event",
				name, before[name], after[name])
		}
	}
}

// One tenant tripping two budgets is one line that names BOTH, with the cause
// that actually refused first.
func TestOnePressureLineNamesEveryCauseRejectingFirst(t *testing.T) {
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.def.store.SetQuota(storage.QuotaConfig{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100, MaxTenantBytes: 1,
	}); err != nil {
		t.Fatal(err)
	}
	srv.def.w.Add(1, map[string]string{"_msg": "padding padding padding"})
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	lines := srv.storagePressureForTest()
	if len(lines) != 1 {
		t.Fatalf("%d lines for one tenant: %v", len(lines), lines)
	}
	line := lines[0]
	reject := strings.Index(line, "reject reserve")
	quota := strings.Index(line, "at its quota")
	if reject < 0 || quota < 0 {
		t.Fatalf("the line does not name both budgets: %q", line)
	}
	if reject > quota {
		t.Errorf("the non-rejecting cause is listed first: %q", line)
	}
}

// The state word says whether writes are REJECTED or merely degraded. A tenant
// refused by the reject reserve reporting as "degraded" understates an outage.
func TestTheStateWordDistinguishesRejectionFromDegradation(t *testing.T) {
	// Refusing.
	restore := storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 0}, nil
	})
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.def.store.SetQuota(storage.QuotaConfig{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100,
	}); err != nil {
		t.Fatal(err)
	}
	lines := srv.storagePressureForTest()
	if len(lines) != 1 || !strings.Contains(lines[0], "writes REJECTED") {
		t.Fatalf("a refused tenant does not say so: %v", lines)
	}
	srv.Close()
	restore()

	// Degraded but accepting.
	defer storage.SetDiskUsageForTest(func(string) (storage.DiskUsage, error) {
		return storage.DiskUsage{Total: 1 << 30, Free: 500}, nil
	})()
	srv2, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	if err := srv2.def.store.SetQuota(storage.QuotaConfig{
		ReserveWarnBytes: 1000, ReserveRejectBytes: 100,
	}); err != nil {
		t.Fatal(err)
	}
	lines = srv2.storagePressureForTest()
	if len(lines) != 1 {
		t.Fatalf("%v", lines)
	}
	if strings.Contains(lines[0], "REJECTED") {
		t.Errorf("a tenant that is still accepting writes reports them as refused: %q", lines[0])
	}
	if !strings.Contains(lines[0], "degraded") {
		t.Errorf("a degraded tenant does not say so: %q", lines[0])
	}
}

// A storage failure tells the client the CLASS and keeps the server's own
// message -- which names the data directory's absolute path -- in the log.
func TestAStorageFailureDoesNotLeakTheServersPath(t *testing.T) {
	dir := t.TempDir()
	c := config.Config{Dir: dir, Limits: config.DefaultLimits()}
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := newTestServer(t, srv)

	if err := chmodReadOnly(dir); err != nil {
		t.Skipf("cannot make the directory read-only here: %v", err)
	}
	t.Cleanup(func() { chmodWritable(dir) })

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(`{"_time":1,"_msg":"x"}`+"\n"))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("AccountID", "91")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := readAllString(t, resp)

	if strings.Contains(body, dir) {
		t.Fatalf("the response body carries the server's data directory: %q", body)
	}
	if !strings.Contains(body, "storage") {
		t.Errorf("the client is not told what class of failure this is: %q", body)
	}
}

// --- helpers -------------------------------------------------------------

func newTestServer(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func readAllString(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func chmodReadOnly(dir string) error { return os.Chmod(dir, 0o555) }
func chmodWritable(dir string) error { return os.Chmod(dir, 0o755) }

// metricsSnapshot scrapes /metrics off the server directly and returns the
// scalar value of every series, so a test can diff two moments.
func metricsSnapshot(t *testing.T, srv *Server) map[string]int64 {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	out := map[string]int64{}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			continue
		}
		out[name] = n
	}
	return out
}
