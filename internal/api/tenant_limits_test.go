package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

func limitedTenantServer(t *testing.T, maxTenants int) (*Server, *httptest.Server) {
	t.Helper()
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxOpenTenants = maxTenants
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return srv, ts
}

func postAs(t *testing.T, ts *httptest.Server, account string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(`{"_time":1700000000000000000,"level":"info"}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("AccountID", account)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// More tenants than the limit must not mean more open stores. Every tenant
// holds mmaps, file descriptors and a pool of flush goroutines, and the map
// was driven by a request header with nothing bounding it.
func TestTenantCountIsBounded(t *testing.T) {
	const max = 4
	srv, ts := limitedTenantServer(t, max)

	baseGoroutines := runtime.NumGoroutine()
	for i := 0; i < 40; i++ {
		resp := postAs(t, ts, fmt.Sprint(i))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tenant %d -> %d", i, resp.StatusCode)
		}
	}

	srv.mu.Lock()
	open := len(srv.tenants)
	srv.mu.Unlock()
	if open > max {
		t.Fatalf("%d tenants open, limit is %d", open, max)
	}
	if TenantsEvicted() == 0 {
		t.Error("40 tenants through a 4-slot limit evicted nothing")
	}

	// Goroutines must not grow with the number of tenants seen: each evicted
	// tenant's flush pool has to be stopped, not abandoned.
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		time.Sleep(2 * time.Millisecond)
	}
	if grew := runtime.NumGoroutine() - baseGoroutines; grew > max*8 {
		t.Fatalf("goroutines grew by %d across 40 tenants with a %d-slot limit", grew, max)
	}
}

// An evicted tenant's data survives: eviction closes the store, it does not
// delete it. Re-opening the tenant must find the rows.
func TestEvictedTenantDataSurvives(t *testing.T) {
	const max = 2
	srv, ts := limitedTenantServer(t, max)

	resp := postAs(t, ts, "1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first write -> %d", resp.StatusCode)
	}
	// Flush so the row is on disk before eviction.
	srv.mu.Lock()
	tn := srv.tenants["1:0"]
	srv.mu.Unlock()
	if tn == nil {
		t.Fatal("tenant 1:0 not open")
	}
	if err := tn.w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Push it out.
	for i := 10; i < 20; i++ {
		postAs(t, ts, fmt.Sprint(i)).Body.Close()
	}

	// Read it back: the tenant reopens from disk.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/query?query=*", nil)
	req.Header.Set("AccountID", "1")
	got, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("reopened tenant query -> %d", got.StatusCode)
	}
	buf := make([]byte, 4096)
	n, _ := got.Body.Read(buf)
	if n == 0 || !strings.Contains(string(buf[:n]), "info") {
		t.Fatalf("reopened tenant lost its data: %q", string(buf[:n]))
	}
}

// Eviction must never close a store a request is still using: that would
// unmap under a live query.
func TestEvictionSkipsBusyTenants(t *testing.T) {
	// Three, not two: the default tenant holds one slot for the life of the
	// process. It is never evicted (see TestDefaultTenantIsNeverEvicted), so
	// it counts against the limit like any other open store.
	const max = 3
	srv, _ := limitedTenantServer(t, max)

	// Open one tenant and mark it busy, as a live request would.
	// tenant() hands the tenant back already busy, so this one stays busy for
	// the whole test -- exactly as a live request would hold it.
	busy, err := srv.tenant("1", "0")
	if err != nil {
		t.Fatal(err)
	}
	// Release it at the end, or Close waits out its drain deadline.
	defer busy.inFlight.Add(-1)

	tn2, err := srv.tenant("2", "0")
	if err != nil {
		t.Fatal(err)
	}
	tn2.inFlight.Add(-1) // this one is idle again

	// The third needs a slot; only the idle one may go.
	tn3, err := srv.tenant("3", "0")
	if err != nil {
		t.Fatal(err)
	}
	tn3.inFlight.Add(-1)

	srv.mu.Lock()
	_, busyStillOpen := srv.tenants["1:0"]
	srv.mu.Unlock()
	if !busyStillOpen {
		t.Fatal("a tenant with a request in flight was evicted")
	}
}

// When every slot is busy the request is refused rather than given a slot
// that does not exist.
func TestAllTenantsBusyIsRejected(t *testing.T) {
	// The default holds one of the three; the other two are filled below.
	const max = 3
	srv, _ := limitedTenantServer(t, max)

	for i := 1; i < max; i++ {
		// Held busy by construction: tenant() returns them marked.
		tn, err := srv.tenant(fmt.Sprint(i), "0")
		if err != nil {
			t.Fatal(err)
		}
		defer tn.inFlight.Add(-1) // let Close finish without waiting out the drain
	}
	before := TenantsRejected()
	_, err := srv.tenant("99", "0")
	if err == nil {
		t.Fatal("a new tenant was opened past the limit with every slot busy")
	}
	if authStatus(err) != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", authStatus(err))
	}
	if TenantsRejected() <= before {
		t.Error("the rejection was not counted")
	}
}

// Routes that have nothing to do with tenant data must not create a tenant
// directory. A health probe from a load balancer would otherwise open a
// store on every poll.
func TestHealthDoesNotCreateTenants(t *testing.T) {
	srv, ts := limitedTenantServer(t, 4)

	before := 0
	srv.mu.Lock()
	before = len(srv.tenants)
	srv.mu.Unlock()

	for _, p := range []string{"/health", "/-/healthy", "/insert/ready"} {
		for i := 0; i < 5; i++ {
			req, _ := http.NewRequest(http.MethodGet, ts.URL+p, nil)
			req.Header.Set("AccountID", fmt.Sprint(100+i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		}
	}

	srv.mu.Lock()
	after := len(srv.tenants)
	srv.mu.Unlock()
	if after != before {
		t.Fatalf("health probes opened %d tenants", after-before)
	}
	// And no directories were created for them.
	ents, err := os.ReadDir(srv.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "tenant-10") {
			t.Fatalf("a health probe created %s", filepath.Join(srv.dir, e.Name()))
		}
	}
}

// Concurrent creation and eviction must not race or double-close. Run under
// -race.
func TestTenantChurnConcurrent(t *testing.T) {
	srv, ts := limitedTenantServer(t, 3)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 12; j++ {
				resp := postAs(t, ts, fmt.Sprint(base*100+j))
				resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()

	srv.mu.Lock()
	open := len(srv.tenants)
	srv.mu.Unlock()
	if open > 3 {
		t.Fatalf("%d tenants open under churn, limit is 3", open)
	}
}
