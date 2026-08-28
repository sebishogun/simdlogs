package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

// Every route that can return or accept tenant data must refuse an
// unauthenticated caller.
//
// This enumerates the surface rather than listing routes by hand, because the
// hand-written matrix is exactly what missed /_search and /_count: both were
// registered bare, so an anonymous POST /_search returned another tenant's
// _source documents, and both also skipped the method guard, the media-type
// allowlist and the body limit.
func TestNoUnauthenticatedDataRoute(t *testing.T) {
	t.Parallel()
	srv, ts := authedServer(t)

	// Routes that are open by design, with the reason each one is.
	open := map[string]string{
		"/health":       "liveness: a probe carries no credential",
		"/-/healthy":    "liveness",
		"/-/ready":      "liveness",
		"/insert/ready": "liveness",
	}

	// The real mux, not a list: enumerating by hand is exactly what missed
	// /_search and /_count.
	paths := srv.registeredPaths()
	// The floor is the real registered count, not a round number: a fixed 30
	// would not notice eleven routes vanishing from a mux of 41.
	if len(paths) != srv.routeCountForTest() {
		t.Fatalf("enumerated %d routes, mux registered %d", len(paths), srv.routeCountForTest())
	}
	if len(paths) < 40 {
		t.Fatalf("only %d routes enumerated; the audit is not seeing the whole mux", len(paths))
	}
	for _, path := range paths {
		if reason, ok := open[path]; ok {
			resp := do(t, ts, http.MethodGet, path, "", nil, "")
			resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				t.Errorf("%s -> 401 but is open by design (%s)", path, reason)
			}
			continue
		}
		// Every method, not just GET and POST: a route that only answers PUT
		// or DELETE would otherwise pass the audit without ever being asked.
		for _, method := range []string{
			http.MethodGet, http.MethodPost, http.MethodPut,
			http.MethodDelete, http.MethodPatch,
		} {
			body := ""
			if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
				body = `{"_time":1700000000000000000,"level":"info"}` + "\n"
			}
			resp := do(t, ts, method, path+"?query=*", "", nil, body)
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusMethodNotAllowed, http.StatusNotFound:
				// Refused, or the method is not offered at all.
			default:
				t.Errorf("%s %s answered %d with no credential; every data route must refuse one",
					method, path, resp.StatusCode)
			}
		}
	}
}

// The Elasticsearch read surface is the one that leaked. Pin it directly, with
// data actually present, so a regression is unambiguous.
func TestESReadSurfaceRequiresAuth(t *testing.T) {
	srv, ts := authedServer(t)

	// Ingest one record as an authorized principal.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil,
		`{"_time":1700000000000000000,"level":"error","_msg":"top-secret-payload"}`+"\n")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest -> %d", resp.StatusCode)
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/_search", "/_count"} {
		resp := do(t, ts, http.MethodPost, path, "", nil, `{"query":{"match_all":{}}}`)
		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("anonymous POST %s -> %d, want 401", path, resp.StatusCode)
		}
		if strings.Contains(string(body[:n]), "top-secret-payload") {
			t.Errorf("anonymous POST %s returned tenant data", path)
		}
	}
}

// Router mode forwards writes before the mux, so none of the per-route
// wrappers run. Unguarded it was an unauthenticated, unbounded ingest proxy.
func TestClusterForwardingIsGuarded(t *testing.T) {
	t.Parallel()
	// A backend that records what it receives.
	var got struct {
		calls int
		bytes int
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<20)
		n := 0
		for {
			m, err := r.Body.Read(buf)
			n += m
			if err != nil {
				break
			}
		}
		got.calls++
		got.bytes += n
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.SetAuth(&config.AuthConfig{Tokens: []config.TokenSpec{
		{SHA256: config.HashToken(tokIngest), Subject: "shipper", Roles: []string{"ingest"}, Tenants: []string{"*"}},
	}}); err != nil {
		t.Fatal(err)
	}
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	line := `{"_time":1700000000000000000,"level":"info"}` + "\n"

	// Anonymous: refused, and nothing reaches the backend.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous forwarded write -> %d, want 401", resp.StatusCode)
	}
	if got.calls != 0 {
		t.Fatalf("an anonymous write reached the backend (%d calls)", got.calls)
	}

	// Oversized: refused on size, not relayed.
	big := strings.Repeat(line, int(c.Limits.MaxBodyBytes/int64(len(line)))+64)
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil, big)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized forwarded write -> %d, want 413", resp.StatusCode)
	}

	// A wrong method is not relayed either.
	resp = do(t, ts, http.MethodDelete, "/insert/jsonline", tokIngest, nil, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE forwarded write -> %d, want 405", resp.StatusCode)
	}

	// Authorized and within limits: forwarded.
	before := got.calls
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil, line)
	resp.Body.Close()
	if got.calls != before+1 {
		t.Fatalf("an authorized write was not forwarded (%d calls)", got.calls)
	}
}

// A request after shutdown must not open a new tenant store, which would take
// a directory lock and start a writer pool nothing will ever close.
func TestNoTenantCreationAfterShutdown(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=*", "",
		map[string]string{"AccountID": "99"}, "")
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("a query after shutdown succeeded and opened tenant 99")
	}

	srv.mu.Lock()
	n := len(srv.tenants)
	srv.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d tenants open after Close", n)
	}
}

// Writing to a closed writer must return an error rather than panicking on a
// closed channel -- which used to unwind past the mutex unlock and deadlock
// every later Close permanently.
func TestWriteAfterCloseDoesNotDeadlock(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// The request must not panic the handler.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
		`{"_time":1700000000000000000,"level":"info"}`+"\n")
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Errorf("an ingest after shutdown reported success")
	}

	// And a second Close must return rather than block forever. The test's
	// own deadline is the assertion: a deadlocked Close never sends.
	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked after a write raced shutdown")
	}
}
