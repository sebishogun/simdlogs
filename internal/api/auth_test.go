package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
)

const (
	tokIngest = "ingest-token-value"
	tokQuery  = "query-token-value"
	tokAdmin  = "admin-token-value"
	tokOther  = "other-tenant-token"
)

func authedServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	auth := &config.AuthConfig{Tokens: []config.TokenSpec{
		{SHA256: config.HashToken(tokIngest), Subject: "shipper", Roles: []string{"ingest"}, Tenants: []string{"0:0"}},
		{SHA256: config.HashToken(tokQuery), Subject: "grafana", Roles: []string{"query"}, Tenants: []string{"0:0"}},
		{SHA256: config.HashToken(tokAdmin), Subject: "ops", Roles: []string{"admin"}, Tenants: []string{"*"}},
		{SHA256: config.HashToken(tokOther), Subject: "tenant7", Roles: []string{"ingest", "query"}, Tenants: []string{"7:0"}},
	}}
	if err := srv.SetAuth(auth); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return srv, ts
}

func do(t *testing.T, ts *httptest.Server, method, path, token string, hdr map[string]string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// The route-permission matrix. Every registered endpoint has a role, and a
// caller without it is refused. Unwrapped, any client could query, ingest,
// read the flag dump and download a full backup.
func TestRoutePermissionMatrix(t *testing.T) {
	_, ts := authedServer(t)
	line := `{"_time":1700000000000000000,"level":"info"}` + "\n"

	for _, c := range []struct {
		path   string
		method string
		body   string
		// the role that should be accepted
		allowed string
	}{
		{"/insert/jsonline", http.MethodPost, line, "ingest"},
		{"/insert/logfmt", http.MethodPost, "level=info\n", "ingest"},
		{"/_bulk", http.MethodPost, `{"index":{}}` + "\n" + line, "ingest"},
		{"/loki/api/v1/push", http.MethodPost, `{"streams":[]}`, "ingest"},
		{"/select/logsql/query", http.MethodGet, "", "query"},
		{"/select/logsql/hits", http.MethodGet, "", "query"},
		{"/select/logsql/field_names", http.MethodGet, "", "query"},
		{"/admin/backup", http.MethodGet, "", "admin"},
		{"/flags", http.MethodGet, "", "admin"},
	} {
		// No credential at all is 401 with a challenge.
		resp := do(t, ts, c.method, c.path+"?query=*", "", nil, c.body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s %s unauthenticated -> %d, want 401", c.method, c.path, resp.StatusCode)
		}
		if resp.Header.Get("WWW-Authenticate") == "" {
			t.Errorf("%s %s: 401 without a WWW-Authenticate challenge", c.method, c.path)
		}

		// A valid credential lacking the role is 403.
		wrong := tokQuery
		if c.allowed == "query" {
			wrong = tokIngest
		}
		resp = do(t, ts, c.method, c.path+"?query=*", wrong, nil, c.body)
		resp.Body.Close()
		if c.allowed == "admin" {
			// Both non-admin tokens must be refused.
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s with a non-admin token -> %d, want 403", c.method, c.path, resp.StatusCode)
			}
		} else if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s with the wrong role -> %d, want 403", c.method, c.path, resp.StatusCode)
		}

		// The right role is not refused.
		right := map[string]string{"ingest": tokIngest, "query": tokQuery, "admin": tokAdmin}[c.allowed]
		resp = do(t, ts, c.method, c.path+"?query=*", right, nil, c.body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			t.Errorf("%s %s with the %s role -> %d, want it accepted", c.method, c.path, c.allowed, resp.StatusCode)
		}
	}
}

// Admin holds every role, so an operator token does not need four entries.
func TestAdminImpliesEveryRole(t *testing.T) {
	_, ts := authedServer(t)
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokAdmin, nil,
		`{"_time":1700000000000000000,"level":"info"}`+"\n")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin ingest -> %d, want 200", resp.StatusCode)
	}
}

// A tenant header is a request, not an identity. Previously any client could
// name any tenant and read or write its data.
func TestTenantHeaderRequiresAuthorization(t *testing.T) {
	_, ts := authedServer(t)
	line := `{"_time":1700000000000000000,"level":"info"}` + "\n"

	// tokIngest is scoped to 0:0 and must not reach tenant 7.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest,
		map[string]string{"AccountID": "7"}, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-tenant write -> %d, want 403", resp.StatusCode)
	}

	// Its own tenant works.
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tokIngest, nil, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("own-tenant write -> %d, want 200", resp.StatusCode)
	}

	// The token scoped to 7:0 can write there and not to 0:0.
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tokOther,
		map[string]string{"AccountID": "7"}, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant-7 write -> %d, want 200", resp.StatusCode)
	}
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tokOther,
		map[string]string{"AccountID": "0"}, line)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("tenant-7 token writing to tenant 0 -> %d, want 403", resp.StatusCode)
	}
}

// A malformed tenant id is rejected. It used to be rewritten to "0", so a
// typo silently wrote into the default tenant.
func TestMalformedTenantIsRejected(t *testing.T) {
	_, ts := authedServer(t)
	line := `{"_time":1700000000000000000,"level":"info"}` + "\n"
	for _, bad := range []string{"abc", "1x", "-1", "0.5", "../../etc"} {
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", tokAdmin,
			map[string]string{"AccountID": bad}, line)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("AccountID %q was accepted; a malformed id must not fall back to tenant 0", bad)
		}
	}
}

// An unknown token is 401 rather than being treated as anonymous.
func TestUnknownTokenIsRejected(t *testing.T) {
	_, ts := authedServer(t)
	resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=*", "not-a-real-token", nil, "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown token -> %d, want 401", resp.StatusCode)
	}
}

// Liveness stays reachable without credentials: a load balancer's health
// probe carries none, and an unauthenticated 401 there takes the node out of
// rotation.
func TestLivenessIsUnauthenticated(t *testing.T) {
	_, ts := authedServer(t)
	for _, p := range []string{"/health", "/-/healthy", "/insert/ready"} {
		resp := do(t, ts, http.MethodGet, p, "", nil, "")
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			t.Errorf("%s -> 401; a liveness probe carries no credential", p)
		}
	}
}

// With auth disabled the server behaves as before, so an existing deployment
// is not broken by upgrading.
func TestAuthDisabledKeepsOpenAccess(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.SetAuth(&config.AuthConfig{Disabled: true}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
		`{"_time":1700000000000000000,"level":"info"}`+"\n")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d with auth disabled, want 200", resp.StatusCode)
	}
}
