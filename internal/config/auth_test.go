package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeAuth(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// An auth file with no tokens is an error rather than an open server. An
// empty list meaning "no auth" would make a misplaced or truncated config
// file silently expose everything.
func TestAuthEmptyTokensIsAnError(t *testing.T) {
	_, err := LoadAuth(writeAuth(t, `{"tokens":[]}`))
	if err == nil {
		t.Fatal("an empty token list was accepted")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("error %q does not point at the explicit opt-out", err)
	}
}

// Running without authentication has to be stated.
func TestAuthDisabledIsExplicit(t *testing.T) {
	a, err := LoadAuth(writeAuth(t, `{"disabled":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if !a.Disabled {
		t.Fatal("disabled did not survive parsing")
	}
}

// A typo'd key is an error, not a silently ignored setting. "disable": true
// must not read as authentication being on with an unknown field.
func TestAuthRejectsUnknownFields(t *testing.T) {
	_, err := LoadAuth(writeAuth(t, `{"disable":true}`))
	if err == nil {
		t.Fatal("an unknown field was accepted")
	}
}

func TestAuthValidatesTokens(t *testing.T) {
	good := HashToken("secret")
	for _, c := range []struct {
		name string
		body string
		want string
	}{
		{"short hash", `{"tokens":[{"sha256":"abc","subject":"s","roles":["query"],"tenants":["0:0"]}]}`, "64 hex"},
		{"non-hex", `{"tokens":[{"sha256":"` + strings.Repeat("z", 64) + `","subject":"s","roles":["query"],"tenants":["0:0"]}]}`, "not hex"},
		{"no subject", `{"tokens":[{"sha256":"` + good + `","roles":["query"],"tenants":["0:0"]}]}`, "no subject"},
		{"no roles", `{"tokens":[{"sha256":"` + good + `","subject":"s","tenants":["0:0"]}]}`, "no roles"},
		{"bad role", `{"tokens":[{"sha256":"` + good + `","subject":"s","roles":["root"],"tenants":["0:0"]}]}`, "unknown role"},
		{"no tenants", `{"tokens":[{"sha256":"` + good + `","subject":"s","roles":["query"]}]}`, "no tenants"},
		{"bad tenant", `{"tokens":[{"sha256":"` + good + `","subject":"s","roles":["query"],"tenants":["abc:0"]}]}`, "not a number"},
		{"duplicate", `{"tokens":[
			{"sha256":"` + good + `","subject":"a","roles":["query"],"tenants":["0:0"]},
			{"sha256":"` + good + `","subject":"b","roles":["query"],"tenants":["0:0"]}]}`, "duplicate"},
	} {
		_, err := LoadAuth(writeAuth(t, c.body))
		if err == nil {
			t.Errorf("%s: accepted", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not mention %q", c.name, err, c.want)
		}
	}
}

// The token itself is never stored, only its hash: an auth file that leaks
// must not be a set of working credentials.
func TestPrincipalsAreKeyedByHash(t *testing.T) {
	a := &AuthConfig{Tokens: []TokenSpec{
		{SHA256: HashToken("s3cret"), Subject: "ops", Roles: []string{"admin"}, Tenants: []string{"*"}},
	}}
	ps, err := a.Principals()
	if err != nil {
		t.Fatal(err)
	}
	p, ok := ps[HashToken("s3cret")]
	if !ok {
		t.Fatal("principal not found by token hash")
	}
	if p.Subject != "ops" || !p.AllTenants {
		t.Fatalf("principal %+v", p)
	}
	if _, ok := ps["s3cret"]; ok {
		t.Fatal("the raw token is a key; the file must store only hashes")
	}
}

// Validation accepts surrounding whitespace and uppercase hex, so the lookup
// table must use that same normalized spelling. Otherwise startup succeeds but
// the credential can never authenticate.
func TestPrincipalHashUsesValidationNormalization(t *testing.T) {
	hash := HashToken("s3cret")
	a := &AuthConfig{Tokens: []TokenSpec{
		{SHA256: " \t" + strings.ToUpper(hash) + "\n", Subject: "ops", Roles: []string{"query"}, Tenants: []string{"0:0"}},
	}}
	ps, err := a.Principals()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ps[hash]; !ok {
		t.Fatal("the accepted hash spelling was not normalized for lookup")
	}
}

// Admin implies every role, so an operator credential does not need four
// entries that can drift apart.
func TestAdminImpliesAllRoles(t *testing.T) {
	p := &Principal{Roles: map[Role]bool{RoleAdmin: true}}
	for _, r := range AllRoles() {
		if !p.Can(r) {
			t.Errorf("admin cannot %s", r)
		}
	}
	q := &Principal{Roles: map[Role]bool{RoleQuery: true}}
	if q.Can(RoleIngest) {
		t.Error("a query-only principal can ingest")
	}
	if q.Can(RoleAdmin) {
		t.Error("a query-only principal is admin")
	}
}

func TestTenantScoping(t *testing.T) {
	p := &Principal{Tenants: map[TenantKey]bool{{Account: "7", Project: "0"}: true}}
	if !p.CanTenant(TenantKey{Account: "7", Project: "0"}) {
		t.Error("own tenant refused")
	}
	if p.CanTenant(TenantKey{Account: "0", Project: "0"}) {
		t.Error("other tenant allowed")
	}
	// A single-tenant principal gets that tenant by default; a multi-tenant
	// one must name it, or the server would pick for it.
	if tk, ok := p.DefaultTenant(); !ok || tk.Account != "7" {
		t.Errorf("DefaultTenant = %v, %v", tk, ok)
	}
	multi := &Principal{Tenants: map[TenantKey]bool{
		{Account: "1", Project: "0"}: true, {Account: "2", Project: "0"}: true,
	}}
	if _, ok := multi.DefaultTenant(); ok {
		t.Error("a multi-tenant principal got an implicit default tenant")
	}
}

func TestParseTenantKey(t *testing.T) {
	for _, c := range []struct {
		in string
		ok bool
	}{
		{"0:0", true}, {"12:34", true},
		{"0", false}, {"a:0", false}, {"0:b", false}, {":0", false},
		{"0:", false}, {"-1:0", false}, {"../x:0", false},
	} {
		_, err := ParseTenantKey(c.in)
		if (err == nil) != c.ok {
			t.Errorf("%q -> err %v, want ok=%v", c.in, err, c.ok)
		}
	}
}
