package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Role names a capability. Roles are coarse on purpose: a log database has
// four things a caller can be trusted to do, and a finer scheme would be
// guessing at distinctions no deployment has asked for.
type Role string

const (
	RoleIngest  Role = "ingest"
	RoleQuery   Role = "query"
	RoleAdmin   Role = "admin"
	RoleMetrics Role = "metrics"
)

// AllRoles is every role, for validation and for the admin grant.
func AllRoles() []Role { return []Role{RoleIngest, RoleQuery, RoleAdmin, RoleMetrics} }

func validRole(r Role) bool {
	for _, a := range AllRoles() {
		if a == r {
			return true
		}
	}
	return false
}

// TenantKey identifies one tenant's store.
type TenantKey struct {
	Account string
	Project string
}

func (t TenantKey) String() string { return t.Account + ":" + t.Project }

// ParseTenantKey parses "account:project". Both parts must be numeric, which
// is what the storage path is built from.
func ParseTenantKey(s string) (TenantKey, error) {
	acc, proj, ok := strings.Cut(s, ":")
	if !ok {
		return TenantKey{}, fmt.Errorf("config: tenant %q is not account:project", s)
	}
	if err := checkNumeric("account", acc); err != nil {
		return TenantKey{}, err
	}
	if err := checkNumeric("project", proj); err != nil {
		return TenantKey{}, err
	}
	// Canonical decimal, not the raw string. ParseUint accepts leading
	// zeros, and the key is what names both the map entry and the directory
	// -- so "7", "07" and "007" were three separate stores with three
	// directories and mutually invisible data, all reachable by varying one
	// request header.
	return TenantKey{Account: canonNum(acc), Project: canonNum(proj)}, nil
}

// canonNum renders a validated numeric id in canonical decimal.
func canonNum(v string) string {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return v // checkNumeric already rejected it; do not mask that
	}
	return strconv.FormatUint(n, 10)
}

func checkNumeric(what, v string) error {
	if v == "" {
		return fmt.Errorf("config: %s is empty", what)
	}
	if _, err := strconv.ParseUint(v, 10, 64); err != nil {
		return fmt.Errorf("config: %s %q is not a number", what, v)
	}
	return nil
}

// Principal is an authenticated caller.
type Principal struct {
	Subject string
	Roles   map[Role]bool
	// Tenants the principal may act on. An empty map with AllTenants false
	// means the principal has no tenant access at all.
	Tenants    map[TenantKey]bool
	AllTenants bool
}

// Can reports whether the principal holds a role.
func (p *Principal) Can(r Role) bool {
	if p == nil {
		return false
	}
	return p.Roles[r] || p.Roles[RoleAdmin]
}

// CanTenant reports whether the principal may act on a tenant.
func (p *Principal) CanTenant(t TenantKey) bool {
	if p == nil {
		return false
	}
	return p.AllTenants || p.Tenants[t]
}

// DefaultTenant is the tenant a principal gets when it names none.
func (p *Principal) DefaultTenant() (TenantKey, bool) {
	if p == nil {
		return TenantKey{}, false
	}
	if p.AllTenants {
		return TenantKey{Account: "0", Project: "0"}, true
	}
	if len(p.Tenants) == 1 {
		for t := range p.Tenants {
			return t, true
		}
	}
	return TenantKey{}, false
}

// CertSpec maps an mTLS client-certificate subject to a principal. It is how
// a verified certificate becomes an identity rather than only a transport
// gate: without it, RequireAndVerifyClientCert proves the client is trusted
// by the CA and then discards who they are.
type CertSpec struct {
	// CommonName matches the client certificate's subject CN exactly.
	CommonName string   `json:"commonName"`
	Subject    string   `json:"subject"`
	Roles      []string `json:"roles"`
	Tenants    []string `json:"tenants"`
}

// TokenSpec is one credential in the auth file.
type TokenSpec struct {
	// SHA256 is the hex-encoded SHA-256 of the bearer token. The token
	// itself is never stored: an auth file that leaks should not be a set of
	// working credentials.
	SHA256  string   `json:"sha256"`
	Subject string   `json:"subject"`
	Roles   []string `json:"roles"`
	// Tenants this token may use, as "account:project". "*" means every
	// tenant, which is what a single-tenant deployment wants.
	Tenants []string `json:"tenants"`
}

// AuthConfig is the parsed auth file.
type AuthConfig struct {
	// Disabled turns authentication off entirely. It is explicit and loud
	// because the alternative -- an empty token list meaning "no auth" --
	// makes a misplaced config file silently open the server to everyone.
	Disabled bool        `json:"disabled"`
	Tokens   []TokenSpec `json:"tokens"`
	// Certs maps mTLS client certificates to principals.
	Certs []CertSpec `json:"certs"`
	// TrustedProxy declares that a terminating proxy in front of this server
	// authenticates callers and sets the tenant headers.
	//
	// When set, a request arriving with no credential of its own is given the
	// principal named by ProxyRoles/ProxyTenants -- the proxy has already
	// decided who it is. It is a deployment assertion, and naming it is the
	// point: the previous behaviour was to believe the headers always.
	//
	// Bind loopback or a private interface when using this. A trusted-proxy
	// server reachable directly is an unauthenticated server.
	TrustedProxy bool `json:"trustedProxy"`
	// ProxyRoles and ProxyTenants are what a proxy-authenticated request may
	// do. Both are REQUIRED when TrustedProxy is set; there is no default.
	//
	// They used to default to every role and every tenant, which made
	// omitting a credential strictly more powerful than presenting one: a
	// least-privilege token got 403 on /admin/backup while an anonymous
	// request got 200 and a tar of the whole store. A default that outranks
	// every configured credential is not a default, it is a bypass.
	ProxyRoles   []string `json:"proxyRoles"`
	ProxyTenants []string `json:"proxyTenants"`
}

// LoadAuth reads and validates an auth file.
func LoadAuth(path string) (*AuthConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var a AuthConfig
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields() // a typo'd key must not silently do nothing
	if err := dec.Decode(&a); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	if err := a.Validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return &a, nil
}

// Validate checks every token entry.
func (a *AuthConfig) Validate() error {
	if a.Disabled {
		return nil
	}
	if len(a.Tokens) == 0 && len(a.Certs) == 0 && !a.TrustedProxy {
		return fmt.Errorf("no tokens, certs or trustedProxy: set disabled:true to run without authentication")
	}
	for i, c := range a.Certs {
		if c.CommonName == "" {
			return fmt.Errorf("cert %d: no commonName", i)
		}
		if c.Subject == "" {
			return fmt.Errorf("cert %d (%s): no subject", i, c.CommonName)
		}
		if len(c.Roles) == 0 {
			return fmt.Errorf("cert %d (%s): no roles", i, c.CommonName)
		}
		for _, r := range c.Roles {
			if !validRole(Role(r)) {
				return fmt.Errorf("cert %d (%s): unknown role %q", i, c.CommonName, r)
			}
		}
		if len(c.Tenants) == 0 {
			return fmt.Errorf("cert %d (%s): no tenants; use \"*\" for all", i, c.CommonName)
		}
		for _, tn := range c.Tenants {
			if tn == "*" {
				continue
			}
			if _, err := ParseTenantKey(tn); err != nil {
				return fmt.Errorf("cert %d (%s): %w", i, c.CommonName, err)
			}
		}
	}
	if a.TrustedProxy {
		if len(a.ProxyRoles) == 0 {
			return fmt.Errorf("trustedProxy needs proxyRoles: an unauthenticated request must not " +
				"get more than a configured token does")
		}
		if len(a.ProxyTenants) == 0 {
			return fmt.Errorf("trustedProxy needs proxyTenants: use [\"*\"] only if the proxy " +
				"really does authorize every tenant")
		}
		if len(a.Tokens) > 0 || len(a.Certs) > 0 {
			// Both configured is legitimate -- a proxy for some clients,
			// direct credentials for others -- but the proxy identity must
			// not exceed what a credential can get, or omitting the
			// credential is the privilege escalation.
			for _, r := range a.ProxyRoles {
				if Role(r) == RoleAdmin {
					return fmt.Errorf("trustedProxy grants the admin role while tokens or certs are " +
						"also configured: an unauthenticated request would outrank every credential")
				}
			}
		}
	}
	for _, r := range a.ProxyRoles {
		if !validRole(Role(r)) {
			return fmt.Errorf("proxyRoles: unknown role %q", r)
		}
	}
	for _, tn := range a.ProxyTenants {
		if tn == "*" {
			continue
		}
		if _, err := ParseTenantKey(tn); err != nil {
			return fmt.Errorf("proxyTenants: %w", err)
		}
	}
	seen := map[string]bool{}
	for i, t := range a.Tokens {
		h := strings.ToLower(strings.TrimSpace(t.SHA256))
		if len(h) != 64 {
			return fmt.Errorf("token %d: sha256 must be 64 hex characters, got %d", i, len(h))
		}
		if _, err := hex.DecodeString(h); err != nil {
			return fmt.Errorf("token %d: sha256 is not hex: %w", i, err)
		}
		if seen[h] {
			return fmt.Errorf("token %d: duplicate sha256", i)
		}
		seen[h] = true
		if t.Subject == "" {
			return fmt.Errorf("token %d: no subject", i)
		}
		if len(t.Roles) == 0 {
			return fmt.Errorf("token %d (%s): no roles", i, t.Subject)
		}
		for _, r := range t.Roles {
			if !validRole(Role(r)) {
				return fmt.Errorf("token %d (%s): unknown role %q", i, t.Subject, r)
			}
		}
		if len(t.Tenants) == 0 {
			return fmt.Errorf("token %d (%s): no tenants; use \"*\" for all", i, t.Subject)
		}
		for _, tn := range t.Tenants {
			if tn == "*" {
				continue
			}
			if _, err := ParseTenantKey(tn); err != nil {
				return fmt.Errorf("token %d (%s): %w", i, t.Subject, err)
			}
		}
	}
	return nil
}

// Principals builds the lookup table: hashed token to principal.
func (a *AuthConfig) Principals() (map[string]*Principal, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]*Principal, len(a.Tokens))
	for _, t := range a.Tokens {
		p := &Principal{Subject: t.Subject, Roles: map[Role]bool{}, Tenants: map[TenantKey]bool{}}
		for _, r := range t.Roles {
			p.Roles[Role(r)] = true
		}
		for _, tn := range t.Tenants {
			if tn == "*" {
				p.AllTenants = true
				continue
			}
			k, err := ParseTenantKey(tn)
			if err != nil {
				return nil, err
			}
			p.Tenants[k] = true
		}
		out[strings.ToLower(strings.TrimSpace(t.SHA256))] = p
	}
	return out, nil
}

// CertPrincipals maps client-certificate common names to principals.
func (a *AuthConfig) CertPrincipals() (map[string]*Principal, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]*Principal, len(a.Certs))
	for _, c := range a.Certs {
		p, err := buildPrincipal(c.Subject, c.Roles, c.Tenants)
		if err != nil {
			return nil, err
		}
		out[c.CommonName] = p
	}
	return out, nil
}

// ProxyPrincipal is the identity a proxy-authenticated request runs as, or
// nil when TrustedProxy is off.
func (a *AuthConfig) ProxyPrincipal() (*Principal, error) {
	if !a.TrustedProxy {
		return nil, nil
	}
	// No defaulting: Validate requires both to be set.
	return buildPrincipal("proxy", a.ProxyRoles, a.ProxyTenants)
}

func buildPrincipal(subject string, roles, tenants []string) (*Principal, error) {
	p := &Principal{Subject: subject, Roles: map[Role]bool{}, Tenants: map[TenantKey]bool{}}
	for _, r := range roles {
		p.Roles[Role(r)] = true
	}
	for _, tn := range tenants {
		if tn == "*" {
			p.AllTenants = true
			continue
		}
		k, err := ParseTenantKey(tn)
		if err != nil {
			return nil, err
		}
		p.Tenants[k] = true
	}
	return p, nil
}

// HashToken is the hex SHA-256 of a bearer token, the form the auth file
// stores.
func HashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}
