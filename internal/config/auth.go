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
	return TenantKey{Account: acc, Project: proj}, nil
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
	// TrustedProxy declares that a terminating proxy in front of this server
	// authenticates callers and sets the tenant headers. When set, tenant
	// headers are believed. It is a deployment assertion, and naming it is
	// the point: the previous behaviour was to believe them always.
	TrustedProxy bool `json:"trustedProxy"`
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
	if len(a.Tokens) == 0 && !a.TrustedProxy {
		return fmt.Errorf("no tokens and no trustedProxy: set disabled:true to run without authentication")
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
		out[strings.ToLower(t.SHA256)] = p
	}
	return out, nil
}

// HashToken is the hex SHA-256 of a bearer token, the form the auth file
// stores.
func HashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}
