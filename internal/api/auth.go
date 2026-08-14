package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/sebishogun/simdlogs/internal/config"
)

type principalKey struct{}

// principalOf returns the authenticated caller, or nil when authentication is
// disabled.
func principalOf(r *http.Request) *config.Principal {
	p, _ := r.Context().Value(principalKey{}).(*config.Principal)
	return p
}

// authState is the server's authentication configuration, resolved once at
// startup.
type authState struct {
	enabled      bool
	trustedProxy bool
	// byHash maps the hex SHA-256 of a bearer token to its principal.
	byHash map[string]*config.Principal
}

// SetAuth installs an authentication configuration.
func (s *Server) SetAuth(a *config.AuthConfig) error {
	if a == nil || a.Disabled {
		s.auth = &authState{enabled: false, trustedProxy: true}
		return nil
	}
	ps, err := a.Principals()
	if err != nil {
		return err
	}
	s.auth = &authState{enabled: true, trustedProxy: a.TrustedProxy, byHash: ps}
	return nil
}

// authFor resolves the caller. It returns nil,true when authentication is
// off.
//
// The token is looked up by its SHA-256 rather than compared directly, and
// the comparison of the found hash is constant-time. Hashing first means a
// timing difference in the map lookup leaks nothing about the token, only
// about its hash, which the attacker would have to invert.
func (s *Server) authFor(r *http.Request) (*config.Principal, bool) {
	st := s.auth
	if st == nil || !st.enabled {
		return nil, true
	}
	tok, ok := bearerToken(r)
	if !ok {
		return nil, false
	}
	h := config.HashToken(tok)
	p, found := st.byHash[h]
	if !found {
		// Do the same work as the found path so an unknown token does not
		// return measurably faster.
		subtle.ConstantTimeCompare([]byte(h), []byte(h))
		return nil, false
	}
	return p, true
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const p = "bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		return strings.TrimSpace(h[len(p):]), true
	}
	// VictoriaLogs' own clients send the token as a bare header on some
	// paths; accept it so a drop-in deployment does not need a new agent.
	if v := r.Header.Get("X-Simdlogs-Token"); v != "" {
		return v, true
	}
	return "", false
}

// withPrincipal resolves the caller before anything else runs, so tenant
// resolution can check the tenant headers against it.
//
// It must be outermost: the per-route role check runs inside the mux, but
// tenant resolution runs in the outer middleware, and a principal that is not
// in the context by then means the tenant headers are believed unconditionally
// -- which is the defect being fixed.
//
// A credential that is present but invalid is refused here. An absent
// credential is not: the route decides, so liveness probes stay open.
func (s *Server) withPrincipal(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := s.auth
		if st == nil || !st.enabled {
			h.ServeHTTP(w, r)
			return
		}
		if _, present := bearerToken(r); present {
			p, ok := s.authFor(r)
			if !ok {
				w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
				s.countErr()
				http.Error(w, "invalid credential", http.StatusUnauthorized)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), principalKey{}, p))
		}
		h.ServeHTTP(w, r)
	})
}

// requireAuth is the per-route role check. The principal was resolved by
// withPrincipal; this decides whether the route is open to it.
//
// Every route names the role it needs. Unwrapped, any client could query,
// ingest, read the flag dump and download a full backup.
func (s *Server) requireAuth(role config.Role, spec routeSpec, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st := s.auth
		if st == nil || !st.enabled {
			h(w, r)
			return
		}
		p := principalOf(r)
		if p == nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
			s.writeErr(w, r, spec, http.StatusUnauthorized, "authentication required")
			return
		}
		if !p.Can(role) {
			s.writeErr(w, r, spec, http.StatusForbidden,
				"principal "+p.Subject+" does not hold the "+string(role)+" role")
			return
		}
		h(w, r)
	}
}

// tenantFor resolves the tenant a request may act on.
//
// The headers are a request, not an identity. Previously they were the
// identity: any client could set AccountID and read or write any tenant's
// data, and a non-numeric value was silently rewritten to 0, so a typo wrote
// into the default tenant instead of failing.
func (s *Server) tenantFor(r *http.Request) (config.TenantKey, error) {
	acc := strings.TrimSpace(r.Header.Get("AccountID"))
	proj := strings.TrimSpace(r.Header.Get("ProjectID"))
	p := principalOf(r)

	if acc == "" && proj == "" {
		if p == nil {
			return config.TenantKey{Account: "0", Project: "0"}, nil
		}
		if t, ok := p.DefaultTenant(); ok {
			return t, nil
		}
		return config.TenantKey{}, errAmbiguousTenant
	}
	if acc == "" {
		acc = "0"
	}
	if proj == "" {
		proj = "0"
	}
	k, err := config.ParseTenantKey(acc + ":" + proj)
	if err != nil {
		return config.TenantKey{}, err
	}
	if p != nil && !p.CanTenant(k) {
		return config.TenantKey{}, errForbiddenTenant
	}
	return k, nil
}

type authError struct {
	msg  string
	code int
}

func (e *authError) Error() string { return e.msg }

var (
	errForbiddenTenant = &authError{msg: "not authorized for that tenant", code: http.StatusForbidden}
	errAmbiguousTenant = &authError{msg: "principal has several tenants; name one with AccountID/ProjectID", code: http.StatusBadRequest}
)

func authStatus(err error) int {
	if ae, ok := err.(*authError); ok {
		return ae.code
	}
	return http.StatusBadRequest
}
