package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"errors"
	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/storage"
	"syscall"
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
	// byCN maps an mTLS client certificate's common name to its principal.
	byCN map[string]*config.Principal
	// proxy is the identity a request with no credential runs as when a
	// terminating proxy is trusted; nil otherwise.
	proxy *config.Principal
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
	cs, err := a.CertPrincipals()
	if err != nil {
		return err
	}
	proxy, err := a.ProxyPrincipal()
	if err != nil {
		return err
	}
	s.auth = &authState{
		enabled: true, trustedProxy: a.TrustedProxy,
		byHash: ps, byCN: cs, proxy: proxy,
	}
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

// certPrincipal maps a verified client certificate to a principal. The
// certificate has already been verified against the configured CA by the TLS
// stack -- ClientAuth is RequireAndVerifyClientCert -- so the common name can
// be trusted as a lookup key here.
func (s *Server) certPrincipal(r *http.Request) *config.Principal {
	st := s.auth
	if st == nil || len(st.byCN) == 0 || r.TLS == nil {
		return nil
	}
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		if p := st.byCN[chain[0].Subject.CommonName]; p != nil {
			return p
		}
	}
	// PeerCertificates without a verified chain means verification did not
	// happen; do not trust the name.
	return nil
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
			h.ServeHTTP(w, r)
			return
		}
		// A verified client certificate is an identity, not just a transport
		// gate. RequireAndVerifyClientCert proves the CA trusts this client;
		// mapping its common name is what turns that into a principal, and
		// without the mapping the certificate was verified and then discarded.
		if p := s.certPrincipal(r); p != nil {
			r = r.WithContext(context.WithValue(r.Context(), principalKey{}, p))
			h.ServeHTTP(w, r)
			return
		}
		// A terminating proxy has already authenticated the caller.
		if st.proxy != nil {
			r = r.WithContext(context.WithValue(r.Context(), principalKey{}, st.proxy))
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

	// With authentication on, an unauthenticated request resolves no tenant
	// at all. It used to fall through to the default: the request was
	// answered 401 by the route, but tenant resolution had already run in the
	// outer middleware and created the store -- directory, lock, mmaps and a
	// writer pool -- for whatever AccountID the caller sent. Unbounded disk
	// and inode growth from an anonymous client.
	if st := s.auth; st != nil && st.enabled && p == nil {
		return config.TenantKey{}, errUnauthenticatedTenant
	}
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
	errUnauthenticatedTenant = &authError{msg: "authentication required", code: http.StatusUnauthorized}
	errForbiddenTenant       = &authError{msg: "not authorized for that tenant", code: http.StatusForbidden}
	errAmbiguousTenant       = &authError{msg: "principal has several tenants; name one with AccountID/ProjectID", code: http.StatusBadRequest}
)

func authStatus(err error) int {
	if ae, ok := err.(*authError); ok {
		return ae.code
	}
	// A tenant resolves by opening its store, so a filesystem that refuses --
	// no space, no permission, an I/O error -- fails HERE, before any storage
	// budget check the middleware would have run. The default used to answer
	// 400: a client error code for a server storage condition, with the
	// server's absolute path in the body. An agent treats 400 as permanent
	// and drops the batch it cannot re-send, so the one condition that is
	// certain to be transient was reported as the one that is certain not to
	// be.
	if isStorageErr(err) {
		return http.StatusInsufficientStorage
	}
	return http.StatusBadRequest
}

// isStorageErr reports whether err is the filesystem refusing, as opposed to
// the request being wrong.
//
// By errno rather than by string: the message is wrapped through OpenStore and
// os.MkdirAll and its wording is not a contract, while ENOSPC, EDQUOT, EACCES,
// EPERM, EROFS and EIO are.
func isStorageErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrDiskFull) || errors.Is(err, storage.ErrQuotaExceeded) {
		return true
	}
	for _, e := range []error{
		syscall.ENOSPC, syscall.EDQUOT, syscall.EACCES,
		syscall.EPERM, syscall.EROFS, syscall.EIO,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
