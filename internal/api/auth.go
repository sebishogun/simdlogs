package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"errors"
	"github.com/sebishogun/simdlogs/internal/config"
	obs "github.com/sebishogun/simdlogs/internal/observability"
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
			// Audited. An unauthenticated request to a privileged route is the
			// first line of any credential-stuffing or misconfiguration
			// incident, and "we have no record" is the answer a security
			// review must never get.
			obs.Audit(r.Context(), obs.EventAuthFailed, "", obs.OutcomeDenied,
				obs.FieldRoute, r.URL.Path,
				obs.FieldMethod, r.Method,
				"required_role", string(role))
			w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
			s.writeErr(w, r, spec, http.StatusUnauthorized, "authentication required")
			return
		}
		if !p.Can(role) {
			// A VALID credential reaching for a role it does not hold is a
			// different event from no credential at all: one is a client that
			// forgot to authenticate, the other is a principal doing something
			// it was not given. They are counted separately because the
			// response to them is different.
			obs.Audit(r.Context(), obs.EventAuthForbidden, p.Subject, obs.OutcomeDenied,
				obs.FieldRoute, r.URL.Path,
				obs.FieldMethod, r.Method,
				"required_role", string(role))
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
	// budget check the middleware would have run. The default answered 400: a
	// client error code for a server storage condition. An agent treats 400 as
	// permanent and drops the batch it cannot re-send, so a transient
	// condition was reported as the one thing it is not.
	//
	// The caller sees the CLASS, not the message: see storageErrMessage.
	switch storageErrKind(err) {
	case storageTransient:
		return http.StatusInsufficientStorage
	case storagePermanent:
		// 500, not 507 and not 400. The server cannot write where it was told
		// to -- a read-only mount, a data directory it has no permission for
		// -- and no retry and no change by the client fixes that. 507 would
		// tell an agent to retry a permission bug until someone notices; 400
		// would tell it the request was malformed and to drop the batch. This
		// says the fault is the server's and is not going away by itself.
		return http.StatusInternalServerError
	}
	return http.StatusBadRequest
}

// storageErrClass says what kind of failure the filesystem reported.
type storageErrClass uint8

const (
	storageNotAnError storageErrClass = iota
	// storageTransient is a failure a retry could survive.
	storageTransient
	// storagePermanent is one it cannot: the server cannot write where it was
	// told to, and nothing the client does changes that.
	storagePermanent
)

// storageErrKind classifies a store-open failure.
//
// By errno rather than by string: the message is wrapped through OpenStore and
// os.MkdirAll and its wording is not a contract, while the errnos are. The
// classification is what decides the status, and getting it backwards is worse
// than not having it: 507 on a permanent failure is an infinite retry loop,
// 400 on a transient one is a dropped batch.
func storageErrKind(err error) storageErrClass {
	if err == nil {
		return storageNotAnError
	}
	if errors.Is(err, storage.ErrDiskFull) || errors.Is(err, storage.ErrQuotaExceeded) ||
		errors.Is(err, storage.ErrLocked) {
		// ErrLocked by SENTINEL, not by errno. lockDir wraps EWOULDBLOCK into
		// storage.ErrLocked and drops the errno, so errors.Is(err,
		// syscall.EAGAIN) is false -- and this function's own comment named
		// EAGAIN as the case it was added for. "The store lock is held by
		// another process" answered 400, and an agent drops the batch on 400.
		// The one condition this mapping exists for was the one it missed.
		return storageTransient
	}
	for _, e := range transientStorageErrnos {
		if errors.Is(err, e) {
			return storageTransient
		}
	}
	for _, e := range permanentStorageErrnos {
		if errors.Is(err, e) {
			return storagePermanent
		}
	}
	return storageNotAnError
}

// permanentStorageErrnos are the failures no retry survives. A data directory
// the process may never write to, or a read-only mount, is a deployment fault
// -- and the first version of this classified all three as retryable.
var permanentStorageErrnos = []error{
	syscall.EACCES, syscall.EPERM, syscall.EROFS, syscall.ENOTDIR,
}

// subjectOf is the authenticated principal's name, or "" when there is none.
// Audit records carry it, and an audit record with no subject is a fact rather
// than a gap: it says the action was taken unauthenticated.
func subjectOf(r *http.Request) string {
	if p := principalOf(r); p != nil {
		return p.Subject
	}
	return ""
}

// storageErrMessage is what the CLIENT is told. The server's own message names
// the data directory and is written to the log instead.
func storageErrMessage(k storageErrClass) string {
	if k == storagePermanent {
		return "simdlogs: this tenant's storage is not writable; the server's log has the detail"
	}
	return "simdlogs: this tenant's storage is temporarily unavailable; retry"
}

// transientStorageErrnos are the failures a retry could survive.
//
// The set is what 507 MEANS: come back and it may work. The first version got
// both directions wrong. It listed EACCES, EPERM and EROFS -- a data directory
// the process may never write to, or a read-only mount, are permanent, and
// telling an agent to retry them forever trades "drops the batch" for "retries
// a permission bug until someone notices". And it omitted every errno by which
// a per-tenant store open actually fails under load: EMFILE and ENFILE (fd
// exhaustion), ENOMEM (the mmap of a new group), EAGAIN, EBUSY, EINTR and
// ESTALE. Those answered 400, and an agent drops a batch on 400, which is the
// defect the 507 mapping was added to close.
//
// The store lock held by another process is storage.ErrLocked, matched by
// sentinel above: lockDir wraps the errno into the sentinel and DROPS it, so
// errors.Is(err, syscall.EAGAIN) is false for it. An earlier version of this
// comment named EAGAIN as the case the list was added for, and the case it
// named was the one it did not catch.
//
// A package-level slice rather than a literal in the function, for
// readability. An earlier version claimed the literal "boxed six syscall.Errno
// values to the heap on every call"; measured, both shapes are 0.00 allocs/op
// and neither has a runtime.newobject site in the disassembly -- the constants
// are below 256 so the interface conversions resolve through
// runtime.staticuint64s, and a non-escaping literal never reaches the heap
// anyway. The claim was written from reading -gcflags=-m output rather than
// from a benchmark.
var transientStorageErrnos = []error{
	syscall.ENOSPC, syscall.EDQUOT,
	syscall.EMFILE, syscall.ENFILE, syscall.ENOMEM,
	syscall.EAGAIN, syscall.EBUSY, syscall.EINTR, syscall.ESTALE,
	syscall.EIO,
}
