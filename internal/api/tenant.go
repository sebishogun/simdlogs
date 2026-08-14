package api

import (
	"context"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// tenant is one isolated store+writer. VictoriaLogs keys tenancy on
// AccountID:ProjectID; each maps to its own group directory, so one tenant's
// data is never visible to another's queries.
type tenant struct {
	store *storage.Store
	w     *ingest.Writer
	mono  int64
}

// fallbackTS is the per-tenant timestamp source for records lacking their own:
// wall-clock plus a monotonic bump (atomic -- the parallel ingest path calls
// it from many goroutines).
func (t *tenant) fallbackTS() func() int64 {
	return func() int64 { return time.Now().UnixNano() + atomic.AddInt64(&t.mono, 1) }
}

type tenantKey struct{}

// tenantOf resolves the request's tenant, creating its store on first use.
//
// The tenant comes from tenantFor, which validates the headers and checks
// them against the authenticated principal. sanitizeID is gone: rewriting a
// malformed AccountID to "0" meant a typo silently wrote into the default
// tenant instead of failing, and treating the header as identity meant any
// client could name any tenant.
func (s *Server) tenantOf(r *http.Request) (*tenant, error) {
	k, err := s.tenantFor(r)
	if err != nil {
		return nil, err
	}
	return s.tenant(k.Account, k.Project)
}

// tenant returns the store+writer for acc:proj, opening it under
// dir/tenant-<acc>-<proj> the first time it is seen.
func (s *Server) tenant(acc, proj string) (*tenant, error) {
	key := acc + ":" + proj
	s.mu.Lock()
	defer s.mu.Unlock()
	if tn := s.tenants[key]; tn != nil {
		return tn, nil
	}
	st, err := storage.OpenStore(filepath.Join(s.dir, "tenant-"+acc+"-"+proj))
	if err != nil {
		return nil, err
	}
	tn := &tenant{store: st, w: ingest.NewWriter(st)}
	if len(s.strmFlds) > 0 {
		tn.w.SetStreamFields(s.strmFlds)
	}
	if s.compact {
		tn.w.SetCompact(true)
	}
	s.tenants[key] = tn
	return tn, nil
}

// withTenant resolves the request's tenant into its context (and counts the
// request), so every handler reads it with s.tn(r) instead of a fixed store.
func (s *Server) withTenant(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tn, err := s.tenantOf(r)
		if err != nil {
			// A rejected tenant is the caller's fault (unparseable header) or
			// a permission failure -- not a 500, which is what it used to be
			// reported as for every cause.
			atomic.AddInt64(&s.nHTTPErrs, 1)
			http.Error(w, err.Error(), authStatus(err))
			return
		}
		s.countRequest(r.URL.Path)
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		h.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tn)))
		if sw.code >= 400 {
			atomic.AddInt64(&s.nHTTPErrs, 1)
		}
	})
}

// statusWriter records the status a handler wrote, so /metrics can report the
// error rate. It forwards Flush and the streaming interfaces the live tail
// needs -- a wrapper that swallowed them would break tailing entirely.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// tn is the request's resolved tenant (set by withTenant).
func (s *Server) tn(r *http.Request) *tenant { return r.Context().Value(tenantKey{}).(*tenant) }

// forEachTenant calls fn under the lock for every open tenant -- the basis for
// store-wide operations (retention, metrics).
func (s *Server) forEachTenant(fn func(*tenant)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tn := range s.tenants {
		fn(tn)
	}
}
