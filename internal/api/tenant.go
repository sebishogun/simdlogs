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

// sanitizeID keeps only digits, so a header cannot direct the storage path
// outside the data directory (AccountID/ProjectID are numeric in VL). A
// non-numeric or empty value falls back to the default tenant.
func sanitizeID(v string) string {
	if v == "" {
		return "0"
	}
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return "0"
		}
	}
	return v
}

// tenantOf resolves the request's tenant from the AccountID/ProjectID headers,
// creating its store on first use. Absent headers select the default 0:0
// tenant, so single-tenant deployments behave exactly as before.
func (s *Server) tenantOf(r *http.Request) (*tenant, error) {
	return s.tenant(sanitizeID(r.Header.Get("AccountID")), sanitizeID(r.Header.Get("ProjectID")))
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
			http.Error(w, err.Error(), 500)
			return
		}
		s.countRequest(r.URL.Path)
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tn)))
	})
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
