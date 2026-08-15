package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
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
	key   string
	// lastUse is the unix-nano of the most recent request, and inFlight the
	// number running. Eviction needs both: least-recently-used to choose, and
	// in-flight to know the store is not being read right now.
	lastUse  atomic.Int64
	inFlight atomic.Int64

	// preFlushing admits one parked pre-flush goroutine per tenant.
	//
	// The backup's pre-flush runs in a goroutine with a timeout so a stalled
	// writer cannot hold the endpoint; the goroutine itself is not bounded by
	// that timeout and stays parked on the stall. backupBusy is released when
	// the HANDLER returns, so polling /admin/backup against a stalled writer
	// spawned one permanently parked goroutine per request, counted by
	// nothing: Server.Close waits on the background loops and on inFlight, and
	// this is neither.
	//
	// At most one. A second pre-flush would wait on the same batches as the
	// first, so skipping it costs nothing and bounds the leak at one goroutine
	// per tenant, released the moment the stall clears.
	preFlushing atomic.Bool

	// backupBusy admits one backup at a time for this tenant.
	//
	// A backup holds a Snapshot for its whole duration, which pins every group
	// it captured against unmapping. Concurrent backups of one tenant multiply
	// that: N streams each hold the full group set, so retention frees nothing
	// while any of them runs, and the mappings are the store's whole footprint
	// rather than its working set. It is also an admin endpoint with no body
	// and a large response -- the cheapest request to issue and the most
	// expensive to serve.
	backupBusy atomic.Bool
}

// Tenant lifecycle counters for /metrics. They carry no tenant-id label: a
// per-tenant label on a map an untrusted header can grow is an unbounded
// cardinality source, which is how a metrics endpoint takes down its own
// scraper.
var (
	tenantsEvicted  atomic.Int64
	tenantsRejected atomic.Int64
)

// TenantsEvicted is the number of tenants closed to make room.
func TenantsEvicted() int64 { return tenantsEvicted.Load() }

// TenantsRejected is the number of requests refused because every tenant slot
// was in use.
func TenantsRejected() int64 { return tenantsRejected.Load() }

func (t *tenant) touch() { t.lastUse.Store(time.Now().UnixNano()) }

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

// account and project split the tenant's "acc:proj" key back apart, for the
// places that need to hand one to a storage node.
func (t *tenant) account() string {
	if i := strings.IndexByte(t.key, ':'); i >= 0 {
		return t.key[:i]
	}
	return t.key
}

func (t *tenant) project() string {
	if i := strings.IndexByte(t.key, ':'); i >= 0 {
		return t.key[i+1:]
	}
	return "0"
}

// tenant returns the store+writer for acc:proj, opening it under
// dir/tenant-<acc>-<proj> the first time it is seen.
//
// The returned tenant is already marked busy (inFlight incremented). The
// caller MUST release it with tn.inFlight.Add(-1) when done, or eviction can
// never reclaim the slot. Handing it back busy is what closes the race
// between this function returning and the caller marking it.
func (s *Server) tenant(acc, proj string) (*tenant, error) {
	key := acc + ":" + proj
	s.mu.Lock()
	defer s.mu.Unlock()
	// Once shutdown starts, no new store is opened. A request arriving after
	// Close used to create a tenant directory, take its lock and start a
	// writer pool that nothing would ever close.
	if s.stopping.Load() {
		return nil, &authError{msg: "server is shutting down", code: http.StatusServiceUnavailable}
	}
	if tn := s.tenants[key]; tn != nil {
		tn.touch()
		// Mark busy here, under the same lock that hands it out. Doing it in
		// withTenant left a window: tenant() returned and released s.mu, and
		// before the caller incremented inFlight an eviction saw zero and
		// closed the store -- a panic on the ingest path, and on the query
		// path a 200 with zero rows, which is worse.
		tn.inFlight.Add(1)
		return tn, nil
	}
	// Bound the number of open tenants. Every tenant holds a store (mmaps,
	// file descriptors) and a writer with a pool of flush goroutines, so an
	// unbounded map is an unbounded resource commitment driven by a request
	// header -- before authentication existed, by any client at all.
	if max := s.limits.MaxOpenTenants; max != config.Unlimited && len(s.tenants) >= max {
		if !s.evictIdleLocked() {
			tenantsRejected.Add(1)
			return nil, &authError{
				msg:  fmt.Sprintf("too many open tenants (%d); all are in use", len(s.tenants)),
				code: http.StatusServiceUnavailable,
			}
		}
	}
	st, err := storage.OpenStoreWith(filepath.Join(s.dir, "tenant-"+acc+"-"+proj),
		storage.OpenOptions{Policy: s.corruptionPolicy})
	if err != nil {
		return nil, err
	}
	// Whether this tenant is degraded is recorded on the SERVER, not only on
	// the store, and it survives eviction. forEachTenant iterates open tenants
	// only, so readiness read "no degraded tenant among those currently open"
	// -- evicting an idle degraded tenant turned a 503 into a 200 while the
	// data was still missing, and a store never opened in this process was
	// invisible to it from the start.
	if h := st.Health(); h.Degraded() {
		s.degradedLocked(key, h)
	} else {
		delete(s.degraded, key)
	}
	tn := &tenant{key: key, store: st, w: ingest.NewWriter(st)}
	tn.touch()
	tn.inFlight.Add(1) // handed out busy, released by the caller
	if len(s.strmFlds) > 0 {
		tn.w.SetStreamFields(s.strmFlds)
	}
	if s.compact {
		tn.w.SetCompact(true)
	}
	if s.limits.MaxLineBytes > 0 {
		tn.w.SetMaxLineBytes(s.limits.MaxLineBytes)
	}
	if s.limits.MaxDecompressed > 0 {
		tn.w.SetMaxDecompressedBytes(int(s.limits.MaxDecompressed))
	}
	tn.w.SetRecordLimits(s.recordLimits())
	s.tenants[key] = tn
	return tn, nil
}

// evictIdleLocked closes the least recently used tenant that has no request
// in flight, returning whether it freed a slot. s.mu must be held.
//
// A tenant with active requests is never evicted: closing its store would
// unmap under a query. If every tenant is busy the caller is told so rather
// than being given a slot that does not exist.
func (s *Server) evictIdleLocked() bool {
	var victim *tenant
	for _, tn := range s.tenants {
		if tn.inFlight.Load() > 0 {
			continue
		}
		// Never the default tenant. It sits in the map with no in-flight
		// reference and the oldest lastUse, so it was the FIRST thing chosen
		// -- and s.def is never re-pointed, so evicting it left a closed
		// writer behind every syslog listener, /alerts and every
		// metrics-from-logs rule, silently and permanently.
		if tn == s.def {
			continue
		}
		if victim == nil || tn.lastUse.Load() < victim.lastUse.Load() {
			victim = tn
		}
	}
	if victim == nil {
		return false
	}
	delete(s.tenants, victim.key)
	// Close outside the caller's critical path would be nicer, but the store
	// must be shut before another OpenStore on the same directory: the
	// directory lock allows one writer.
	if err := victim.w.Close(); err != nil {
		log.Printf("tenant %s: flush on eviction failed: %v", victim.key, err)
	}
	if err := victim.store.Close(); err != nil {
		log.Printf("tenant %s: close on eviction failed: %v", victim.key, err)
	}
	tenantsEvicted.Add(1)
	return true
}

// withTenant resolves the request's tenant into its context (and counts the
// request), so every handler reads it with s.tn(r) instead of a fixed store.
// tenantPaths are the routes that read or write tenant data, and the only
// ones for which a tenant is resolved.
//
// This is an allowlist because the deny-list version was trivially
// sidestepped: it matched the raw path exactly, so "/health/", "//health",
// "/vmui" and any 404 path all fell through and created a tenant -- and
// withTenant runs before the mux, so it sees the uncleaned path. An allowlist
// fails closed: a new route gets no tenant until it is named here.
var tenantPaths = map[string]bool{
	"/insert/jsonline":                   true,
	"/insert/logfmt":                     true,
	"/insert/syslog":                     true,
	"/insert/journald":                   true,
	"/_bulk":                             true,
	"/insert/elasticsearch/_bulk":        true,
	"/loki/api/v1/push":                  true,
	"/insert/loki/api/v1/push":           true,
	"/api/v2/logs":                       true,
	"/v1/input":                          true,
	"/insert/datadog/api/v2/logs":        true,
	"/v1/logs":                           true,
	"/insert/opentelemetry/v1/logs":      true,
	"/select/logsql/query":               true,
	"/select/logsql/hits":                true,
	"/select/logsql/tail":                true,
	"/select/logsql/field_names":         true,
	"/select/logsql/field_values":        true,
	"/select/logsql/facets":              true,
	"/select/logsql/stats_query":         true,
	"/select/logsql/stats_query_range":   true,
	"/select/logsql/streams":             true,
	"/select/logsql/stream_ids":          true,
	"/select/logsql/stream_field_names":  true,
	"/select/logsql/stream_field_values": true,
	"/select/sql":                        true,
	"/select/vector":                     true,
	"/_search":                           true,
	"/_count":                            true,
	"/admin/backup":                      true,
}

func (s *Server) withTenant(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path.Clean first: "//health" and "/health/" reach here as different
		// strings than "/health", and an exact match on the raw path missed
		// both -- withTenant runs before the mux, so it sees what the client
		// sent.
		if !tenantPaths[path.Clean(r.URL.Path)] {
			s.countRequest(r.URL.Path)
			h.ServeHTTP(w, r)
			return
		}
		tn, err := s.tenantOf(r)
		if err != nil {
			// A rejected tenant is the caller's fault (unparseable header) or
			// a permission failure -- not a 500, which is what it used to be
			// reported as for every cause.
			//
			// The challenge header belongs on every 401. This runs before the
			// mux, so requireAuth -- the other place that sets it -- is never
			// reached for these paths.
			code := authStatus(err)
			if code == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
			}
			atomic.AddInt64(&s.nHTTPErrs, 1)
			http.Error(w, err.Error(), code)
			return
		}
		// Stamp the RESOLVED key over whatever the client sent, before any
		// handler runs. The federated read helpers copy AccountID/ProjectID
		// out of the request and forward them to a storage node that normally
		// has no -auth.config of its own, so this is the only place a read's
		// tenant is ever checked. Without it a principal whose default tenant
		// is 9:0, sending no header at all, forwarded no header at all -- and
		// every backend answered out of ITS default, 0:0. routeWrites does
		// the same for the write path.
		r.Header.Set("AccountID", tn.account())
		r.Header.Set("ProjectID", tn.project())
		s.countRequest(r.URL.Path)
		// tenantOf handed it back already marked busy; release it when the
		// request ends -- or earlier, if the handler is a stream that will
		// outlive any sensible notion of "in flight" (see tenantRef).
		ref := &tenantRef{tn: tn}
		defer ref.release()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		h.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), tenantKey{}, ref)))
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
func (s *Server) tn(r *http.Request) *tenant { return tenantRefOf(r).tn }

// tenantRef is a request's claim on a tenant. The claim blocks eviction, so a
// handler that runs for as long as its client cares to leave the connection
// open -- the live tail -- must give it up early or a handful of idle
// connections pin every tenant slot and every other tenant gets 503.
//
// Giving it up is safe for a stream because the stream re-leases a Snapshot
// on every poll and SnapshotAfterID reports a closing store, so an eviction
// underneath ends the tail cleanly instead of unmapping under it.
type tenantRef struct {
	tn   *tenant
	done atomic.Bool
}

// release drops the claim. Idempotent: the tail calls it explicitly and
// withTenant calls it again on the way out.
func (r *tenantRef) release() {
	if r.done.CompareAndSwap(false, true) {
		r.tn.inFlight.Add(-1)
	}
}

func tenantRefOf(r *http.Request) *tenantRef {
	return r.Context().Value(tenantKey{}).(*tenantRef)
}

// forEachTenant calls fn under the lock for every open tenant -- the basis for
// store-wide operations (retention, metrics).
func (s *Server) forEachTenant(fn func(*tenant)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tn := range s.tenants {
		fn(tn)
	}
}

// recordLimits is the per-record cap set, in one place so the serial writer
// and the parallel shards cannot disagree. They did: ParallelConfig carried
// no limits at all, so a body over MinParallelBytes bypassed every one.
func (s *Server) recordLimits() ingest.RecordLimits {
	return ingest.RecordLimits{
		MaxFields:     s.limits.MaxFieldsPerRecord,
		MaxNameBytes:  s.limits.MaxFieldNameBytes,
		MaxValueBytes: s.limits.MaxFieldValueBytes,
	}
}

// tenantDir is the store directory for a tenant key ("account:project"). It
// mirrors the join in tenant(); a key that is not in that form gives a path
// that does not exist, which every caller treats as "nothing to do".
func (s *Server) tenantDir(key string) string {
	acc, proj, ok := strings.Cut(key, ":")
	if !ok {
		return filepath.Join(s.dir, "tenant-"+key)
	}
	return filepath.Join(s.dir, "tenant-"+acc+"-"+proj)
}
