// Package api serves the log database over HTTP. The surface tracks
// VictoriaLogs' paths where they exist (/insert/jsonline,
// /select/logsql/query, /select/logsql/hits) so the head-to-head harness
// drives both engines through the same wire calls, and adds the ES search
// surface the reference lacks. This file is the server and the two
// load-bearing endpoints; the fuller surface builds on it.
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/ingest"
	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Server holds the per-tenant stores behind the HTTP surface. A request's
// tenant (AccountID:ProjectID headers, default 0:0) selects an isolated store;
// see tenant.go.
type Server struct {
	dir     string
	mu      sync.Mutex
	tenants map[string]*tenant
	def     *tenant // the default 0:0 tenant, used by the non-HTTP paths (syslog listener)
	// nStreamedSelects counts bare selects answered without materializing.
	nStreamedSelects int64
	// cursors signs pagination cursors. Per process and never persisted: a
	// cursor that survived a restart would resume into a store that has since
	// compacted, retired and re-ingested.
	cursors *cursorSigner
	// lastSyslogRefusal throttles the native transport's budget-refusal log.
	// Nanos, atomic: written from every listener goroutine.
	lastSyslogRefusal int64
	strmFlds          []string
	compact           bool // compact mode default for new tenants (flate dict)
	// corruptionPolicy is what a tenant store does with an unreadable group.
	// The zero value is storage.CorruptionFail, so a server configured with
	// nothing refuses to open a damaged tenant rather than serving it short.
	corruptionPolicy storage.CorruptionPolicy
	// vecFlds is which record fields are embeddings, stamped on every tenant
	// writer as it opens.
	vecFlds ingest.VectorFields
	// peers is the client for internal cluster traffic: its own transport,
	// its own timeouts, its own pool, and a bounded response body. It replaced
	// http.DefaultClient, which has no timeout at all -- a peer that accepts
	// the connection and never answers held a router goroutine, a pool slot
	// and the caller's request for as long as the caller waited, times the
	// number of shards.
	peers *clusterClient
	// shardID and replicaID are this node's identity in the cluster, reported
	// in every internal response so a router can say WHICH shard was
	// incomplete rather than that something was.
	shardID   int
	replicaID int
	// hw is the highest HighWatermark this router has seen from each PEER,
	// which is what makes a lagging replica's answer detectable at all -- see
	// checkWatermark. hwMu guards creation of an entry; each entry is atomic
	// because the fan-out writes them from one goroutine per shard.
	//
	// Keyed by shard, because the signal wanted is CROSS-REPLICA -- two
	// replicas of one shard holding 12 and 8 rows -- and a peer compared only
	// against its own history cannot show that. Each entry also records WHICH
	// peer set the high, so a SetBackends that repoints an index at a different
	// machine does not hand the new machine the old one's floor: a high set by
	// a peer no longer in the shard is discarded rather than enforced.
	hwMu sync.Mutex
	hw   map[int]*shardHigh
	// hwOwn is the newest timestamp THIS node has accepted, kept as a running
	// maximum so that evicting a tenant or expiring data cannot lower what it
	// reports. See highWatermark.
	hwOwn atomic.Int64
	// repairBusy admits one cluster repair at a time on this router. Repair
	// mutates, and two overlapping passes read the same missing set before
	// either writes it -- see repairCluster.
	repairBusy atomic.Bool

	// quota is the storage budget every tenant store opens under. Validated
	// once at construction, so a tenant opening later cannot fail for a
	// configuration problem the operator was never told about.
	quota storage.QuotaConfig
	// degraded is every tenant key whose store reported degraded when it was
	// opened, and what it reported. Guarded by mu. It outlives the tenant so
	// eviction cannot turn readiness green.
	degraded map[string]storage.Health
	// lastDirReread is when the snapshot last re-read the store directories of
	// the degraded tenants that are not open. Guarded by mu.
	lastDirReread time.Time
	// dirRereadEvery is the throttle window. A field rather than the constant
	// so a test can assert the re-read SEMANTICS at zero and the throttle
	// itself separately -- a test that sleeps out a window is measuring the
	// clock, and one that cannot set the window has to.
	dirRereadEvery time.Duration
	backends       []string // peer node base URLs; when set, selects fan out and merge (vmselect role)
	replicas       int      // replication factor: backends group into shards of this many replicas
	maxRows        int      // cap on a bare (no-pipe) select's rows. Errors, never truncates.
	limits         config.Limits
	started        time.Time
	nIngestReq     int64 // ingest requests (atomic)
	nQueryReq      int64 // query requests (atomic)
	nRowsIn        int64 // log entries ingested (atomic)
	nBytesIn       int64 // bytes of log data ingested (atomic)
	nRowsDrop      int64 // entries rejected as malformed (atomic)
	nHTTPErrs      int64 // responses with a 4xx/5xx status (atomic)
	nTails         int64 // live tail requests currently open (atomic)

	szMu    sync.Mutex // guards the cached store footprint
	szBytes int64
	szAt    time.Time
	rr      int64 // round-robin cursor for write routing (atomic)

	rmu   sync.Mutex
	rules []*logRule // metrics-from-logs: LogsQL evaluated on a timer, exposed on /metrics

	amu    sync.Mutex
	alerts []*alertRule // alerting: LogsQL count vs a threshold, exposed on /alerts

	// Background lifecycle. Every periodic loop -- retention, tiering, log
	// rules, alert rules -- runs under bgCtx and is counted in bg, so Close
	// can cancel them and WAIT. Stopping without waiting is not enough: a
	// retention pass in flight when the stores close would unmap under
	// itself, and the alert and rule tickers had no stop at all and ran for
	// the life of the process.
	auth *authState
	// readyOnce records that readiness has answered at least once, so the
	// startup grace stops applying to a server that has already reported.
	readyOnce atomic.Bool
	querySem  chan struct{} // MaxConcurrentQuery
	// admission bounds reads per tenant, which the class semaphores cannot:
	// they are process-wide, so one tenant can hold every slot.
	admission *query.Admission
	// workers is the shared scan worker budget. Without it every concurrent
	// query sized its own fan-out at GOMAXPROCS.
	workers  *query.WorkerBudget
	writeSem chan struct{} // MaxConcurrentWrite
	tailSem  chan struct{} // MaxConcurrentTail
	// Syslog listeners accept outside the HTTP server, so shutdown has to
	// close their connections explicitly.
	syslogMu        sync.Mutex
	syslogConns     map[net.Conn]struct{}
	syslogListeners []io.Closer
	syslogClosing   bool
	syslogWG        sync.WaitGroup
	// retentionMaxAge is this node's retention horizon in nanoseconds, 0 when
	// retention is off. Read by the adopt path, which must not accept a group
	// the next sweep would delete -- see StartRetention.
	retentionMaxAge atomic.Int64

	routeMu  sync.Mutex
	routes   []string
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup
	bgMu     sync.Mutex // orders the stopping check against bg.Add
	stopping atomic.Bool
}

// goBackground runs fn on an interval until the server shuts down. It is the
// only way this package starts a periodic loop, so no loop can be added that
// shutdown does not know about.
func (s *Server) goBackground(interval time.Duration, fn func()) (stop func()) {
	if interval <= 0 {
		return func() {}
	}
	// The check and the Add happen under the same lock Close takes before
	// waiting, so a loop cannot register after bg.Wait() has returned --
	// which is the documented WaitGroup misuse.
	s.bgMu.Lock()
	if s.stopping.Load() {
		s.bgMu.Unlock()
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	s.bg.Add(1)
	s.bgMu.Unlock()
	go func() {
		defer s.bg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.bgCtx.Done():
				return
			case <-done:
				return
			case <-t.C:
				if s.stopping.Load() {
					return
				}
				fn()
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// NewServer opens (or creates) the data directory at dir and returns the
// server with its default tenant ready.
// NewServer opens (or creates) the data directory at dir with the production
// default limits. NewServerConfig takes an explicit configuration.
func NewServer(dir string) (*Server, error) {
	c := config.Default()
	c.Dir = dir
	return NewServerConfig(c)
}

// NewServerConfig opens the server with an explicit configuration. The
// configuration is validated first, so a limit that is neither positive nor
// config.Unlimited fails at startup rather than at the request that would
// have tripped it.
func NewServerConfig(c config.Config) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	dir := c.Dir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	pol, err := storage.ParseCorruptionPolicy(c.CorruptionPolicy)
	if err != nil {
		return nil, err
	}
	// Parsed once, at startup: a typo in -vector-fields is a startup failure
	// rather than a field silently stored as text and invisible to the one
	// search it exists for.
	vecFlds, err := ingest.ParseVectorFields(c.VectorFields)
	if err != nil {
		return nil, err
	}
	srv := &Server{dir: dir, tenants: map[string]*tenant{},
		degraded: map[string]storage.Health{}, dirRereadEvery: DefaultDirRereadEvery,
		started: time.Now()}
	srv.vecFlds = vecFlds
	// Built even on a non-router: SetBackends can be called after
	// construction, and a nil client at that point would be a nil dereference
	// on the first peer call rather than at configuration time.
	srv.peers = newClusterClient(nil)
	if c.DirRereadInterval != 0 {
		srv.dirRereadEvery = c.DirRereadInterval
		if c.DirRereadInterval < 0 {
			srv.dirRereadEvery = 0 // negative means "every call", like zero
		}
	}
	srv.corruptionPolicy = pol
	srv.limits = c.Limits
	// Validated here rather than at each store open, so a bad budget refuses
	// to start the server instead of failing the first tenant to arrive.
	srv.quota = storage.QuotaConfig{
		ReserveWarnBytes:   c.Storage.ReserveWarnBytes,
		ReserveRejectBytes: c.Storage.ReserveRejectBytes,
		MaxTenantBytes:     c.Storage.MaxTenantBytes,
	}
	if err := srv.quota.Normalize(); err != nil {
		return nil, err
	}
	// Concurrency budgets. A nil channel means unbounded.
	if n := c.Limits.MaxConcurrentQuery; n > 0 {
		srv.querySem = make(chan struct{}, n)
	}
	if n := c.Limits.MaxConcurrentWrite; n > 0 {
		srv.writeSem = make(chan struct{}, n)
	}
	if n := c.Limits.MaxConcurrentTail; n > 0 {
		srv.tailSem = make(chan struct{}, n)
	}
	if n := c.Limits.MaxQueriesPerTenant; n > 0 {
		srv.admission = query.NewAdmission(query.AdmissionConfig{
			MaxPerTenant: n,
			Wait:         c.Limits.QueryQueueWait,
		})
	}
	// The scan worker budget is process-wide and installed once. Three scan
	// paths used to size their fan-out at GOMAXPROCS EACH, so ten concurrent
	// queries on a 32-core box spawned 320 workers for 32 cores -- all doing
	// memory-bound column decode and evicting each other's cache lines.
	//
	// Installed even when nothing else is configured, because the default it
	// replaces is the pathological one.
	srv.workers = query.NewWorkerBudget(c.Limits.MaxScanWorkers)
	query.SetWorkerBudget(srv.workers)
	cs, err := newCursorSigner()
	if err != nil {
		return nil, err
	}
	srv.cursors = cs
	srv.compact = c.Compact
	if len(c.StreamFields) > 0 {
		srv.strmFlds = append([]string(nil), c.StreamFields...)
	}
	// A bare select is bounded by the configured row limit. It used to
	// default to zero and be read as "no cap", so a single query could
	// materialize an entire store.
	if c.Limits.MaxQueryRows != config.Unlimited {
		srv.maxRows = c.Limits.MaxQueryRows
	}
	srv.bgCtx, srv.bgCancel = context.WithCancel(context.Background())
	// Optional stream-field default from the environment, so a deployment can
	// synthesize _stream without a code change. Set before the default tenant
	// opens so it inherits the policy.
	if v := strings.TrimSpace(os.Getenv("SIMDLOGS_STREAM_FIELDS")); v != "" {
		srv.strmFlds = splitCSV(v)
	}
	def, err := srv.tenant("0", "0")
	if err != nil {
		return nil, err
	}
	// tenant() hands it back marked busy; this one is the server's default,
	// not an in-flight request, so release the reference or every request
	// sees one permanently busy tenant. It is kept out of eviction by
	// identity instead (evictIdleLocked), which is what a request-count
	// reference cannot express: the default is not busy, it is not
	// evictable.
	def.inFlight.Add(-1)
	srv.def = def
	// Every tenant already on disk that is degraded, so readiness is right
	// from the first probe rather than from the first request that happens to
	// open one.
	srv.scanDegradedTenants()

	return srv, nil
}

// closeDrainTimeout bounds how long Close waits for in-flight requests. Past
// it, closing anyway is the lesser evil: the alternative is a process that
// will not exit because one client will not hang up.
const closeDrainTimeout = 10 * time.Second

// drainInFlight waits until no tenant has a request in flight, or until the
// deadline. It reports whether the wait succeeded.
//
// s.stopping is already set, so no new request gets a tenant; this is only
// about the ones already inside.
func (s *Server) drainInFlight(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		busy := 0
		s.mu.Lock()
		for _, tn := range s.tenants {
			if n := tn.inFlight.Load(); n > 0 {
				busy += int(n)
			}
		}
		s.mu.Unlock()
		if busy == 0 {
			return true
		}
		if time.Now().After(deadline) {
			obs.L().Warn("closing with requests still in flight",
				obs.FieldEvent, "shutdown.drain_timeout",
				"in_flight", busy, "waited", d.String())
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Dir is the data directory the server was opened on -- what a caller measures
// to report the store's footprint.
func (s *Server) Dir() string { return s.dir }

// Close shuts the server down cleanly: every tenant's writer is flushed and
// its pool stopped, and every store is unmapped. Call it at process shutdown
// after the HTTP server has stopped accepting requests. Safe to call once.
func (s *Server) Close() error {
	// Stop accepting new background work, cancel what is running, and wait
	// for it. Closing the stores first would unmap under a retention or
	// recompaction pass that is still walking them.
	s.bgMu.Lock()
	s.stopping.Store(true)
	s.bgMu.Unlock()
	if s.bgCancel != nil {
		s.bgCancel()
	}
	s.bg.Wait()
	// Syslog connections next: they write through the tenant writers, so they
	// must be gone before those writers close.
	s.closeSyslogConns()

	// Then in-flight HTTP requests. Closing a store unmaps it, and a handler
	// still inside a query is reading that mapping -- Close returning while a
	// request was mid-flight was a use-after-unmap waiting for the timing to
	// line up. http.Server.Shutdown is not enough on its own: it is the
	// caller's to run, main.go logs its expiry and closes anyway, and a live
	// tail is not an idle connection it would ever drain.
	s.drainInFlight(closeDrainTimeout)

	s.mu.Lock()
	tenants := make([]*tenant, 0, len(s.tenants))
	for _, t := range s.tenants {
		tenants = append(tenants, t)
	}
	s.mu.Unlock()
	var firstErr error
	for _, t := range tenants {
		// ErrWriterClosed on a second Close is the writer reporting it is
		// already shut, not a failure -- Close is documented idempotent.
		if err := t.w.Close(); err != nil && !errors.Is(err, ingest.ErrWriterClosed) && firstErr == nil {
			firstErr = err
		}
		if err := t.store.Close(); err != nil && firstErr == nil { // unmap
			firstErr = err
		}
	}
	// Forget them. Leaving the map populated meant a request arriving after
	// Close found a closed tenant, and a request for a NEW tenant opened a
	// store -- with its directory lock and writer pool -- that nothing would
	// ever close.
	s.mu.Lock()
	s.tenants = map[string]*tenant{}
	s.mu.Unlock()
	return firstErr
}

// SetCompact enables compact mode on every tenant: flushed groups flate their
// dictionaries for a smaller footprint at the cost of slower dict decode.
// Opt-in; the default stays fast LZ4.
func (s *Server) SetCompact(on bool) {
	s.mu.Lock()
	for _, tn := range s.tenants {
		tn.w.SetCompact(on)
	}
	s.compact = on
	s.mu.Unlock()
}

// SetStreamFields declares the fields that identify a log stream; ingested
// records then carry a synthesized _stream label built from them. Applies to
// existing tenants and any opened later.
func (s *Server) SetStreamFields(fields []string) {
	s.mu.Lock()
	s.strmFlds = append([]string(nil), fields...)
	for _, tn := range s.tenants {
		tn.w.SetStreamFields(fields)
	}
	s.mu.Unlock()
}

// applyQueryBudget stamps the configured wall-clock and byte budgets onto a
// query, and returns a flag the caller checks afterwards.
//
// The request context alone did nothing: Go does not abort a handler when its
// context is cancelled, and internal/query took no context, so
// -search.maxDuration bounded the cluster fan-out and nothing else. The scan
// checks these per group.
func (s *Server) applyQueryBudget(r *http.Request, q *query.Query) *atomic.Bool {
	stopped := new(atomic.Bool)
	q.Stopped = stopped
	if d := s.limits.MaxQueryDuration; d > 0 {
		q.Deadline = time.Now().Add(d)
	}
	if dl, ok := r.Context().Deadline(); ok {
		if q.Deadline.IsZero() || dl.Before(q.Deadline) {
			q.Deadline = dl
		}
	}
	if n := s.limits.MaxQueryBytes; n > 0 {
		q.MaxBytes = n
	}
	// The REQUEST's context, so a client that hangs up ends the scan.
	//
	// Go does not abort a handler when its context is cancelled, so before
	// this a disconnected client's query ran to completion and threw the
	// answer away -- the whole cost, none of the benefit, and on a server
	// under load that is the difference between shedding work and doing it
	// twice. The Deadline above is kept as well: it is what -search.maxDuration
	// sets, and a caller with no context deadline still gets it.
	q.Bind(r.Context(), query.Limits{
		MaxGroupKeys: s.limits.MaxGroupKeys,
		MaxPipeRows:  s.limits.MaxPipeRows,
	})
	return stopped
}

// queryStopped answers a query that hit a budget. A short result presented as
// complete is the silent truncation this exists to prevent.
//
// The bool says THAT it stopped; qerr says WHY, when the query was bound to a
// context. Every stop used to answer 504 whatever caused it, so a client that
// disconnected, one that asked for too many bytes and one that ran out of time
// were indistinguishable in an access log -- and a client retrying a 504
// against a byte budget retried forever.
func (s *Server) queryStopped(w http.ResponseWriter, r *http.Request, stopped *atomic.Bool) bool {
	return s.queryStoppedErr(w, r, stopped, nil)
}

// queryStoppedErr is queryStopped with the query's own stop reason.
func (s *Server) queryStoppedErr(w http.ResponseWriter, r *http.Request, stopped *atomic.Bool, q *query.Query) bool {
	if stopped == nil || !stopped.Load() {
		return false
	}
	status := http.StatusGatewayTimeout
	msg := "query exceeded its time or byte budget; narrow the window, add a filter, " +
		"or raise -search.maxDuration / -search.maxQueryBytes"
	if q != nil {
		if err := q.StopErr(); err != nil {
			// The cause AND the remedy. A message that says only what went
			// wrong leaves an operator to guess which knob moves it, and a
			// regression test pins that these name the real flags -- the
			// budget errors used to name flags that did not exist.
			status = query.HTTPStatus(err)
			switch {
			case errors.Is(err, query.ErrDeadlineExceeded):
				msg = err.Error() + "; narrow the window, add a filter, or raise " +
					"-search.maxDuration"
			case errors.Is(err, query.ErrByteLimit), errors.Is(err, query.ErrMemoryLimit):
				msg = err.Error() + "; narrow the window, add a filter, or raise " +
					"-search.maxQueryBytes"
			case errors.Is(err, query.ErrRowLimit):
				msg = err.Error() + "; add a `| limit N`, a stats pipe, or raise " +
					"-search.maxRows"
			default:
				msg = err.Error()
			}
			if errors.Is(err, query.ErrCanceled) {
				// Nobody is reading: the connection is gone. The status is for
				// the access log, and writing a body to a closed connection is
				// an error the handler would then log as a second failure.
				return true
			}
		}
	}
	s.writeErr(w, r, readSpec(), status, msg)
	return true
}

// parallelCfg is the deployment writer configuration the temporary shard
// writers of a large ingest must inherit. It reads the same two settings the
// persistent per-tenant writer is built with (tenant), so a large body and a
// small one produce the same schema. Copying only Compact here is what made
// _stream appear under the small-body path and vanish under the parallel one.
func (s *Server) parallelCfg() ingest.ParallelConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ingest.ParallelConfig{
		Compact:      s.compact,
		StreamFields: append([]string(nil), s.strmFlds...),
		MaxLineBytes: s.limits.MaxLineBytes,
		Limits:       s.recordLimits(),
	}
}

// failIngest reports a partially or wholly failed ingest. The durable rows
// are counted into the metrics before the error is written, so /metrics and
// the store cannot disagree: they landed, whatever the response says.
//
// The body names how much is durable so an operator (and a shipper that reads
// it) can tell "nothing was written, retry everything" from "most of it was
// written, a retry duplicates it". Deduplicating that retry is a write-ID
// problem, not something this handler can solve.
func (s *Server) failIngest(w http.ResponseWriter, err error, ingested, skipped, nbytes int) {
	s.countRows(ingested, skipped, nbytes)
	body := map[string]any{
		"error":    err.Error(),
		"ingested": ingested,
		"skipped":  skipped,
		"durable":  ingested,
	}
	// The same retry facts every other write path reports. This one answered
	// a flat 500 with no Retry-After and none of them, so a shipper posting a
	// body over MinParallelBytes got a different -- and less useful -- answer
	// to the same disk failure than one posting a smaller body. A fact that
	// depends on the size of the request is a fact a client cannot rely on.
	code := http.StatusInternalServerError
	var we *ingest.WriteError
	if errors.As(err, &we) {
		code = we.HTTPStatus()
		if after := int(we.RetryAfter() / time.Second); after > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(after))
			body["retryAfterSeconds"] = after
		}
		body["retryable"] = we.Retryable()
		body["duplicateOnRetry"] = we.DuplicatesOnRetry()
		body["groupsFailed"] = we.FailedGroups
		body["groupsTotal"] = we.TotalGroups
		// On this path they are shard writers rather than groups.
		body["unit"] = we.Units()
	}
	s.countErr()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}

// splitCSV splits a comma-separated list, trimming spaces and dropping empties.
func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// registeredPaths records every path Handler() registers, in registration
// order. It exists so the route audit can enumerate the real mux instead of a
// hand-written list -- the list is what missed /_search and /_count.
// routeCount is the number of paths Handler() registered, so the audit can
// assert it saw all of them rather than compare against a constant.
func (s *Server) routeCount() int {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	return len(s.routes)
}

func (s *Server) registeredPaths() []string {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	return append([]string(nil), s.routes...)
}

// Handler wires the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.routeMu.Lock()
	s.routes = nil
	s.routeMu.Unlock()
	handle := func(pattern string, h http.HandlerFunc) {
		s.routeMu.Lock()
		s.routes = append(s.routes, pattern)
		s.routeMu.Unlock()
		mux.HandleFunc(pattern, h)
	}
	// Every ingest route is wrapped: method, media type and a bounded body.
	// Unwrapped, each handler read r.Body with io.ReadAll and no limit, took
	// any method, and ignored Content-Type entirely.
	nd := ndjsonSpec()
	// ingest is the role every write path needs; query for reads; admin for
	// the backup and diagnostic surfaces. in/rd/adm wrap guard with it.
	// Authentication is outermost, then the request-shape guard. An
	// unauthenticated caller gets 401 whatever it sends; with the guard
	// outside, a wrong Content-Type answered 415 first, which tells an
	// anonymous caller which media types a route accepts.
	in := func(spec routeSpec, h http.HandlerFunc) http.HandlerFunc {
		return s.requireAuth(config.RoleIngest, spec, s.guard(spec, s.checkStorage(spec, h)))
	}
	rd := func(h http.HandlerFunc) http.HandlerFunc {
		sp := readSpec()
		return s.requireAuth(config.RoleQuery, sp, s.guard(sp, h))
	}
	adm := func(h http.HandlerFunc) http.HandlerFunc {
		sp := adminSpec()
		return s.requireAuth(config.RoleAdmin, sp, s.guard(sp, h))
	}
	// st: reads neither a form nor a body -- the UI and the alerts page.
	st := func(h http.HandlerFunc) http.HandlerFunc {
		sp := staticSpec()
		return s.requireAuth(config.RoleQuery, sp, s.guard(sp, h))
	}
	// es: reads r.Body ITSELF, so nothing upstream may consume it.
	es := func(h http.HandlerFunc) http.HandlerFunc {
		sp := rawBodySpec()
		return s.requireAuth(config.RoleQuery, sp, s.guard(sp, h))
	}
	handle("/insert/jsonline", in(nd, s.insertJSONLine))
	handle("/insert/logfmt", in(nd, s.insertLogfmt))
	handle("/_bulk", in(nd, s.esBulk))                                                // Elasticsearch bulk ingest
	handle("/loki/api/v1/push", in(lokiSpec(), s.insertLoki))                         // Grafana Loki push
	handle("/api/v2/logs", in(nd, s.insertDatadog))                                   // Datadog logs intake
	handle("/v1/input", in(nd, s.insertDatadog))                                      // Datadog legacy intake
	handle("/insert/syslog", in(nd, s.insertSyslog))                                  // syslog over HTTP (native transport: ListenSyslog)
	handle("/v1/logs", in(specForPath("/v1/logs"), s.insertOTLPLogs))                 // OpenTelemetry OTLP/HTTP logs
	handle("/insert/journald", in(specForPath("/insert/journald"), s.insertJournald)) // systemd journal export
	// /insert/ready stays UNCONDITIONAL. A quarantined group is old data and
	// the store takes writes normally, so failing the ingest probe converts a
	// read-side loss into an ingest outage: agents stop shipping to a node
	// that would have accepted the writes, and the pod leaves the ingest
	// Service. docs/lld/api.md lists this path under "200 probes" and
	// cluster.go calls it a liveness probe; only /-/ready reflects storage
	// health.
	handle("/insert/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	// VictoriaLogs serves every third-party ingest protocol under /insert/<vendor>/.
	// An agent whose config was written against VictoriaLogs sends the prefixed
	// path, so serving only the vendor-native path 404s a drop-in client. Both
	// spellings are registered; the unprefixed ones are what the vendors' own
	// agents use when pointed at a bare host.
	handle("/insert/elasticsearch/_bulk", in(nd, s.esBulk))
	handle("/insert/loki/api/v1/push", in(lokiSpec(), s.insertLoki))
	handle("/insert/datadog/api/v2/logs", in(nd, s.insertDatadog))
	// Datadog agents call this to check their API key. Answering 200
	// unconditionally told every agent its key was valid; behind the ingest
	// role it now answers for credentials this server actually accepts.
	handle("/insert/datadog/api/v1/validate",
		in(specForPath("/insert/datadog/api/v1/validate"), func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	handle("/insert/opentelemetry/v1/logs", in(specForPath("/insert/opentelemetry/v1/logs"), s.insertOTLPLogs))
	handle("/admin/backup", adm(s.backup)) // tar snapshot for offline restore
	// Anti-entropy. The state and group endpoints are what a peer calls; the
	// repair endpoint is what an operator calls on a router. All three are
	// admin-authorized: the group endpoint WRITES into the store, and the state
	// endpoint discloses the shape of the data.
	handle(pathReplicaState, adm(s.serveReplicaState))
	handle(pathReplicaGroup, func() http.HandlerFunc {
		sp := replicaGroupSpec()
		return s.requireAuth(config.RoleAdmin, sp, s.guard(sp, s.serveReplicaGroup))
	}())
	handle("/admin/storage/quarantine", adm(s.listQuarantined)) // which group, why, how big
	handle("/admin/cluster/repair", adm(s.repairCluster))
	handle("/admin/cluster/backup", adm(s.clusterBackup))
	// Exempt from the query budget, for opsSpec's own argument with one word
	// changed: a scraper that gets 429 under load loses the telemetry that
	// explains the load, and an operator who gets 429 under load loses the
	// button that ENDS it. 32 in-flight queries is enough to make this return
	// "too many concurrent requests", and the replica it would have restored
	// is the one under that load.
	handle("/admin/acknowledge-degraded", func() http.HandlerFunc {
		sp := adminSpec()
		sp.nosem = true
		return s.requireAuth(config.RoleAdmin, sp, s.guard(sp, s.acknowledgeDegraded))
	}()) // accept a degraded store
	handle("/metrics", s.requireAuth(config.RoleMetrics, opsSpec(), s.guard(opsSpec(), s.metrics))) // Prometheus text exposition
	handle("/alerts", st(s.alertsHandler))                                                          // alerting rule state
	// /health and /-/healthy are LIVENESS: the process. They answered a
	// literal "OK" whatever the server was doing, including while draining --
	// so an orchestrator kept routing to a process that was going to exit.
	// Every store, disk and peer condition belongs to readiness: a full disk
	// that failed liveness would kill the process, which would restart onto
	// the same full disk and lose the rows a graceful drain flushes.
	handle("/health", s.healthHandler(healthLive))
	handle("/-/healthy", s.healthHandler(healthLive))
	handle("/-/ready", s.healthHandler(healthReady))
	handle("/flags", adm(s.flagsHandler)) // flag dump: administrative
	handle("/vmui", st(s.ui))             // web UI (vmui equivalent); same gate as /select/vmui
	handle("/select/vmui", st(s.ui))
	// The catch-all serves the same UI page /vmui and /select/vmui gate, so
	// it carries the same role. It was the one registration in this function
	// with no wrapper.
	handle("/", st(s.ui))
	handle("/select/logsql/query", rd(s.selectQuery))
	handle("/select/sql", rd(s.sqlQuery))        // SQL SELECT subset (beyond VL)
	handle("/select/vector", es(s.vectorSearch)) // k-NN over embeddings (beyond VL); body is a JSON document
	// The tail has no deadline: it is long-lived by design.
	handle("/select/logsql/tail", s.requireAuth(config.RoleQuery, tailSpec(), s.guard(tailSpec(), s.tail))) // live tail: stream matching rows as they arrive
	handle("/select/logsql/hits", rd(s.selectHits))
	handle("/select/logsql/field_names", rd(s.fieldNames))
	handle("/select/logsql/field_values", rd(s.fieldValues))
	handle("/select/logsql/facets", rd(s.facets))
	handle("/select/logsql/stats_query", rd(s.statsQuery))
	handle("/select/logsql/stats_query_range", rd(s.statsQueryRange))
	handle("/select/logsql/streams", rd(s.streamsHandler))
	handle("/select/logsql/stream_ids", rd(s.streamIDsHandler))
	handle("/select/logsql/stream_field_names", rd(s.streamFieldNamesHandler))
	handle("/select/logsql/stream_field_values", rd(s.streamFieldValuesHandler))
	// The Elasticsearch search surface VictoriaLogs lacks.
	// The Elasticsearch read surface returns tenant _source documents, so it
	// needs the query role like every other read route. Registered bare, it
	// also skipped the method guard, the media-type allowlist and the body
	// limit: an anonymous POST /_search returned another tenant's payloads.
	handle("/_search", es(s.esSearch))
	handle("/_count", es(s.esCount))
	// In router mode, writes forward to storage nodes (outermost, before the
	// tenant/local path); reads fall through to withTenant -> federatedSelect.
	// recoverPanic is outermost so one bad request can never take the server down.
	// The envelope is OUTERMOST on the serving side, so it is stamped before
	// any handler writes a byte. Headers set after the first write are
	// silently dropped, and a dropped Complete header reads as "the peer did
	// not say" -- which the router treats as not-complete, so a late stamp
	// would make every answer look partial.
	return recoverPanic(s.serveEnvelope(s.withPrincipal(s.routeWrites(s.withTenant(mux)))))
}

// recoverPanic turns a handler panic into a 500 and keeps the server serving --
// a single malformed request must never crash the process.
//
// http.ErrAbortHandler is re-panicked, not converted. It is net/http's sentinel
// meaning "abandon this response without a reply", and only net/http's own
// conn.serve honours it -- silently, without logging. Because this middleware
// is the OUTERMOST wrapper (see Handler above), swallowing it here made the
// sentinel unreachable: /admin/backup's abort became a 200 with "internal
// error" appended to a truncated tar, which is the exact failure the abort was
// added to prevent. A handler that needs to abandon a half-written response has
// no other mechanism, because the status and the first bytes are already gone.
func recoverPanic(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler {
				panic(v) // net/http handles this one; it must reach conn.serve
			}
			obs.L().Error("panic serving request",
				obs.FieldEvent, "http.panic",
				obs.FieldRoute, r.URL.Path,
				obs.FieldMethod, r.Method,
				obs.FieldErrorClass, string(obs.ClassInternal),
				"panic", v)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}()
		h.ServeHTTP(w, r)
	})
}

// insertJSONLine ingests an NDJSON body and flushes it into a group.
func (s *Server) insertJSONLine(w http.ResponseWriter, r *http.Request) {
	body, berr := s.readBody(w, r)
	if berr != nil {
		s.writeErr(w, r, ndjsonSpec(), berr.code, berr.msg)
		return
	}
	// Fallback timestamp for a line missing _time; atomic because the
	// parallel path calls it from many shard goroutines.
	tn := s.tn(r)
	fallback := tn.fallbackTS()
	opts := ingestOptions(r)
	var ing, skip int
	if len(body) >= ingest.MinParallelBytes {
		var werr error
		ing, skip, werr = ingest.IngestJSONLinesParallelCfg(tn.store, body, fallback, s.parallelCfg(), &opts)
		if werr != nil {
			// Some or all of the rows were parsed but did not reach the
			// store. Answering with a count alone is the silent data loss
			// this path used to have. The request fails -- but the rows that
			// DID land are durable and are reported and counted, because a
			// shipper retrying a bare 500 would otherwise duplicate them with
			// no way to know.
			s.failIngest(w, werr, ing, skip, len(body))
			return
		}
	} else {
		// Small body: reuse the persistent writer, no per-request pool churn.
		// Mark first: that writer's buffer is shared with every other
		// request on this tenant, so only FlushMark can say whether THESE
		// rows landed.
		mark := tn.w.Mark()
		res, perr := ingest.IngestJSONLinesOpts(tn.w, body, fallback, &opts)
		ing, skip = res.Accepted, res.Rejected
		if perr != nil {
			s.countRows(ing, skip, len(body))
			s.writeErr(w, r, ndjsonSpec(), ingest.StatusFor(perr), perr.Error())
			return
		}
		if err := tn.w.FlushMark(mark); err != nil {
			// Rows added AFTER Close are dropped silently -- Add returns
			// early once closed -- so counting them would inflate the
			// ingested total. Rows added BEFORE it are a different case:
			// Close flushes the shared buffer before it sets closed, so
			// those are durable and this under-counts them.
			//
			// Under-counting a metric is the safe side of that trade; the
			// alternative over-states durability, which is the claim this
			// whole task exists to stop being wrong about. The response
			// itself does not under-claim: the WriteError carries
			// Partial, so the client is told a retry may duplicate.
			if !errors.Is(err, ingest.ErrWriterClosed) {
				s.countRows(ing, skip, len(body))
			}
			s.writeFlushErr(w, r, ndjsonSpec(), err)
			return
		}
	}
	s.countRows(ing, skip, len(body))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"ingested": ing, "skipped": skip})
}

// insertLogfmt ingests a logfmt body (key=value lines) and flushes it.
func (s *Server) insertLogfmt(w http.ResponseWriter, r *http.Request) {
	body, berr := s.readBody(w, r)
	if berr != nil {
		s.writeErr(w, r, ndjsonSpec(), berr.code, berr.msg)
		return
	}
	tn := s.tn(r)
	lfOpts := ingestOptions(r)
	lfMark := tn.w.Mark()
	lfRes, lfErr := ingest.IngestLogfmtOpts(tn.w, body, tn.fallbackTS(), &lfOpts)
	if lfErr != nil {
		s.writeErr(w, r, ndjsonSpec(), ingest.StatusFor(lfErr), lfErr.Error())
		return
	}
	if err := tn.w.FlushMark(lfMark); err != nil {
		s.writeFlushErr(w, r, ndjsonSpec(), err)
		return
	}
	// After the flush: a counter must not go backwards.
	s.countRows(lfRes.Accepted, lfRes.Rejected, len(body))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"ingested": lfRes.Accepted, "skipped": lfRes.Rejected})
}

// selectQuery runs a parsed LogsQL query and streams matched rows as
// NDJSON, the reference's /select/logsql/query response shape.
// wroteSpy records whether any byte reached the ResponseWriter.
//
// It decides whether a failure can still be reported as a status. Once the
// first byte is out the status line is gone and a 4xx is no longer available;
// before that it still is, and the difference is not observable from the
// bufio.Writer in front of it, which may hold everything written so far.
type wroteSpy struct {
	w     io.Writer
	wrote bool
}

func (f *wroteSpy) Write(p []byte) (int, error) {
	f.wrote = true
	return f.w.Write(p)
}

// streamSelect answers a bare select without materializing it.
//
// Peak memory is one row group's matches (or a bounded window of them when the
// scan fans out), not the size of the answer. See internal/query/iterator.go.
//
// The hard part is not the streaming, it is the failure. An NDJSON body that
// stops early is indistinguishable from a complete one -- there is no length,
// no terminator, and every line already written parses. So a scan that fails
// after the first byte is out does NOT return: it aborts the connection, and
// the client sees a broken stream rather than a short answer it would have
// believed. Before the first byte the status is still available and the error
// is reported properly.
func (s *Server) streamSelect(w http.ResponseWriter, r *http.Request, q *query.Query, stopped *atomic.Bool) {
	// Counted, and exposed. Which path answered is not visible from the body
	// -- the two are byte-identical by construction -- so without a counter an
	// operator cannot tell whether raising -search.maxRows moved anything, and
	// a test cannot tell that it exercised this function at all rather than
	// the materialized one returning the same rows.
	atomic.AddInt64(&s.nStreamedSelects, 1)
	spy := &wroteSpy{w: w}
	// 64 KiB rather than bufio's default 4 KiB: this is a bulk transfer, and
	// the syscall count is the only thing the buffer size changes.
	bw := bufio.NewWriterSize(spy, 64<<10)
	w.Header().Set("Content-Type", ndjsonContentType)

	var buf []byte
	err := query.ScanEach(s.tn(r).store, q, func(rows []query.Row) error {
		for _, row := range rows {
			buf = appendRowJSON(buf[:0], row, q.MatAll)
			if _, werr := bw.Write(buf); werr != nil {
				// The client hung up. Returned rather than swallowed, so the
				// scan stops instead of filling a buffer nobody reads.
				return werr
			}
		}
		return nil
	})
	if err == nil {
		err = bw.Flush()
	}
	// ScanEach returns the query's own stop reason, so a budget or
	// cancellation stop arrives as err. The atomic is checked too because it
	// is the signal the materialized path uses and the two must not disagree.
	if err == nil && stopped != nil && stopped.Load() {
		if e := q.StopErr(); e != nil {
			err = e
		} else {
			err = query.ErrDeadlineExceeded
		}
	}
	if err == nil {
		return
	}
	if !spy.wrote {
		// Nothing is on the wire yet, so the status line is still available
		// and the failure is reported the same way the materialized path
		// reports it -- same codes, same remedies.
		s.queryStoppedErr(w, r, stopped, q)
		if stopped == nil || !stopped.Load() {
			s.writeErr(w, r, readSpec(), query.HTTPStatus(err), err.Error())
		}
		return
	}
	// Bytes are already out. An NDJSON body that stops early parses line for
	// line and carries no length and no terminator, so returning here would
	// hand the client a short answer it has no way to tell from a complete
	// one. Aborting the connection is the only signal left.
	panic(http.ErrAbortHandler)
}

// pagedSelect answers one page of a stable total order.
//
// The cursor is opaque and signed; see cursor.go for why. What it costs the
// caller is one rule: page until `More` is false, and do not edit the token.
func (s *Server) pagedSelect(w http.ResponseWriter, r *http.Request, q *query.Query,
	stopped *atomic.Bool, size int,
) {
	dir := query.Oldest
	if v := r.FormValue("direction"); v == "newest" {
		dir = query.Newest
	} else if v != "" && v != "oldest" {
		http.Error(w, "simdlogs: direction must be `oldest` or `newest`", 400)
		return
	}
	// The window is resolved BEFORE the hash, because a relative window
	// (`_time:5m`) is a different absolute window on every request and a
	// cursor bound to the unresolved text would slide forward as the clock
	// moves -- repeating rows the caller has seen and skipping ones it has
	// not.
	query.ResolveWindow(q)
	qh := queryHash(r.FormValue("query"), q.From, q.To)
	tenant := tenantKeyOf(r)

	var after *query.RowKey
	if tok := r.FormValue("cursor"); tok != "" {
		k, err := s.cursors.decode(tok, tenant, qh, dir)
		if err != nil {
			// 400, not 403. A cursor that names another tenant is a client
			// holding a stale or hand-edited token, not an authorization
			// decision about this request -- the tenant middleware already
			// made that one, and answering 403 here would tell a prober that
			// the cursor it forged was otherwise well-formed.
			http.Error(w, err.Error(), 400)
			return
		}
		after = &k
	}

	page, err := query.ScanPage(s.tn(r).store, q, after, dir, size)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	if err != nil {
		s.writeErr(w, r, readSpec(), query.HTTPStatus(err), err.Error())
		return
	}
	if page.More {
		// The cursor goes in a header, not in the body: the body is NDJSON,
		// one JSON object per row, and a trailing object of a different shape
		// is a row as far as every client that reads the stream is concerned.
		w.Header().Set("X-Simdlogs-Cursor", s.cursors.encode(cursorPayload{
			tenant: tenant, queryHash: qh, dir: dir, key: page.Next,
		}))
	}
	w.Header().Set("Content-Type", ndjsonContentType)
	bw := bufio.NewWriterSize(w, 64<<10)
	defer bw.Flush()
	var buf []byte
	for _, row := range page.Rows {
		buf = appendRowJSON(buf[:0], row, q.MatAll)
		bw.Write(buf)
	}
}

func (s *Server) selectQuery(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 { // select-router: fan out to the storage nodes and merge
		s.federatedSelect(w, r)
		return
	}
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	bareSelect := len(q.Pipes) == 0
	if bareSelect || !query.PipesProject(q.Pipes) {
		// A bare select returns whole records -- and so does one whose pipes only
		// slice or reorder them (limit/head/sort/offset). Only a pipe that projects
		// or aggregates (fields/stats/uniq/...) narrows the output, so anything else
		// must still materialize every column, which is what VictoriaLogs returns.
		q.MatAll = true
		// Bound peak memory on an unbounded select, then ERROR rather than silently
		// truncate. MaxRows (not Limit) so the scan stays PARALLEL -- Limit forces
		// the serial path because it must return the first N in time order, while
		// MaxRows only has to detect overflow. Only a bare select: a stats/pipe
		// query's input must not be bounded, and an explicit limit= is respected.
		if q.Limit == 0 && s.maxRows > 0 {
			q.MaxRows = s.maxRows
		}
	}
	stopped := s.applyQueryBudget(r, q)
	if n := intParam(r, "page_size", 0); n > 0 {
		// Pagination is opt-in per request. Without page_size the endpoint
		// answers exactly as it did -- the ordering below is a total order the
		// old path never promised, and imposing it on every select would
		// change answers this campaign's whole point is not to change.
		s.pagedSelect(w, r, q, stopped, n)
		return
	}
	if query.Streamable(q) {
		// No pipes, no `limit=`, and no row cap in force -- so nothing has to
		// see the whole answer before the first byte can go out. That is
		// exactly the query that used to be dangerous: with
		// -search.maxRows=-1 a bare select materialized every matching row
		// before writing any of them, so an answer that did not fit was not
		// slow, it was impossible.
		s.streamSelect(w, r, q, stopped)
		return
	}
	rows := query.RunPipeline(s.tn(r).store, q) // applies the pipe chain; == Run when there are none
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	if s.maxRows > 0 && len(rows) > s.maxRows {
		// The belt to the scan's braces. The scan now records ErrRowLimit when
		// it trips the cap, and queryStoppedErr above reports it -- but a pipe
		// chain can also GROW its input past the cap without the scan ever
		// tripping (a join that multiplies, a union that appends), and that
		// result is as unbounded as the one the cap exists to refuse.
		//
		// Not `bareSelect &&` any more. That was the defect: the HTTP layer
		// set MaxRows for every non-projecting chain and reported the overflow
		// for exactly one of them, so `| sort`, `| offset`, `| rename`,
		// `| format`, `| join` and `| union` each answered from an input the
		// scan had silently cut. A sort of the first N rows is not the first N
		// of the sort.
		s.writeErr(w, r, readSpec(), http.StatusRequestEntityTooLarge,
			fmt.Sprintf("simdlogs: result exceeds -search.maxRows=%d; add a `| limit N`, "+
				"a stats pipe, or narrow the query", s.maxRows))
		return
	}
	// NDJSON, so say so: this is what the reference sends, and a client that
	// switches on Content-Type was previously told text/plain for a stream of
	// JSON objects. Set before the first write -- after it the header is
	// already on the wire and Set is silently ignored.
	w.Header().Set("Content-Type", ndjsonContentType)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	// Hand-built NDJSON: no map[string]any, no reflection. The engine
	// produces rows in ~1.5ms; the reflective encoder was doubling the
	// wire time, so the result bytes are appended directly here.
	var buf []byte
	for _, row := range rows {
		buf = appendRowJSON(buf[:0], row, q.MatAll)
		bw.Write(buf)
	}
}

// appendRowJSON serializes one result row as an NDJSON object (trailing
// newline). Shared by the batch select and the live tail so both emit the
// identical wire shape.
func appendRowJSON(buf []byte, row query.Row, withStream bool) []byte {
	buf = append(buf, '{')
	first := true
	if !row.NoTime { // a stats row or a projection without _time carries no timestamp
		buf = append(buf, `"_time":"`...)
		buf = time.Unix(0, row.Time).UTC().AppendFormat(buf, time.RFC3339Nano)
		buf = append(buf, '"')
		first = false
	}
	stream, streamID := "", ""
	sawStream, sawStreamID := false, false
	// AT MOST ONCE, whichever source it comes from.
	//
	// A NoTime row can carry TWO `_time` fields -- `rename x as _time` does
	// not overwrite an existing key and `copy x as _time` does not either --
	// and the first version of this guard kept both, so
	// `* | stats by (_time) count() c | copy _time as t2 | rename t2 as _time`
	// emitted {"_time":"…","c":"1","_time":"…"}: one JSON object with a
	// duplicate key, which every decoder resolves differently. The old
	// unconditional skip dropped both, which was the other defect.
	emittedTime := !row.NoTime
	for _, f := range row.Fields {
		// Conditional on the emit above, exactly as the pack is.
		//
		// THE THIRD COPY of this pattern and the one on the wire. packJSON and
		// packLogfmt were fixed to keep a `_time` FIELD that a NoTime row
		// carries; the serializer they are supposed to mirror still dropped
		// it, so a response contained both answers at once:
		//
		//	* | stats by (_time) count() c
		//	  VL        {"_time":"2026-08-16T03:00:00Z","c":"1"}
		//	  this      {"c":"1"}
		//	  ... | pack_json as p
		//	            {"c":"1","p":"{\"_time\":\"…\",\"c\":\"1\"}"}
		//
		// which is verbatim the failure the pack fix existed to remove -- "a
		// client reading `p` got a different record from a client reading the
		// row, out of one response" -- inverted. Reachable through
		// `stats by (_time)`, `rename x as _time`, `copy x as _time` and the
		// router's jsonLineToRow.
		if f.Key == "_time" {
			if emittedTime {
				continue
			}
			emittedTime = true
		}
		// A NON-EMPTY value counts as present. The store materializes a column
		// for the whole group, so a row that never carried _stream_id comes
		// back with "" once any row in its flush did -- and treating that as
		// present suppressed the synthesis for every other row in the flush.
		// One client-supplied value blanked the field group-wide, at HTTP 200,
		// on the field a client groups and colours by.
		if f.Key == "_stream" && f.Value != "" {
			stream, sawStream = f.Value, true
		}
		if f.Key == "_stream_id" && f.Value != "" {
			streamID, sawStreamID = f.Value, true
		}
		if withStream && (f.Key == "_stream" || f.Key == "_stream_id") {
			// Emitted once, below, from the row's value or the synthesized
			// one. Emitting it here as well is what produced the duplicate
			// key; skipping it here and synthesizing unconditionally is what
			// dropped the ingested value at the shard. One place decides.
			continue
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = append(buf, '"')
		buf = appendJSONString(buf, f.Key)
		buf = append(buf, '"', ':', '"')
		buf = appendJSONString(buf, f.Value)
		buf = append(buf, '"')
	}
	// A full record carries its stream membership, which is what a client groups
	// and colours by. With no stream fields configured every row is in the empty
	// stream -- that is still a stream, and omitting the pair left a client's
	// stream column blank rather than uniform.
	if withStream {
		// Exactly one _stream and one _stream_id, the row's own value when it
		// has one and the synthesized value when it does not.
		//
		// Both were guarded, differently, and both were wrong: _stream tested
		// the VALUE (`stream == ""`) and duplicated the key for a row carrying
		// an empty one; _stream_id tested PRESENCE and blanked the field for
		// every row sharing a group with one that carried it. Four lines apart,
		// in one function.
		if !sawStream {
			stream = query.EmptyStream
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = append(buf, `"_stream":"`...)
		buf = appendJSONString(buf, stream)
		buf = append(buf, '"')
		if !sawStreamID {
			if stream == query.EmptyStream {
				streamID = string(emptyStreamID) // hashed once, not per row
			} else {
				streamID = query.StreamID(stream)
			}
		}
		buf = append(buf, ',')
		buf = append(buf, `"_stream_id":"`...)
		buf = appendJSONString(buf, streamID)
		buf = append(buf, '"')
	}
	buf = append(buf, '}', '\n')
	return buf
}

// emptyStreamID is the id of the empty stream, the value nearly every row gets.
var emptyStreamID = query.StreamID(query.EmptyStream)

// readerStore adapts a fixed set of group readers to the query.Store
// interface, so the live tail can run the ordinary filter/materialize path
// over just the groups that arrived since the last poll.
type readerStore []*storage.Reader

// Snapshot hands back the fixed readers. They are already owned by the
// caller's own snapshot for the duration of this call -- the live tail holds
// one while it drains -- so this adapter takes no further reference and its
// Close is a no-op.
func (rs readerStore) Snapshot(_, _ int64) (*storage.Snapshot, error) {
	return &storage.Snapshot{Groups: rs}, nil
}

// tail streams matching rows as new groups are ingested: VictoriaLogs'
// /select/logsql/tail. It subscribes at the current high-water group id and
// polls for later ones, running the LogsQL filter over each and flushing
// matches as NDJSON. The connection lives until the client disconnects.
func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
	if s.refuseInRouterMode(w, r, "live tail",
		"a cluster tail is a long-lived stream from every shard merged by arrival "+
			"time, and the merge has no completeness signal: a shard that stops "+
			"answering drops out of the stream with nothing to say so") {
		return
	}
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q.From, q.To = 0, int64(1)<<62 // live: match every timestamp in the new groups
	q.Pipes = nil                  // tail streams raw records; a stats/sort pipe would never terminate
	q.Limit = 0
	q.MatAll = true // whole records, like the batch select
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Accel-Buffering", "no") // don't let a proxy buffer the stream
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // send headers now so the client's request returns and it can read
	atomic.AddInt64(&s.nTails, 1)
	defer atomic.AddInt64(&s.nTails, -1)
	store := s.tn(r).store
	// The store pointer is all this loop needs from here on, and every read
	// through it takes its own lease. Holding the tenant claim for the life
	// of the connection is what let a handful of anonymous tails fill the
	// tenant table and 503 everyone else.
	tenantRefOf(r).release()
	cursor := store.TailCursor() // only groups that arrive after we subscribe
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	bw := bufio.NewWriter(w)
	ctx := r.Context()
	var buf []byte

	// Replay the recent window before streaming, the way the reference does: a
	// client that opens a live tail expects to see the last few seconds of the
	// stream immediately, not a blank pane until the next record happens to
	// arrive. start_offset names how far back to begin.
	offset := 5 * time.Second
	if v := r.FormValue("start_offset"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			offset = d
		}
	}
	// The backlog is a batch query, not a stream: start_offset is a
	// client-supplied duration, so without a budget one request replayed the
	// whole store, and MaxConcurrentTail (64 by default) was the only bound
	// on how many did it at once. tailSpec drops the deadline for the
	// STREAM, which is right; this part is not the stream.
	backlog := *q
	backlog.From = time.Now().Add(-offset).UnixNano()
	backlog.To = int64(1) << 62
	backlogStopped := s.applyQueryBudget(r, &backlog)
	if n := s.maxRows; n > 0 && backlog.MaxRows == 0 {
		backlog.MaxRows = n
	}
	replayed := query.RunPipeline(store, &backlog)
	for _, row := range replayed {
		buf = appendRowJSON(buf[:0], row, backlog.MatAll)
		bw.Write(buf)
	}
	// MaxRows exits Run early WITHOUT setting Stopped -- that path is the
	// one the batch select turns into a 400 by comparing len(rows). The tail
	// has no such comparison, so the replay was silently short.
	rowCap := backlog.MaxRows > 0 && len(replayed) > backlog.MaxRows
	if rowCap || (backlogStopped != nil && backlogStopped.Load()) {
		// The replay hit its budget. The headers are already sent, so this
		// cannot be a 504 -- say so in the stream and start tailing, which
		// is what the client asked for.
		line, _ := json.Marshal(map[string]string{
			".error": "the backlog replay was cut short (budget or row cap); " +
				"narrow start_offset. Live tailing continues.",
		})
		bw.Write(line)
		bw.WriteByte('\n')
	}
	bw.Flush()
	flusher.Flush()
	for {
		// SnapshotAfterID, not GroupsAfterID: the latter hands out raw
		// *Reader values with no reference taken, so retention could unmap a
		// group this loop was still decoding. Task 4.1 built the leased form
		// and nothing called it.
		snap, nc, serr := store.SnapshotAfterID(cursor)
		if serr != nil {
			// The store is closing -- shutdown, or this tenant being evicted
			// out from under the stream. Returning silently gave the client
			// a clean end-of-stream on a 200, indistinguishable from a
			// normal close, with no way to know it must reconnect.
			// Not "_stream_error": nothing reserves that name, so a stored
			// row can carry it and a client cannot tell a marker from a log
			// line. The leading dot is not a legal field name here, and the
			// line is encoded rather than concatenated so an error text with
			// a quote in it cannot break the stream's framing.
			line, _ := json.Marshal(map[string]string{
				".error": "the log stream ended: " + serr.Error() + "; reconnect to resume",
			})
			bw.Write(line)
			bw.WriteByte('\n')
			bw.Flush()
			flusher.Flush()
			return
		}
		// The lease is released in a closure, so the defer runs per
		// iteration. A defer written directly in the loop body fires only at
		// function return: the panic-safety held, but every poll pushed
		// another closure retaining a *Snapshot, so a day-long tail at two
		// polls a second accumulated ~172,800 of them.
		func() {
			defer snap.Close()
			readers := snap.Groups
			if len(readers) > 0 {
				cursor = nc
				for _, row := range query.RunPipeline(readerStore(readers), q) {
					buf = appendRowJSON(buf[:0], row, q.MatAll)
					bw.Write(buf)
				}
				bw.Flush()
				flusher.Flush()
			}
		}()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sqlQuery runs a SQL SELECT (subset) by translating it to LogsQL -- the query
// interface VictoriaLogs does not have. Results stream as NDJSON like the
// LogsQL select.
func (s *Server) sqlQuery(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 { // select-router: federate the row-local case, refuse the rest
		s.federatedSQL(w, r)
		return
	}
	q, err := query.ParseSQL(r.FormValue("query"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q.From, q.To = timeWindow(r)
	if len(q.Pipes) == 0 || !query.PipesProject(q.Pipes) {
		q.MatAll = true
		// The same row cap the LogsQL select gets. SQL had none at all: a
		// `SELECT * FROM logs` with no LIMIT materialized every matching row,
		// and `ORDER BY` over an unbounded input is the exact shape task 6.4
		// was about -- with the difference that nothing here even bounded it.
		//
		// An explicit LIMIT becomes a `| limit` pipe, which RunPipeline pushes
		// into the scan, so a bounded query is answered rather than refused.
		if q.Limit == 0 && s.maxRows > 0 {
			q.MaxRows = s.maxRows
		}
	}
	// The scan BEFORE the header. This handler used to set Content-Type and
	// take a writer first, so a budget stop wrote its status into a response
	// already committed to NDJSON -- and it used queryStopped rather than
	// queryStoppedErr, so every cause reported the generic 504 whatever
	// actually stopped it.
	sqlStopped := s.applyQueryBudget(r, q)
	sqlRows := query.RunPipeline(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, sqlStopped, q) {
		return
	}
	if s.maxRows > 0 && len(sqlRows) > s.maxRows {
		s.writeErr(w, r, readSpec(), http.StatusRequestEntityTooLarge,
			fmt.Sprintf("simdlogs: result exceeds -search.maxRows=%d; add a LIMIT, "+
				"an aggregate, or narrow the query", s.maxRows))
		return
	}
	w.Header().Set("Content-Type", ndjsonContentType)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	var buf []byte
	for _, row := range sqlRows {
		buf = appendRowJSON(buf[:0], row, q.MatAll)
		bw.Write(buf)
	}
}

// vectorSearch runs cosine k-NN over an embedding column -- semantic/vector log
// search (beyond VL). Body: {"field":"emb","vector":[...],"k":10}; the time
// window comes from start/end params. Embeddings are bring-your-own (logs carry
// a vector column).
func (s *Server) vectorSearch(w http.ResponseWriter, r *http.Request) {
	if s.refuseInRouterMode(w, r, "vector search",
		"a k-nearest-neighbour search over shards needs each shard's top k merged "+
			"by distance, and returning one shard's neighbours or concatenating "+
			"them both answer a different question") {
		return
	}
	var body struct {
		Field  string    `json:"field"`
		Vector []float32 `json:"vector"`
		K      int       `json:"k"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if body.Field == "" {
		body.Field = "emb"
	}
	from, to := timeWindowURL(r) // the body is the document; see timeWindowURL
	vq := &query.Query{From: from, To: to}
	vStopped := s.applyQueryBudget(r, vq)
	rows := query.VectorSearch(s.tn(r).store, from, to, body.Field, body.Vector, body.K,
		vq, query.VectorLimits{
			MaxK:           s.limits.MaxVectorK,
			MaxDim:         s.limits.MaxVectorDim,
			MaxCandidates:  s.limits.MaxVectorCandidates,
			MaxResultBytes: s.limits.MaxQueryBytes,
		})
	// Before the header and the first byte: a refusal has to be reportable,
	// and this handler used to set Content-Type and take a bufio.Writer
	// BEFORE the search ran, so a budget stop wrote its status into a response
	// already committed to NDJSON.
	if s.queryStoppedErr(w, r, vStopped, vq) {
		return
	}
	w.Header().Set("Content-Type", ndjsonContentType)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	var buf []byte
	for _, row := range rows {
		buf = appendRowJSON(buf[:0], row, false)
		bw.Write(buf)
	}
}

// selectHits returns per-bucket counts over the time window: the
// reference's /select/logsql/hits shape (a histogram for dashboards).
// maxHitsBuckets bounds a histogram response.
//
// 10,000 buckets is more than any graph can draw -- a 1920-pixel wide chart has
// 1920 columns -- and small enough that the dense array stays a few hundred
// kilobytes. Past it the caller is asking for a shape no renderer uses.
const maxHitsBuckets = 10_000

// defaultHitsBuckets is the window a request with no time range gets, measured
// in steps. Enough to draw a graph, far short of the ceiling.
const defaultHitsBuckets = 240

func (s *Server) selectHits(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedHits(w, r)
		return
	}
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	step := int64(60_000_000_000) // 1 minute default
	if v := r.FormValue("step"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			step = int64(d)
		}
	}
	// `field` (repeatable in the reference, one here) splits the histogram into
	// a series per value, which is how a dashboard draws a stacked graph.
	by := r.FormValue("field")
	if by == "" {
		by = r.FormValue("fields")
	}
	// The bucket count is bounded BEFORE the scan.
	//
	// The response is dense -- one bucket per step across the whole window,
	// present or not -- so its size is (window / step) and has nothing to do
	// with how much data exists. With no window and the default one-minute
	// step that is a bucket per minute since 1970: about 29 million of them,
	// tens of megabytes of RFC3339 timestamps, from an EMPTY store.
	//
	// An UNSPECIFIED window is defaulted rather than refused. A caller that
	// named no range did not ask for all of history, it just did not say; and
	// answering an unstated question with a 413 breaks every client that was
	// getting away with it. An EXPLICIT range too wide for its step is a
	// refusal, because that one was asked for.
	explicit := r.FormValue("start") != "" || r.FormValue("end") != ""
	if step > 0 {
		if !explicit && (q.To-q.From)/step > maxHitsBuckets {
			q.To = time.Now().UnixNano()
			q.From = q.To - step*defaultHitsBuckets
		}
		if n := (q.To - q.From) / step; n > maxHitsBuckets {
			s.writeErr(w, r, readSpec(), http.StatusRequestEntityTooLarge, fmt.Sprintf(
				"simdlogs: %d buckets requested (window %s at step %s); the maximum is %d. "+
					"Narrow the time range or increase the step -- the response is dense, "+
					"so its size is the window divided by the step and does not depend on "+
					"how much data matched",
				n, time.Duration(q.To-q.From), time.Duration(step), maxHitsBuckets))
			return
		}
	}
	stopped := s.applyQueryBudget(r, q)
	series := query.Hits(s.tn(r).store, q, step, by)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	// fields_limit keeps the busiest N series and folds the rest into one
	// unlabelled remainder, so a graph of a high-cardinality field stays
	// readable instead of returning a series per value.
	series = foldHitsTail(series, intParam(r, "fields_limit", 0))

	// The reference shape: a dense timestamp/value pair of arrays per series,
	// not a bag of {time, count} objects. A client indexes the two arrays
	// together, so the buckets must be ascending and gap-free.
	type hitSeries struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []string          `json:"timestamps"`
		Values     []int             `json:"values"`
		Total      int               `json:"total"`
	}
	out := make([]hitSeries, 0, len(series))
	for _, se := range series {
		ts := make([]string, 0, len(se.Timestamps))
		for _, t := range se.Timestamps {
			ts = append(ts, time.Unix(0, t).UTC().Format(time.RFC3339Nano))
		}
		if se.Fields == nil {
			se.Fields = map[string]string{}
		}
		out = append(out, hitSeries{Fields: se.Fields, Timestamps: ts, Values: se.Values, Total: se.Total})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"hits": out})
}

// parseRequest turns the LogsQL query and time params into a Query.
func parseRequest(r *http.Request) (*query.Query, error) {
	// The reference requires `query` on every select endpoint and rejects a
	// request without one. Defaulting to match-all answered a client's bug with
	// the entire store.
	raw := r.FormValue("query")
	if strings.TrimSpace(raw) == "" {
		return nil, errMissingQuery
	}
	q, err := query.ParseLogsQL(raw)
	if err != nil {
		return nil, err
	}
	q.Now = time.Now().UnixNano() // request time, for relative _time:<dur> filters
	if v := r.FormValue("start"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			q.From = n
		}
	}
	if v := r.FormValue("end"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			q.To = n
		}
	}
	if q.To == 0 {
		q.To = int64(1) << 62
	}
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			// The endpoint's `limit` is the most RECENT n, newest first -- what a
			// log viewer shows. The `| limit n` pipe is the other one, the first
			// n, and sets q.Limit; conflating them returned the oldest rows in
			// the store to a client asking for the tail of the stream.
			q.LastN = n
		}
	}
	return q, nil
}

// parseTimeParam accepts a unix-nanoseconds integer or an RFC3339 string
// (the format VictoriaLogs uses), returning nanoseconds. Accepting both
// lets the head-to-head hand both engines the identical window string.
func parseTimeParam(v string) (int64, bool) {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return unixToNanos(n), true
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil { // seconds with a fractional part
		return int64(f * 1e9), true
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339,
		"2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02", "2006-01", "2006",
	} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UnixNano(), true
		}
	}
	return 0, false
}

// unixToNanos infers the unit of a bare unix timestamp from its magnitude, the
// way VictoriaLogs does: seconds, milliseconds, microseconds or nanoseconds.
// Each boundary is around the year 5138 in the smaller unit, so no realistic
// timestamp is misread. Reading every bare integer as nanoseconds -- which this
// did -- put a Grafana datasource's epoch-seconds window in 1970 and answered
// every query empty.
func unixToNanos(n int64) int64 {
	switch {
	case n < 0:
		return n
	case n < 1e11:
		return n * int64(time.Second)
	case n < 1e14:
		return n * int64(time.Millisecond)
	case n < 1e17:
		return n * int64(time.Microsecond)
	default:
		return n
	}
}

// writeValues emits the {"values":[{"value":..,"hits":..}]} envelope. Six
// endpoints share it -- field_names, field_values, stream_field_names,
// stream_field_values, stream_ids and streams -- so a client decodes them all
// with one type, which is why they must not each invent a key.
func writeValues(w http.ResponseWriter, vcs []query.ValueCount) {
	if vcs == nil {
		vcs = []query.ValueCount{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"values": vcs})
}

// selectQueryOf parses the request's LogsQL and window for the introspection
// endpoints, which scope their answer to the matching rows. A missing query
// means every row, spelled the way LogsQL spells it.
func selectQueryOf(r *http.Request) (*query.Query, error) { return parseRequest(r) }

// errMissingQuery is the empty-`query` rejection, spelled once so every select
// endpoint answers it the same way.
var errMissingQuery = errors.New("simdlogs: missing `query` arg")

func (s *Server) fieldNames(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/field_names")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vals := query.FieldNameCounts(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	writeValues(w, limitValues(vals, r))
}

func (s *Server) fieldValues(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/field_values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vcs := query.StatsByField(s.tn(r).store, q, r.FormValue("field"))
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	query.SortValueCounts(vcs)
	if n := intParam(r, "limit", 0); n > 0 && len(vcs) > n {
		vcs = vcs[:n]
	}
	writeValues(w, vcs)
}

func (s *Server) facets(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 { // select-router: a facet over the router's own store is empty
		s.federatedFacets(w, r)
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	facets := query.FacetList(s.tn(r).store, q,
		intParam(r, "limit", query.DefaultFacetLimit),
		intParam(r, "max_values_per_field", query.DefaultFacetMaxValues),
		r.FormValue("keep_const_fields") == "1")
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	if facets == nil {
		facets = []query.FieldFacet{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"facets": facets})
}

// statsQuery answers a stats query at a single instant: the Prometheus vector
// envelope, so the same dashboard panel that graphs a range can read a value.
func (s *Server) statsQuery(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedStatsQuery(w, r)
		return
	}
	from, to := timeWindow(r)
	// The reference's stats_query is an INSTANT query: `time` names the end of
	// the window, and start/end are the extension. A client that sends only
	// `time` got the whole store here before.
	if v := r.FormValue("time"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			to = n
		}
	}
	if to == int64(1)<<62 {
		to = time.Now().UnixNano()
	}
	sq := &query.Query{From: from, To: to}
	stopped := s.applyQueryBudget(r, sq)
	samples, err := query.StatsQueryInstant(s.tn(r).store, r.FormValue("query"), from, to, time.Now().UnixNano(), sq)
	if s.queryStoppedErr(w, r, stopped, sq) {
		return
	}
	if err != nil {
		// A query with no stats pipe has no series; the group-by form below is
		// the older shape and still answers it.
		if by := r.FormValue("by"); by != "" {
			q, perr := selectQueryOf(r)
			if perr != nil {
				http.Error(w, perr.Error(), 400)
				return
			}
			// `by=` is an extension the reference has no equivalent for, so it
			// keeps its own key rather than pretending to be a Prometheus vector.
			// It re-parses, so it needs its own budget: the one applied above
			// belongs to the Query that failed, and without this the fallback
			// answered a COMPLETE 200 with the deadline already spent.
			byStopped := s.applyQueryBudget(r, q)
			vcs := query.StatsByField(s.tn(r).store, q, by)
			if s.queryStopped(w, r, byStopped) {
				return
			}
			query.SortValueCounts(vcs)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"stats": vcs})
			return
		}
		http.Error(w, err.Error(), 400)
		return
	}
	result := make([]map[string]any, 0, len(samples))
	for _, sm := range samples {
		result = append(result, map[string]any{
			"metric": sm.Metric,
			"value":  [2]any{to / 1e9, sm.Value},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(promResponse{
		Status: "success",
		Data:   promData{ResultType: "vector", Result: result},
	})
}

// ndjsonContentType is the media type every endpoint that streams NDJSON
// rows announces. One constant because the single-node select and the
// router's federatedSelect answer the SAME path: they disagreed before this,
// text/plain against application/x-ndjson, so a client's behaviour depended on
// which deployment mode it happened to be talking to.
const ndjsonContentType = "application/x-ndjson"

// promResponse is the Prometheus query envelope both stats endpoints return.
// A struct rather than a map so the fields keep the reference's order on the
// wire; JSON object order carries no meaning, but a byte-comparable body makes
// a difference visible in the diff instead of hiding in it.
type promResponse struct {
	Status string   `json:"status"`
	Data   promData `json:"data"`
}

type promData struct {
	ResultType string           `json:"resultType"`
	Result     []map[string]any `json:"result"`
}

// foldHitsTail keeps the n series with the most hits and merges everything
// after them into a single series with no labels -- the "other" bucket the
// reference returns.
func foldHitsTail(series []query.HitsSeries, n int) []query.HitsSeries {
	if n <= 0 || len(series) <= n {
		return series
	}
	sort.SliceStable(series, func(i, j int) bool { return series[i].Total > series[j].Total })
	rest := series[n:]
	other := query.HitsSeries{Fields: map[string]string{}}
	for _, se := range rest {
		if other.Timestamps == nil {
			other.Timestamps = append([]int64(nil), se.Timestamps...)
			other.Values = make([]int, len(se.Values))
		}
		for i := range se.Values {
			if i < len(other.Values) {
				other.Values[i] += se.Values[i]
			}
		}
		other.Total += se.Total
	}
	return append(series[:n:n], other)
}

// limitValues applies the request's `limit` to a values response. Every values
// endpoint in the reference honours it, and a dashboard that asks for ten
// values should not be sent ten thousand.
func limitValues(vcs []query.ValueCount, r *http.Request) []query.ValueCount {
	if n := intParam(r, "limit", 0); n > 0 && len(vcs) > n {
		return vcs[:n]
	}
	return vcs
}

// intParam reads a non-negative integer form value, or def when absent or bad.
func intParam(r *http.Request, name string, def int) int {
	if v := r.FormValue(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// streamsHandler lists the distinct _stream label sets in the window.
func (s *Server) streamsHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/streams")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vals := query.Streams(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	writeValues(w, limitValues(vals, r))
}

// streamIDsHandler lists the distinct stream ids in the window.
func (s *Server) streamIDsHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_ids")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vals := query.StreamIDs(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	writeValues(w, limitValues(vals, r))
}

// streamFieldNamesHandler lists the distinct stream label names.
func (s *Server) streamFieldNamesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_field_names")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vals := query.StreamFieldNames(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	writeValues(w, limitValues(vals, r))
}

// streamFieldValuesHandler lists the distinct values of one stream label.
func (s *Server) streamFieldValuesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_field_values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	vals := query.StreamFieldValues(s.tn(r).store, q, r.FormValue("field"))
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	writeValues(w, limitValues(vals, r))
}

// statsQueryRange buckets a stats query over the time range and returns a
// Prometheus-style matrix: one series per group-by tuple, a point per step.
func (s *Server) statsQueryRange(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedMatrix(w, r)
		return
	}
	// A MISSING query is refused, as it is on every other select endpoint.
	//
	// The raw string went through unchecked, so this route answered 200 with a
	// fabricated matrix:
	//
	//	GET /select/logsql/stats_query_range          (no parameters at all)
	//	  200 {"resultType":"matrix","result":[{"metric":{},
	//	       "values":[[1690951540,""],[1690951540,""],[1690951540,""]]}]}
	//
	// A constant garbage epoch and empty-string values, from a request that
	// asked nothing. docs/lld/api.md says `query` is required on every select
	// endpoint and a request without one is a 400; this was the route where
	// that was false, and it made a router (which does refuse) disagree with
	// the node it fronts.
	if strings.TrimSpace(r.FormValue("query")) == "" {
		http.Error(w, errMissingQuery.Error(), 400)
		return
	}
	from, to := timeWindow(r)
	step := parseStepNs(r.FormValue("step"), from, to)
	rq := &query.Query{From: from, To: to}
	rStopped := s.applyQueryBudget(r, rq)
	series, err := query.StatsQueryRange(s.tn(r).store, r.FormValue("query"), from, to, step, time.Now().UnixNano(), rq)
	if s.queryStopped(w, r, rStopped) {
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	result := make([]map[string]any, 0, len(series))
	for _, se := range series {
		vals := make([][2]any, 0, len(se.Values))
		for _, v := range se.Values {
			ts, _ := strconv.ParseInt(v[0], 10, 64)
			vals = append(vals, [2]any{ts, v[1]})
		}
		result = append(result, map[string]any{"metric": se.Metric, "values": vals})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(promResponse{
		Status: "success",
		Data:   promData{ResultType: "matrix", Result: result},
	})
}

// parseStepNs reads the `step` param (a duration like "5m" or bare seconds),
// defaulting to 1/30th of the range so a graph gets ~30 points.
func parseStepNs(s string, from, to int64) int64 {
	if s == "" {
		if to > from {
			return (to - from) / 30
		}
		return int64(time.Minute)
	}
	if d, err := time.ParseDuration(s); err == nil {
		return int64(d)
	}
	if n, err := strconv.Atoi(s); err == nil {
		return int64(n) * int64(time.Second)
	}
	return int64(time.Minute)
}

// timeWindowURL is timeWindow for a route whose BODY is a document.
//
// /select/vector decodes r.Body as JSON and also wants `start`/`end`.
// r.FormValue parses the body for a form content type, and guard's multipart
// pre-parse consumes it outright -- measured, the same JSON document under
// multipart/form-data went from 200 to 400 "EOF" while application/json,
// urlencoded and text/plain all answered 200. It is the third route in the
// class the Elasticsearch pair defines, and the one where `form` cannot be set
// correctly either way: false consumes nothing but drops the multipart time
// window, true consumes the document.
//
// The URL is the only place these can be. A caller's body IS the JSON
// document, so it cannot also be a urlencoded form carrying start and end;
// there is no request that loses a parameter by this. protocols.go states the
// same rule for the ingest routes, after a line-protocol write stored nothing
// while answering 204.
func timeWindowURL(r *http.Request) (int64, int64) {
	q := r.URL.Query()
	from, to := int64(0), int64(1)<<62
	if v := q.Get("start"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			from = n
		}
	}
	if v := q.Get("end"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			to = n
		}
	}
	return from, to
}

func timeWindow(r *http.Request) (int64, int64) {
	from, to := int64(0), int64(1)<<62
	if v := r.FormValue("start"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			from = n
		}
	}
	if v := r.FormValue("end"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			to = n
		}
	}
	return from, to
}

// appendJSONString appends s with the JSON-mandatory escapes only (quote,
// backslash, controls) -- enough for header/field values, and far cheaper
// than encoding/json's reflection path for a hot result loop.
func appendJSONString(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b = append(b, '\\', '"')
		case c == '\\':
			b = append(b, '\\', '\\')
		case c < 0x20:
			b = append(b, '\\', 'u', '0', '0', hexdig(c>>4), hexdig(c&0xf))
		default:
			b = append(b, c)
		}
	}
	return b
}

func hexdig(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + n - 10
}

// readiness answers whether this server should serve QUERIES.
//
// Liveness and readiness are different questions and used to give the same
// answer. /health and /-/healthy stay unconditional: the process is up, and a
// liveness probe that fails restarts it, which fixes nothing that a degraded
// store suffers from. /insert/ready stays unconditional too, because the
// degradation is on the read side and the store takes writes normally.
//
// /-/ready is the query-side probe, so a tenant serving less than it was given
// takes this replica out of rotation until an operator acknowledges it.
//
// That is deliberately conservative. A degraded store WORKS -- it opens, it
// serves, its queries return -- and that is exactly what makes it dangerous:
// every query touching a quarantined group returns fewer rows and nothing in
// the response says so. A replica silently answering short is worse than a
// replica out of rotation, so the default is out.
//
// The body names every degraded tenant, because "not ready" without a reason
// sends an operator to the logs of the wrong process.
func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	var bad []tenantHealth
	for _, t := range s.degradedSnapshot() {
		if !t.health.Ready() {
			bad = append(bad, t)
		}
	}
	// Storage pressure degrades readiness BEFORE any write fails, which is the
	// whole point of having a warn threshold as well as a reject one: an
	// operator watching /-/ready gets the warning while the store is still
	// accepting everything. A probe that only went red once writes started
	// failing would report the outage rather than prevent it.
	pressure := s.storagePressure()
	if len(bad) == 0 && len(pressure) == 0 {
		w.Write([]byte("OK"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	if len(bad) > 0 {
		fmt.Fprintf(w, "NOT READY: %d degraded tenant(s)\n", len(bad))
		for _, d := range bad {
			fmt.Fprintf(w, "%s: %s\n", d.key, d.health)
		}
	}
	if len(pressure) > 0 {
		fmt.Fprintf(w, "NOT READY: %d tenant(s) under storage pressure\n", len(pressure))
		for _, p := range pressure {
			fmt.Fprintf(w, "%s\n", p)
		}
	}
}

// storagePressure describes every tenant at or past a storage threshold.
//
// Detached, like the compaction walk: it samples every open store, and holding
// the lock every request needs while doing that would make a readiness probe a
// source of latency.
// storagePressure is the typed conditions rendered as one line per tenant.
//
// One function, not two. It and storagePressureConditions had drifted into
// different wordings of the same facts -- which is how the dead OverQuota arm
// survived, and how the "3 tenant(s)" count bug got in: two renderings of one
// state, each tested against itself.
func (s *Server) storagePressure() []string {
	conds := s.storagePressureConditions()
	out := make([]string, 0, len(conds))
	for _, c := range conds {
		out = append(out, c.Detail)
	}
	return out
}

// The corruption policy is set through config.Config and nowhere else.
//
// A setter existed briefly. It was removed because the policy has to be in
// force before any store opens and there is no moment after construction that
// is reliably before that: NewServerConfig opens the default tenant, and every
// other tenant opens on its first request. A setter would have worked for the
// lazily-opened ones and silently missed the default one -- which is the shape
// of API that is worse than none.

// AcknowledgeDegraded records operator acceptance of every degraded tenant and
// reports how many were acknowledged. A server-wide acknowledgement, because
// that is the granularity an operator acts at: they have looked at the alert,
// they know what is quarantined, and they are putting the replica back in
// rotation.
func (s *Server) AcknowledgeDegraded() (int, error) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.degraded))
	for k := range s.degraded {
		keys = append(keys, k)
	}
	s.mu.Unlock()

	n := 0
	var firstErr error
	for _, key := range keys {
		s.mu.Lock()
		tn, open := s.tenants[key]
		s.mu.Unlock()
		if !open {
			// Evicted. The acknowledgement is a file in the store's own
			// quarantine directory, so it is written where the store will
			// read it at its next open rather than reopening the tenant here
			// -- reopening to acknowledge would evict something else.
			// Skip one already acknowledged: AcknowledgeDegradedDir ran
			// unconditionally, so a second call counted the same evicted
			// tenant again and reported "acknowledged 1" twice.
			s.mu.Lock()
			known, ok := s.degraded[key]
			s.mu.Unlock()
			if ok && known.Acknowledged {
				continue
			}
			if err := storage.AcknowledgeDegradedDir(s.tenantDir(key)); err != nil {
				// Nothing quarantined is not a failure and is not an
				// acknowledgement: counting it would report a tenant accepted
				// with no marker written, and it would be unacknowledged again
				// at the next open.
				if storage.ErrNothingToAcknowledge(err) {
					// The quarantine directory is empty: the operator has
					// dealt with the evidence, which is the remediation the
					// LLD documents. Drop the record rather than skipping --
					// skipping left the replica at 503 and the alert metric at
					// 1 for an empty directory, with no escape but a restart.
					s.mu.Lock()
					if _, open := s.tenants[key]; !open {
						delete(s.degraded, key)
					}
					s.mu.Unlock()
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			n++
			s.mu.Lock()
			if h, ok := s.degraded[key]; ok {
				h.Acknowledged = true
				s.degraded[key] = h
			}
			s.mu.Unlock()
			continue
		}
		if h := tn.store.Health(); h.Degraded() && !h.Acknowledged {
			if err := tn.store.AcknowledgeDegraded(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			n++
			s.mu.Lock()
			h.Acknowledged = true
			s.degraded[key] = h
			s.mu.Unlock()
		}
	}
	return n, firstErr
}

// degradedLocked records a tenant's degraded health. s.mu must be held.
func (s *Server) degradedLocked(key string, h storage.Health) {
	if s.degraded == nil {
		s.degraded = map[string]storage.Health{}
	}
	s.degraded[key] = h
}

// acknowledgeDegraded is the operator's accept button: POST it and every
// degraded tenant becomes ready.
//
// Administrative, because it silences a readiness failure. It reports how many
// tenants it accepted and what is still degraded, so the operator sees what
// they just took responsibility for rather than a bare 200.
// listQuarantined answers what this node has quarantined and why.
//
// The COUNT already reached an operator through the
// simdlogs_storage_quarantined_groups gauge, so an alert could fire and nothing
// could say WHICH group, why, how many bytes, or when. That is the shape the
// unwired-mechanism gate exists to surface: a mechanism built, documented with
// the failure it prevents, and connected to nothing.
//
// Admin-authorized, like every other storage endpoint: the reasons name file
// paths and checksums, which describe the shape of the data.
//
// Refused in router mode rather than answered empty. A router's own store never
// quarantines anything, so an empty list there reads as "nothing is wrong"
// about shards this node has not asked.
func (s *Server) listQuarantined(w http.ResponseWriter, r *http.Request) {
	if s.refuseInRouterMode(w, r, "listing quarantined groups",
		"a router's own store holds no data and quarantines nothing, so this "+
			"would answer an empty list about shards it never asked") {
		return
	}
	recs, err := s.tn(r).store.Quarantined()
	if err != nil {
		s.writeErr(w, r, adminSpec(), http.StatusInternalServerError, err.Error())
		return
	}
	// A NON-NIL slice, so the body is `[]` and not `null`: a client that
	// distinguishes them reads null as "this node cannot say" and an empty
	// list as "nothing is quarantined", and those are different answers.
	if recs == nil {
		recs = []storage.QuarantineRecord{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Count  int                        `json:"count"`
		Groups []storage.QuarantineRecord `json:"groups"`
	}{len(recs), recs})
}

func (s *Server) acknowledgeDegraded(w http.ResponseWriter, r *http.Request) {
	if s.refuseInRouterMode(w, r, "acknowledging a degraded store",
		"the router's own store is empty and never degrades; acknowledging here "+
			"clears nothing on the shards that are actually degraded, and would "+
			"report success for an operation that did nothing") {
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		http.Error(w, "acknowledging a degraded store is a POST", http.StatusMethodNotAllowed)
		return
	}
	n, err := s.AcknowledgeDegraded()
	// Audited whichever way it went. Acknowledging corruption is a person
	// deciding that data loss is accepted and the server may serve on -- the
	// single most consequential administrative action here, and the one whose
	// absence from a record would be least explicable afterwards.
	outcome := obs.OutcomeOK
	if err != nil {
		outcome = obs.OutcomeFailed
	}
	obs.Audit(r.Context(), obs.EventCorruptionAck, subjectOf(r), outcome,
		"tenants_acknowledged", n, "error", logErrText(err))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "acknowledged %d tenant(s), then failed: %v\n", n, err)
		return
	}
	fmt.Fprintf(w, "acknowledged %d degraded tenant(s)\n", n)
	s.mu.Lock()
	keys := make([]string, 0, len(s.degraded))
	for k, h := range s.degraded {
		if h.Degraded() {
			keys = append(keys, fmt.Sprintf("%s: %s", k, h))
		}
	}
	s.mu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintln(w, k)
	}
}

// scanDegradedTenants records every tenant directory already holding
// quarantined groups, without opening a store.
//
// A tenant is marked degraded when its store OPENS, and NewServerConfig opens
// only the default one — every other tenant opens on its first request. So a
// replica restarted onto a disk with a degraded tenant nobody had queried yet
// reported ready, and only went 503 once a request happened to touch it. That
// is the wrong way round: the probe exists to keep traffic off, and it went
// green until traffic arrived.
//
// It reads directories rather than opening stores, so the cost is one ReadDir
// per tenant and no mmap, no lock, and no manifest replay.
func (s *Server) scanDegradedTenants() {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		rest, ok := strings.CutPrefix(e.Name(), "tenant-")
		if !ok {
			continue
		}
		acc, proj, ok := strings.Cut(rest, "-")
		if !ok {
			continue
		}
		dir := filepath.Join(s.dir, e.Name())
		h, ok := storage.HealthOfDir(dir)
		if !ok || !h.Degraded() {
			continue
		}
		key := acc + ":" + proj
		s.mu.Lock()
		if _, open := s.tenants[key]; !open {
			s.degradedLocked(key, h)
		}
		s.mu.Unlock()
	}
}

// storageHealthTotals sums the storage-health gauges across every tenant the
// server knows to be degraded, open or not, plus the healthy open ones.
//
// It reads s.degraded, which readiness reads, so the two cannot disagree. The
// map is copied under the lock and the live stores are read outside it, for
// the reason readiness does the same: s.mu is held across a cold tenant open.
// degradedSnapshot is every tenant the server knows to be degraded, plus the
// healthy open ones, with each one's current health.
//
// ONE implementation, because readiness and /metrics both need it and having
// two was the shape docs/wrong.md names: a fix that changes where a fact comes
// from has to change every reader of that fact. They differed in their
// population -- inert, because an open tenant absent from s.degraded is Ready
// -- and that is the next drift, not a safe difference.
//
// The map and the store pointers are copied under s.mu and the health is read
// outside it: s.mu is held across a cold tenant open, which mmaps and parses
// every group, and a readiness probe queued behind that fails and pulls the
// pod.
//
// A tenant the record names but nothing has open is RE-READ from its
// directory. That is what makes the documented remediation work: an operator
// who empties quarantine/ has dealt with the evidence, and without the re-read
// the startup record kept the replica at 503 and the alert metric at 1 for an
// empty directory, with no escape but a restart. One ReadDir, the same cost
// the startup scan already pays.
func (s *Server) degradedSnapshot() []tenantHealth {
	type entry struct {
		key   string
		h     storage.Health
		store *storage.Store
	}
	s.mu.Lock()
	// Whether this call re-reads the directories, decided once for the whole
	// snapshot and recorded under the same lock, so concurrent probes do not
	// each pay for it.
	now := time.Now()
	reread := now.Sub(s.lastDirReread) >= s.dirRereadEvery
	if reread {
		s.lastDirReread = now
	}
	snap := make([]entry, 0, len(s.degraded)+len(s.tenants))
	seen := make(map[string]bool, len(s.degraded))
	for key, h := range s.degraded {
		e := entry{key: key, h: h}
		if tn, ok := s.tenants[key]; ok {
			e.store = tn.store
		}
		snap = append(snap, e)
		seen[key] = true
	}
	// Open tenants the record does not name. They are healthy by construction
	// -- an open store with anything quarantined is in s.degraded, because
	// Degraded() reads Quarantined -- so they contribute zeros. They are here
	// so a caller counting tenants from this snapshot sees all of them.
	for key, tn := range s.tenants {
		if !seen[key] {
			snap = append(snap, entry{key: key, store: tn.store})
		}
	}
	s.mu.Unlock()

	out := make([]tenantHealth, 0, len(snap))
	type staleKey struct {
		key string
		was storage.Health
	}
	type freshKey struct {
		key      string
		was, now storage.Health
	}
	var stale []staleKey
	var fresher []freshKey

	for _, e := range snap {
		h := e.h
		if e.store != nil {
			// The live store is the freshest answer.
			out = append(out, tenantHealth{key: e.key, health: e.store.Health()})
			continue
		}
		// Not open: re-read the directory, skipping the read when the
		// quarantine directory has not changed since the last one. The
		// recorded Health is a snapshot from startup or from the last time the
		// tenant closed, and the operator's remediation happens on disk.
		if !reread {
			// Inside the throttle window: the recorded answer stands. Open
			// tenants are never throttled -- their health is in memory -- so
			// this only delays noticing a change an operator made on disk, by
			// at most dirRereadEvery.
			out = append(out, tenantHealth{key: e.key, health: h})
			continue
		}
		dir := s.tenantDir(e.key)
		fresh, ok := storage.HealthOfDir(dir)
		switch {
		case ok:
			h = fresh
		case dirGone(dir):
			// The tenant directory is GONE. Deleting a tenant is an ordinary
			// operator action -- more common than emptying one quarantine
			// directory -- and treating "not a store" as "keep the recorded
			// answer" left the probe reporting a quarantined group for a
			// directory that does not exist, recoverable only through the
			// acknowledge endpoint, which reported acknowledging nothing
			// while doing it.
			h = storage.Health{}
		}
		if !h.Degraded() {
			stale = append(stale, staleKey{key: e.key, was: e.h})
		} else if h != e.h {
			// Still degraded and CHANGED: write it back, or the record never
			// advances past what startup recorded and every throttled probe
			// reverts to it. Same on-disk state, back to back: the re-reading
			// call answered 200 and the throttled one 503 -- and /-/ready and
			// /metrics disagreed with each other, which is the invariant the
			// one-snapshot change exists to hold.
			fresher = append(fresher, freshKey{key: e.key, was: e.h, now: h})
		}
		out = append(out, tenantHealth{key: e.key, health: h})
	}
	// Drop the tenants whose evidence is gone, so the next probe does not pay
	// the ReadDir again.
	if len(stale) > 0 || len(fresher) > 0 {
		s.mu.Lock()
		for _, f := range fresher {
			// Guarded the same way the delete is: the record must be the one
			// the re-read was taken against.
			if cur, ok := s.degraded[f.key]; ok && cur == f.was {
				s.degraded[f.key] = f.now
			}
		}
		for _, e := range stale {
			// The record must be the SAME one the re-read was taken against.
			// Between releasing s.mu and reacquiring it a tenant can open
			// degraded -- repopulating the record with fresh health -- and
			// then be evicted, at which point "is it open" answers no and a
			// correct record would be deleted on the strength of a read that
			// predates it. Health is comparable, so the check is exact.
			if cur, ok := s.degraded[e.key]; ok && cur == e.was {
				delete(s.degraded, e.key)
			}
		}
		s.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// tenantHealth pairs a tenant key with its health, for the readers of
// degradedSnapshot.
type tenantHealth struct {
	key    string
	health storage.Health
}

// storageHealthTotals sums the storage-health gauges over the same snapshot
// readiness reads, so the two endpoints cannot disagree.
func (s *Server) storageHealthTotals() (corrupt, quarantined, degraded, unacked int64) {
	for _, t := range s.degradedSnapshot() {
		corrupt += int64(t.health.Corrupt)
		quarantined += int64(t.health.Quarantined)
		if t.health.Degraded() {
			degraded++
			if !t.health.Acknowledged {
				unacked++
			}
		}
	}
	return corrupt, quarantined, degraded, unacked
}

// dirGone reports whether a tenant directory is ABSENT, which is the only
// condition that may drop a degraded record.
//
// It used to be `err == nil && fi.IsDir()`, read as "deleted" for ANY error --
// and EACCES is an error. `chmod 000` on the data directory therefore deleted
// every degraded record, reported the server READY, and did not recover when
// the permissions were restored, because the record was gone and only a
// restart rebuilds it.
//
// That is the same anti-pattern this task has now fixed three times:
// countQuarantined returning 0 on any error, HealthOfDir synthesising a
// corrupt count for an unreadable directory, and this. It is worse than
// either, because misreporting is recoverable and destroying the record is
// not. An unreadable directory is a problem to report, never an absence.
func dirGone(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

// dirRereadEvery throttles how often the readiness snapshot re-reads the store
// directories of degraded tenants that are not open.
//
// A per-directory mtime cache was the first attempt and is unsound twice over.
// Part of the cached answer is the CONTENTS of quarantine/ACKNOWLEDGED, an
// exported operator-visible file: rewriting it in place changes the answer and
// not the directory. And an equal mtime is not proof of an unchanged
// directory -- one second is the natural timestamp granularity on ext3, ext4
// with 128-byte inodes, HFS+ and many NFS servers, and two on exFAT, so "the
// last probe and the operator's rm land in the same second" is a routine
// coincidence. Measured by forcing the collision: 503 and a gauge of 1 for an
// empty directory, permanently, because no probe ever re-reads.
//
// A time window has neither problem: it depends on no filesystem property and
// caches nothing per file. 250ms against a probe interval of one to ten
// seconds keeps the "the answer changes the moment the operator acts"
// requirement -- an operator cannot observe a quarter second -- and takes the
// steady-state cost of a degraded fleet from every probe to four per second.
const DefaultDirRereadEvery = 250 * time.Millisecond

// SetDirRereadIntervalForTest sets how often readiness re-reads the data
// directory.
//
// ForTest by name: production sets it through config.DirRereadInterval
// (-readiness-reread-interval), and every caller of this is a test that needs
// the re-read to be immediate. It was on the unwired baseline as a dead
// exported setter; the setter is not dead, its name was.
func (s *Server) SetDirRereadIntervalForTest(d time.Duration) {
	s.mu.Lock()
	s.dirRereadEvery = d
	s.mu.Unlock()
}
