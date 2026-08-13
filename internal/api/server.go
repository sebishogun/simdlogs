// Package api serves the log database over HTTP. The surface tracks
// VictoriaLogs' paths where they exist (/insert/jsonline,
// /select/logsql/query, /select/logsql/hits) so the head-to-head harness
// drives both engines through the same wire calls, and adds the ES search
// surface the reference lacks. This file is the server and the two
// load-bearing endpoints; the fuller surface builds on it.
package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Server holds the per-tenant stores behind the HTTP surface. A request's
// tenant (AccountID:ProjectID headers, default 0:0) selects an isolated store;
// see tenant.go.
type Server struct {
	dir        string
	mu         sync.Mutex
	tenants    map[string]*tenant
	def        *tenant // the default 0:0 tenant, used by the non-HTTP paths (syslog listener)
	strmFlds   []string
	compact    bool     // compact mode default for new tenants (flate dict)
	backends   []string // peer node base URLs; when set, selects fan out and merge (vmselect role)
	replicas   int      // replication factor: backends group into shards of this many replicas
	maxRows    int      // cap on a bare (no-pipe) select's rows; 0 = unlimited. Errors, never truncates.
	started    time.Time
	nIngestReq int64 // ingest requests (atomic)
	nQueryReq  int64 // query requests (atomic)
	rr         int64 // round-robin cursor for write routing (atomic)

	rmu   sync.Mutex
	rules []*logRule // metrics-from-logs: LogsQL evaluated on a timer, exposed on /metrics

	amu    sync.Mutex
	alerts []*alertRule // alerting: LogsQL count vs a threshold, exposed on /alerts
}

// NewServer opens (or creates) the data directory at dir and returns the
// server with its default tenant ready.
func NewServer(dir string) (*Server, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	srv := &Server{dir: dir, tenants: map[string]*tenant{}, started: time.Now()}
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
	srv.def = def
	return srv, nil
}

// Dir is the data directory the server was opened on -- what a caller measures
// to report the store's footprint.
func (s *Server) Dir() string { return s.dir }

// Close shuts the server down cleanly: every tenant's writer is flushed and
// its pool stopped, and every store is unmapped. Call it at process shutdown
// after the HTTP server has stopped accepting requests. Safe to call once.
func (s *Server) Close() error {
	s.mu.Lock()
	tenants := make([]*tenant, 0, len(s.tenants))
	for _, t := range s.tenants {
		tenants = append(tenants, t)
	}
	s.mu.Unlock()
	var firstErr error
	for _, t := range tenants {
		if err := t.w.Close(); err != nil && firstErr == nil { // flush buffered rows, stop the pool
			firstErr = err
		}
		if err := t.store.Close(); err != nil && firstErr == nil { // unmap
			firstErr = err
		}
	}
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

// Handler wires the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/insert/jsonline", s.insertJSONLine)
	mux.HandleFunc("/insert/logfmt", s.insertLogfmt)
	mux.HandleFunc("/_bulk", s.esBulk)                   // Elasticsearch bulk ingest
	mux.HandleFunc("/loki/api/v1/push", s.insertLoki)    // Grafana Loki push
	mux.HandleFunc("/api/v2/logs", s.insertDatadog)      // Datadog logs intake
	mux.HandleFunc("/v1/input", s.insertDatadog)         // Datadog legacy intake
	mux.HandleFunc("/insert/syslog", s.insertSyslog)     // syslog over HTTP (native transport: ListenSyslog)
	mux.HandleFunc("/v1/logs", s.insertOTLPLogs)         // OpenTelemetry OTLP/HTTP logs (JSON)
	mux.HandleFunc("/insert/journald", s.insertJournald) // systemd journal export (systemd-journal-upload)
	mux.HandleFunc("/insert/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	// VictoriaLogs serves every third-party ingest protocol under /insert/<vendor>/.
	// An agent whose config was written against VictoriaLogs sends the prefixed
	// path, so serving only the vendor-native path 404s a drop-in client. Both
	// spellings are registered; the unprefixed ones are what the vendors' own
	// agents use when pointed at a bare host.
	mux.HandleFunc("/insert/elasticsearch/_bulk", s.esBulk)
	mux.HandleFunc("/insert/loki/api/v1/push", s.insertLoki)
	mux.HandleFunc("/insert/datadog/api/v2/logs", s.insertDatadog)
	mux.HandleFunc("/insert/datadog/api/v1/validate", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/insert/opentelemetry/v1/logs", s.insertOTLPLogs)
	mux.HandleFunc("/admin/backup", s.backup)  // tar snapshot for offline restore
	mux.HandleFunc("/metrics", s.metrics)      // Prometheus text exposition
	mux.HandleFunc("/alerts", s.alertsHandler) // alerting rule state
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("OK")) })
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("OK")) })
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("OK")) })
	mux.HandleFunc("/flags", s.flagsHandler)
	mux.HandleFunc("/vmui", s.ui) // web UI (vmui equivalent)
	mux.HandleFunc("/select/vmui", s.ui)
	mux.HandleFunc("/", s.ui) // catch-all: serve the UI at the root
	mux.HandleFunc("/select/logsql/query", s.selectQuery)
	mux.HandleFunc("/select/sql", s.sqlQuery)        // SQL SELECT subset (beyond VL)
	mux.HandleFunc("/select/vector", s.vectorSearch) // k-NN over embeddings (beyond VL)
	mux.HandleFunc("/select/logsql/tail", s.tail)    // live tail: stream matching rows as they arrive
	mux.HandleFunc("/select/logsql/hits", s.selectHits)
	mux.HandleFunc("/select/logsql/field_names", s.fieldNames)
	mux.HandleFunc("/select/logsql/field_values", s.fieldValues)
	mux.HandleFunc("/select/logsql/facets", s.facets)
	mux.HandleFunc("/select/logsql/stats_query", s.statsQuery)
	mux.HandleFunc("/select/logsql/stats_query_range", s.statsQueryRange)
	mux.HandleFunc("/select/logsql/streams", s.streamsHandler)
	mux.HandleFunc("/select/logsql/stream_ids", s.streamIDsHandler)
	mux.HandleFunc("/select/logsql/stream_field_names", s.streamFieldNamesHandler)
	mux.HandleFunc("/select/logsql/stream_field_values", s.streamFieldValuesHandler)
	// The Elasticsearch search surface VictoriaLogs lacks.
	mux.HandleFunc("/_search", s.esSearch)
	mux.HandleFunc("/_count", s.esCount)
	// In router mode, writes forward to storage nodes (outermost, before the
	// tenant/local path); reads fall through to withTenant -> federatedSelect.
	// recoverPanic is outermost so one bad request can never take the server down.
	return recoverPanic(s.routeWrites(s.withTenant(mux)))
}

// recoverPanic turns a handler panic into a 500 and keeps the server serving --
// a single malformed request must never crash the process.
func recoverPanic(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("simdlogs: panic serving %s: %v", r.URL.Path, v)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		h.ServeHTTP(w, r)
	})
}

// insertJSONLine ingests an NDJSON body and flushes it into a group.
func (s *Server) insertJSONLine(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	// Fallback timestamp for a line missing _time; atomic because the
	// parallel path calls it from many shard goroutines.
	tn := s.tn(r)
	fallback := tn.fallbackTS()
	opts := ingestOptions(r)
	var ing, skip int
	if len(body) >= ingest.MinParallelBytes {
		ing, skip = ingest.IngestJSONLinesParallelOpts(tn.store, body, fallback, s.compact, &opts)
	} else {
		// Small body: reuse the persistent writer, no per-request pool churn.
		ing, skip = ingest.IngestJSONLinesOpts(tn.w, body, fallback, &opts)
		if err := tn.w.Flush(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]int{"ingested": ing, "skipped": skip})
}

// insertLogfmt ingests a logfmt body (key=value lines) and flushes it.
func (s *Server) insertLogfmt(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	tn := s.tn(r)
	lfOpts := ingestOptions(r)
	ing, skip := ingest.IngestLogfmtOpts(tn.w, body, tn.fallbackTS(), &lfOpts)
	if err := tn.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]int{"ingested": ing, "skipped": skip})
}

// selectQuery runs a parsed LogsQL query and streams matched rows as
// NDJSON, the reference's /select/logsql/query response shape.
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
	rows := query.RunPipeline(s.tn(r).store, q) // applies the pipe chain; == Run when there are none
	if bareSelect && s.maxRows > 0 && len(rows) > s.maxRows {
		http.Error(w, fmt.Sprintf("simdlogs: result exceeds -search.maxRows=%d; add a `| limit N`, a stats pipe, or narrow the query", s.maxRows), 400)
		return
	}
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
	stream := ""
	for _, f := range row.Fields {
		if f.Key == "_time" {
			continue
		}
		if f.Key == "_stream" {
			stream = f.Value
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
		if stream == "" {
			stream = query.EmptyStream
			if !first {
				buf = append(buf, ',')
			}
			first = false
			buf = append(buf, `"_stream":"`...)
			buf = appendJSONString(buf, stream)
			buf = append(buf, '"')
		}
		if !first {
			buf = append(buf, ',')
		}
		buf = append(buf, `"_stream_id":"`...)
		if stream == query.EmptyStream {
			buf = append(buf, emptyStreamID...) // hashed once, not per row
		} else {
			buf = append(buf, query.StreamID(stream)...)
		}
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

func (rs readerStore) Groups(_, _ int64) []*storage.Reader { return rs }

// tail streams matching rows as new groups are ingested: VictoriaLogs'
// /select/logsql/tail. It subscribes at the current high-water group id and
// polls for later ones, running the LogsQL filter over each and flushing
// matches as NDJSON. The connection lives until the client disconnects.
func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
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
	store := s.tn(r).store
	cursor := store.TailCursor() // only groups that arrive after we subscribe
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	bw := bufio.NewWriter(w)
	ctx := r.Context()
	var buf []byte
	for {
		readers, nc := store.GroupsAfterID(cursor)
		if len(readers) > 0 {
			cursor = nc
			for _, row := range query.RunPipeline(readerStore(readers), q) {
				buf = appendRowJSON(buf[:0], row, q.MatAll)
				bw.Write(buf)
			}
			bw.Flush()
			flusher.Flush()
		}
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
	q, err := query.ParseSQL(r.FormValue("query"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q.From, q.To = timeWindow(r)
	if len(q.Pipes) == 0 {
		q.MatAll = true
	}
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	var buf []byte
	for _, row := range query.RunPipeline(s.tn(r).store, q) {
		buf = appendRowJSON(buf[:0], row, q.MatAll)
		bw.Write(buf)
	}
}

// vectorSearch runs cosine k-NN over an embedding column -- semantic/vector log
// search (beyond VL). Body: {"field":"emb","vector":[...],"k":10}; the time
// window comes from start/end params. Embeddings are bring-your-own (logs carry
// a vector column).
func (s *Server) vectorSearch(w http.ResponseWriter, r *http.Request) {
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
	from, to := timeWindow(r)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	var buf []byte
	for _, row := range query.VectorSearch(s.tn(r).store, from, to, body.Field, body.Vector, body.K) {
		buf = appendRowJSON(buf[:0], row, false)
		bw.Write(buf)
	}
}

// selectHits returns per-bucket counts over the time window: the
// reference's /select/logsql/hits shape (a histogram for dashboards).
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
	series := query.Hits(s.tn(r).store, q, step, by)
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
		if n, err := strconv.Atoi(v); err == nil {
			q.Limit = n
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
		s.federatedValueCounts(w, r, "/select/logsql/field_names", "values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeValues(w, limitValues(query.FieldNameCounts(s.tn(r).store, q), r))
}

func (s *Server) fieldValues(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/field_values", "values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	vcs := query.StatsByField(s.tn(r).store, q, r.FormValue("field"))
	query.SortValueCounts(vcs)
	if n := intParam(r, "limit", 0); n > 0 && len(vcs) > n {
		vcs = vcs[:n]
	}
	writeValues(w, vcs)
}

func (s *Server) facets(w http.ResponseWriter, r *http.Request) {
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	facets := query.FacetList(s.tn(r).store, q,
		intParam(r, "limit", query.DefaultFacetLimit),
		intParam(r, "max_values_per_field", query.DefaultFacetMaxValues),
		r.FormValue("keep_const_fields") == "1")
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
	samples, err := query.StatsQueryInstant(s.tn(r).store, r.FormValue("query"), from, to, time.Now().UnixNano())
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
			vcs := query.StatsByField(s.tn(r).store, q, by)
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
		s.federatedValueCounts(w, r, "/select/logsql/streams", "streams")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeValues(w, limitValues(query.Streams(s.tn(r).store, q), r))
}

// streamIDsHandler lists the distinct stream ids in the window.
func (s *Server) streamIDsHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_ids", "stream_ids")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeValues(w, limitValues(query.StreamIDs(s.tn(r).store, q), r))
}

// streamFieldNamesHandler lists the distinct stream label names.
func (s *Server) streamFieldNamesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_field_names", "values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeValues(w, limitValues(query.StreamFieldNames(s.tn(r).store, q), r))
}

// streamFieldValuesHandler lists the distinct values of one stream label.
func (s *Server) streamFieldValuesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_field_values", "values")
		return
	}
	q, err := selectQueryOf(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeValues(w, limitValues(query.StreamFieldValues(s.tn(r).store, q, r.FormValue("field")), r))
}

// statsQueryRange buckets a stats query over the time range and returns a
// Prometheus-style matrix: one series per group-by tuple, a point per step.
func (s *Server) statsQueryRange(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedMatrix(w, r)
		return
	}
	from, to := timeWindow(r)
	step := parseStepNs(r.FormValue("step"), from, to)
	series, err := query.StatsQueryRange(s.tn(r).store, r.FormValue("query"), from, to, step, time.Now().UnixNano())
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
