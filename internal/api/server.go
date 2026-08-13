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
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
	mux.HandleFunc("/admin/backup", s.backup)  // tar snapshot for offline restore
	mux.HandleFunc("/metrics", s.metrics)      // Prometheus text exposition
	mux.HandleFunc("/alerts", s.alertsHandler) // alerting rule state
	mux.HandleFunc("/vmui", s.ui)              // web UI (vmui equivalent)
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
	var ing, skip int
	if len(body) >= ingest.MinParallelBytes {
		ing, skip = ingest.IngestJSONLinesParallel(tn.store, body, fallback, s.compact)
	} else {
		// Small body: reuse the persistent writer, no per-request pool churn.
		ing, skip = ingest.IngestJSONLines(tn.w, body, fallback)
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
	ing, skip := ingest.IngestLogfmt(tn.w, body, tn.fallbackTS())
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
		buf = appendRowJSON(buf[:0], row)
		bw.Write(buf)
	}
}

// appendRowJSON serializes one result row as an NDJSON object (trailing
// newline). Shared by the batch select and the live tail so both emit the
// identical wire shape.
func appendRowJSON(buf []byte, row query.Row) []byte {
	buf = append(buf, '{')
	first := true
	if !row.NoTime { // a stats row or a projection without _time carries no timestamp
		buf = append(buf, `"_time":"`...)
		buf = time.Unix(0, row.Time).UTC().AppendFormat(buf, time.RFC3339Nano)
		buf = append(buf, '"')
		first = false
	}
	for _, f := range row.Fields {
		if f.Key == "_time" {
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
	buf = append(buf, '}', '\n')
	return buf
}

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
				buf = appendRowJSON(buf[:0], row)
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
		buf = appendRowJSON(buf[:0], row)
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
		buf = appendRowJSON(buf[:0], row)
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
		if d, err := time.ParseDuration(v); err == nil {
			step = int64(d)
		}
	}
	buckets := query.Histogram(s.tn(r).store, q, step)
	type hit struct {
		Time  string `json:"_time"`
		Count int    `json:"hits"`
	}
	out := make([]hit, 0, len(buckets))
	for t, c := range buckets {
		out = append(out, hit{Time: time.Unix(0, t).UTC().Format(time.RFC3339Nano), Count: c})
	}
	json.NewEncoder(w).Encode(map[string]any{"hits": out})
}

// parseRequest turns the LogsQL query and time params into a Query.
func parseRequest(r *http.Request) (*query.Query, error) {
	q, err := query.ParseLogsQL(r.FormValue("query"))
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
		return n, true
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UnixNano(), true
		}
	}
	return 0, false
}

func (s *Server) fieldNames(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"names": query.FieldNames(s.tn(r).store, from, to)})
}

func (s *Server) fieldValues(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"values": query.FieldValues(s.tn(r).store, r.FormValue("field"), from, to)})
}

func (s *Server) facets(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	k := 10
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			k = n
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"facets": query.Facets(s.tn(r).store, from, to, k)})
}

func (s *Server) statsQuery(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedStatsQuery(w, r)
		return
	}
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	by := r.FormValue("by")
	if by == "" {
		json.NewEncoder(w).Encode(map[string]any{"count": query.Count(s.tn(r).store, q)})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"stats": query.StatsByField(s.tn(r).store, q, by)})
}

// streamsHandler lists the distinct _stream label sets in the window.
func (s *Server) streamsHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/streams", "streams")
		return
	}
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"streams": query.Streams(s.tn(r).store, from, to)})
}

// streamIDsHandler lists the distinct stream ids in the window.
func (s *Server) streamIDsHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_ids", "stream_ids")
		return
	}
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"stream_ids": query.StreamIDs(s.tn(r).store, from, to)})
}

// streamFieldNamesHandler lists the distinct stream label names.
func (s *Server) streamFieldNamesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedStrings(w, r, "/select/logsql/stream_field_names", "names")
		return
	}
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"names": query.StreamFieldNames(s.tn(r).store, from, to)})
}

// streamFieldValuesHandler lists the distinct values of one stream label.
func (s *Server) streamFieldValuesHandler(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedValueCounts(w, r, "/select/logsql/stream_field_values", "values")
		return
	}
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"values": query.StreamFieldValues(s.tn(r).store, r.FormValue("field"), from, to)})
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
	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "matrix", "result": result},
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
