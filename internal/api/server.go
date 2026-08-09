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
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Server holds the store and the ingest writer behind the HTTP surface.
type Server struct {
	store *storage.Store
	w     *ingest.Writer
	mono  int64
}

// NewServer opens (or creates) a store at dir and returns the server.
func NewServer(dir string) (*Server, error) {
	s, err := storage.OpenStore(dir)
	if err != nil {
		return nil, err
	}
	return &Server{store: s, w: ingest.NewWriter(s)}, nil
}

// Handler wires the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/insert/jsonline", s.insertJSONLine)
	mux.HandleFunc("/insert/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/select/logsql/query", s.selectQuery)
	mux.HandleFunc("/select/logsql/hits", s.selectHits)
	mux.HandleFunc("/select/logsql/field_names", s.fieldNames)
	mux.HandleFunc("/select/logsql/field_values", s.fieldValues)
	mux.HandleFunc("/select/logsql/facets", s.facets)
	mux.HandleFunc("/select/logsql/stats_query", s.statsQuery)
	// The Elasticsearch search surface VictoriaLogs lacks.
	mux.HandleFunc("/_search", s.esSearch)
	mux.HandleFunc("/_count", s.esCount)
	return mux
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
	fallback := func() int64 { return time.Now().UnixNano() + atomic.AddInt64(&s.mono, 1) }
	var ing, skip int
	if len(body) >= ingest.MinParallelBytes {
		ing, skip = ingest.IngestJSONLinesParallel(s.store, body, fallback)
	} else {
		// Small body: reuse the persistent writer, no per-request pool churn.
		ing, skip = ingest.IngestJSONLines(s.w, body, fallback)
		if err := s.w.Flush(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]int{"ingested": ing, "skipped": skip})
}

// selectQuery runs a parsed LogsQL query and streams matched rows as
// NDJSON, the reference's /select/logsql/query response shape.
func (s *Server) selectQuery(w http.ResponseWriter, r *http.Request) {
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	rows := query.Run(s.store, q)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	// Hand-built NDJSON: no map[string]any, no reflection. The engine
	// produces rows in ~1.5ms; the reflective encoder was doubling the
	// wire time, so the result bytes are appended directly here.
	var buf []byte
	for _, row := range rows {
		buf = buf[:0]
		buf = append(buf, `{"_time":"`...)
		buf = time.Unix(0, row.Time).UTC().AppendFormat(buf, time.RFC3339Nano)
		buf = append(buf, '"')
		for _, f := range row.Fields {
			if f.Key == "_time" {
				continue
			}
			buf = append(buf, ',', '"')
			buf = appendJSONString(buf, f.Key)
			buf = append(buf, '"', ':', '"')
			buf = appendJSONString(buf, f.Value)
			buf = append(buf, '"')
		}
		buf = append(buf, '}', '\n')
		bw.Write(buf)
	}
}

// selectHits returns per-bucket counts over the time window: the
// reference's /select/logsql/hits shape (a histogram for dashboards).
func (s *Server) selectHits(w http.ResponseWriter, r *http.Request) {
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
	buckets := query.Histogram(s.store, q, step)
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
	json.NewEncoder(w).Encode(map[string]any{"names": query.FieldNames(s.store, from, to)})
}

func (s *Server) fieldValues(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	json.NewEncoder(w).Encode(map[string]any{"values": query.FieldValues(s.store, r.FormValue("field"), from, to)})
}

func (s *Server) facets(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	k := 10
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			k = n
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"facets": query.Facets(s.store, from, to, k)})
}

func (s *Server) statsQuery(w http.ResponseWriter, r *http.Request) {
	q, err := parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	by := r.FormValue("by")
	if by == "" {
		json.NewEncoder(w).Encode(map[string]any{"count": query.Count(s.store, q)})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"stats": query.StatsByField(s.store, q, by)})
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
