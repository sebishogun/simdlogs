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
	return mux
}

// insertJSONLine ingests an NDJSON body and flushes it into a group.
func (s *Server) insertJSONLine(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ing, skip := ingest.IngestJSONLines(s.w, body, func() int64 {
		s.mono++
		return time.Now().UnixNano() + s.mono
	})
	if err := s.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
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
	enc := json.NewEncoder(bw)
	for _, row := range rows {
		obj := map[string]any{"_time": time.Unix(0, row.Time).UTC().Format(time.RFC3339Nano)}
		for k, v := range row.Fields {
			if k != "_time" {
				obj[k] = v
			}
		}
		enc.Encode(obj)
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
	rows := query.Run(s.store, q)
	buckets := map[int64]int{}
	for _, row := range rows {
		buckets[row.Time/step*step]++
	}
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
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			q.From = n
		}
	}
	if v := r.FormValue("end"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
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
