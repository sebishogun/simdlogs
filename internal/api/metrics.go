package api

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// countRequests wraps the mux to tally ingest vs query requests by path, so
// /metrics can report them without every handler bumping a counter.
func (s *Server) countRequests(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/insert"), p == "/_bulk", p == "/v1/logs", p == "/v1/input",
			strings.HasPrefix(p, "/api/"), strings.HasPrefix(p, "/loki"):
			atomic.AddInt64(&s.nIngestReq, 1)
		case strings.HasPrefix(p, "/select"), p == "/_search", p == "/_count":
			atomic.AddInt64(&s.nQueryReq, 1)
		}
		h.ServeHTTP(w, r)
	})
}

// metrics serves Prometheus text-format gauges and counters -- the /metrics
// endpoint VictoriaLogs exposes for scraping.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m := func(name, help, typ string, v int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, v)
	}
	m("simdlogs_groups", "Number of stored groups.", "gauge", int64(s.store.Len()))
	m("simdlogs_rows", "Number of stored log records.", "gauge", int64(s.store.TotalRows()))
	m("simdlogs_insert_requests_total", "Ingest requests received.", "counter", atomic.LoadInt64(&s.nIngestReq))
	m("simdlogs_query_requests_total", "Query requests received.", "counter", atomic.LoadInt64(&s.nQueryReq))
	m("simdlogs_uptime_seconds", "Process uptime in seconds.", "gauge", int64(time.Since(s.started).Seconds()))
}
