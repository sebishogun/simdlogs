package api

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// countRequest tallies a request as ingest or query by path, so /metrics can
// report the two without every handler bumping a counter. Called by withTenant.
func (s *Server) countRequest(p string) {
	switch {
	case strings.HasPrefix(p, "/insert"), p == "/_bulk", p == "/v1/logs", p == "/v1/input",
		strings.HasPrefix(p, "/api/"), strings.HasPrefix(p, "/loki"):
		atomic.AddInt64(&s.nIngestReq, 1)
	case strings.HasPrefix(p, "/select"), p == "/_search", p == "/_count":
		atomic.AddInt64(&s.nQueryReq, 1)
	}
}

// metrics serves Prometheus text-format gauges and counters -- the /metrics
// endpoint VictoriaLogs exposes for scraping. Group and row gauges sum across
// all tenants.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var groups, rows, tenants int64
	s.forEachTenant(func(tn *tenant) {
		groups += int64(tn.store.Len())
		rows += int64(tn.store.TotalRows())
		tenants++
	})
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m := func(name, help, typ string, v int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, v)
	}
	m("simdlogs_tenants", "Number of active tenants.", "gauge", tenants)
	m("simdlogs_groups", "Number of stored groups across tenants.", "gauge", groups)
	m("simdlogs_rows", "Number of stored log records across tenants.", "gauge", rows)
	m("simdlogs_insert_requests_total", "Ingest requests received.", "counter", atomic.LoadInt64(&s.nIngestReq))
	m("simdlogs_query_requests_total", "Query requests received.", "counter", atomic.LoadInt64(&s.nQueryReq))
	m("simdlogs_uptime_seconds", "Process uptime in seconds.", "gauge", int64(time.Since(s.started).Seconds()))
}
