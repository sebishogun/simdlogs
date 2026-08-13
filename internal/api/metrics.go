package api

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

// flagsHandler serves the non-default command-line flags in the plain-text form
// VictoriaLogs uses for /flags: one -name="value" per line. Only flags that were
// actually set are listed, which is what makes the output useful in a bug report.
func (s *Server) flagsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() != f.DefValue {
			fmt.Fprintf(w, "-%s=%q\n", f.Name, f.Value.String())
		}
	})
}

// countRows records one ingest request's outcome for /metrics. Called by every
// insert handler: an operator watching rows-per-second needs the rows, not the
// requests, and rows dropped as malformed is the number that catches a broken
// shipper.
func (s *Server) countRows(ingested, skipped, bytes int) {
	atomic.AddInt64(&s.nRowsIn, int64(ingested))
	atomic.AddInt64(&s.nRowsDrop, int64(skipped))
	atomic.AddInt64(&s.nBytesIn, int64(bytes))
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

	// The same numbers under the reference's names, so a dashboard written for
	// it graphs this server unchanged. Only the metrics whose meaning we can
	// actually honour are emitted: a metric named after a structure we do not
	// have (an index database, background merges) would be a fabricated zero,
	// which is worse for an operator than a panel that plainly has no data.
	ingested := atomic.LoadInt64(&s.nRowsIn)
	m("vl_rows_ingested_total", "Log entries ingested.", "counter", ingested)
	m("vl_bytes_ingested_total", "Bytes of log data ingested.", "counter", atomic.LoadInt64(&s.nBytesIn))
	m("vl_rows_dropped_total", "Log entries rejected as malformed.", "counter", atomic.LoadInt64(&s.nRowsDrop))
	m("vl_http_requests_total", "HTTP requests served.",
		"counter", atomic.LoadInt64(&s.nIngestReq)+atomic.LoadInt64(&s.nQueryReq))
	m("vl_http_errors_total", "HTTP requests answered with an error.", "counter", atomic.LoadInt64(&s.nHTTPErrs))
	m("vl_live_tailing_requests", "Live tail requests currently open.", "gauge", atomic.LoadInt64(&s.nTails))
	m("vl_partitions", "Stored row groups.", "gauge", groups)
	m("vl_storage_rows", "Log entries currently stored.", "gauge", rows)
	if size := s.storeBytes(); size >= 0 {
		m("vl_data_size_bytes", "Bytes on disk.", "gauge", size)
		m("vl_compressed_data_size_bytes", "Bytes on disk after compression.", "gauge", size)
	}
	if free := freeDiskBytes(s.dir); free >= 0 {
		m("vl_free_disk_space_bytes", "Free bytes on the storage filesystem.", "gauge", free)
	}
	s.writeRuleMetrics(w) // metrics-from-logs rules
}

// storeBytes is the store's footprint on disk, cached briefly: a scrape every
// fifteen seconds must not walk the whole data directory every time.
func (s *Server) storeBytes() int64 {
	s.szMu.Lock()
	defer s.szMu.Unlock()
	if time.Since(s.szAt) < 15*time.Second {
		return s.szBytes
	}
	var total int64
	err := filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // a file vanishing mid-walk is normal; report what we saw
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return -1
	}
	s.szBytes, s.szAt = total, time.Now()
	return total
}
