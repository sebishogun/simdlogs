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

	"github.com/sebishogun/simdlogs/internal/storage"
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
	// Storage health comes from the SERVER's record, the same source
	// readiness reads -- not from a walk of the open tenants.
	//
	// This walked forEachTenant, which is open tenants only, and so was blind
	// to exactly the case the startup scan was added for: a degraded tenant no
	// request has touched made /-/ready answer 503 while every one of these
	// gauges read 0. The probe pulled the pod and the alert never fired, and
	// two endpoints on one server disagreed about one tenant.
	corrupt, quarantined, degraded, unacked := s.storageHealthTotals()
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

	// Storage health. Four numbers rather than one, because they answer
	// different questions and an operator needs all four: how much data is
	// unreadable right now (corrupt), how much has ever been set aside
	// (quarantined), how many tenants are serving less than they were given
	// (degraded), and how many of those nobody has looked at yet
	// (unacknowledged). The last is the one to alert on: a degraded tenant an
	// operator has accepted is a known state, and one nobody has accepted is
	// silently answering queries with missing rows.
	m("simdlogs_storage_corrupt_groups", "Committed groups that could not be read at open.", "gauge", corrupt)
	m("simdlogs_storage_quarantined_groups", "Groups moved into quarantine, over the store's history.", "gauge", quarantined)
	m("simdlogs_storage_degraded_tenants", "Tenants serving less than their committed data.", "gauge", degraded)
	m("simdlogs_storage_degraded_unacknowledged_tenants", "Degraded tenants no operator has acknowledged.", "gauge", unacked)

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
	// The storage budget: what it is, where it is, and what it has refused.
	//
	// Capacity and free space are gauges an operator alerts on before writes
	// fail; the two counters are how they tell "the machine is full" from
	// "this tenant is over its share" after the fact, which are different
	// incidents with different fixes.
	var warn, reject, over int64
	var capacity int64
	s.forEachTenantDetached(func(tn *tenant) {
		st := tn.store.QuotaState()
		if st.Usage.Total > capacity {
			capacity = st.Usage.Total
		}
		if st.Warn {
			warn++
		}
		if st.Reject {
			reject++
		}
		if st.OverQuota {
			over++
		}
	})
	if capacity > 0 {
		m("simdlogs_storage_capacity_bytes", "Total bytes of the storage filesystem.",
			"gauge", capacity)
	}
	m("simdlogs_storage_warn_tenants", "Tenants whose free space is below the warn reserve.",
		"gauge", warn)
	m("simdlogs_storage_reject_tenants", "Tenants whose free space is below the reject reserve.",
		"gauge", reject)
	m("simdlogs_storage_over_quota_tenants", "Tenants at or above their byte quota.",
		"gauge", over)
	// Query governance: what the scan workers and the admission slots are
	// doing. The worker gauge is the one that says whether the fan-out budget
	// is the bottleneck; the rejection counter is what an operator alerts on.
	if s.workers != nil {
		m("simdlogs_scan_workers_total", "Scan worker slots available to all queries.",
			"gauge", int64(s.workers.Total()))
		m("simdlogs_scan_workers_in_use", "Scan worker slots currently held.",
			"gauge", int64(s.workers.InUse()))
	}
	// Emitted unconditionally, zeroed when admission is not configured.
	//
	// They used to be inside `if s.admission != nil`, so a default server's
	// /metrics had none of them while two documents listed them without
	// qualification -- and a dashboard panel that silently has no series looks
	// exactly like a server with nothing to report.
	inFlight, queuedQ, rejectedQ := s.admission.Stats()
	m("simdlogs_query_admission_in_flight", "Queries admitted and running.", "gauge", inFlight)
	m("simdlogs_query_admission_queued", "Queries waiting for an admission slot.",
		"gauge", queuedQ)
	m("simdlogs_query_admission_rejected_total", "Queries refused by admission.",
		"counter", rejectedQ)
	m("simdlogs_query_streamed_total",
		"Bare selects answered a group at a time, without materializing the result.",
		"counter", atomic.LoadInt64(&s.nStreamedSelects))
	rejDisk, rejQuota := storage.RejectedWrites()
	m("simdlogs_writes_rejected_disk_total", "Writes refused because free space is below the reserve.",
		"counter", rejDisk)
	m("simdlogs_writes_rejected_quota_total", "Writes refused because the tenant is at its quota.",
		"counter", rejQuota)
	// Retention health. A removal is committed to the manifest before the
	// unlink, so a failing unlink costs disk rather than correctness -- but it
	// costs disk silently unless it is counted.
	m("simdlogs_retention_failures_total", "Group unlinks that failed during retention.",
		"counter", storage.RetentionFailures())
	m("simdlogs_retention_tombstones", "Groups committed as removed whose file is still on disk.",
		"gauge", storage.PendingTombstones())
	// Tenant lifecycle. No tenant-id label: a per-tenant label on a map an
	// untrusted header can grow is unbounded cardinality, which is how a
	// metrics endpoint takes down its own scraper.
	s.mu.Lock()
	openTenants := int64(len(s.tenants))
	s.mu.Unlock()
	m("simdlogs_tenants_open", "Tenants currently held open.", "gauge", openTenants)
	m("simdlogs_tenants_evicted_total", "Tenants closed to make room for another.", "counter", TenantsEvicted())
	m("simdlogs_tenants_rejected_total", "Requests refused because every tenant slot was busy.", "counter", TenantsRejected())
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
