package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
)

// The metric contract: a name means one thing forever.
//
// A metric name is an API. A dashboard, an alert rule and a capacity model are
// all written against a name and a meaning, and changing either silently is
// worse than deleting the series -- a deleted series makes the panel go blank
// and a redefined one makes it lie. This campaign has already done both:
// refused syslog bytes were added to `vl_bytes_ingested_total` ("Bytes of log
// data ingested") for bytes that were never ingested, and three admission
// series vanished from a default server while two documents listed them
// unconditionally.
//
// So the exposition is pinned here: every name, its type, and a one-line
// meaning. Adding a series means adding a line; changing what one MEANS means
// changing a line and explaining why in the commit.

// metricSpec is one series' contract.
type metricSpec struct {
	name string
	typ  string // "counter" or "gauge"
	// meaning is what the number counts, in the terms an operator reasons in.
	// It is prose on purpose: the test asserts the name and type, and the
	// meaning is here so a future change has to read what it is breaking.
	meaning string
}

// theContract is every series this server emits.
//
// Emitted UNCONDITIONALLY, all of them. A series that appears only when a
// feature is configured makes a dashboard panel silently empty, which reads
// exactly like a server with nothing to report -- the failure the admission
// gauges shipped with.
var theContract = []metricSpec{
	// Own names.
	{"simdlogs_tenants", "gauge", "tenants with a store open right now"},
	{"simdlogs_groups", "gauge", "row groups across every open tenant"},
	{"simdlogs_rows", "gauge", "rows across every open tenant"},
	{"simdlogs_uptime_seconds", "gauge", "seconds since this process started"},
	{"simdlogs_insert_requests_total", "counter", "ingest requests accepted for parsing"},
	{"simdlogs_query_requests_total", "counter", "read requests accepted for execution"},

	{"simdlogs_storage_corrupt_groups", "gauge", "groups that failed verification"},
	{"simdlogs_storage_quarantined_groups", "gauge", "groups moved aside by the corruption policy"},
	{"simdlogs_storage_degraded_tenants", "gauge", "tenants open in a degraded state"},
	{"simdlogs_storage_degraded_unacknowledged_tenants", "gauge",
		"degraded tenants an operator has not accepted"},

	{"simdlogs_storage_capacity_bytes", "gauge",
		"filesystem size; 0 where free space cannot be measured"},
	{"simdlogs_tenants_open", "gauge", "tenant stores currently open"},
	{"simdlogs_tenants_evicted_total", "counter", "tenants closed to stay under the open limit"},
	{"simdlogs_tenants_rejected_total", "counter",
		"requests refused because every tenant slot was in use"},
	{"simdlogs_retention_tombstones", "gauge", "groups removed but not yet unlinked"},
	{"simdlogs_retention_failures_total", "counter", "retention passes that did not complete"},
	{"simdlogs_storage_warn_tenants", "gauge", "tenants at or below the warn reserve"},
	{"simdlogs_storage_reject_tenants", "gauge", "tenants refusing writes for space"},
	{"simdlogs_storage_over_quota_tenants", "gauge", "tenants at their byte quota"},
	{"simdlogs_writes_rejected_disk_total", "counter",
		"writes refused because the machine is out of room"},
	{"simdlogs_writes_rejected_quota_total", "counter",
		"writes refused because the tenant is over its share"},

	{"simdlogs_scan_workers_total", "gauge", "scan worker slots the budget holds"},
	{"simdlogs_scan_workers_in_use", "gauge",
		"slots currently out; may exceed the total by the per-caller floor"},
	{"simdlogs_query_admission_in_flight", "gauge", "reads admitted and running"},
	{"simdlogs_query_admission_queued", "gauge", "reads waiting for a slot"},
	{"simdlogs_query_admission_rejected_total", "counter",
		"reads refused by admission; NOT including clients that hung up while queued"},
	{"simdlogs_query_streamed_total", "counter",
		"bare selects answered a group at a time rather than materialized"},

	// The reference's names, carrying the same numbers, so a dashboard written
	// for VictoriaLogs graphs this server unchanged.
	{"vl_rows_ingested_total", "counter", "rows stored"},
	{"vl_bytes_ingested_total", "counter",
		"bytes of log data ingested; NOT bytes refused before ingest"},
	{"vl_rows_dropped_total", "counter",
		"records rejected as malformed; NOT records refused by a budget"},
	{"vl_http_requests_total", "counter", "HTTP requests served"},
	{"vl_http_errors_total", "counter", "HTTP requests answered with an error"},
	{"vl_live_tailing_requests", "gauge", "live tails open"},
	{"vl_partitions", "gauge", "row groups (the reference's word for them)"},
	{"vl_storage_rows", "gauge", "rows stored"},
	{"vl_data_size_bytes", "gauge", "store footprint on disk"},
	{"vl_compressed_data_size_bytes", "gauge", "store footprint on disk, compressed"},
	{"vl_free_disk_space_bytes", "gauge", "free space on the storage filesystem"},
}

// scrape returns the parsed exposition: name -> type, and the set of names
// that carried a value.
func scrape(t *testing.T, ts *httptest.Server) (types map[string]string, seen map[string]bool, raw string) {
	t.Helper()
	code, body := get(t, ts, "/metrics")
	if code != 200 {
		t.Fatalf("/metrics returned %d", code)
	}
	types, seen = map[string]string{}, map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		switch {
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			if len(f) == 4 {
				types[f[2]] = f[3]
			}
		case line == "" || line[0] == '#':
		default:
			name, _, ok := strings.Cut(line, " ")
			if ok {
				seen[name] = true
			}
		}
	}
	return types, seen, body
}

// Every contracted series is present on a DEFAULT server, with its contracted
// type.
func TestEveryContractedMetricIsPresentAndTyped(t *testing.T) {
	ts := quotaServer(t, config.Storage{})
	types, seen, raw := scrape(t, ts)

	for _, m := range theContract {
		if !seen[m.name] {
			t.Errorf("%s is missing from a default server's /metrics (%s)", m.name, m.meaning)
			continue
		}
		if got := types[m.name]; got != m.typ {
			t.Errorf("%s is TYPE %q, the contract says %q", m.name, got, m.typ)
		}
	}
	if len(seen) == 0 {
		t.Fatalf("nothing scraped at all:\n%s", raw)
	}
}

// Nothing is emitted that the contract does not name.
//
// The other direction matters as much: a series added without a line here is a
// name nobody agreed to, and renaming or removing it later breaks whatever
// started using it in the meantime.
func TestNoMetricIsEmittedOutsideTheContract(t *testing.T) {
	ts := quotaServer(t, config.Storage{})
	_, seen, _ := scrape(t, ts)

	contracted := map[string]bool{}
	for _, m := range theContract {
		contracted[m.name] = true
	}
	for name := range seen {
		// Metrics-from-logs rules append their own series at runtime; they are
		// named by the operator, not by this package.
		if strings.HasPrefix(name, "simdlogs_rule_") || strings.HasPrefix(name, "log_") {
			continue
		}
		if !contracted[name] {
			t.Errorf("%s is exposed but not in the contract; add a line to theContract "+
				"naming what it counts", name)
		}
	}
}

// No metric carries an unbounded label.
//
// A tenant key or a field name in a label is a new time series per value, and
// a log server sees unbounded numbers of both. That is how a metrics backend
// falls over, and it falls over on the monitoring system rather than here --
// which is why it has to be refused at the source. Every per-tenant number
// this server exposes is an AGGREGATE (how many tenants are degraded, over
// quota, refusing writes) for exactly this reason.
func TestNoMetricCarriesAnUnboundedLabel(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Several tenants, each with data, so a per-tenant label would appear.
	for i := 0; i < 5; i++ {
		req := fmt.Sprintf(`{"_time":%d,"_msg":"x","field%d":"v"}`, i+1, i)
		postAsTenant(t, ts, fmt.Sprint(i), req)
	}

	_, seen, raw := scrape(t, ts)
	for name := range seen {
		if strings.ContainsAny(name, "{}") {
			t.Errorf("%s carries labels; this exposition is label-free by design", name)
		}
	}
	for _, forbidden := range []string{"tenant=", "account=", "project=", "field="} {
		if strings.Contains(raw, forbidden) {
			t.Errorf("the exposition contains %q -- an unbounded label", forbidden)
		}
	}
}

func postAsTenant(t *testing.T, ts *httptest.Server, acct, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(body+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("AccountID", acct)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}
