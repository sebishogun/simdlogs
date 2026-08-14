package bench

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestAPISurface probes the endpoints a VictoriaLogs client calls against BOTH
// engines and reports the difference. The LogsQL differential proves the query
// language matches; this asks the drop-in question: does every call a configured
// client makes actually land somewhere.
//
//	SIMDLOGS_COMPAT=1 go test -run TestAPISurface -v ./internal/bench/
func TestAPISurface(t *testing.T) {
	if os.Getenv("SIMDLOGS_COMPAT") == "" {
		t.Skip("set SIMDLOGS_COMPAT=1 to run the API-surface probe")
	}
	body := compatCorpus()

	_, slURL, stopSL := startNode(t)
	defer stopSL()
	postNDJSON(t, slURL+"/insert/jsonline", body)

	if _, err := os.Stat("victoria-logs"); err != nil {
		skipNoVL(t, "the API-surface probe")
	}
	vlDir, _ := os.MkdirTemp("", "surface-vl-")
	defer os.RemoveAll(vlDir)
	abs, _ := filepath.Abs("victoria-logs")
	cmd := exec.Command(abs, "-httpListenAddr=127.0.0.1:19450", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19450"
	waitReadyCompat(t, vl+"/insert/ready", 30*time.Second)
	postNDJSON(t, vl+"/insert/jsonline", body)
	waitFor(t, readyAtLeast(vl, compatFrom, compatTo, compatRows), time.Minute,
		"victoria-logs never made the compatibility corpus queryable")

	q := url.QueryEscape("*")
	esc := url.QueryEscape("level:=error")
	probes := []struct{ name, method, path, ct, body string }{
		// select surface
		{"query", "GET", "/select/logsql/query?query=" + q + "&limit=5", "", ""},
		{"hits", "GET", "/select/logsql/hits?query=" + q + "&step=1h", "", ""},
		{"facets", "GET", "/select/logsql/facets?query=" + q, "", ""},
		{"field_names", "GET", "/select/logsql/field_names?query=" + q, "", ""},
		{"field_values", "GET", "/select/logsql/field_values?query=" + q + "&field=level", "", ""},
		{"stream_field_names", "GET", "/select/logsql/stream_field_names?query=" + q, "", ""},
		{"stream_field_values", "GET", "/select/logsql/stream_field_values?query=" + q + "&field=level", "", ""},
		{"stream_ids", "GET", "/select/logsql/stream_ids?query=" + q, "", ""},
		{"streams", "GET", "/select/logsql/streams?query=" + q, "", ""},
		{"stats_query", "GET", "/select/logsql/stats_query?query=" + url.QueryEscape("* | stats count() n"), "", ""},
		{"stats_query_range", "GET", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats count() n") + "&start=1714521600&end=1714608000&step=1h", "", ""},
		// insert surface -- VictoriaLogs prefixes these; a client configured for VL
		// uses the prefixed path, so an unprefixed-only server 404s for it.
		{"insert/jsonline", "POST", "/insert/jsonline", "application/x-ndjson", `{"_msg":"probe","_time":"2024-05-01T00:00:00Z"}` + "\n"},
		{"insert/elasticsearch_bulk", "POST", "/insert/elasticsearch/_bulk", "application/x-ndjson",
			"{\"create\":{}}\n{\"_msg\":\"probe\",\"@timestamp\":\"2024-05-01T00:00:00Z\"}\n"},
		{"insert/loki_push", "POST", "/insert/loki/api/v1/push", "application/json",
			`{"streams":[{"stream":{"app":"p"},"values":[["1714521600000000000","probe"]]}]}`},
		{"insert/otel_logs", "POST", "/insert/opentelemetry/v1/logs", "application/json",
			`{"resourceLogs":[]}`},
		{"insert/datadog", "POST", "/insert/datadog/api/v2/logs", "application/json",
			`[{"message":"probe","ddsource":"p"}]`},
		{"insert/journald", "POST", "/insert/journald", "application/octet-stream", "MESSAGE=probe\n\n"},
		{"insert/syslog", "POST", "/insert/syslog", "text/plain", "<13>1 2024-05-01T00:00:00Z h a - - - probe\n"},
		{"insert/ready", "GET", "/insert/ready", "", ""},
		// ops / ui
		{"metrics", "GET", "/metrics", "", ""},
		{"vmui", "GET", "/select/vmui", "", ""},
		{"health", "GET", "/health", "", ""},
		{"flags", "GET", "/flags", "", ""},
		// unprefixed aliases we also serve (VL does not)
		{"bulk_unprefixed", "POST", "/_bulk", "application/x-ndjson",
			"{\"create\":{}}\n{\"_msg\":\"probe\"}\n"},
		{"tail", "GET", "/select/logsql/tail?query=" + esc, "", ""},
	}

	type row struct{ name, detail string }
	var gaps, extras, both, neither []row
	for _, p := range probes {
		if p.name == "tail" {
			continue // a streaming endpoint never returns; covered by its own test
		}
		slc := probeCode(t, slURL, p.method, p.path, p.ct, p.body)
		vlc := probeCode(t, vl, p.method, p.path, p.ct, p.body)
		slOK, vlOK := slc < 400, vlc < 400
		switch {
		case vlOK && slOK:
			both = append(both, row{p.name, ""})
		case vlOK && !slOK:
			gaps = append(gaps, row{p.name, "VL " + itoa(vlc) + " / simdlogs " + itoa(slc)})
		case !vlOK && slOK:
			extras = append(extras, row{p.name, "ours only (VL " + itoa(vlc) + ")"})
		default:
			// Neither served it: usually a probe body the endpoint rejects, not a
			// gap. Reported rather than dropped -- a silently-skipped row makes the
			// coverage count look better than the probe actually proved.
			neither = append(neither, row{p.name, "VL " + itoa(vlc) + " / simdlogs " + itoa(slc)})
		}
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].name < gaps[j].name })
	t.Logf("=== API surface: %d/%d served by both, %d GAPS, %d ours-only ===",
		len(both), len(probes)-1, len(gaps), len(extras))
	for _, g := range gaps {
		t.Logf("GAP     %-28s %s", g.name, g.detail)
	}
	for _, e := range extras {
		t.Logf("EXTRA   %-28s %s", e.name, e.detail)
	}
	for _, n := range neither {
		t.Logf("NEITHER %-28s %s", n.name, n.detail)
	}
	if len(gaps) > 0 {
		t.Errorf("%d endpoints VictoriaLogs serves and simdlogs does not", len(gaps))
	}
}

func probeCode(t *testing.T, base, method, path, ct, body string) int {
	t.Helper()
	var req *http.Request
	var err error
	if method == "POST" {
		req, err = http.NewRequest("POST", base+path, strings.NewReader(body))
		if req != nil && ct != "" {
			req.Header.Set("Content-Type", ct)
		}
	} else {
		req, err = http.NewRequest("GET", base+path, nil)
	}
	if err != nil {
		return 599
	}
	cl := &http.Client{Timeout: 10 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return 598
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
