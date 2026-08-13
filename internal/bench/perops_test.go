package bench

import (
	"bytes"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestPerOperation is the "faster on EVERY operation" gate for simdlogs: the
// same call issued against simdlogs and the real victoria-logs binary, minimum
// of N, one line per operation with the ratio. Anything below 1.0 is an
// operation VictoriaLogs wins and is a bug to fix, not a footnote.
//
//	SIMDLOGS_OPS=1 go test -run TestPerOperation -v -timeout 30m ./internal/bench/
func TestPerOperation(t *testing.T) {
	if os.Getenv("SIMDLOGS_OPS") == "" {
		t.Skip("set SIMDLOGS_OPS=1 to run the per-operation head-to-head")
	}
	const nRows = 200_000
	body := clusterCorpus(nRows, "NEEDLEops")

	_, sl, stopSL := startNode(t)
	defer stopSL()

	vlBin, _ := filepath.Abs("victoria-logs")
	if _, err := os.Stat(vlBin); err != nil {
		t.Skip("victoria-logs binary not staged")
	}
	vlDir, _ := os.MkdirTemp("", "perops-vl-")
	defer os.RemoveAll(vlDir)
	cmd := exec.Command(vlBin, "-httpListenAddr=127.0.0.1:19460", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19460"
	waitReadyCompat(t, vl+"/insert/ready", 30*time.Second)

	type result struct {
		op     string
		sl, vd time.Duration
	}
	var results []result
	record := func(op string, a, b time.Duration) { results = append(results, result{op, a, b}) }

	// ---- ingest ----
	record("insert/jsonline",
		timeIt(func() { postNDJSON(t, sl+"/insert/jsonline", body) }),
		timeIt(func() { postNDJSON(t, vl+"/insert/jsonline", body) }))

	esBody := toESBulk(body)
	record("insert/elasticsearch",
		timeIt(func() { postNDJSON(t, sl+"/insert/elasticsearch/_bulk", esBody) }),
		timeIt(func() { postNDJSON(t, vl+"/insert/elasticsearch/_bulk", esBody) }))
	time.Sleep(3 * time.Second) // let VL flush its in-memory parts before reading

	// ---- reads (min of N) ----
	// start/end are passed to BOTH: VictoriaLogs scopes several of these
	// endpoints to a recent window by default, and against a 2023 corpus it
	// would answer empty in microseconds -- a ratio that measures nothing.
	const N = 5
	const window = "&start=1700000000&end=1700001000"
	q := func(expr string) string {
		return "/select/logsql/query?query=" + url.QueryEscape(expr) + window
	}
	reads := []struct{ op, path string }{
		{"query/needle", q(`NEEDLEops`)},
		{"query/common", q(`level:=error`)},
		{"query/and", q(`level:=error AND service:=api`)},
		{"query/or", q(`level:=error OR level:=warn`)},
		{"query/substring", q(`_msg:~"timed out"`)},
		{"query/limit", q(`* | limit 100`)},
		{"query/range", q(`latency_ms:>100 AND latency_ms:<200`)},
		// A NARROW window plus an equality, materializing thousands of full
		// records: the shape the 3M harness caught losing while every
		// full-window query here won. The window is 2% of the corpus's span.
		{"query/windowed", "/select/logsql/query?query=" + url.QueryEscape("service:=api") +
			"&start=1700000010&end=1700000012"},
		{"stats/count", q(`* | stats count() n`)},
		{"stats/groupby", q(`* | stats by (service) count() n`)},
		{"stats/topk", q(`* | top 10 by (host)`)},
		{"stats/uniq", q(`* | uniq by (region)`)},
		{"hits", "/select/logsql/hits?query=" + url.QueryEscape("*") + "&step=1m" + window},
		{"facets", "/select/logsql/facets?query=" + url.QueryEscape("*") + window},
		{"field_names", "/select/logsql/field_names?query=" + url.QueryEscape("*") + window},
		{"field_values", "/select/logsql/field_values?query=" + url.QueryEscape("*") + "&field=service" + window},
		{"stream_field_names", "/select/logsql/stream_field_names?query=" + url.QueryEscape("*") + window},
		{"stats_query", "/select/logsql/stats_query?query=" + url.QueryEscape("* | stats count() n") + window},
		{"stats_query_range", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats count() n") + "&start=1700000000&end=1700001000&step=1m"},
	}
	for _, rd := range reads {
		a, an := minGet(t, sl+rd.path, N)
		b, bn := minGet(t, vl+rd.path, N)
		if an == 0 || bn == 0 {
			t.Logf("SKIP   %-20s empty response (simdlogs %d bytes, VL %d)", rd.op, an, bn)
			continue
		}
		if d := float64(an) / float64(bn); d < 0.5 || d > 2.0 {
			// Comparing our full answer against a near-empty one measures nothing;
			// fail loudly rather than bank a meaningless ratio.
			t.Errorf("UNFAIR %s: simdlogs returned %d bytes, VL %d -- not the same work", rd.op, an, bn)
			continue
		}
		record(rd.op, a, b)
	}

	t.Logf("=== per-operation vs VictoriaLogs (%d rows) ===", nRows)
	losses := 0
	for _, r := range results {
		ratio := float64(r.vd) / float64(r.sl)
		verdict := "FASTER"
		if ratio < 1.0 {
			verdict = "SLOWER <-- VL wins"
			losses++
		}
		t.Logf("%-20s simdlogs %10v  VL %10v  %5.2fx  %s",
			r.op, r.sl.Round(time.Microsecond), r.vd.Round(time.Microsecond), ratio, verdict)
	}
	if losses > 0 {
		t.Errorf("%d operations where VictoriaLogs is faster", losses)
	}
}

// toESBulk wraps NDJSON records in the action lines an Elasticsearch _bulk body
// needs, so the two ingest paths carry the same records.
func toESBulk(nd []byte) []byte {
	var out bytes.Buffer
	out.Grow(len(nd) * 3 / 2)
	for _, line := range bytes.Split(nd, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		out.WriteString("{\"create\":{}}\n")
		out.Write(line)
		out.WriteByte('\n')
	}
	return out.Bytes()
}
