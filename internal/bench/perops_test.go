package bench

import (
	"bytes"
	"net/url"
	"os"
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
	// The quiet-machine gate, on the harness that produces the README table.
	//
	// It was wired to the realistic, scale and cluster harnesses and NOT to
	// this one -- so the twenty rows README publishes were the only numbers the
	// gate did not cover, while three documents said it now refuses to produce
	// a number above load average 1. Re-running here on a quiet machine also
	// stamps the result with the machine, commit and load it ran under, which
	// the published table does not carry.
	facts := requireQuiet(t)
	defer func() { t.Logf("measured at: %s", facts) }()

	const nRows = 200_000
	body := clusterCorpus(nRows, "NEEDLEops")
	esBody := toESBulk(body)

	// The corpus's own span, with slack for the ~0.5% of records the
	// generator backdates by up to two seconds.
	const corpusFrom, corpusTo = 1_700_000_000 - 10, 1_700_001_000

	slp := newSL(t)
	defer slp.stop()

	vlp := newVL(t, "127.0.0.1:19460")
	if vlp == nil {
		skipNoVL(t, "the per-operation head-to-head")
	}
	defer vlp.stop()
	if err := vlp.start(); err != nil {
		t.Fatal(err)
	}

	type result struct {
		op     string
		sl, vd time.Duration
	}
	var results []result
	record := func(op string, a, b time.Duration) { results = append(results, result{op, a, b}) }

	// ---- ingest: a fresh store per sample ----
	//
	// Previously timed with timeIt, which warms up once and then samples
	// seven times. Nothing reset the store between passes, so each format
	// wrote the corpus eight times and the two formats together left 3.2M
	// rows behind -- against which every read below was then timed, under a
	// heading that said 200000. sampleIngest demands the reset, and the
	// requireRows call after this phase is what would catch a regression by
	// measurement rather than by reading the harness.
	//
	// accept and queryable are reported separately: VictoriaLogs acknowledges
	// a write before it is readable and simdlogs does not, so the accept
	// column alone is not a comparison of the same event.
	const ingestSamples = 3
	type ingestRow struct {
		format string
		sl, vd ingestTiming
	}
	var ingests []ingestRow
	for _, f := range []struct {
		format, path string
		payload      []byte
	}{
		{"insert/jsonline", "/insert/jsonline", body},
		{"insert/elasticsearch", "/insert/elasticsearch/_bulk", esBody},
	} {
		slT, err := sampleIngest(ingestSamples,
			slp.reset,
			func() { postNDJSON(t, slp.url+f.path, f.payload) },
			func() bool { return readyAtLeast(slp.url, corpusFrom, corpusTo, nRows)() },
			25*time.Millisecond, 10*time.Minute)
		if err != nil {
			t.Fatalf("simdlogs %s: %v", f.format, err)
		}
		vlT, err := sampleIngest(ingestSamples,
			vlp.reset,
			func() { postNDJSON(t, vlp.url+f.path, f.payload) },
			readyAtLeast(vlp.url, corpusFrom, corpusTo, nRows),
			25*time.Millisecond, 10*time.Minute)
		if err != nil {
			t.Fatalf("victorialogs %s: %v", f.format, err)
		}
		ingests = append(ingests, ingestRow{f.format, slT, vlT})
		// NOT recorded into the gated ratio table. Six lines above, this file
		// states that accept is not the same event on both sides -- simdlogs'
		// rows are durable when the POST returns, VictoriaLogs' are not -- and
		// then fed accept into the table whose losses become a t.Errorf. The
		// "faster on EVERY operation" gate was failing on a comparison this
		// file documents as invalid. queryable IS the same event on both
		// sides, so that is what the gate uses.
		record(f.format+" (queryable)", slT.queryable, vlT.queryable)
	}

	// ---- one fixed corpus for every read ----
	//
	// Rebuilt from empty and loaded once, through one format, so both engines
	// hold exactly nRows and the number in the report heading is the number
	// the reads ran against.
	if err := slp.reset(); err != nil {
		t.Fatal(err)
	}
	if err := vlp.reset(); err != nil {
		t.Fatal(err)
	}
	postNDJSON(t, slp.url+"/insert/jsonline", body)
	postNDJSON(t, vlp.url+"/insert/jsonline", body)
	if !readyAtLeast(slp.url, corpusFrom, corpusTo, nRows)() {
		t.Fatal("simdlogs did not have the read corpus after a synchronous insert")
	}
	waitFor(t, readyAtLeast(vlp.url, corpusFrom, corpusTo, nRows), 5*time.Minute,
		"victorialogs never made the read corpus queryable")
	requireRows(t, "simdlogs", slp.url, corpusFrom, corpusTo, nRows)
	requireRows(t, "victorialogs", vlp.url, corpusFrom, corpusTo, nRows)
	sl, vl := slp.url, vlp.url

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

	t.Logf("=== per-operation vs VictoriaLogs (%d rows, verified in both engines before every read) ===", nRows)
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

	// The second ingest number, kept out of the ratio table because it is not
	// the same event on both sides: simdlogs' rows are readable when the POST
	// returns, VictoriaLogs' are not. A reader comparing ingest throughput
	// wants accept; a reader asking "when can I query what I just wrote"
	// wants this.
	t.Logf("--- ingest, accept vs queryable (min of %d samples, fresh store per sample) ---", ingestSamples)
	for _, r := range ingests {
		t.Logf("%-20s simdlogs accept %10v queryable %10v | VL accept %10v queryable %10v",
			r.format,
			r.sl.accept.Round(time.Microsecond), r.sl.queryable.Round(time.Microsecond),
			r.vd.accept.Round(time.Microsecond), r.vd.queryable.Round(time.Microsecond))
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
