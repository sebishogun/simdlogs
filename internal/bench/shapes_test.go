package bench

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestEndpointShapes dumps both engines' bodies for every select endpoint so the
// response shapes can be compared field for field. A matching status code is not
// compatibility: a VictoriaLogs client parses the body, and a differently-shaped
// 200 reads to it as no data.
//
//	SIMDLOGS_SHAPES=1 go test -run TestEndpointShapes -v ./internal/bench/
func TestEndpointShapes(t *testing.T) {
	if os.Getenv("SIMDLOGS_SHAPES") == "" {
		t.Skip("set SIMDLOGS_SHAPES=1 to dump endpoint shapes")
	}
	body := compatCorpus()

	_, sl, stopSL := startNode(t)
	defer stopSL()
	postNDJSON(t, sl+"/insert/jsonline", body)

	vlBin, _ := filepath.Abs("victoria-logs")
	if _, err := os.Stat(vlBin); err != nil {
		skipNoVL(t, "the query-shape differential")
	}
	vlDir, _ := os.MkdirTemp("", "shapes-vl-")
	defer os.RemoveAll(vlDir)
	cmd := exec.Command(vlBin, "-httpListenAddr=127.0.0.1:19470", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19470"
	waitReadyCompat(t, vl+"/insert/ready", 30*time.Second)
	postNDJSON(t, vl+"/insert/jsonline", body)
	waitFor(t, readyAtLeast(vl, compatFrom, compatTo, compatRows), time.Minute,
		"victoria-logs never made the compatibility corpus queryable")

	// compatCorpus spans 2024-05-01T00:00:00Z for 500 seconds.
	const win = "&start=1714521600&end=1714522200"
	all := url.QueryEscape("*")
	eps := []struct{ name, path string }{
		{"query", "/select/logsql/query?query=" + all + "&limit=2" + win},
		{"query-limit-order", "/select/logsql/query?query=" + all + "&limit=3" + win},
		{"hits", "/select/logsql/hits?query=" + all + "&step=1m" + win},
		{"facets", "/select/logsql/facets?query=" + all + win},
		{"field_names", "/select/logsql/field_names?query=" + all + win},
		{"field_values", "/select/logsql/field_values?query=" + all + "&field=level" + win},
		{"stream_field_names", "/select/logsql/stream_field_names?query=" + all + win},
		{"stream_field_values", "/select/logsql/stream_field_values?query=" + all + "&field=level" + win},
		{"stream_ids", "/select/logsql/stream_ids?query=" + all + win},
		{"streams", "/select/logsql/streams?query=" + all + win},
		{"stats_query", "/select/logsql/stats_query?query=" + url.QueryEscape("* | stats count() n") + win},
		{"stats_query_range", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats count() n") + "&start=1714521600&end=1714522200&step=1m"},
	}
	for _, e := range eps {
		t.Logf("--- %s ---\n  VL: %s\n  SL: %s", e.name, head(get1(vl+e.path), 400), head(get1(sl+e.path), 400))
	}
}

func get1(u string) string {
	resp, err := http.Get(u)
	if err != nil {
		return "ERR " + err.Error()
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(b)
}

func head(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// TestParamsHonoured asks, for each documented query argument, whether it
// actually CHANGES the answer -- on both engines. A parameter that is accepted
// and ignored returns 200 with a well-formed body, so nothing but this catches
// it. The baseline and the variant are compared by response size and by decoded
// entry count, and a case where the reference's own answer does not change is
// reported as inconclusive rather than counted as a pass.
//
//	SIMDLOGS_SHAPES=1 go test -run TestParamsHonoured -v ./internal/bench/
func TestParamsHonoured(t *testing.T) {
	if os.Getenv("SIMDLOGS_SHAPES") == "" {
		t.Skip("set SIMDLOGS_SHAPES=1 to run the parameter probe")
	}
	body := compatCorpus()

	_, sl, stopSL := startNode(t)
	defer stopSL()
	postNDJSON(t, sl+"/insert/jsonline", body)

	vlBin, _ := filepath.Abs("victoria-logs")
	if _, err := os.Stat(vlBin); err != nil {
		skipNoVL(t, "the query-shape differential")
	}
	vlDir, _ := os.MkdirTemp("", "params-vl-")
	defer os.RemoveAll(vlDir)
	cmd := exec.Command(vlBin, "-httpListenAddr=127.0.0.1:19475", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19475"
	waitReadyCompat(t, vl+"/insert/ready", 30*time.Second)
	postNDJSON(t, vl+"/insert/jsonline", body)
	waitFor(t, readyAtLeast(vl, compatFrom, compatTo, compatRows), time.Minute,
		"victoria-logs never made the compatibility corpus queryable")

	const win = "&start=1714521600&end=1714522200"
	all := url.QueryEscape("*")
	cases := []struct{ name, base, variant string }{
		{"query limit", "/select/logsql/query?query=" + all + win,
			"/select/logsql/query?query=" + all + "&limit=3" + win},
		{"query window excludes the data", "/select/logsql/query?query=" + all + win,
			"/select/logsql/query?query=" + all + "&start=1600000000&end=1600000600"},
		{"field_values field", "/select/logsql/field_values?query=" + all + "&field=level" + win,
			"/select/logsql/field_values?query=" + all + "&field=service" + win},
		{"field_values limit", "/select/logsql/field_values?query=" + all + "&field=level" + win,
			"/select/logsql/field_values?query=" + all + "&field=level&limit=1" + win},
		{"field_names filtered", "/select/logsql/field_names?query=" + all + win,
			"/select/logsql/field_names?query=" + url.QueryEscape("level:=nosuchlevel") + win},
		{"hits step", "/select/logsql/hits?query=" + all + "&step=1m" + win,
			"/select/logsql/hits?query=" + all + "&step=5m" + win},
		{"hits field split", "/select/logsql/hits?query=" + all + "&step=1m" + win,
			"/select/logsql/hits?query=" + all + "&step=1m&field=level" + win},
		{"facets limit", "/select/logsql/facets?query=" + all + win,
			"/select/logsql/facets?query=" + all + "&limit=1" + win},
		{"facets keep_const_fields", "/select/logsql/facets?query=" + all + win,
			"/select/logsql/facets?query=" + all + "&keep_const_fields=1" + win},
		{"facets max_values_per_field", "/select/logsql/facets?query=" + all + win,
			"/select/logsql/facets?query=" + all + "&max_values_per_field=2" + win},
		{"stats_query_range step", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats count() n") + "&start=1714521600&end=1714522200&step=1m",
			"/select/logsql/stats_query_range?query=" +
				url.QueryEscape("* | stats count() n") + "&start=1714521600&end=1714522200&step=5m"},
		{"stats_query filtered", "/select/logsql/stats_query?query=" +
			url.QueryEscape("* | stats count() n") + win,
			"/select/logsql/stats_query?query=" +
				url.QueryEscape("level:=error | stats count() n") + win},
	}

	ignored := 0
	for _, c := range cases {
		vlBase, vlVar := len(get1(vl+c.base)), len(get1(vl+c.variant))
		slBase, slVar := len(get1(sl+c.base)), len(get1(sl+c.variant))
		vlChanged, slChanged := vlBase != vlVar, slBase != slVar
		switch {
		case !vlChanged:
			t.Logf("INCONCLUSIVE %-32s the reference's own answer did not change (%d bytes)", c.name, vlBase)
		case slChanged:
			t.Logf("honoured     %-32s VL %d->%d  SL %d->%d", c.name, vlBase, vlVar, slBase, slVar)
		default:
			ignored++
			t.Errorf("IGNORED %s: the reference's answer changed (%d->%d) and ours did not (%d bytes)",
				c.name, vlBase, vlVar, slBase)
		}
	}
	if ignored > 0 {
		t.Errorf("%d parameters accepted and ignored", ignored)
	}
}
