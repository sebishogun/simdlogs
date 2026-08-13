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
		t.Skip("victoria-logs binary not staged")
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
	time.Sleep(2 * time.Second)

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
