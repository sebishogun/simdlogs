package bench

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
	"github.com/sebishogun/simdlogs/internal/bench/corpus"
)

// TestClusterScaling measures the cluster (N storage nodes behind a router)
// against a single node holding the same data: ingest throughput and query
// latency for a selective query, a big-result query, and an aggregation. The
// router's job is to make the selective queries scale (each node scans its own
// slice in parallel) without a merge cost that eats the win on the big ones.
//
//	SIMDLOGS_CLUSTER=1 go test -run TestClusterScaling -v ./internal/bench/
func TestClusterScaling(t *testing.T) {
	if os.Getenv("SIMDLOGS_CLUSTER") == "" {
		t.Skip("set SIMDLOGS_CLUSTER=1 to run the cluster scaling benchmark")
	}
	const N = 400_000
	const needle = "NEEDLEclusterc0ffee42deadbeef01"
	body := clusterCorpus(N, needle)

	// ---- single node baseline ----
	single, singleURL, stopSingle := startNode(t)
	defer stopSingle()
	_ = single
	t0 := time.Now()
	postNDJSON(t, singleURL+"/insert/jsonline", body)
	singleIngest := time.Since(t0)

	// ---- 3 storage nodes + 1 router ----
	var backends []string
	for i := 0; i < 3; i++ {
		_, u, stop := startNode(t)
		defer stop()
		backends = append(backends, u)
	}
	rsrv, err := api.NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer rsrv.Close()
	rsrv.SetBackends(backends)
	rsrv.SetReplicas(1)
	router := httptest.NewServer(rsrv.Handler())
	defer router.Close()

	t0 = time.Now()
	postNDJSON(t, router.URL+"/insert/jsonline", body)
	clusterIngest := time.Since(t0)

	t.Logf("ingest: single %v (%.2fM/s) | cluster-3 %v (%.2fM/s) = %.2fx",
		singleIngest.Round(time.Millisecond), float64(N)/singleIngest.Seconds()/1e6,
		clusterIngest.Round(time.Millisecond), float64(N)/clusterIngest.Seconds()/1e6,
		float64(singleIngest)/float64(clusterIngest))

	queries := []struct{ name, q string }{
		{"needle", "trace_id:=" + needle},
		{"common", "level:=error"},
		{"groupby", "* | stats by (service) count() n"},
	}
	for _, qq := range queries {
		path := "/select/logsql/query?query=" + url.QueryEscape(qq.q)
		s, sn := minGet(t, singleURL+path, 5)
		c, cn := minGet(t, router.URL+path, 5)
		if sn != cn {
			t.Errorf("%s: single returned %d bytes, cluster %d -- results differ", qq.name, sn, cn)
		}
		t.Logf("%-8s single %12v | cluster-3 %12v = %.2fx", qq.name, s, c, float64(s)/float64(c))
	}
}

func clusterCorpus(n int, needle string) []byte {
	var buf bytes.Buffer
	buf.Grow(n * 220)
	i := 0
	corpus.GenRealistic(11, n, func(r corpus.RealisticRecord) {
		buf.WriteString(`{"_time":"`)
		buf.WriteString(r.Time.UTC().Format(time.RFC3339Nano))
		buf.WriteByte('"')
		for _, f := range r.Fields {
			v := f.Value
			if f.Key == "trace_id" && i == n-500 {
				v = needle
			}
			fmt.Fprintf(&buf, `,"%s":"%s"`, f.Key, strings.ReplaceAll(v, `"`, `\"`))
		}
		buf.WriteString("}\n")
		i++
	})
	return buf.Bytes()
}

func startNode(t *testing.T) (*api.Server, string, func()) {
	t.Helper()
	srv, err := api.NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	return srv, ts.URL, func() { ts.Close(); srv.Close() }
}

func postNDJSON(t *testing.T, url string, body []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// minGet returns the minimum latency over n requests and the response size.
func minGet(t *testing.T, url string, n int) (time.Duration, int) {
	t.Helper()
	best := time.Duration(1<<62 - 1)
	size := 0
	for i := 0; i < n; i++ {
		t0 := time.Now()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if d := time.Since(t0); d < best {
			best = d
		}
		size = len(b)
	}
	return best, size
}
