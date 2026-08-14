package bench

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
)

// TestScaleVsVL is the from-disk head-to-head at scale: the same corpus is
// streamed in chunks to both engines over HTTP (no giant NDJSON buffer), each
// writes to its own disk-backed store, then the same needle / selective /
// aggregation queries hit both. This is the fair comparison -- both through
// their HTTP surface, both from disk -- that the cache-hot 3M numbers were
// not. Run:
//
//	SIMDLOGS_SCALEVL=1 SIMDLOGS_SCALEVL_N=300000000 go test -run TestScaleVsVL -v -timeout 60m ./internal/bench/
//
// Needs the VL binary at internal/bench/victoria-logs. Disk-heavy; pick N to
// fit RAM headroom (VL buffers a lot ingesting at scale).
func TestScaleVsVL(t *testing.T) {
	if os.Getenv("SIMDLOGS_SCALEVL") == "" {
		t.Skip("set SIMDLOGS_SCALEVL=1 to run the scale head-to-head")
	}
	N := 100_000_000
	if v := os.Getenv("SIMDLOGS_SCALEVL_N"); v != "" {
		if x, err := strconv.Atoi(v); err == nil {
			N = x
		}
	}
	// Big chunks so each shard of the parallel ingest fills whole 128K
	// groups rather than many small ones (a 1M chunk split across shards
	// made sub-128K groups and inflated the group count).
	const chunkRows = 8_000_000
	const needle = "NEEDLEc0ffee42scale"
	services := []string{"api", "auth", "billing", "cache", "db", "gateway", "worker", "scheduler"}
	base := time.Unix(1_700_000_000, 0).UTC()
	// span: 1us per row keeps a wide window; lo/hi in nanos.
	lo := base.UnixNano()
	hi := lo + int64(N)*1000

	// streamChunks generates the deterministic corpus in NDJSON chunks and
	// hands each to fn, once -- so the same bytes go to both engines.
	streamChunks := func(fn func(chunk []byte)) {
		var buf bytes.Buffer
		buf.Grow(chunkRows * 96)
		for i := 0; i < N; i++ {
			ts := base.Add(time.Duration(i) * time.Microsecond)
			tr := strconv.FormatInt(int64(i), 16)
			if i == N-1000 {
				tr = needle
			}
			fmt.Fprintf(&buf, `{"_time":%q,"service":%q,"trace":%q}`+"\n",
				ts.Format(time.RFC3339Nano), services[i%len(services)], tr)
			if (i+1)%chunkRows == 0 {
				fn(buf.Bytes())
				buf.Reset()
			}
		}
		if buf.Len() > 0 {
			fn(buf.Bytes())
		}
	}

	dirBase := os.Getenv("SIMDLOGS_SCALE_DIR")
	if dirBase == "" {
		dirBase = "/var/tmp"
	}

	// ---- simdlogs, HTTP, disk-backed ----
	slDir, _ := os.MkdirTemp(dirBase, "scalevl-sl-")
	defer os.RemoveAll(slDir)
	srv, err := api.NewServer(slDir)
	if err != nil {
		t.Fatal(err)
	}
	sl := httptest.NewServer(srv.Handler())
	defer sl.Close()

	full := time.Unix(0, lo).UTC().Format(time.RFC3339Nano)
	fullEnd := time.Unix(0, hi+1).UTC().Format(time.RFC3339Nano)
	wlo := time.Unix(0, lo+(hi-lo)/2).UTC().Format(time.RFC3339Nano)
	whi := time.Unix(0, lo+(hi-lo)/2+(hi-lo)/50).UTC().Format(time.RFC3339Nano)

	// Measured the same way as VL's, so the ratio compares the same event.
	slIngest, err := timeIngest(
		func() { streamChunks(func(c []byte) { post(t, sl.URL+"/insert/jsonline", c) }) },
		readyAtLeast(sl.URL, lo/1e9, hi/1e9+1, N),
		200*time.Millisecond, 60*time.Minute)
	if err != nil {
		t.Fatalf("simdlogs ingest: %v", err)
	}
	requireRows(t, "simdlogs", sl.URL, lo/1e9, hi/1e9+1, N)
	nq := url.Values{"query": {"trace:=" + needle}, "start": {full}, "end": {fullEnd}}.Encode()
	sq := url.Values{"query": {"service:=auth"}, "start": {wlo}, "end": {whi}}.Encode()
	hq := url.Values{"query": {"service:=auth"}, "start": {wlo}, "end": {whi}, "step": {"1m"}}.Encode()
	// Measure the three queries in a randomized order per run, so no query
	// systematically pays first-touch costs.
	measure := func(base string) map[string]time.Duration {
		qs := []struct{ name, url string }{
			{"needle", base + "/select/logsql/query?" + nq},
			{"selective", base + "/select/logsql/query?" + sq},
			{"agg", base + "/select/logsql/hits?" + hq},
		}
		rand.Shuffle(len(qs), func(i, j int) { qs[i], qs[j] = qs[j], qs[i] })
		out := map[string]time.Duration{}
		for _, q := range qs {
			out[q.name] = timeQuery(t, func() { get(t, q.url) })
		}
		return out
	}
	slM := measure(sl.URL)
	slNeedle, slSel, slAgg := slM["needle"], slM["selective"], slM["agg"]
	t.Logf("simdlogs  N=%d: ingest accept %v (%.2fM rec/s) queryable %v (%.2fM rec/s) | needle %v selective %v agg %v",
		N, slIngest.accept.Round(time.Millisecond), float64(N)/slIngest.accept.Seconds()/1e6,
		slIngest.queryable.Round(time.Millisecond), float64(N)/slIngest.queryable.Seconds()/1e6,
		slNeedle, slSel, slAgg)

	// ---- VictoriaLogs, HTTP, disk-backed ----
	binPath := "victoria-logs"
	if _, err := os.Stat(binPath); err != nil {
		t.Log("VL binary not staged; simdlogs numbers recorded, VL half skipped")
		return
	}
	vlDir, _ := os.MkdirTemp(dirBase, "scalevl-vl-")
	defer os.RemoveAll(vlDir)
	abs, _ := filepath.Abs(binPath)
	// VL runs with its own defaults (memory included) -- the fair comparison.
	// SIMDLOGS_VL_MEMPCT can still cap it on a constrained box.
	args := []string{"-httpListenAddr=127.0.0.1:19429", "-storageDataPath=" + vlDir, "-retentionPeriod=10y"}
	if memPct := os.Getenv("SIMDLOGS_VL_MEMPCT"); memPct != "" {
		args = append(args, "-memory.allowedPercent="+memPct)
	}
	cmd := exec.Command(abs, args...)
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19429"
	waitReady(t, vl+"/insert/ready", 30*time.Second)

	// Polled, not slept. The fixed five seconds this used to spend landed in
	// vlIngest and so in every ingest ratio this harness printed; it was also
	// a floor, so a VL that had flushed in 200ms still reported five seconds.
	vlIngest, err := timeIngest(
		func() { streamChunks(func(c []byte) { post(t, vl+"/insert/jsonline", c) }) },
		readyAtLeast(vl, lo/1e9, hi/1e9+1, N),
		200*time.Millisecond, 60*time.Minute)
	if err != nil {
		t.Fatalf("victorialogs ingest: %v", err)
	}
	requireRows(t, "victorialogs", vl, lo/1e9, hi/1e9+1, N)
	vlM := measure(vl)
	vlNeedle, vlSel, vlAgg := vlM["needle"], vlM["selective"], vlM["agg"]
	t.Logf("victorialogs N=%d: ingest accept %v (%.2fM rec/s) queryable %v (%.2fM rec/s) | needle %v selective %v agg %v",
		N, vlIngest.accept.Round(time.Millisecond), float64(N)/vlIngest.accept.Seconds()/1e6,
		vlIngest.queryable.Round(time.Millisecond), float64(N)/vlIngest.queryable.Seconds()/1e6,
		vlNeedle, vlSel, vlAgg)

	slSize := dirSize(slDir)
	vlSize := dirSize(vlDir)
	t.Logf("FOOTPRINT N=%d | simdlogs %.2fGB vs VL %.2fGB (%.2fx of VL)",
		N, float64(slSize)/1e9, float64(vlSize)/1e9, float64(slSize)/float64(vlSize))
	t.Logf("SCALE HEAD-TO-HEAD N=%d | needle %.1fx | selective %.1fx | agg %.1fx | ingest accept sl/vl %.2f | ingest queryable sl/vl %.2f",
		N, ratio(vlNeedle, slNeedle), ratio(vlSel, slSel), ratio(vlAgg, slAgg),
		vlIngest.accept.Seconds()/slIngest.accept.Seconds(),
		vlIngest.queryable.Seconds()/slIngest.queryable.Seconds())
}

func ratio(a, b time.Duration) float64 { return float64(a) / float64(b) }

func dirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

var _ = http.MethodGet
