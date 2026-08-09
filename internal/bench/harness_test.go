package bench

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"net/http/httptest"
	"net/url"
)

// The head-to-head: same corpus, same wire calls, both engines on this
// machine. simdlogs runs in-process (httptest); VictoriaLogs runs as a
// subprocess from the reference clone if its prebuilt binary is present
// (internal/bench/victoria-logs, produced by `go build ./app/victoria-logs`
// in ../victorialogs-reference). Absent the binary, the VL half skips and
// the simdlogs half still records a number.
//
// This is a report, not a gate -- the numbers land in the commit message
// and the README, with losses shown.

func corpusNDJSON(n int) ([]byte, int64, int64) {
	var b bytes.Buffer
	var lo, hi int64
	corpus.Gen(42, n, func(r corpus.Record) {
		t := r.Time.UnixNano()
		if lo == 0 || t < lo {
			lo = t
		}
		if t > hi {
			hi = t
		}
		fmt.Fprintf(&b, `{"_time":%q,"level":%q,"service":%q,"_msg":%q}`+"\n",
			r.Time.UTC().Format(time.RFC3339Nano), r.Level, r.Service, r.Message)
	})
	return b.Bytes(), lo, hi
}

func TestHeadToHead(t *testing.T) {
	if testing.Short() {
		t.Skip("head-to-head is a report, run with -run TestHeadToHead")
	}
	n := 200_000
	nd, lo, hi := corpusNDJSON(n)

	// simdlogs in-process.
	srv, err := api.NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sl := httptest.NewServer(srv.Handler())
	defer sl.Close()

	t0 := time.Now()
	post(t, sl.URL+"/insert/jsonline", nd)
	slIngest := time.Since(t0)

	// Selective query: a narrow window plus an equality, expressed
	// identically to both engines as RFC3339 (the format VL uses).
	wlo := time.Unix(0, lo+(hi-lo)/2).UTC().Format(time.RFC3339Nano)
	whi := time.Unix(0, lo+(hi-lo)/2+(hi-lo)/50).UTC().Format(time.RFC3339Nano)
	qstr := url.Values{"query": {"service:=auth"}, "start": {wlo}, "end": {whi}}.Encode()
	slQuery := timeQuery(t, func() { get(t, sl.URL+"/select/logsql/query?"+qstr) })

	t.Logf("simdlogs: ingest %d recs in %v (%.0f rec/s), selective query %v",
		n, slIngest, float64(n)/slIngest.Seconds(), slQuery)

	// VictoriaLogs subprocess, if the binary is here.
	binPath := "victoria-logs"
	if _, err := os.Stat(binPath); err != nil {
		t.Log("VictoriaLogs binary not staged (internal/bench/victoria-logs); simdlogs number recorded, VL half skipped")
		return
	}
	vlDir := t.TempDir()
	abs, _ := filepath.Abs(binPath)
	cmd := exec.Command(abs,
		"-httpListenAddr=127.0.0.1:19428",
		"-storageDataPath="+vlDir,
		"-retentionPeriod=10y",
	)
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19428"
	waitReady(t, vl+"/insert/ready", 30*time.Second)

	t0 = time.Now()
	post(t, vl+"/insert/jsonline", nd)
	// VL ingest is async; give it a moment to flush before querying.
	time.Sleep(3 * time.Second)
	vlIngest := time.Since(t0)
	vlQuery := timeQuery(t, func() { get(t, vl+"/select/logsql/query?"+qstr) })
	t.Logf("victorialogs: ingest %d recs in %v (%.0f rec/s), selective query %v",
		n, vlIngest, float64(n)/vlIngest.Seconds(), vlQuery)
	t.Logf("HEAD-TO-HEAD selective query: simdlogs %v vs VL %v = %.1fx",
		slQuery, vlQuery, float64(vlQuery)/float64(slQuery))
}

func post(t *testing.T, url string, body []byte) {
	r, err := http.Post(url, "application/x-ndjson", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
}

func get(t *testing.T, url string) {
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
}

func timeQuery(t *testing.T, fn func()) time.Duration {
	best := time.Hour
	for i := 0; i < 5; i++ {
		s := time.Now()
		fn()
		if d := time.Since(s); d < best {
			best = d
		}
	}
	return best
}

func waitReady(t *testing.T, url string, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if r, err := http.Get(url); err == nil {
			r.Body.Close()
			if r.StatusCode < 500 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("VL did not become ready")
}
