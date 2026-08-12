package bench

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"net/http/httptest"
	"net/url"
)

// TestRealistic is the production-representative head-to-head: a ~15-field
// corpus with skewed distributions, a templated _msg, and bursty/out-of-order
// timestamps (corpus.GenRealistic), streamed to both engines, then a QUERY MIX
// -- needle, common-value, AND, OR, substring on _msg, top-N, group-by,
// histogram -- with a footprint du. This is what gets REPORTED; the lean
// 3-field corpus stays a micro-benchmark. Default N is the 1M short headline
// (a regression guard); SIMDLOGS_REAL_N runs the curve points.
//
//	SIMDLOGS_REAL=1 go test -run TestRealistic -v -timeout 90m ./internal/bench/
func TestRealistic(t *testing.T) {
	if os.Getenv("SIMDLOGS_REAL") == "" {
		t.Skip("set SIMDLOGS_REAL=1 to run the realistic head-to-head")
	}
	N := 1_000_000
	if v := os.Getenv("SIMDLOGS_REAL_N"); v != "" {
		if x, err := strconv.Atoi(v); err == nil {
			N = x
		}
	}
	const chunkRows = 2_000_000
	const needle = "NEEDLErealc0ffee42deadbeef000001"
	dirBase := os.Getenv("SIMDLOGS_SCALE_DIR")
	if dirBase == "" {
		dirBase = "/var/tmp"
	}

	var lo, hi int64
	stream := func(fn func([]byte)) {
		var buf bytes.Buffer
		buf.Grow(chunkRows * 256)
		i := 0
		corpus.GenRealistic(7, N, func(r corpus.RealisticRecord) {
			ns := r.Time.UnixNano()
			if lo == 0 || ns < lo {
				lo = ns
			}
			if ns > hi {
				hi = ns
			}
			buf.WriteString(`{"_time":"`)
			buf.WriteString(r.Time.UTC().Format(time.RFC3339Nano))
			buf.WriteByte('"')
			for _, f := range r.Fields {
				v := f.Value
				if f.Key == "trace_id" && i == N-1000 {
					v = needle
				}
				buf.WriteString(`,"`)
				buf.WriteString(f.Key)
				buf.WriteString(`":"`)
				writeJSONEsc(&buf, v)
				buf.WriteByte('"')
			}
			buf.WriteString("}\n")
			i++
			if i%chunkRows == 0 {
				fn(buf.Bytes())
				buf.Reset()
			}
		})
		if buf.Len() > 0 {
			fn(buf.Bytes())
		}
	}

	// ---- simdlogs ----
	slDir, _ := os.MkdirTemp(dirBase, "real-sl-")
	defer os.RemoveAll(slDir)
	srv, err := api.NewServer(slDir)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SIMDLOGS_COMPACT") != "" {
		srv.SetCompact(true) // A/B: compact-mode dict codec
		t.Log("compact mode ON")
	}
	sl := httptest.NewServer(srv.Handler())
	defer sl.Close()
	t0 := time.Now()
	stream(func(c []byte) { post(t, sl.URL+"/insert/jsonline", c) })
	slIngest := time.Since(t0)

	mix := realQueries(lo, hi, needle)
	rand.Shuffle(len(mix), func(i, j int) { mix[i], mix[j] = mix[j], mix[i] }) // randomize query order per run
	t.Logf("simdlogs N=%d: ingest %v (%.2fM rec/s)", N, slIngest.Round(time.Millisecond), float64(N)/slIngest.Seconds()/1e6)
	slT := map[string]time.Duration{}
	for _, m := range mix {
		slT[m.name] = timeQuery(t, func() { get(t, sl.URL+m.path+"?"+m.qs) })
		t.Logf("  simdlogs %-14s %v", m.name, slT[m.name])
	}

	// ---- VictoriaLogs ----
	if _, err := os.Stat("victoria-logs"); err != nil {
		t.Log("VL binary not staged; simdlogs numbers recorded, VL half skipped")
		return
	}
	vlDir, _ := os.MkdirTemp(dirBase, "real-vl-")
	defer os.RemoveAll(vlDir)
	abs, _ := filepath.Abs("victoria-logs")
	cmd := exec.Command(abs, "-httpListenAddr=127.0.0.1:19430", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vl := "http://127.0.0.1:19430"
	waitReady(t, vl+"/insert/ready", 30*time.Second)
	t0 = time.Now()
	stream(func(c []byte) { post(t, vl+"/insert/jsonline", c) })
	time.Sleep(5 * time.Second)
	vlIngest := time.Since(t0)
	t.Logf("victorialogs N=%d: ingest %v (%.2fM rec/s)", N, vlIngest.Round(time.Millisecond), float64(N)/vlIngest.Seconds()/1e6)
	for _, m := range mix {
		vt := timeQuery(t, func() { get(t, vl+m.path+"?"+m.qs) })
		t.Logf("  %-14s simdlogs %v vs VL %v = %.1fx", m.name, slT[m.name], vt, float64(vt)/float64(slT[m.name]))
	}
	slSize, vlSize := dirSize(slDir), dirSize(vlDir)
	t.Logf("REALISTIC N=%d | ingest %.2fx | footprint simdlogs %.2fGB vs VL %.2fGB (%.2fx of VL)",
		N, vlIngest.Seconds()/slIngest.Seconds(),
		float64(slSize)/1e9, float64(vlSize)/1e9, float64(slSize)/float64(vlSize))
}

type realQ struct {
	name string
	path string
	qs   string
}

func realQueries(lo, hi int64, needle string) []realQ {
	full := time.Unix(0, lo).UTC().Format(time.RFC3339Nano)
	fullEnd := time.Unix(0, hi+1).UTC().Format(time.RFC3339Nano)
	tr := func(q string) string {
		return url.Values{"query": {q}, "start": {full}, "end": {fullEnd}}.Encode()
	}
	return []realQ{
		{"needle", "/select/logsql/query", tr("trace_id:=" + needle)},
		{"common", "/select/logsql/query", tr("level:=error")},
		{"and", "/select/logsql/query", tr("service:=api AND status:=500")},
		{"or", "/select/logsql/query", tr("status:=500 OR status:=503")},
		{"substring", "/select/logsql/query", tr(`_msg:~"timed out"`)},
		{"topN", "/select/logsql/query", tr("* | stats by (service) count() as c | sort by (c) desc | limit 5")},
		{"groupby", "/select/logsql/query", tr("* | stats by (status) count()")},
		{"histogram", "/select/logsql/hits", url.Values{"query": {"level:=error"}, "start": {full}, "end": {fullEnd}, "step": {"1h"}}.Encode()},
	}
}

func writeJSONEsc(b *bytes.Buffer, s string) {
	if !strings.ContainsAny(s, `"\`) {
		b.WriteString(s)
		return
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
}
