package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// TestLogsQLCompat is the compatibility proof: the same LogsQL query run against
// simdlogs and against the real VictoriaLogs binary over byte-identical data,
// with the results compared. A checklist of implemented pipe names proves
// nothing; this shows, per query, whether simdlogs (a) rejects it -- a missing
// feature, (b) answers differently -- a semantic gap, or (c) matches.
//
//	SIMDLOGS_COMPAT=1 go test -run TestLogsQLCompat -v ./internal/bench/
func TestLogsQLCompat(t *testing.T) {
	if os.Getenv("SIMDLOGS_COMPAT") == "" {
		t.Skip("set SIMDLOGS_COMPAT=1 to run the VictoriaLogs compatibility differential")
	}
	body := compatCorpus()

	sl, slURL, stopSL := startNode(t)
	defer stopSL()
	_ = sl
	postNDJSON(t, slURL+"/insert/jsonline", body)

	if _, err := os.Stat("victoria-logs"); err != nil {
		skipNoVL(t, "the LogsQL compatibility differential")
	}
	vlDir, _ := os.MkdirTemp("", "compat-vl-")
	defer os.RemoveAll(vlDir)
	abs, _ := filepath.Abs("victoria-logs")
	cmd := exec.Command(abs, "-httpListenAddr=127.0.0.1:19440", "-storageDataPath="+vlDir, "-retentionPeriod=10y")
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start VL: %v", err)
	}
	defer cmd.Process.Kill()
	vlURL := "http://127.0.0.1:19440"
	waitReadyCompat(t, vlURL+"/insert/ready", 30*time.Second)
	postNDJSON(t, vlURL+"/insert/jsonline", body)
	waitFor(t, readyAtLeast(vlURL, compatFrom, compatTo, compatRows), time.Minute,
		"victoria-logs never made the compatibility corpus queryable")

	// The LogsQL surface, grouped so a failure names the feature.
	queries := []struct{ group, q string }{
		{"filter/word", `error`},
		{"filter/phrase", `"connection refused"`},
		{"filter/field-eq", `level:=error`},
		{"filter/field-word", `level:error`},
		{"filter/prefix", `path:/api*`},
		{"filter/substring", `_msg:~"timed out"`},
		{"filter/regex", `_msg:~"time[d]? out"`},
		{"filter/and", `level:=error AND service:=api`},
		{"filter/or", `level:=error OR level:=warn`},
		{"filter/not", `level:=error NOT service:=api`},
		{"filter/parens", `(level:=error OR level:=warn) AND service:=api`},
		{"filter/range", `latency_ms:range(100, 500)`},
		{"filter/gt", `latency_ms:>100`},
		{"filter/lt", `latency_ms:<50`},
		{"filter/len_range", `path:len_range(1, 12)`},
		{"filter/string_range", `service:string_range(a, m)`},
		{"filter/in", `level:in(error, warn)`},
		{"filter/empty", `path:=""`},
		{"pipe/limit", `* | limit 5`},
		{"pipe/fields", `level:=error | fields level, service`},
		{"pipe/stats-count", `* | stats count() total`},
		{"pipe/stats-by", `* | stats by (level) count() n`},
		{"pipe/stats-sum", `* | stats sum(latency_ms) s`},
		{"pipe/stats-avg", `* | stats avg(latency_ms) a`},
		{"pipe/stats-min-max", `* | stats min(latency_ms) lo, max(latency_ms) hi`},
		{"pipe/stats-uniq", `* | stats count_uniq(service) u`},
		{"pipe/stats-quantile", `* | stats quantile(0.5, latency_ms) p50`},
		{"pipe/sort", `* | stats by (level) count() n | sort by (n desc)`},
		{"pipe/uniq", `* | uniq by (level)`},
		{"pipe/top", `* | top 3 by (level)`},
		{"pipe/head", `* | head 5`},
		{"pipe/rename", `level:=error | fields level | rename level as lvl`},
		{"pipe/delete", `level:=error | delete trace_id`},
		{"pipe/copy", `level:=error | copy level as lvl | fields level, lvl`},
		{"pipe/filter", `* | filter level:=error | stats count() n`},
		{"pipe/math", `* | math latency_ms * 2 as double | fields double | limit 3`},
		{"pipe/format", `level:=error | format "<level>/<service>" as combo | fields combo | limit 3`},
		{"pipe/unpack_json", `* | limit 1 | unpack_json from _msg`},
		{"pipe/offset", `* | stats by (level) count() n | sort by (level) | offset 1`},
		{"pipe/multiple-stats", `* | stats by (service) count() n, sum(latency_ms) s | sort by (service)`},
	}

	var missing, differing, matching []string
	for _, qq := range queries {
		slBody, slCode := compatGet(t, slURL+"/select/logsql/query?query="+url.QueryEscape(qq.q))
		vlBody, vlCode := compatGet(t, vlURL+"/select/logsql/query?query="+url.QueryEscape(qq.q))
		switch {
		case vlCode != 200:
			t.Logf("SKIP  %-24s VL itself rejected it (%d) -- not a simdlogs gap", qq.group, vlCode)
		case slCode != 200:
			missing = append(missing, fmt.Sprintf("%s (%s) -> simdlogs %d: %s", qq.group, qq.q, slCode, first(slBody, 120)))
		default:
			a, b := normalizeNDJSON(slBody), normalizeNDJSON(vlBody)
			if a == b {
				matching = append(matching, qq.group)
			} else {
				la, lb := strings.Split(a, "\n"), strings.Split(b, "\n")
				detail := fmt.Sprintf("%s (%s): simdlogs %d rows, VL %d rows", qq.group, qq.q, len(la), len(lb))
				for i := 0; i < len(la) || i < len(lb); i++ {
					x, y := "", ""
					if i < len(la) {
						x = la[i]
					}
					if i < len(lb) {
						y = lb[i]
					}
					if x != y {
						detail += fmt.Sprintf("\n    first diff at row %d:\n      simdlogs: %s\n      VL      : %s", i, first(x, 160), first(y, 160))
						break
					}
				}
				differing = append(differing, detail)
			}
		}
	}
	t.Logf("=== LogsQL compatibility: %d/%d identical, %d differing, %d rejected ===",
		len(matching), len(queries), len(differing), len(missing))
	for _, m := range missing {
		t.Errorf("REJECTED (missing feature): %s", m)
	}
	// A differing answer FAILS. It used to be logged, which meant the
	// differential could be fully red on semantics and still exit 0 -- the
	// report shape this task exists to remove. A normalized body difference is
	// a semantic gap, and the only reason to soften one is a documented,
	// enumerated exception below, not a t.Logf.
	for _, d := range differing {
		if reason, ok := compatKnownDiff[diffGroup(d)]; ok {
			t.Logf("KNOWN DIFFERENCE %s (%s)", diffGroup(d), reason)
			continue
		}
		t.Errorf("DIFFERS: %s", d)
	}
}

// compatKnownDiff enumerates the query groups whose answers legitimately differ
// from the reference, each with the reason. Empty by design: an entry here is
// a claim that a difference is CORRECT, and every one of them has to be argued
// for rather than accumulated by a logging loop.
var compatKnownDiff = map[string]string{}

// diffGroup pulls the group name off the front of a detail line, which is
// formatted "<group> (<query>): ...".
func diffGroup(detail string) string {
	if i := strings.IndexAny(detail, " ("); i > 0 {
		return detail[:i]
	}
	return detail
}

// normalizeNDJSON makes two NDJSON result sets comparable: each line's keys
// sorted, lines sorted, and the fields that legitimately differ between engines
// (internal ids, per-engine metadata) dropped.
func normalizeNDJSON(b []byte) string {
	var lines []string
	for _, ln := range bytes.Split(b, []byte{'\n'}) {
		ln = bytes.TrimSpace(ln)
		if len(ln) == 0 {
			continue
		}
		var m map[string]any
		if json.Unmarshal(ln, &m) != nil {
			lines = append(lines, string(ln))
			continue
		}
		// _stream_id is a hash, so the two engines' values cannot agree; its
		// PRESENCE is compared instead, because a client that reads it must find
		// it there. _stream itself is no longer dropped -- both engines
		// synthesize it, so both must report the same label set.
		if _, ok := m["_stream_id"]; ok {
			m["_stream_id"] = "present"
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "%s=%v", k, m[k])
		}
		lines = append(lines, sb.String())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func first(s any, n int) string {
	var str string
	switch v := s.(type) {
	case []byte:
		str = string(v)
	case string:
		str = v
	}
	str = strings.ReplaceAll(str, "\n", " ")
	if len(str) > n {
		return str[:n] + "..."
	}
	return str
}

func compatGet(t *testing.T, url string) ([]byte, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode
}

func waitReadyCompat(t *testing.T, url string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(200 * time.Millisecond) // bench:untimed -- readiness poll
	}
	t.Fatalf("%s not ready", url)
}

// compatCorpus is a small deterministic corpus with the field shapes the queries
// above exercise (levels, services, paths, numeric latency, a JSON _msg).
func compatCorpus() []byte {
	var buf bytes.Buffer
	levels := []string{"error", "warn", "info", "debug"}
	services := []string{"api", "auth", "billing", "worker"}
	paths := []string{"/api/v1/users", "/api/v1/orders", "/health", "/metrics"}
	msgs := []string{"connection refused", "request timed out", "ok", "retrying upstream"}
	base := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&buf,
			`{"_time":"%s","level":"%s","service":"%s","path":"%s","_msg":"%s","latency_ms":"%d","trace_id":"t%04d"}`+"\n",
			base.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano),
			levels[i%len(levels)], services[i%len(services)], paths[i%len(paths)],
			msgs[i%len(msgs)], (i*7)%600, i)
	}
	return buf.Bytes()
}
