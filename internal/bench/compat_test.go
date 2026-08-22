package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

	vlURL := startVL(t, "127.0.0.1:19440", "the LogsQL compatibility differential").url
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

// TestRangeSurfaceCompat is the compatibility proof for the two RANGE
// surfaces: /select/logsql/hits and /select/logsql/stats_query_range, against
// the real VictoriaLogs binary over byte-identical data.
//
// THE DIFFERENTIAL EXISTED AND DID NOT COVER THEM, which is why a divergence in
// how those two surfaces bucket a window was pinned as a "convention
// difference" reasoned from Prometheus while the reference implementation sat
// on disk in this directory, never asked. `stats_query_range` anchored its
// buckets on the request's own `start` where VictoriaLogs floors to a step
// multiple, and both surfaces clamped their first and last buckets to
// [start,end) where VictoriaLogs gives every bucket its whole step. One window
// produced a different bucket count, different labels and different
// per-bucket values.
//
// THE WINDOWS ARE DELIBERATELY UNALIGNED. A window whose start and end are
// both multiples of the step cannot see either fault: the floor is the
// identity and no bucket is partial. Two of the three below start and end
// mid-step, and the corpus has rows in the edge buckets on both sides.
//
//	SIMDLOGS_COMPAT=1 go test -run TestRangeSurfaceCompat -v ./internal/bench/
func TestRangeSurfaceCompat(t *testing.T) {
	if os.Getenv("SIMDLOGS_COMPAT") == "" {
		t.Skip("set SIMDLOGS_COMPAT=1 to run the VictoriaLogs compatibility differential")
	}
	body := compatCorpus()

	_, slURL, stopSL := startNode(t)
	defer stopSL()
	postNDJSON(t, slURL+"/insert/jsonline", body)

	vlURL := startVL(t, "127.0.0.1:19441", "the range-surface compatibility differential").url
	postNDJSON(t, vlURL+"/insert/jsonline", body)
	waitFor(t, readyAtLeast(vlURL, compatFrom, compatTo, compatRows), time.Minute,
		"victoria-logs never made the compatibility corpus queryable")

	// The corpus is 500 rows one second apart from 2024-05-01T00:00:00Z, so
	// every window below has rows on both sides of both edges.
	//
	// `aligned/2m` is the CONTROL: its start is a step multiple and the corpus
	// ends inside its last bucket, so it is identical under either convention
	// and stays green when the two below go red. That is what makes it worth
	// keeping and what makes it useless on its own -- an aligned window cannot
	// see the anchoring at all, which is how the divergence survived a
	// differential that already existed. Verified by mutation: reverting the
	// floor, or narrowing the scan back to [start,end), reddens the two
	// unaligned windows and leaves this one alone.
	windows := []struct{ name, win string }{
		{"unaligned/1m", "start=2024-05-01T00:00:20Z&end=2024-05-01T00:07:40Z&step=1m"},
		{"aligned/2m", "start=2024-05-01T00:00:00Z&end=2024-05-01T00:08:20Z&step=2m"},
		{"unaligned/90s", "start=2024-05-01T00:01:40Z&end=2024-05-01T00:06:20Z&step=90s"},
	}
	surfaces := []struct {
		name string
		path string
		norm func([]byte) string
	}{
		{"hits", "/select/logsql/hits?query=" + url.QueryEscape("*") + "&", normalizeHits},
		{"hits-by-field", "/select/logsql/hits?field=level&query=" + url.QueryEscape("*") + "&", normalizeHits},
		{"stats_query_range", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats count() n") + "&", normalizeMatrix},
		{"stats_query_range-by", "/select/logsql/stats_query_range?query=" +
			url.QueryEscape("* | stats by (level) count() n") + "&", normalizeMatrix},
	}

	matching := 0
	for _, w := range windows {
		for _, s := range surfaces {
			name := s.name + " " + w.name
			slBody, slCode := compatGet(t, slURL+s.path+w.win)
			vlBody, vlCode := compatGet(t, vlURL+s.path+w.win)
			if vlCode != 200 {
				t.Errorf("REJECTED BY THE REFERENCE %s: VL answered %d: %s",
					name, vlCode, first(vlBody, 160))
				continue
			}
			if slCode != 200 {
				t.Errorf("REJECTED (missing feature) %s: simdlogs %d: %s",
					name, slCode, first(slBody, 160))
				continue
			}
			a, b := s.norm(slBody), s.norm(vlBody)
			if a == b {
				matching++
				continue
			}
			la, lb := strings.Split(a, "\n"), strings.Split(b, "\n")
			detail := fmt.Sprintf("%s (%s): simdlogs %d series, VL %d series", name, w.win, len(la), len(lb))
			for i := 0; i < len(la) || i < len(lb); i++ {
				x, y := "", ""
				if i < len(la) {
					x = la[i]
				}
				if i < len(lb) {
					y = lb[i]
				}
				if x != y {
					detail += fmt.Sprintf("\n    first diff at series %d:\n      simdlogs: %s\n      VL      : %s",
						i, first(x, 240), first(y, 240))
					break
				}
			}
			t.Errorf("DIFFERS: %s", detail)
		}
	}
	t.Logf("=== range-surface compatibility: %d/%d identical ===",
		matching, len(windows)*len(surfaces))
}

// normalizeHits canonicalises a /select/logsql/hits body: one line per series
// with its fields sorted, its total, and every bucket in order; the lines
// sorted.
//
// SERIES ORDER IS THE ONLY THING SORTED AWAY, and it is sorted away because
// neither engine promises one -- VictoriaLogs happens to emit `field` series in
// label order and simdlogs in first-seen order. Bucket order is NOT sorted:
// the response is documented dense and ascending, so a reordering there is a
// difference and has to show as one.
func normalizeHits(b []byte) string {
	var env struct {
		Hits []struct {
			Fields     map[string]string `json:"fields"`
			Timestamps []string          `json:"timestamps"`
			Values     []int             `json:"values"`
			Total      int               `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return "UNPARSEABLE: " + first(b, 200)
	}
	lines := make([]string, 0, len(env.Hits))
	for _, h := range env.Hits {
		keys := make([]string, 0, len(h.Fields))
		for k := range h.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s=%s,", k, h.Fields[k])
		}
		// The two array lengths are part of the shape: a client indexes them
		// together, so a series with more timestamps than values is a defect
		// however the numbers compare.
		fmt.Fprintf(&sb, " total=%d buckets=%d/%d", h.Total, len(h.Timestamps), len(h.Values))
		for i, ts := range h.Timestamps {
			v := -1
			if i < len(h.Values) {
				v = h.Values[i]
			}
			fmt.Fprintf(&sb, " %s:%d", ts, v)
		}
		lines = append(lines, sb.String())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// normalizeMatrix canonicalises a Prometheus-shaped matrix body the same way.
func normalizeMatrix(b []byte) string {
	var env struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return "UNPARSEABLE: " + first(b, 200)
	}
	lines := make([]string, 0, len(env.Data.Result))
	for _, se := range env.Data.Result {
		keys := make([]string, 0, len(se.Metric))
		for k := range se.Metric {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s=%s,", k, se.Metric[k])
		}
		for _, v := range se.Values {
			fmt.Fprintf(&sb, " %v:%v", v[0], v[1])
		}
		lines = append(lines, sb.String())
	}
	sort.Strings(lines)
	// The envelope itself is a contract too: a matrix that came back as a
	// vector is a difference no per-series comparison would show.
	return env.Status + "/" + env.Data.ResultType + "\n" + strings.Join(lines, "\n")
}
