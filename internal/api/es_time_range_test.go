package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The spellings of an Elasticsearch time bound.
//
// `esTime` accepted RFC3339 and NOTHING else, and the range loop fell through
// with the bound never applied -- so epoch millis (what Grafana and Kibana
// send), epoch seconds, `now-5m`, a zoneless datetime and a bare date were all
// DROPPED at HTTP 200, every one answering MORE documents than the client
// asked for, in a structurally valid response. docs/wrong.md entry 124 holds
// the original measurements; these are its table as a gate.
//
// EVERY far-future case has a far-past control in the same spelling. A bound
// test fed only out-of-window values cannot tell a correct bound from one
// that refuses everything -- total 0 satisfies both.
func TestESTimeRangeSpellingsAllApplyTheBound(t *testing.T) {
	ts := esServer(t,
		map[string]string{"_msg": "a", "level": "error"},
		map[string]string{"_msg": "b", "level": "warn"},
		map[string]string{"_msg": "c", "level": "info"},
		map[string]string{"_msg": "d", "level": "info"},
	)
	// The documents are stamped at ingest, i.e. now. 1893456000 is
	// 2030-01-01T00:00:00Z; the magnitude rule reads it in seconds, millis,
	// or as a string the same way the HTTP time params do.
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		{"RFC3339 gte future", `{"query":{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}},"size":100}`, 0},
		{"RFC3339 gte past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}},"size":100}`, 4},
		{"epoch millis number gte future", `{"query":{"range":{"@timestamp":{"gte":1893456000000}}},"size":100}`, 0},
		{"epoch millis number gte past (control)", `{"query":{"range":{"@timestamp":{"gte":946684800000}}},"size":100}`, 4},
		{"epoch millis string gte future", `{"query":{"range":{"@timestamp":{"gte":"1893456000000"}}},"size":100}`, 0},
		{"epoch millis string gte past (control)", `{"query":{"range":{"@timestamp":{"gte":"946684800000"}}},"size":100}`, 4},
		{"epoch seconds gte future", `{"query":{"range":{"@timestamp":{"gte":1893456000}}},"size":100}`, 0},
		{"epoch seconds gte past (control)", `{"query":{"range":{"@timestamp":{"gte":946684800}}},"size":100}`, 4},
		{"date math gte now+1h", `{"query":{"range":{"@timestamp":{"gte":"now+1h"}}},"size":100}`, 0},
		{"date math gte now-1h (control)", `{"query":{"range":{"@timestamp":{"gte":"now-1h"}}},"size":100}`, 4},
		{"date math lt now-1h", `{"query":{"range":{"@timestamp":{"lt":"now-1h"}}},"size":100}`, 0},
		{"date math chain lte now+1d-1h (control)", `{"query":{"range":{"@timestamp":{"lte":"now+1d-1h"}}},"size":100}`, 4},
		{"no zone gte future", `{"query":{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00"}}},"size":100}`, 0},
		{"no zone gte past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00"}}},"size":100}`, 4},
		{"date only gte future", `{"query":{"range":{"@timestamp":{"gte":"2030-01-01"}}},"size":100}`, 0},
		{"date only gte past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01"}}},"size":100}`, 4},
		// Grafana's exact shape: numeric strings under an explicit
		// epoch_millis format. The strict decoder refused the whole request
		// (400 `unknown field "format"`) before the field existed.
		{"grafana epoch_millis future window", `{"query":{"range":{"@timestamp":{"gte":"1893456000000","lte":"1993456000000","format":"epoch_millis"}}},"size":100}`, 0},
		{"grafana epoch_millis covering window (control)", `{"query":{"range":{"@timestamp":{"gte":"946684800000","lte":"4102444800000","format":"epoch_millis"}}},"size":100}`, 4},
		// time_zone completes a zoneless spelling. 2030 in Tokyo is still the
		// future; 2000 in Tokyo is still the past.
		{"time_zone zoneless future", `{"query":{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00","time_zone":"+09:00"}}},"size":100}`, 0},
		{"time_zone zoneless past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00","time_zone":"+09:00"}}},"size":100}`, 4},
		// `_time` is the same field.
		{"_time spelling gte future", `{"query":{"range":{"_time":{"gte":1893456000000}}},"size":100}`, 0},
		// THE OVERFLOW FAMILY. Every one of these bounds means "far future",
		// and every one used to WRAP: `9999-01-01` overflowed UnixNano into
		// the past, `13000000000` (epoch seconds, year 2381) wrapped negative
		// in unixToNanos' multiply -- measured answering all 30 rows of a
		// store at 200 -- and `now+300y` wrapped in the date-math Duration
		// arithmetic. A saturated bound is +infinity: gte matches nothing,
		// lte matches everything, which the controls pin.
		{"year 9999 gte", `{"query":{"range":{"@timestamp":{"gte":"9999-01-01"}}},"size":100}`, 0},
		{"year 9999 lte (control)", `{"query":{"range":{"@timestamp":{"lte":"9999-01-01"}}},"size":100}`, 4},
		{"epoch micros year 2408 gte", `{"query":{"range":{"@timestamp":{"gte":13830000000000000}}},"size":100}`, 0},
		{"epoch seconds year 2381 gte", `{"query":{"range":{"@timestamp":{"gte":13000000000}}},"size":100}`, 0},
		{"date math now+300y gte", `{"query":{"range":{"@timestamp":{"gte":"now+300y"}}},"size":100}`, 0},
		{"date math now-300y gte (control)", `{"query":{"range":{"@timestamp":{"gte":"now-300y"}}},"size":100}`, 4},
		// A big-but-in-range offset in a small unit must still mean what it
		// says: one year spelled in seconds. A flat cap on the COUNT rather
		// than on the product would turn this into days, silently.
		{"one year in seconds (control)", `{"query":{"range":{"@timestamp":{"gte":"now-31536000s"}}},"size":100}`, 4},
		// A BARE YEAR AS A STRING IS A DATE, NOT AN EPOCH NUMBER.
		//
		// `ParseInt` ran before the layouts, so `"2030"` was read as 2030
		// SECONDS since the epoch -- 1970-01-01T00:33:50Z -- and matched every
		// document. Elasticsearch parses a string against
		// `strict_date_optional_time` first and answers none. The `"2006"`
		// layout was in the list and unreachable, since a bare year is all
		// digits.
		{"bare year string, future", `{"query":{"range":{"@timestamp":{"gte":"2030"}}},"size":100}`, 0},
		{"bare year string, past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000"}}},"size":100}`, 4},
		{"bare year string as an upper bound", `{"query":{"range":{"@timestamp":{"lt":"2000"}}},"size":100}`, 0},
		{"bare year-month string, future", `{"query":{"range":{"@timestamp":{"gte":"2030-01"}}},"size":100}`, 0},
		{"bare year-month string, past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01"}}},"size":100}`, 4},
		// THE EPOCH SPELLINGS MUST NOT MOVE. A 10- or 13-digit numeric string
		// has too many digits for a year layout and still reads by magnitude;
		// these rows are the ones that would break if the reorder were wrong.
		{"epoch seconds string still epoch, future", `{"query":{"range":{"@timestamp":{"gte":"1893456000"}}},"size":100}`, 0},
		{"epoch seconds string still epoch, past (control)", `{"query":{"range":{"@timestamp":{"gte":"946684800"}}},"size":100}`, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("total = %d, want %d -- the bound was %s: %s",
					total, tc.total,
					map[bool]string{true: "dropped", false: "misapplied"}[total > tc.total], raw)
			}
			// /_count runs the same mapping and must agree.
			var body2 = strings.Replace(tc.body, `,"size":100`, "", 1)
			cCode, count := esCountRaw(t, ts, body2)
			if cCode != 200 || count != tc.total {
				t.Fatalf("/_count answered %d with count %d, want 200 with %d",
					cCode, count, tc.total)
			}
		})
	}
}

// A time range under `must_not` or `should` keeps its boolean meaning.
//
// The range loop lifted EVERY time range onto the query's scan window, and the
// window is an AND over the whole query -- so a negated range was applied
// un-negated (`must_not` a far-past window answered 0 where every document
// matches), and two `should` alternatives were intersected (complementary
// ranges answered 0 where the union is everything). Both at HTTP 200. In those
// contexts the range now becomes an ordinary predicate leaf the boolean tree
// negates or unions; only positive conjunctive context still narrows the
// window.
func TestESRangeUnderMustNotAndShouldKeepsItsMeaning(t *testing.T) {
	ts := esServer(t,
		map[string]string{"_msg": "a", "level": "error"},
		map[string]string{"_msg": "b", "level": "warn"},
		map[string]string{"_msg": "c", "level": "info"},
		map[string]string{"_msg": "d", "level": "info"},
	)
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		// NOT (before 2000) = everything: the documents are stamped now.
		{"must_not far past", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"lt":"2000-01-01T00:00:00Z"}}}]}},"size":100}`, 4},
		// NOT (after 2030) = everything.
		{"must_not far future", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}}]}},"size":100}`, 4},
		// NOT (covering window) = nothing: the negation itself, as a control
		// that the predicate is applied at all rather than dropped.
		{"must_not covering window (control)", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z","lt":"2100-01-01T00:00:00Z"}}}]}},"size":100}`, 0},
		// (before 2000) OR (from 2000 on) = everything.
		{"should complementary ranges", `{"query":{"bool":{"should":[{"range":{"@timestamp":{"lt":"2000-01-01T00:00:00Z"}}},{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}}]}},"size":100}`, 4},
		// (after 2030) OR level:error = just the error row: the range arm of
		// the union contributes nothing and must not clamp the window.
		{"should range or term", `{"query":{"bool":{"should":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}},{"term":{"level":"error"}}]}},"size":100}`, 1},
		// A single should arm out of window: union of one empty set.
		{"should future alone (control)", `{"query":{"bool":{"should":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}}]}},"size":100}`, 0},
		// Nested: a positive range INSIDE a must_not bool stays negated.
		{"must_not nested bool", `{"query":{"bool":{"must_not":[{"bool":{"must":[{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}}]}}]}},"size":100}`, 0},
		// And a positive range NEXT TO a must_not one: the window narrows for
		// the first and the tree negates the second.
		{"must and must_not together", `{"query":{"bool":{"must":[{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}}],"must_not":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}}]}},"size":100}`, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("total = %d, want %d: %s", total, tc.total, raw)
			}
		})
	}

	// The `_source` a predicate-path hit carries is the same document a
	// lifted-path hit carries. The tree evaluator materialized a formatted
	// `_time` field for the time leaf (filterFields had no time-pred skip,
	// where the flat-Preds loop has had one all along), so the same document's
	// field set changed with the SPELLING of the filter.
	_, _, _, raw := esSearchRaw(t, ts,
		`{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"lt":"2000-01-01T00:00:00Z"}}}]}},"size":1}`)
	var env struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal([]byte(raw), &env); err != nil || len(env.Hits.Hits) == 0 {
		t.Fatalf("no hit to inspect: %v %.200s", err, raw)
	}
	src := env.Hits.Hits[0].Source
	if _, ok := src["_time"]; ok {
		t.Errorf("a must_not-filtered hit carries a `_time` field the same "+
			"document does not carry under a lifted filter: %v", src)
	}
	for _, k := range []string{"@timestamp", "_msg", "level"} {
		if _, ok := src[k]; !ok {
			t.Errorf("_source is missing %q: %v", k, src)
		}
	}
}

// Every bound in a clause applies, and clauses intersect.
//
// `gte` used to shadow `gt` in the same clause (the second lower bound was
// read only when the first was absent or unreadable), and a second range
// clause on the same field OVERWROTE the first -- so a stricter bound
// vanished, silently, in both shapes.
func TestESRangeBoundsCombine(t *testing.T) {
	ts := esServer(t,
		map[string]string{"_msg": "a"},
		map[string]string{"_msg": "b"},
	)
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		{"gt future beside gte past", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z","gt":"2030-01-01T00:00:00Z"}}},"size":100}`, 0},
		{"gt past beside gte past (control)", `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z","gt":"2001-01-01T00:00:00Z"}}},"size":100}`, 2},
		{"second clause must not widen", `{"query":{"bool":{"must":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}},{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}}]}},"size":100}`, 0},
		{"two agreeing clauses (control)", `{"query":{"bool":{"must":[{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z"}}},{"range":{"@timestamp":{"lt":"2100-01-01T00:00:00Z"}}}]}},"size":100}`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("total = %d, want %d: %s", total, tc.total, raw)
			}
		})
	}
}

// A bound this build cannot read is a 400 NAMING it, never a bound dropped.
//
// This is the file's own contract ("never a filter dropped on the floor"),
// and the one entry 124 measured it breaking: six spellings fell through
// `esTime` with the bound silently unapplied.
func TestESUnreadableTimeBoundsAre400(t *testing.T) {
	ts := esServer(t, manyDocs(4)...)
	for _, tc := range []struct{ name, body, wants string }{
		{"date math rounding", `{"query":{"range":{"@timestamp":{"gte":"now-1d/d"}}}}`, "rounding"},
		{"date math anchor", `{"query":{"range":{"@timestamp":{"gte":"2026-08-22||+1M"}}}}`, "anchor"},
		{"date math trailing sign", `{"query":{"range":{"@timestamp":{"gte":"now-"}}}}`, "unit"},
		{"unknown format", `{"query":{"range":{"@timestamp":{"gte":"20260101","format":"basic_date"}}}}`, "basic_date"},
		{"unknown time_zone", `{"query":{"range":{"@timestamp":{"gte":"2026-01-01T00:00:00","time_zone":"Mars/Olympus"}}}}`, "Mars/Olympus"},
		{"garbage string", `{"query":{"range":{"@timestamp":{"gte":"yesterday"}}}}`, "yesterday"},
		{"empty range object", `{"query":{"range":{"@timestamp":{}}}}`, "no bound"},
		{"date under epoch format", `{"query":{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z","format":"epoch_millis"}}}}`, "epoch_millis"},
		{"bool bound", `{"query":{"range":{"@timestamp":{"gte":true}}}}`, "bool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 400 {
				t.Fatalf("%d, want 400 -- an unreadable bound must refuse, not "+
					"drop: %s", code, raw)
			}
			if !strings.Contains(raw, tc.wants) {
				t.Errorf("the refusal does not name the problem (want %q): %s",
					tc.wants, raw)
			}
			// The count surface refuses identically rather than counting the
			// whole store.
			if cCode, _ := esCountRaw(t, ts, tc.body); cCode != 400 {
				t.Errorf("/_count answered %d for the same body, want 400", cCode)
			}
		})
	}
}

// The same Grafana-shaped body answers the same through a router.
//
// The federated `_search` decodes the body with the same strict decoder the
// node uses, so the `format` field had to be added to BOTH or the router would
// go on refusing what the node had learned to answer. The body is forwarded to
// the shards (reframed for paging only), so the bound itself is parsed
// shard-side; this pins the two ends of that trip to the same answer.
func TestGrafanaRangeBodyAgreesAcrossACluster(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	body := fmt.Sprintf(`{"query":{"range":{"@timestamp":{"gte":"%d","lt":"%d",`+
		`"format":"epoch_millis"}}},"size":100}`,
		base.Add(5*time.Second).UnixMilli(), base.Add(15*time.Second).UnixMilli())

	codeS, totalS, hitsS, rawS := esSearchRaw(t, single, body)
	if codeS != 200 || totalS != 10 {
		t.Fatalf("the node answered %d with total %d, want 200 with 10 -- the "+
			"window is rows 5..14 of the corpus: %.300s", codeS, totalS, rawS)
	}
	codeC, totalC, hitsC, rawC := esSearchRaw(t, cluster, body)
	if codeC != codeS || totalC != totalS || hitsC != hitsS {
		t.Fatalf("node %d/%d/%d, cluster %d/%d/%d (code/total/hits):\n  node:    %.250s\n"+
			"  cluster: %.250s", codeS, totalS, hitsS, codeC, totalC, hitsC, rawS, rawC)
	}
}

// esCountRaw posts a DSL body to /_count and returns the status and count.
func esCountRaw(t *testing.T, ts *httptest.Server, body string) (int, int) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/_count", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return resp.StatusCode, 0
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("/_count answered 200 with an unreadable body: %v: %.200s", err, b)
	}
	return 200, out.Count
}

// THE TWO RANGE SURFACES ANSWER THE SAME BUCKETS, AND THEY ARE THE
// REFERENCE'S BUCKETS.
//
// This gate used to assert the opposite. `/select/logsql/hits` floored every
// bucket to a multiple of the step and both range-stats surfaces --
// `query.StatsQueryRange` on a node, `exactMatrix` on a router -- walked
// `for bs := from`, anchored on the request's own `start`; the difference was
// pinned as "a convention difference", reasoned from Prometheus.
//
// THE REFERENCE IMPLEMENTATION IS IN THIS REPOSITORY and it was never asked.
// `internal/bench/victoria-logs`, six rows at 00:15, 00:45, 01:15, 01:45,
// 02:15 and 02:45 on 2026-06-01Z, both binaries on one machine:
//
//	window                       surface              VictoriaLogs        simdlogs, before
//	start=00:30Z step=1h         hits                 00:00,01:00,02:00   same labels
//	                                                  = 2,2,2 total 6     = 1,2,1 total 4
//	start=00:30Z step=1h         stats_query_range    00:00,01:00,02:00   00:30,01:30
//	                                                  = 2,2,2             = 2,2
//	start=00:10Z step=30m        stats_query_range    00:00,00:30,01:00,  00:10,00:40,01:10,
//	                                                  01:30,02:00          01:40,02:10
//
// VictoriaLogs has no divergence: it floors BOTH surfaces, and on both a
// bucket is the whole [k*step, (k+1)*step) whatever the request's bounds fall
// on. Two things were wrong here, not one -- the anchoring, and the partial
// edge buckets, which the four-row fixture the old gate used could not see
// because it had no row outside the requested window. Both are fixed and both
// are asserted below.
//
// The widening is confined to the RANGE surfaces: `/select/logsql/query` over
// the same window still answers the four rows inside it, which is also what
// the reference does.
//
// Entry 134 attributed the walk to `exactVector`. `exactVector` is the INSTANT
// handler and has no bucket loop at all -- it stamps `instantStamp(to, nowNs)`
// once. The loop is `exactMatrix`'s.
func TestTheTwoRangeSurfacesAgreeOnBuckets(t *testing.T) {
	t.Parallel()
	node := realShard(t, nil)
	var sb strings.Builder
	for _, ns := range []int64{
		1780272900000000000, // 2026-06-01T00:15:00Z -- BEFORE the requested start
		1780274700000000000, // 2026-06-01T00:45:00Z
		1780276500000000000, // 2026-06-01T01:15:00Z
		1780278300000000000, // 2026-06-01T01:45:00Z
		1780280100000000000, // 2026-06-01T02:15:00Z
		1780281900000000000, // 2026-06-01T02:45:00Z -- AFTER the requested end
	} {
		fmt.Fprintf(&sb, `{"_time":%d,"_msg":"m","level":"error"}`+"\n", ns)
	}
	resp, err := http.Post(node.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Neither bound is a multiple of the step, so both edge buckets are the
	// ones the old code got wrong.
	const win = "start=2026-06-01T00:30:00Z&end=2026-06-01T02:30:00Z&step=1h"
	get := func(path string) []byte {
		t.Helper()
		r, err := http.Get(node.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %.200s", path, r.StatusCode, raw)
		}
		return raw
	}

	// /hits, byte for byte what victoria-logs answered.
	var hits struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
			Values     []int    `json:"values"`
			Total      int      `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(get("/select/logsql/hits?query=*&"+win), &hits); err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 1 {
		t.Fatalf("%d hits series, want 1", len(hits.Hits))
	}
	h := hits.Hits[0]
	wantStamps := []string{"2026-06-01T00:00:00Z", "2026-06-01T01:00:00Z", "2026-06-01T02:00:00Z"}
	wantVals := []int{2, 2, 2}
	if fmt.Sprint(h.Timestamps) != fmt.Sprint(wantStamps) || fmt.Sprint(h.Values) != fmt.Sprint(wantVals) {
		t.Errorf("/hits buckets %v = %v, want %v = %v. A bucket is the whole "+
			"[k*step, (k+1)*step): the 00:00Z bucket counts the row at 00:15Z, which "+
			"is before the requested start, and the 02:00Z bucket counts 02:45Z, "+
			"which is after the requested end. victoria-logs answers %v.",
			h.Timestamps, h.Values, wantStamps, wantVals, wantVals)
	}
	if h.Total != 6 {
		t.Errorf("/hits total %d, want 6 -- the sum of the three whole buckets, "+
			"which is MORE than the four rows inside [start,end). That is what the "+
			"reference answers.", h.Total)
	}

	// stats_query_range, the same buckets and the same values.
	var matrix struct {
		Data struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/select/logsql/stats_query_range?query="+
		url.QueryEscape("* | stats count() n")+"&"+win), &matrix); err != nil {
		t.Fatal(err)
	}
	if len(matrix.Data.Result) != 1 {
		t.Fatalf("%d series, want 1", len(matrix.Data.Result))
	}
	vals := matrix.Data.Result[0].Values
	// 00:00Z, 01:00Z, 02:00Z in unix seconds -- the labels /hits stamps.
	wantMatrix := [][2]any{{1780272000.0, "2"}, {1780275600.0, "2"}, {1780279200.0, "2"}}
	if fmt.Sprint(vals) != fmt.Sprint(wantMatrix) {
		t.Errorf("stats_query_range = %v, want %v. The walk is floored to a step "+
			"multiple and each bucket is a whole step (stats_range.go / exactMatrix); "+
			"victoria-logs answers the same three buckets.", vals, wantMatrix)
	}

	// THE AGREEMENT ITSELF, checked as one statement rather than as two
	// independent shapes. This is the assertion the old gate had inverted.
	if len(h.Timestamps) != len(vals) {
		t.Fatalf("/hits returned %d buckets and stats_query_range %d. The two "+
			"surfaces answer one window; victoria-logs floors both and so must "+
			"this. If a divergence was reintroduced deliberately, "+
			"docs/compatibility.md and the differential in internal/bench have to "+
			"move with it.", len(h.Timestamps), len(vals))
	}
	for i := range vals {
		sec, _ := time.Parse(time.RFC3339, h.Timestamps[i])
		if got, want := vals[i][0].(float64), float64(sec.Unix()); got != want {
			t.Errorf("bucket %d: stats_query_range stamps %v, /hits stamps %v (%s)",
				i, got, want, h.Timestamps[i])
		}
		var n int
		fmt.Sscan(vals[i][1].(string), &n)
		if n != h.Values[i] {
			t.Errorf("bucket %d (%s): stats_query_range counts %d, /hits counts %d",
				i, h.Timestamps[i], n, h.Values[i])
		}
	}

	// AND THE POINT QUERY IS NOT WIDENED. The range surfaces read whole
	// buckets; /select/logsql/query reads the window it was given, which is
	// four of the six rows. The reference draws the same line.
	rows := strings.TrimSpace(string(get("/select/logsql/query?query=*&start=2026-06-01T00:30:00Z&end=2026-06-01T02:30:00Z")))
	if got := len(strings.Split(rows, "\n")); got != 4 {
		t.Errorf("/select/logsql/query returned %d rows over [00:30Z,02:30Z), want 4. "+
			"Widening the bucket walk must not widen the point query.", got)
	}
}

// The ROUTER walks the same buckets as the node, over the same window.
//
// `exactMatrix` is the second copy of the bucket walk -- the path a
// non-mergeable aggregate takes across shards -- and it carried the same two
// faults: anchored on the request's `start`, and fetching only [from,to) so the
// two edge buckets were partial. A cluster then answered a smaller number than
// the same rows on one machine, which is the class the single-node/cluster
// differential exists to catch and which no window in it could see, every one
// being step-aligned.
//
// `avg()` rather than `count()` because count MERGES from per-shard outputs and
// never reaches this function; avg is one of the five that force the exact
// path (see needsExactStats).
func TestTheRouterAndNodeAgreeOnRangeBuckets(t *testing.T) {
	t.Parallel()
	// n is chosen so each whole bucket averages a round number and the two
	// rows of each bucket sit on DIFFERENT shards: 00:00 -> 2, 01:00 -> 6,
	// 02:00 -> 10. A router that aggregated per shard cannot produce those.
	type row struct {
		ns int64
		n  int
	}
	rows := []row{
		{1780272900000000000, 1},  // 00:15Z -- before the requested start
		{1780274700000000000, 3},  // 00:45Z
		{1780276500000000000, 5},  // 01:15Z
		{1780278300000000000, 7},  // 01:45Z
		{1780280100000000000, 9},  // 02:15Z
		{1780281900000000000, 11}, // 02:45Z -- after the requested end
	}
	var all []string
	shard := [2][]string{}
	for i, rw := range rows {
		line := fmt.Sprintf(`{"_time":%d,"_msg":"m","n":"%d"}`, rw.ns, rw.n)
		all = append(all, line)
		shard[i%2] = append(shard[i%2], line)
	}
	node := realShard(t, all)
	cluster := router(t, realShard(t, shard[0]).URL, realShard(t, shard[1]).URL)

	const path = "/select/logsql/stats_query_range?step=1h" +
		"&start=2026-06-01T00:30:00Z&end=2026-06-01T02:30:00Z&query="
	q := url.QueryEscape("* | stats avg(n) a")
	read := func(ts *httptest.Server) string {
		t.Helper()
		r, err := http.Get(ts.URL + path + q)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%d: %.200s", r.StatusCode, raw)
		}
		var m struct {
			Data struct {
				Result []struct {
					Values [][2]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unreadable matrix: %v: %.200s", err, raw)
		}
		if len(m.Data.Result) != 1 {
			t.Fatalf("%d series, want 1: %.300s", len(m.Data.Result), raw)
		}
		return fmt.Sprint(m.Data.Result[0].Values)
	}

	// 00:00Z, 01:00Z, 02:00Z -- the floored labels, each a whole step, so the
	// first bucket includes 00:15Z and the last includes 02:45Z.
	want := fmt.Sprint([][2]any{
		{1780272000.0, "2"}, {1780275600.0, "6"}, {1780279200.0, "10"},
	})
	gotNode, gotCluster := read(node), read(cluster)
	if gotNode != want {
		t.Errorf("node    = %v, want %v", gotNode, want)
	}
	if gotCluster != want {
		t.Errorf("cluster = %v, want %v. exactMatrix must floor the walk and fetch "+
			"whole buckets, the same as query.StatsQueryRange.", gotCluster, want)
	}
	if gotNode != gotCluster {
		t.Errorf("the router and the node disagree over one window:\n  node:    %v\n"+
			"  cluster: %v", gotNode, gotCluster)
	}
}
