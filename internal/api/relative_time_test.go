package api

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A RELATIVE `_time:` filter works through an HTTP route.
//
// Nothing drove one. `parseRequest` is the entry point every read surface goes
// through and the only place a request's `now` is set, and reverting its
// `q.SetNow(...)` to a bare `q.Now = ...` -- the exact hazard the NowSet flag
// exists to prevent -- left the ENTIRE suite green while:
//
//	GET /select/logsql/query?query=_time:5m   200, 0 rows   (correct: 1)
//
// `TestARelativeTimeFilterWithNoRequestTimeUsesTheClock` covers the fallback
// inside `resolveTimePreds` and not the setter that feeds it, so the round's
// headline invariant was unenforced on every route at once.
//
// RELATIVE times in the fixture, deliberately. The shared `corpus` helper pins
// rows to 2026-06-01, which no `_time:5m` can ever match; a test written on it
// would compare two empty answers and pass on the defect.
func TestARelativeTimeFilterWorksThroughTheRouteThatSetsNow(t *testing.T) {
	now := time.Now()
	rows := []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"recent"}`, now.Add(-1*time.Minute).UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"old"}`, now.Add(-1*time.Hour).UnixNano()),
	}
	node := realShard(t, rows)

	for _, tc := range []struct {
		name, query string
		want        []string
	}{
		// The whole point: five minutes back from NOW selects the recent row
		// and not the hour-old one.
		{"last 5 minutes", `_time:5m`, []string{"recent"}},
		{"last 2 hours", `_time:2h`, []string{"recent", "old"}},
		{"one-sided relative", `_time:>=5m`, []string{"recent"}},
		// The control: no time filter at all returns both, so a case above
		// returning one row is the filter and not an empty store.
		{"no filter", `*`, []string{"recent", "old"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/logsql/query?query="+
				urlEscape(tc.query))
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			for _, want := range tc.want {
				if !strings.Contains(body, `"`+want+`"`) {
					t.Errorf("%q did not return the %q row. A relative filter "+
						"resolves against the request's `now`, and a `now` that "+
						"was never set resolves it into a window nothing "+
						"matches:\n%.300s", tc.query, want, body)
				}
			}
			// And the rows it must NOT return.
			for _, all := range []string{"recent", "old"} {
				wanted := false
				for _, w := range tc.want {
					if w == all {
						wanted = true
					}
				}
				if !wanted && strings.Contains(body, `"`+all+`"`) {
					t.Errorf("%q returned the %q row, which is outside it:\n%.300s",
						tc.query, all, body)
				}
			}
		})
	}
}

// The same filter through the STATS surfaces, which resolve `now` themselves.
//
// `StatsQuery`/`StatsQueryRange` call SetNow on their own rather than
// inheriting parseRequest's, so they are a second entry point with the same
// invariant and no coverage either.
func TestARelativeTimeFilterWorksThroughTheStatsSurfaces(t *testing.T) {
	now := time.Now()
	rows := []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"recent","n":"1"}`, now.Add(-1*time.Minute).UnixNano()),
		// Inside the `end=now-30m` window and outside the `_time:5m` one, so
		// the two cannot be satisfied by the same instant.
		fmt.Sprintf(`{"_time":%d,"_msg":"middle","n":"1"}`, now.Add(-32*time.Minute).UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"old","n":"1"}`, now.Add(-1*time.Hour).UnixNano()),
	}
	node := realShard(t, rows)

	// AN EXPLICIT `end` IS WHAT SEPARATES THE TWO. Without one, `statsQuery`
	// sets `to = time.Now()`, and the `!NowSet` fallback resolves against `To`
	// -- the same instant -- so a case with no `end` passes whether the setter
	// is there or not. Reverting `stats_range.go`'s SetNow left the whole
	// suite green until these rows existed.
	//
	// With `end` at now-30m and a `_time:5m` filter, the request window ends
	// half an hour ago and the filter's five minutes are measured from NOW, so
	// they do not overlap and the answer is empty. Resolving the filter against
	// `To` instead makes them the same five minutes and returns the row.
	endAt := now.Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano)
	for _, tc := range []struct{ name, path, want string }{
		{"stats_query 5m", "/select/logsql/stats_query?query=" +
			urlEscape(`_time:5m | stats count() c`), `"1"`},
		{"stats_query 2h", "/select/logsql/stats_query?query=" +
			urlEscape(`_time:2h | stats count() c`), `"3"`},
		{"stats_query, end before the relative window", "/select/logsql/stats_query?end=" +
			endAt + "&query=" + urlEscape(`_time:5m | stats count() c`), `"result":[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+tc.path)
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			if !strings.Contains(body, tc.want) {
				t.Errorf("want %s, got %.250s. A relative filter on this "+
					"surface resolves against the `now` it sets itself",
					tc.want, body)
			}
		})
	}

	// THE RANGE SURFACE resolves per bucket, and nothing covered it at all.
	//
	// `stats_range.go`'s second SetNow is on that path. Reverting it leaves the
	// entire suite green while a `_time:5m` filter over a 60-minute range at a
	// 10-minute step goes from one bucket to three: the relative filter
	// resolving against each bucket's own end rather than one request instant,
	// so it drifts backwards across the graph.
	t.Run("stats_query_range does not drift per bucket", func(t *testing.T) {
		p := "/select/logsql/stats_query_range?step=10m" +
			"&start=" + now.Add(-60*time.Minute).UTC().Format(time.RFC3339Nano) +
			"&end=" + now.UTC().Format(time.RFC3339Nano) +
			"&query=" + urlEscape(`_time:5m | stats count() c`)
		code, body := chaosGet(t, node.URL+p)
		if code != 200 {
			t.Fatalf("%d: %.200s", code, body)
		}
		// One bucket holds the recent row; a filter that drifts puts a count in
		// three of them.
		if n := strings.Count(body, `"1"`); n != 1 {
			t.Errorf("%d buckets hold a count, want 1. A relative filter is "+
				"measured from the REQUEST's now, one for the whole graph, not "+
				"from each bucket's own end:\n%.400s", n, body)
		}
	})
}
