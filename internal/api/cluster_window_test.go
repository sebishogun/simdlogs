package api

// The window a coordinator PRICES is the window the shards SCANNED.
//
// A router applies the pipes over rows it was handed, so it has to be told
// what window those rows came from. `rate()` is a count divided by the
// window's seconds, and there are two ways to get that wrong, both at HTTP 200
// and both found by review rather than by these tests, which is why they exist
// now.
//
// One: the request's `end` and the query's own `_time:` filter are different
// windows, and the shards scan the intersection. Pricing the request's window
// divides 30 seconds of rows by the 146 years `to` defaults to.
//
// Two: `end=0` meant two things. `parseRequest` read a zero `To` as "no end"
// and so did `resolveTimePreds`, so an explicit epoch end was honoured for a
// bare query and ignored for one carrying an absolute `_time:` filter -- one
// binary, one parameter, two answers.

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func TestTheCoordinatorPricesTheWindowTheShardsScanned(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	// THE `| sort by (n) |` IS LOAD-BEARING, and not for the reason it looks.
	//
	// It forces the EXACT path for the mergeable aggregates: `count()` has a
	// mergeable partial state, so without a non-row-local prefix its rows take
	// the merge branch, which never prices a window at the coordinator -- and
	// this test is about coordinator pricing. `rate()` is non-mergeable and
	// would take the exact path either way; the prefix puts both on the same
	// branch so the count rows control for the rate rows.
	//
	// An earlier version of this comment gave a different reason: that a
	// LEADING stats pipe under an absolute `_time:` filter answers empty on a
	// single node, "a node defect of its own". That was true when written and
	// entry 113's `matchBitset` fix closed it -- measured now,
	// `_time:[..] | stats count() c` answers {"c":"30"} on a node -- so the
	// justification outlived the condition, which is the shape docs/wrong.md
	// files under "a deleted behaviour leaves its justification behind". The
	// sort stays for the branch-forcing reason above.
	const filtered = `_time:[2026-06-01T12:00:00Z, 2026-06-01T12:00:30Z] | sort by (n)`
	for _, tc := range []struct {
		name, query, win string
		wantRows         bool
	}{
		// The measured case: no start/end at all, so the request window is
		// [0, 1<<62) and the filter's is 30 seconds. node 0.967741935483871,
		// router 0.000000006505213034913027 before the fix.
		{"filter, no request window", filtered + ` | stats rate() r`, "", true},
		{"filter, wide request window", filtered + ` | stats rate() r`,
			"&start=2020-01-01T00:00:00Z&end=2030-01-01T00:00:00Z", true},
		{"filter, narrow request window", filtered + ` | stats rate() r`,
			"&start=2026-06-01T12:00:10Z&end=2026-06-01T12:00:20Z", true},
		// An explicit epoch end, which used to mean "no end" to the narrowing
		// and so let the filter's window win.
		{"epoch end with a filter", filtered + ` | stats rate() r`, "&end=0", false},
		{"epoch end, negative start", filtered + ` | stats rate() r`, "&start=-1&end=0", false},
		{"epoch end, RFC3339", filtered + ` | stats rate() r`,
			"&end=1970-01-01T00:00:00Z", false},
		// And the same three without a filter, where the request window is the
		// only one there is.
		{"no filter, no window", `* | stats rate() r`, "", true},
		{"no filter, epoch end", `* | stats rate() r`, "&end=0", false},
		// count() as well as rate(): a count does not divide by the window, so
		// if only rate() agrees the window is still wrong and something else
		// is hiding it.
		{"filter, count", filtered + ` | stats count() c`, "", true},
		{"epoch end, count", filtered + ` | stats count() c`, "&end=0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := "/select/logsql/query?query=" + urlEscape(tc.query) + tc.win
			codeS, bodyS := chaosGet(t, single.URL+p)
			codeC, bodyC := chaosGet(t, cluster.URL+p)
			if codeS != 200 {
				t.Fatalf("the node cannot answer this: %d %.200s", codeS, bodyS)
			}
			if codeC != codeS {
				t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
			}
			if got := strings.TrimSpace(bodyS) != ""; got != tc.wantRows {
				t.Fatalf("the node answered rows=%v, want %v -- the case is not "+
					"the shape it claims and proves nothing: %.200s",
					got, tc.wantRows, bodyS)
			}
			if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
				t.Fatalf("the coordinator priced a different window:\n"+
					"  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
			}
		})
	}
}

// /select/logsql/hits: the router's ceiling uses the endpoint's OWN step rule.
//
// The router grew a bucket ceiling for this endpoint and reached for
// `parseStepNs`, which is a different rule from the one `selectHits` applies:
// it takes a bare integer as seconds where selectHits ignores anything
// `time.ParseDuration` rejects. So `?step=1` was 1440 one-minute buckets to
// the node and 86400 one-second buckets to the ceiling, and the router
// refused what the node answered.
func TestTheHitsCeilingUsesTheHitsStepRule(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, step := range []string{
		"1",    // not a duration: the node uses its 1-minute default
		"5",    // same
		"1s",   // a duration, and small enough over a day to hit the ceiling
		"1m",   // a duration, fine
		"1h",   // a duration, fine
		"0s",   // not positive: the node uses its default
		"-5m",  // same
		"",     // absent: the node uses its default
		"abc",  // unparseable: the node uses its default
		"1.5m", // a duration Go accepts
	} {
		t.Run("step="+step, func(t *testing.T) {
			p := "/select/logsql/hits?query=*&start=2024-01-01T00:00:00Z" +
				"&end=2024-01-02T00:00:00Z"
			if step != "" {
				p += "&step=" + step
			}
			codeS, bodyS := chaosGet(t, single.URL+p)
			codeC, bodyC := chaosGet(t, cluster.URL+p)
			if codeC != codeS {
				t.Fatalf("step=%q: node %d, cluster %d\n  node:    %.150s\n"+
					"  cluster: %.150s", step, codeS, codeC, bodyS, bodyC)
			}
			// THE BODIES TOO. The first version compared status codes only,
			// so it would not have noticed the two sides answering a
			// different number of buckets with the same 200.
			if codeS == 200 && !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
				t.Errorf("step=%q: same status, different answer\n"+
					"  node:    %.200s\n  cluster: %.200s", step, bodyS, bodyC)
			}
		})
	}
}

// A `_time:` FILTER NARROWS THE WINDOW. It never widens it.
//
// Single node, no cluster: this is the plainest statement of the rule and it
// was broken by the fix for the opposite problem. `Query.ToSet` was added so a
// resolved epoch end would stop being read as "no end"; the narrowing then
// reads `!q.ToSet` as "the caller resolved nothing, take the filter's end" --
// and the two stats entry points set From/To/Now without setting ToSet, so
// for them the filter's end replaced the request's outright.
//
//	stats_query?start=12:00:00&end=12:00:10, _time:[12:00:00,12:00:30]
//	  before   10 rows        after the ToSet change   30 rows
//	stats_query_range, same filter, step=10s over 30s
//	  before   [10,10,10]     after                    [30,20,10]
//
// The range row is the one worth staring at: only `From` moves per bucket, so
// every bucket ends at the filter's end and each is a superset of the next.
// Buckets that are not buckets, at HTTP 200.
//
// `Query.SetWindow` is the answer -- one function that sets all three, so the
// only way to resolve a window is to mark it resolved.
func TestATimeFilterNarrowsTheWindowAndNeverWidensIt(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])

	const filter = `_time:[2026-06-01T12:00:00Z, 2026-06-01T12:00:30Z] | sort by (n)`
	for _, tc := range []struct {
		name, path, want string
	}{
		{
			// The request window is a third of the filter's. The answer is the
			// intersection, which is the request's ten seconds.
			name: "instant, request window inside the filter's",
			path: "/select/logsql/stats_query?start=2026-06-01T12:00:00Z" +
				"&end=2026-06-01T12:00:10Z&query=" + urlEscape(filter+` | stats count() c`),
			want: `"10"`,
		},
		{
			// And the control: a request window WIDER than the filter takes the
			// filter's, which is the narrowing doing its job.
			name: "instant, filter inside the request window",
			path: "/select/logsql/stats_query?start=2026-06-01T11:00:00Z" +
				"&end=2026-06-01T13:00:00Z&query=" + urlEscape(filter+` | stats count() c`),
			want: `"30"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, single.URL+tc.path)
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			if !strings.Contains(body, tc.want) {
				t.Errorf("want a count of %s, got %.250s", tc.want, body)
			}
		})
	}

	// SQL IS A READ SURFACE AND NARROWS THE SAME WAY.
	//
	// Driven here because it is the route the rule broke. `WHERE _time <= X`
	// becomes a TimeRange pred exactly like a LogsQL `_time:<=X`, and
	// `sqlQuery` resolved its window with a bare `q.From, q.To = timeWindow(r)`
	// -- no ToSet -- so the new `!q.ToSet` clause read "the caller resolved
	// nothing" and took the filter's end. Thirty rows where the same window on
	// `/select/logsql/query` returns ten, from one binary with one `end`, and
	// `federatedSQL` forwarded the widened window to every shard.
	//
	// The old clause was `q.To == 0`, which never fires for a real `end`, so
	// the site was invisible until the rule changed under it.
	t.Run("sql, an absolute _time bound does not widen past end", func(t *testing.T) {
		const q = `SELECT * FROM logs WHERE _time <= '2026-06-01T12:00:30Z'`
		p := "/select/sql?start=2026-06-01T12:00:00Z&end=2026-06-01T12:00:10Z" +
			"&query=" + urlEscape(q)
		code, body := chaosGet(t, single.URL+p)
		if code != 200 {
			t.Fatalf("%d: %.200s", code, body)
		}
		rows := strings.Count(strings.TrimSpace(body), "\n") + 1
		if strings.TrimSpace(body) == "" {
			rows = 0
		}
		// Ten seconds of the corpus is ten rows. Thirty is the filter's whole
		// window, which is the widening.
		if rows != 10 {
			t.Errorf("the request asked for ten seconds and got %d rows; the "+
				"filter's own window holds 30. A `_time` bound must narrow "+
				"the request's window and never widen it:\n%.300s", rows, body)
		}
		// And the LogsQL route with the same window is the reference: one
		// binary, one `end`, one answer.
		lp := "/select/logsql/query?start=2026-06-01T12:00:00Z" +
			"&end=2026-06-01T12:00:10Z&query=" +
			urlEscape(`_time:<=2026-06-01T12:00:30Z`)
		lcode, lbody := chaosGet(t, single.URL+lp)
		if lcode != 200 {
			t.Fatalf("logsql control %d: %.200s", lcode, lbody)
		}
		lrows := strings.Count(strings.TrimSpace(lbody), "\n") + 1
		if strings.TrimSpace(lbody) == "" {
			lrows = 0
		}
		if rows != lrows {
			t.Errorf("sql answered %d rows and logsql %d for the same window "+
				"and the same bound", rows, lrows)
		}
	})

	// EVERY BUCKET HOLDS ITS OWN TEN SECONDS. Three of them, ten rows each.
	t.Run("range, each bucket is its own window", func(t *testing.T) {
		p := "/select/logsql/stats_query_range?step=10s" +
			"&start=2026-06-01T12:00:00Z&end=2026-06-01T12:00:30Z&query=" +
			urlEscape(filter+` | stats count() c`)
		code, body := chaosGet(t, single.URL+p)
		if code != 200 {
			t.Fatalf("%d: %.200s", code, body)
		}
		for _, want := range []string{`"10"`} {
			if !strings.Contains(body, want) {
				t.Fatalf("no bucket holds %s: %.300s", want, body)
			}
		}
		// The failure shape was [30,20,10]: each bucket ending at the filter's
		// end rather than its own. Either of those values present means the
		// buckets are nested rather than adjacent.
		for _, bad := range []string{`"30"`, `"20"`} {
			if strings.Contains(body, bad) {
				t.Errorf("a bucket holds %s, so the buckets are nested rather "+
					"than adjacent -- every one is ending at the filter's end "+
					"instead of its own: %.300s", bad, body)
			}
		}
	})
}

// EVERY read surface answers an absolute `_time:` filter, and the cluster
// agrees with the node.
//
// This replaces a test that pinned the opposite. `matchBitset` had its own
// copy of `leafBitset`'s dispatch and the copy was missing the `isTimePred`
// case, so `_time` -- a ColTimestamp, not a ColDict -- matched nothing, and
// every surface reading through it answered EMPTY for a filter that matches
// every row. The same filter with no stats pipe returned all 30, because that
// path goes through `leafBitset`, which is what made it look like a
// stats-pipe problem rather than a dispatch problem.
//
// The pin said "when the node is fixed this test fails, which is the point";
// it did, and this is what replaced it. `query.Count` is in the blast radius
// too, so an alert rule whose query carries a `_time:` filter never fired.
func TestEveryReadSurfaceAnswersATimeFilter(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	const win = "&start=2026-06-01T11:00:00Z&end=2026-06-01T13:00:00Z"
	const filt = `_time:[2026-06-01T12:00:00Z, 2026-06-01T12:00:30Z]`
	// sumHits marks a surface whose empty answer is not an empty LITERAL.
	//
	// `/select/logsql/hits` returns a dense array, so the defect this test
	// exists for produced `[0,0,0]`: a non-empty body containing no
	// `"hits":[]`, identical on node and cluster. It passed every check below.
	// A reviewer restored the old dispatch and this subtest stayed GREEN while
	// the other seven reddened. Counting the buckets is what makes the row
	// mean anything.
	for _, tc := range []struct {
		name, path string
		sumHits    bool
		// wantFields are facet field names that must be present. An empty
		// answer for THIS surface is not an empty literal either: under the
		// defect `facets` degrades to `_time` alone, which is a non-empty body
		// containing `"values":[{...}]`, identical on node and cluster. It
		// passed every check below, and a reviewer proved it by restoring the
		// old dispatch and watching eight surfaces redden while this one
		// stayed green.
		wantFields []string
		// wantSubstr is a REFERENCE NUMBER the answer must contain.
		//
		// Non-emptiness and node==cluster leave a PARTIAL degradation green: 1
		// row of 30 on both sides satisfies both. The numbers are already
		// written down -- engine.go's dispatch comment records what each
		// surface answers for this exact filter -- and none of them was
		// asserted, so a surface that lost 29 of its 30 rows passed.
		wantSubstr string
		// wantHitSum is the exact bucket total, where 0 means "not this case".
		wantHitSum int
		// wantRowCount is the exact NDJSON row count, where 0 means "not this
		// case".
		wantRowCount int
	}{
		{name: "query", wantRowCount: 30, path: "/select/logsql/query?query=" + urlEscape(filt)},
		{name: "hits", sumHits: true, wantHitSum: 30,
			path: "/select/logsql/hits?step=10s" + win + "&query=" + urlEscape(filt)},
		{name: "field_values", path: "/select/logsql/field_values?field=level" + win +
			"&query=" + urlEscape(filt)},
		{name: "field_names", path: "/select/logsql/field_names?" + win + "&query=" + urlEscape(filt)},
		{name: "streams", path: "/select/logsql/streams?" + win + "&query=" + urlEscape(filt)},
		// facets is named in the defect list and in docs/wrong.md and had no
		// case here at all -- it degraded to `_time` only under the filter.
		{name: "facets", wantFields: []string{"_msg", "level", "user"},
			path: "/select/logsql/facets?" + win + "&query=" + urlEscape(filt)},
		{name: "stats_query count", wantSubstr: `"30"`, path: "/select/logsql/stats_query?" + win +
			"&query=" + urlEscape(filt+` | stats count() c`)},
		{name: "stats_query by level", path: "/select/logsql/stats_query?" + win +
			"&query=" + urlEscape(filt+` | stats by (level) count() c`)},
		{name: "top", wantRowCount: 2, path: "/select/logsql/query?" + win + "&query=" + urlEscape(filt+` | top 2 by (level)`)},
		{name: "uniq", wantRowCount: 3, path: "/select/logsql/query?" + win + "&query=" + urlEscape(filt+` | uniq by (level)`)},
		// SQL is a read surface like the others and was not driven here. That
		// is how `q.From, q.To = timeWindow(r)` survived the round that made
		// `!q.ToSet` mean "take the filter's end": nothing asked this route a
		// question with a `_time` bound in it.
		{name: "sql", path: "/select/sql?" + win + "&query=" +
			urlEscape(`SELECT * FROM logs WHERE _time >= '2026-06-01T12:00:00Z'`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codeS, bodyS := chaosGet(t, single.URL+tc.path)
			if codeS != 200 {
				t.Fatalf("the node answered %d: %.200s", codeS, bodyS)
			}
			// THE FILTER MATCHES EVERY ROW, so an empty answer is the defect
			// this test replaced, not a legitimate result.
			if strings.TrimSpace(bodyS) == "" {
				t.Fatalf("the node answered nothing for a filter that matches " +
					"every row in the corpus")
			}
			for _, empty := range []string{`"values":[]`, `"result":[]`, `"hits":[]`} {
				if strings.Contains(bodyS, empty) {
					t.Errorf("the node answered %s for a filter matching every "+
						"row: %.250s", empty, bodyS)
				}
			}
			if len(tc.wantFields) > 0 {
				got := facetFields(t, bodyS)
				for _, want := range tc.wantFields {
					if !got[want] {
						t.Errorf("the facets answer names no %q field for a "+
							"filter matching every row; it has %v. Degrading "+
							"to _time alone is the defect this case exists "+
							"for, and it is a non-empty body", want, sortedFacetFields(got))
					}
				}
			}
			if tc.wantSubstr != "" && !strings.Contains(bodyS, tc.wantSubstr) {
				t.Errorf("the node's answer does not contain %s, which is what "+
					"this surface answers for this filter. Non-emptiness and "+
					"node==cluster both pass on a partial degradation; the "+
					"number is what does not: %.250s", tc.wantSubstr, bodyS)
			}
			if tc.wantRowCount > 0 {
				got := 0
				for _, l := range strings.Split(strings.TrimSpace(bodyS), "\n") {
					if strings.TrimSpace(l) != "" {
						got++
					}
				}
				if got != tc.wantRowCount {
					t.Errorf("the node returned %d rows, want %d. A surface "+
						"that lost most of its rows is still non-empty and "+
						"still agrees with a cluster that lost the same ones",
						got, tc.wantRowCount)
				}
			}
			if tc.sumHits {
				// A dense array of zeros is the empty answer for this surface,
				// and no literal above matches it. `hitBuckets` counts the
				// BUCKETS, which is 3 either way -- the values are the thing.
				sum := hitValues(t, bodyS)
				if sum == 0 {
					t.Errorf("the node counted %d hits over a filter matching "+
						"every row in the corpus -- the buckets are all "+
						"present and all zero: %.250s", sum, bodyS)
				} else if tc.wantHitSum > 0 && sum != tc.wantHitSum {
					t.Errorf("the node counted %d hits, want %d. A non-zero "+
						"sum is not the right sum, and half the rows missing "+
						"passes every other check here: %.250s",
						sum, tc.wantHitSum, bodyS)
				}
			}
			codeC, bodyC := chaosGet(t, cluster.URL+tc.path)
			if codeC != codeS {
				t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
			}
			if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
				t.Errorf("node and cluster disagree:\n  node:    %.200s\n"+
					"  cluster: %.200s", bodyS, bodyC)
			}
		})
	}
}

// The EXACT-STATS surfaces price the scanned window too.
//
// Split from the test above because that one drove `/select/logsql/query`
// only, and a reviewer found the same defect alive on the sibling routes:
//
//	query=_time:[12:00:00Z,12:00:30Z] | sort by (n) | stats rate() r
//	  /select/logsql/query        node 0.967741935483871  cluster same
//	  /select/logsql/stats_query  node 0.967741935483871  cluster 0.00000001678877628337586
//
// A factor of 5.8e7, both 200. One route was fixed and its two siblings were
// not, and the test could not tell because it did not ask them.
func TestTheExactStatsSurfacesPriceTheScannedWindow(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	const filt = `_time:[2026-06-01T12:00:00Z, 2026-06-01T12:00:30Z] | sort by (n)`
	for _, agg := range []string{
		` | stats rate() r`, // divides by the window: the sensitive one
		` | stats avg(n) a`, // non-mergeable, takes the exact path
		` | stats quantile(0.5, n) p`,
		` | stats count() c`, // mergeable, takes the merge path
	} {
		for _, route := range []string{
			"/select/logsql/query?query=",
			"/select/logsql/stats_query?query=",
			"/select/logsql/stats_query_range?step=1h&start=2026-06-01T00:00:00Z" +
				"&end=2026-06-02T00:00:00Z&query=",
		} {
			name := route[15:strings.Index(route, "?")] + agg
			t.Run(name, func(t *testing.T) {
				p := route + urlEscape(filt+agg)
				codeS, bodyS := chaosGet(t, single.URL+p)
				codeC, bodyC := chaosGet(t, cluster.URL+p)
				if codeS != 200 {
					t.Skipf("the node cannot answer this: %d %.150s", codeS, bodyS)
				}
				if codeC != codeS {
					t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
				}
				setS := statsSet(t, bodyS)
				if len(setS) == 0 {
					t.Fatalf("the node answered nothing, so this compares two "+
						"empties: %.200s", bodyS)
				}
				if !equalSets(setS, statsSet(t, bodyC)) {
					t.Fatalf("the coordinator priced a different window:\n"+
						"  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
				}
			})
		}
	}
}

// The hits ceiling holds when the window's WIDTH OVERFLOWS, and holds the same
// way on both sides.
//
// `to - from` is int64 nanoseconds. `?start=-4700000000000000000` against the
// default `to` of 1<<62 wraps negative, so a raw `(to-from)/step` came out
// negative and neither the narrowing nor the 413 fired. The router had an
// overflow-safe width and the node had its own raw copy, so:
//
//	/select/logsql/hits?query=*&start=-4700000000000000000
//	  node    200, a body over a megabyte      cluster 413
//
// which is the unbounded response the ceiling exists to stop, reachable with
// one negative `start`. There is one ceiling now.
//
// Also the WINDOW, not just the refusal: with no start/end each shard used to
// resolve its own `now`, and this merge folds points by timestamp, so two
// shards a nanosecond apart produced twice the buckets.
func TestTheHitsCeilingHoldsWhenTheWindowOverflows(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, tc := range []struct{ name, q string }{
		{"negative start, no end", "&start=-4700000000000000000"},
		{"negative start with a step", "&start=-4700000000000000000&step=1m"},
		{"both ends, wrapping width",
			"&start=-9000000000000000000&end=4611686018427387904"},
		{"negative start, no wrap", "&start=-1000000000000000000"},
		{"no window at all, fine step", "&step=1ns"},
		{"no window at all, default step", ""},
		{"no window at all, coarse step", "&step=100ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := "/select/logsql/hits?query=*" + tc.q
			codeS, bodyS := chaosGet(t, single.URL+p)
			codeC, bodyC := chaosGet(t, cluster.URL+p)
			if codeC != codeS {
				t.Fatalf("node %d, cluster %d\n  node:    %.180s\n  cluster: %.180s",
					codeS, codeC, bodyS, bodyC)
			}
			// `>= chaosReadCap`, not `> chaosReadCap`.
			//
			// `chaosGet` reads through an `io.LimitReader` at that cap, so a
			// body larger than it arrives truncated to exactly the cap and
			// `> cap` is unreachable by construction -- the assertion this
			// replaced could never fire. When the overflow-safe width was
			// mutated back to a raw `to-from`, the node did answer 200 with a
			// 2,500,061-byte body, and this test went red only because
			// `hitBuckets` failed to parse the truncated JSON: "unexpected end
			// of JSON input", which reads as a JSON bug rather than as "the
			// ceiling did not fire".
			if codeS == 200 && len(bodyS) >= chaosReadCap {
				t.Errorf("the node answered 200 with a body at or past the "+
					"%d-byte read cap, so the ceiling did not fire on a window "+
					"it cannot have meant", chaosReadCap)
			}
			// A dense response is one bucket per step, so the BUCKET COUNT is
			// the thing to compare. Not the byte length: with no start/end the
			// node and the router each read their own clock, so the timestamp
			// TEXT differs by a digit or two between two processes that agree
			// completely about how many buckets there are.
			//
			// The defect this catches was 240 against 480 -- the shards each
			// resolving their own `now`, and this merge folding points by
			// timestamp, so nothing folded and the buckets doubled.
			if codeS == 200 {
				if nS, nC := hitBuckets(t, bodyS), hitBuckets(t, bodyC); nS != nC {
					t.Errorf("the node answered %d buckets and the cluster %d "+
						"-- the shards resolved different windows", nS, nC)
				}
			}
		})
	}
}

// hitBuckets counts the points in a /select/logsql/hits answer.
//
// The dense shape is one bucket per step across the window, so this is the
// number the window determines and the clock does not.
// hitValues sums the per-bucket COUNTS of a hits answer.
//
// Separate from hitBuckets, which counts the buckets. The defect that made
// eight read surfaces answer empty under a `_time:` filter produced the right
// NUMBER of buckets with zero in every one of them, so a test written on
// hitBuckets sees 3 both ways and passes on the defect.
func hitValues(t *testing.T, body string) int {
	t.Helper()
	var v struct {
		Hits []struct {
			Values []int `json:"values"`
		} `json:"hits"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("not a hits answer: %v: %.200s", err, body)
	}
	sum := 0
	for _, h := range v.Hits {
		for _, n := range h.Values {
			sum += n
		}
	}
	return sum
}

func hitBuckets(t *testing.T, body string) int {
	t.Helper()
	var v struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
		} `json:"hits"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("not a hits answer: %v: %.200s", err, body)
	}
	n := 0
	for _, h := range v.Hits {
		if len(h.Timestamps) > n {
			n = len(h.Timestamps)
		}
	}
	return n
}

// facetFields is the set of field names a facets answer reports.
//
// The defect it exists for degrades the answer to `_time` alone -- a non-empty
// body, with no empty literal in it, identical on node and cluster. Counting
// the FIELDS is what tells them apart.
func facetFields(t *testing.T, body string) map[string]bool {
	t.Helper()
	var v struct {
		Facets []struct {
			FieldName string `json:"field_name"`
		} `json:"facets"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("not a facets answer: %v: %.200s", err, body)
	}
	out := map[string]bool{}
	for _, f := range v.Facets {
		out[f.FieldName] = true
	}
	return out
}

func sortedFacetFields(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
