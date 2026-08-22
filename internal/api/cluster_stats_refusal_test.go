package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/query"
)

// Every stats surface ANSWERS the same aggregates, with the same numbers a
// single node gives.
//
// This test used to assert the opposite -- that all three surfaces refused
// avg, quantile and count_uniq with 400 -- and what it was defending was real:
// federatedMatrix summed whatever the shards returned while its own comment
// claimed it was restricted to additive aggregates, so two shards averaging 10
// answered 20. The refusal was right about the MERGE.
//
// It was wrong about the alternative. A router does not have to merge shard
// aggregates; it can ask for the rows and aggregate once, which is what a
// single node does with the same rows. So the guarantee gets stronger rather
// than weaker: not "all three refuse alike" but "all three answer alike, and
// the answer is the node's".
//
// All three surfaces are checked because the failure this replaces was one
// binary answering the same aggregate three ways depending on the endpoint.
// Whichever one a client tried first became the one they trusted.
func TestEveryStatsSurfaceAnswersTheSameAggregates(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	// An EXPLICIT window, on every surface.
	//
	// Without one, `end` defaults to "now" and the two servers resolve it
	// microseconds apart -- so `rate()`, which is a count divided by the
	// window's seconds, differed in the last four digits of 1.678e-8. That is
	// two processes disagreeing about the time, not about the data, and a test
	// that compares it is asserting that two clock reads are equal.
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
	win := fmt.Sprintf("&start=%d&end=%d", base, base+86400)
	// A range surface whose buckets actually SPLIT the data.
	//
	// The corpus is 30 rows one second apart, so a 1h step puts every row in a
	// single bucket -- and a router that reused one parsed query across every
	// bucket, letting each bucket aggregate on top of the last one's state,
	// stayed green: with one non-empty bucket there is no "last one". The
	// 10-second step over the corpus's own minute gives three non-empty
	// buckets, which is the smallest fixture that can tell.
	rows := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).Unix()
	fine := fmt.Sprintf("&start=%d&end=%d", rows, rows+60)
	surfaces := []struct{ name, path string }{
		{"query", "/select/logsql/query?" + win + "&query="},
		{"stats_query", "/select/logsql/stats_query?" + win + "&query="},
		{"stats_query_range", "/select/logsql/stats_query_range?step=1h" + win + "&query="},
		{"stats_query_range_10s", "/select/logsql/stats_query_range?step=10s" + fine + "&query="},
	}
	for _, q := range []string{
		`* | stats avg(n) a`,
		`* | stats quantile(0.5, n) p`,
		`* | stats quantile(0.9, n) p`,
		`* | stats uniq(user) u`,
		`* | stats count_uniq(user) u`,
		`* | stats histogram(n) h`,
		`* | stats rate() r`,
		`* | stats by (level) avg(n) a`,
		`* | stats by (level) quantile(0.5, n) p`,
		// ORDER-SENSITIVE. values() emits in encounter order, so these are the
		// queries that can tell whether the router merged the shards' rows into
		// the order a single node produces or left them in whichever order the
		// fan-out goroutines finished. Without one of these in the table,
		// deleting the merge sort changed nothing any test could see.
		`* | stats values(user) v`,
		`* | stats by (level) values(user) v`,
		`* | stats row_any(level) ra`,
		`* | stats count() c`, // the mergeable path, unchanged and still answered
		`* | stats by (level) count() c`,
	} {
		for _, sf := range surfaces {
			t.Run(sf.name+" "+q, func(t *testing.T) {
				codeS, bodyS := chaosGet(t, single.URL+sf.path+urlEscape(q))
				codeC, bodyC := chaosGet(t, cluster.URL+sf.path+urlEscape(q))
				if codeS != 200 {
					t.Skipf("the single node cannot answer this: %d %.200s", codeS, bodyS)
				}
				if codeC != codeS {
					t.Fatalf("%s: single node %d, cluster %d: %.300s",
						sf.name, codeS, codeC, bodyC)
				}
				gotS, gotC := statsSet(t, bodyS), statsSet(t, bodyC)
				if !equalSets(gotS, gotC) {
					t.Fatalf("%s answered a DIFFERENT number across shards than on one "+
						"node.\nsingle:  %v\ncluster: %v", sf.name, gotS, gotC)
				}
				if len(gotS) == 0 {
					t.Fatalf("%s answered nothing on either side, so this compared "+
						"two empty results and asserted nothing: %.200s", sf.name, bodyS)
				}
			})
		}
	}
}

// statsSet reduces a stats answer to a comparable SET of values.
//
// A set, not a sequence: the group order of a stats answer is map iteration
// order in this build and the single node's own order changes between runs.
// Comparing sequences would fail on a difference that is not an answer, and a
// test that fails for a reason nobody believes is a test that gets explained
// away when it catches something real.
//
// The Prometheus envelope is flattened to one entry per (label set, point) so
// a reordered `result` array compares equal while a changed value does not.
func statsSet(t *testing.T, body string) []string {
	t.Helper()
	var env struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &env); err == nil && env.Data.Result != nil {
		var out []string
		for _, se := range env.Data.Result {
			keys := make([]string, 0, len(se.Metric))
			for k := range se.Metric {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			label := ""
			for _, k := range keys {
				label += k + "=" + se.Metric[k] + ","
			}
			if se.Value != nil {
				out = append(out, label+jsonOf(t, se.Value))
			}
			for _, v := range se.Values {
				out = append(out, label+jsonOf(t, v))
			}
		}
		sort.Strings(out)
		return out
	}
	// NDJSON rows, one per group.
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The refusal that remains is the one about a PUSHED-DOWN aggregate, and it
// still names what a caller can do instead.
//
// Asserted on NonMergeableReason rather than through HTTP because no request
// can reach it today: PlanDistributed never puts a stats pipe in ShardPipes, so
// rejectReason has nothing to refuse. Asserting a 400 through an endpoint would
// be asserting a status that cannot happen -- a test that passes because its
// condition is unreachable rather than because the message is right. The day a
// pushdown lands, this is the message it has to keep.
func TestTheRemainingRefusalNamesTheAlternative(t *testing.T) {
	for _, tc := range []struct{ q, mentions string }{
		{`* | stats avg(n) a`, "sum()"},
		{`* | stats quantile(0.5, n) p`, "sketch"},
		{`* | stats count_uniq(user) u`, "HLL"},
	} {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := query.ParseLogsQL(tc.q)
			if err != nil {
				t.Fatal(err)
			}
			why := ""
			for _, p := range parsed.Pipes {
				if sp, is := p.(*query.StatsPipe); is {
					why = query.NonMergeableReason(sp.Aggs)
				}
			}
			if why == "" {
				t.Fatalf("%q has no reason at all; a pushdown would ship it silently", tc.q)
			}
			if !strings.Contains(why, tc.mentions) {
				t.Errorf("the reason does not tell the caller what to do instead "+
					"(expected %q): %s", tc.mentions, why)
			}
		})
	}
}

// `time=` alone resolves the window ONCE, and the shards are asked for that
// window.
//
// stats_query is an instant query: `time` names the end of it, and start/end
// are the extension. The shards are asked a ROW query, which has no `time`
// parameter at all -- so a router that resolved the instant here and forwarded
// the request unchanged would have every shard answer from its whole
// retention, and the aggregate would cover the store while claiming to cover
// the instant.
//
// The mutation this exists for is deleting the two Set calls that put the
// resolved window on the shard request. It stayed green against a suite that
// pinned start and end on every request, because a pinned window survives the
// clone; only a request that leaves the window IMPLICIT can tell the
// difference.
func TestATimeOnlyInstantQueryAsksTheShardsForThatInstant(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	// Halfway through the corpus: the rows run from base to base+29s, so an
	// instant at +15s is 16 rows on one node and 30 if the window is lost.
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	at := base.Add(15 * time.Second).UnixNano()

	for _, q := range []string{
		`* | stats avg(n) a`,  // the exact path
		`* | stats count() c`, // the merge path, for the same reason
		`* | stats quantile(0.5, n) p`,
	} {
		t.Run(q, func(t *testing.T) {
			path := fmt.Sprintf("/select/logsql/stats_query?time=%d&query=", at)
			codeS, bodyS := chaosGet(t, single.URL+path+urlEscape(q))
			codeC, bodyC := chaosGet(t, cluster.URL+path+urlEscape(q))
			if codeS != 200 {
				t.Skipf("the single node cannot answer this: %d %.200s", codeS, bodyS)
			}
			if codeC != codeS {
				t.Fatalf("single node %d, cluster %d: %.300s", codeS, codeC, bodyC)
			}
			gotS, gotC := statsSet(t, bodyS), statsSet(t, bodyC)
			if !equalSets(gotS, gotC) {
				t.Fatalf("the cluster answered a different instant than the node:\n"+
					"single:  %v\ncluster: %v", gotS, gotC)
			}
			// The fixture only bites if the instant EXCLUDES rows. If the node
			// itself counted all 30, the window was not applied on either side
			// and this compared two whole-store answers.
			if strings.Contains(q, "count()") && strings.Contains(gotS[0], `"30"`) {
				t.Fatalf("the instant did not bound the node's own answer (%v), so "+
					"this test cannot see a router that ignores it", gotS)
			}
		})
	}
}

// A DEGENERATE window answers what the node answers.
//
// Every other test here pins a sensible start and end, which is why this was
// invisible. `end=0` is where the two readers of the time parameters disagree:
// timeWindow returns it verbatim, parseRequest turns a zero To into 1<<62 ("no
// end"). The router resolved the window with the first and the shards read it
// with the second, so the exact path asked every shard for its whole retention
// and answered the aggregate over all of it -- for an instant containing no
// rows. Measured, 2 shards x 15 rows against 1 node x 30:
//
//	stats_query?query=*|stats avg(n) a&end=0
//	  node    {"resultType":"vector","result":[]}
//	  cluster {"__name__":"a","value":[0,"14.5"]}   HTTP 200
//
// which is worse than the 400 it replaced: a refusal is visible.
//
// THE NEGATIVE-START WINDOWS ARE THE ONES THE FIRST FIX DID NOT COVER. That
// fix was a short-circuit on `to <= from` in the router, which answers a window
// the ROUTER reads as empty and nothing else. `start=-1&end=0` is not empty --
// it is one nanosecond wide, ending at the epoch -- so it went out to the
// shards, whose parseRequest still read the zero as "no end":
//
//	stats_query?start=-1&end=0&query=*|stats avg(n) a
//	  node    {"resultType":"vector","result":[]}
//	  cluster {"__name__":"a","value":[0,"14.5"]}   HTTP 200
//	stats_query?start=-1&end=0&query=*|stats rate() r
//	  node    []                 cluster "30000000000"
//
// So the collision is closed where it happens instead: `parseRequest` now tells
// an `end` that is given from an `end` that is absent, which is what
// `timeWindow` always did. Both readings agree, the resolved window forwards
// verbatim, and the short-circuit above is back to being an optimization.
//
// The `query` route is checked too, and since that fix a node answers it the
// same way -- empty. Before it, `end=0` meant the whole store on `query` and an
// instant containing nothing on `stats_query`: one binary reading one parameter
// two ways.
func TestADegenerateWindowAnswersWhatTheNodeAnswers(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, win := range []string{
		"end=0", "start=100&end=50",
		// Non-empty to the router, still zero-ended at a shard.
		"start=-1&end=0",
		"start=-100&end=0",
		"start=-1&end=1970-01-01T00:00:00Z",
		"start=-1&end=1970-01-01T00:00:00.000000000Z",
	} {
		for _, q := range []string{
			`* | stats avg(n) a`,
			`* | stats count() c`,
			`* | stats rate() r`, // the rate-zero residue of entry 106
			`* | stats quantile(0.5, n) p`,
		} {
			for _, route := range []string{
				"/select/logsql/stats_query?",
				"/select/logsql/query?",
				"/select/logsql/stats_query_range?step=1h&",
			} {
				t.Run(win+" "+route[15:len(route)-1]+" "+q, func(t *testing.T) {
					p := route + win + "&query=" + urlEscape(q)
					codeS, bodyS := chaosGet(t, single.URL+p)
					codeC, bodyC := chaosGet(t, cluster.URL+p)
					if codeC != codeS {
						t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
					}
					if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
						t.Fatalf("a degenerate window gave different answers:\n"+
							"  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
					}
				})
			}
		}
	}
}

// `limit` with a stats pipe that is NOT leading bounds the aggregate's input.
//
// A node applies the endpoint's `limit` as LastN on the scan -- the newest n
// rows -- and every pipe then runs over those n. The exception is a LEADING
// stats or uniq pipe, whose own scan bypasses the bound, and limitBoundsOutput
// used to treat that exception as covering any pipeline containing one. So the
// router left the merged set unbounded whenever a stats pipe appeared anywhere,
// and computed the aggregate over every row where the node computed it over
// five. Measured, 30 rows over two shards, `&limit=5`:
//
//	query                            node        cluster before
//	| sort by (n) | stats avg(n) a   {"a":"27"}  {"a":"14.5"}
//	| limit 5 | stats avg(n) a       {"a":"27"}  {"a":"2"}
//	| sort by (n) | stats count() c  {"c":"5"}   {"c":"30"}
//
// The count row was wrong before the exact path existed. The avg row is one
// the router used to refuse, so lifting the refusal turned a 400 into a
// plausible number -- the failure mode this whole change has to avoid.
func TestLimitBoundsTheInputOfANonLeadingAggregate(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, q := range []string{
		`* | sort by (n) | stats avg(n) a`,
		`* | limit 5 | stats avg(n) a`,
		`* | sort by (n) | stats count() c`,
		`* | sort by (n) | stats quantile(0.5, n) p`,
		// A ROW-LOCAL prefix, which is the case the first fix missed.
		//
		// planQuery pushes a one-to-one row-local pipe to the shards, so the
		// suffix it hands the merge BEGINS with the aggregate -- and a bound
		// predicate reading the suffix's first pipe concludes "leading
		// aggregate, no bound" for a pipeline whose aggregate is not leading.
		// Every one of these was a plausible number at HTTP 200, and the avg
		// and quantile ones were a 400 before the refusal was lifted.
		`* | fields n, _time | stats avg(n) a`,
		`* | fields n, _time | stats count() c`,
		`* | fields n, _time | stats quantile(0.5, n) p`,
		`* | math n * 2 as m | stats avg(m) a`,
		`* | rename n as k | stats count() c`,
		`* | fields n, _time | uniq by (n)`,
		// And a filter in front, which is NOT row-count-preserving: planQuery
		// withholds it from the shards and returns the whole pipeline as the
		// coordinator half. exactMatrix re-derived that half with
		// PlanDistributed and dropped the filter.
		`* | filter level:error | stats avg(n) a`,
		`* | filter level:error | stats quantile(0.5, n) p`,
		`* | stats avg(n) a`,  // LEADING: the bound does not apply, and must not
		`* | stats count() c`, // same
	} {
		t.Run(q, func(t *testing.T) {
			base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
			routes := []string{"/select/logsql/query?limit=5&query="}
			// The range route only for queries that HAVE a stats pipe, which
			// is what it is for. `| uniq by (n)` on it makes a node answer 200
			// with a series whose values are empty strings -- statsShape finds
			// no aggregate, so the alias is "" and every point reads "" -- and
			// the router refuses that with 502 rather than counting the empty
			// string as zero. The refusal is the better answer of the two and
			// the divergence is its own finding, not this test's subject.
			if strings.Contains(q, "stats ") {
				routes = append(routes, fmt.Sprintf(
					"/select/logsql/stats_query_range?step=1h&start=%d&end=%d&limit=5&query=",
					base, base+86400))
			}
			for _, route := range routes {
				p := route + urlEscape(q)
				codeS, bodyS := chaosGet(t, single.URL+p)
				codeC, bodyC := chaosGet(t, cluster.URL+p)
				if codeS != 200 {
					continue // the node cannot answer this shape on this route
				}
				if codeC != codeS {
					t.Fatalf("%s: node %d, cluster %d: %.250s",
						route[15:len(route)-7], codeS, codeC, bodyC)
				}
				if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
					t.Fatalf("%s: `limit` shaped a different input on the cluster:\n"+
						"  node:    %.200s\n  cluster: %.200s",
						route[15:len(route)-7], bodyS, bodyC)
				}
			}
		})
	}
}

// The range query's deadline is the REQUEST's, not each bucket's.
//
// applyQueryBudget sets `Deadline = now + MaxQueryDuration` every time it runs,
// and exactMatrix calls into the coordinator pipeline once per bucket. Calling
// it per bucket gave a 30-bucket graph thirty fresh deadlines, so
// -search.maxDuration bounded ONE BUCKET rather than the request -- on the one
// path that holds every matching row of the cluster in memory while it works.
// The node does not do this: statsQueryRange builds one budget query and copies
// its fixed deadline onto every bucket.
//
// Asserted on the helper rather than through a request, because a timing
// fixture cannot separate the two shapes reliably. The discriminating input is
// a deadline that expires DURING the run and never within one bucket, and the
// first attempt at that -- 9,000 buckets against a 20ms ceiling -- ran in 10ms
// and passed on both shapes. Handing the helper a budget whose deadline has
// already passed tests the same property with no clock in it: the caller's
// budget must be USED, and a re-stamp would replace it with a fresh one and
// succeed.
func TestARangeBucketUsesTheCallersBudgetRatherThanAFreshOne(t *testing.T) {
	srv, _ := limitServer(t, func(l *config.Limits) {
		l.MaxQueryDuration = time.Hour // a re-stamp would be generous
	})
	r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?query=*", nil)

	q, err := query.ParseLogsQL(`* | stats count() c`)
	if err != nil {
		t.Fatal(err)
	}
	pipes := query.PlanDistributed(q.Pipes).CoordinatorPipes
	rows := []query.Row{{Fields: []query.Field{{Key: "n", Value: "1"}}}}

	// A budget built the way exactMatrix builds it, then aged past its
	// deadline -- which is what the thirtieth bucket of a slow request faces.
	budget := &query.Query{}
	srv.applyQueryBudget(r, budget)
	budget.Deadline = time.Now().Add(-time.Second)

	if _, err := srv.applyCoordinatorPipesBudgeted(
		r, rows, pipes, 0, int64(1)<<62, budget); err == nil {
		t.Fatal("a bucket ran to completion under a budget whose deadline had " +
			"passed: the caller's budget was replaced by a fresh one, so " +
			"-search.maxDuration bounds a bucket and not the request")
	}
	// And with no budget the same call is stamped fresh and succeeds, so the
	// assertion above is about the copying and not about the pipeline.
	if _, err := srv.applyCoordinatorPipesBudgeted(
		r, rows, pipes, 0, int64(1)<<62, nil); err != nil {
		t.Fatalf("the same bucket with a fresh budget failed: %v", err)
	}
}

// A window whose span OVERFLOWS int64 is bounded, not iterated.
//
// The ceiling divides `to - from` by the step. With `?start` far negative and
// no `end`, `to` is 1<<62 and the subtraction wraps: the quotient came out
// negative, so neither the narrowing branch nor the 413 fired and the bucket
// loop ran about 1.5e8 times. On a node that is a hang; on a router it is a
// hang holding every matching row of the cluster in memory.
func TestAnOverflowingRangeSpanIsBoundedRatherThanIterated(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, q := range []string{`* | stats count() c`, `* | stats avg(n) a`} {
		t.Run(q, func(t *testing.T) {
			p := "/select/logsql/stats_query_range?step=1m&start=-4700000000000000000&query=" +
				urlEscape(q)
			done := make(chan struct{})
			var codeS, codeC int
			var bodyS, bodyC string
			go func() {
				defer close(done)
				codeS, bodyS = chaosGet(t, single.URL+p)
				codeC, bodyC = chaosGet(t, cluster.URL+p)
			}()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("no answer in 20s: the bucket loop is iterating an " +
					"overflowed span")
			}
			if codeS != codeC {
				t.Fatalf("node %d, cluster %d: one binary must not answer the same "+
					"over-wide range two ways:\n  node:    %.150s\n  cluster: %.150s",
					codeS, codeC, bodyS, bodyC)
			}
			// AND THE BODIES. `bodyS`/`bodyC` were captured and used only in
			// the failure message above, so two 200s carrying a different
			// number of buckets passed -- the same status-only hole that was
			// already found and fixed one file over, in the hits ceiling test.
			if codeS == 200 && !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
				t.Errorf("same status, different answer for an over-wide "+
					"range:\n  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
			}
		})
	}
}

// A SMALL time window survives the hop to the shards.
//
// The router resolves the window and writes it onto the shard request, and the
// shard reads it back with parseTimeParam -- which infers the UNIT of a bare
// integer from its magnitude: under 1e11 seconds, under 1e14 milliseconds,
// under 1e17 microseconds. So a nanosecond count written as a plain integer is
// re-read as a different instant for everything before 1973-03-03. Measured
// before the fix, 2 shards against 1 node:
//
//	?start=100&end=200&step=3h  | stats count() c   node 6,    cluster 4
//	?start=100&end=200&step=3h  | stats sum(n) s    node 15,   cluster 406
//	?start=100&end=200          | stats avg(n) a    node 2.5,  cluster 101.5
//
// -- a window three decades from the one asked for, at HTTP 200. Every other
// window fixture in this file uses 2026 dates, which are above 1e17 ns and
// round-trip whatever encoding is used, which is why none of them saw it.
func TestASmallTimeWindowReachesTheShardsUnchanged(t *testing.T) {
	// Rows INSIDE the window both sides resolve `start=100&end=200` to.
	//
	// parseTimeParam reads a bare 100 as 100 SECONDS, so the window is
	// [1e11, 2e11) nanoseconds on the node and on the router alike -- 1970,
	// and below the 1e17 threshold, which is the band where a re-encoded bare
	// integer changes meaning. The first version of this fixture put the rows
	// at nanosecond 100, which is outside that window: both sides answered
	// empty and agreed, and the test could not see the defect it names.
	const base = int64(100) * 1e9
	var rows [][]string
	rows = append(rows, nil, nil)
	for i := 0; i < 20; i++ {
		row := fmt.Sprintf(`{"_time":%d,"_msg":"m%d","n":"%d"}`,
			base+int64(i)*5e9, i, i)
		rows[i%2] = append(rows[i%2], row)
	}
	var all []string
	all = append(all, rows[0]...)
	all = append(all, rows[1]...)
	single := realShard(t, all)
	cluster := router(t, realShard(t, rows[0]).URL, realShard(t, rows[1]).URL)

	for _, q := range []string{
		`* | stats count() c`,
		`* | stats sum(n) s`,
		`* | stats avg(n) a`,
		`* | stats quantile(0.5, n) p`,
	} {
		for _, route := range []string{
			"/select/logsql/stats_query?start=100&end=200&query=",
			"/select/logsql/stats_query_range?start=100&end=200&step=3h&query=",
			"/select/logsql/query?start=100&end=200&query=",
		} {
			t.Run(route[15:len(route)-7]+" "+q, func(t *testing.T) {
				p := route + urlEscape(q)
				codeS, bodyS := chaosGet(t, single.URL+p)
				codeC, bodyC := chaosGet(t, cluster.URL+p)
				if codeS != codeC {
					t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
				}
				if codeS != 200 {
					return
				}
				// THE NODE MUST HAVE FOUND ROWS. Without this, two empty
				// answers are equal and every case passes -- which is exactly
				// how this test's FIRST fixture failed: the rows were at
				// nanosecond 100, outside the window both sides resolve, so
				// both answered empty and the defect it names was invisible.
				// The fixture was repaired and the assertion was not, so the
				// same hole is one bad fixture away from reopening.
				if strings.TrimSpace(bodyS) == "" || len(statsSet(t, bodyS)) == 0 {
					t.Fatalf("the node answered nothing for a window its own "+
						"fixture puts rows in, so comparing it with the "+
						"cluster proves nothing: %.200s", bodyS)
				}
				if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
					t.Fatalf("a 1970 window gave different answers:\n"+
						"  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
				}
			})
		}
	}
}

// `top` leads the scan the same way `stats` and `uniq` do.
//
// RunPipeline's leading fast path dispatches on StatsPipe, TopPipe and
// UniqPipe: each runs its own scan and never sees the endpoint's `limit`. The
// bound predicate named two of the three, so a leading `top` had the bound
// applied where a node does not apply it. Measured, 3 shards, `?limit=1`:
// `| top 2 by (level) | stats count() c` gave node 2 and cluster 1.
func TestALeadingTopBypassesTheEndpointBoundLikeStats(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL,
		realShard(t, parts[1]).URL, realShard(t, parts[2]).URL)

	for _, q := range []string{
		`* | top 2 by (level)`,
		`* | top 2 by (level) | stats count() c`,
		`* | top 2 by (level) | stats by (level) count() c`,
		`* | uniq by (level)`, // the one that was already named
		`* | stats count() c`, // and the other
		// MULTI-FIELD. `runTopFast`/`runUniqFast` decline a tuple -- it is not
		// a single dictionary -- so the dispatch falls through to `Run` and
		// LastN bounds the scan exactly as it does for any other pipe. Naming
		// the TYPE in `limitBoundsOutput` and not the fast path's CONDITION
		// made these unbounded on the cluster and bounded on the node:
		//
		//	| top 2 by (level, user), ?limit=1   node 1 row, cluster 2
		//	| uniq by (level, user),  ?limit=1   node 1 row, cluster 14
		//
		// Every single-field entry above passes either way, which is why the
		// first version of this test could not see it.
		`* | top 2 by (level, user)`,
		`* | top 2 by (level, user) | stats count() c`,
		`* | uniq by (level, user)`,
		`* | uniq by (level, user) | stats count() c`,
	} {
		t.Run(q, func(t *testing.T) {
			p := "/select/logsql/query?limit=1&query=" + urlEscape(q)
			codeS, bodyS := chaosGet(t, single.URL+p)
			codeC, bodyC := chaosGet(t, cluster.URL+p)
			if codeS != 200 {
				t.Skipf("the node cannot answer this: %d %.150s", codeS, bodyS)
			}
			if codeC != codeS {
				t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
			}
			if !equalSets(statsSet(t, bodyS), statsSet(t, bodyC)) {
				t.Fatalf("a leading `top` was bounded differently:\n"+
					"  node:    %.200s\n  cluster: %.200s", bodyS, bodyC)
			}
		})
	}
}

// A PREFIX THAT IS NOT ROW-LOCAL is computed once, not once per shard.
//
// The merge path -- whole pipeline to every shard, outputs combined by name --
// is equal to what a node does only when everything ahead of the aggregate is
// row-local. A filter decides each row on its own, so running it per shard and
// running it once give the same rows. `limit`, `offset`, `sort`, `uniq` and
// `top` are decisions about the SET, and each shard taking one over its own
// rows takes a different one. Measured, 3 shards, explicit window, no endpoint
// `limit`:
//
//	| limit 5 | stats count() c                 node 5, cluster 15
//	| sort by (n) | limit 5 | stats count() c   node 5, cluster 15
//	| uniq by (user) | stats count() c          node 7, cluster 21
//	| top 2 by (level) | stats count() c        node 2, cluster 6
//	| offset 25 | stats count() c               node 5, cluster []
//
// all at HTTP 200, and `/select/logsql/query` gets every one of them right --
// it plans, and these two surfaces did not.
//
// THE ASYMMETRY IS THE POINT. `| limit 5 | stats avg(n) a` agreed while
// `| limit 5 | stats count() c` did not: avg is non-mergeable, so it took the
// exact path, and count took the merge path. One endpoint, one prefix, right
// or wrong depending on which aggregate came after it. Both halves of each
// pair are here so a future change cannot fix one and leave the other.
func TestAPrefixThatIsNotRowLocalIsComputedOnceNotPerShard(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL,
		realShard(t, parts[1]).URL, realShard(t, parts[2]).URL)

	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
	for _, q := range []string{
		// Set-shaping prefix, MERGEABLE aggregate -- the merge path's cases.
		`* | limit 5 | stats count() c`,
		`* | sort by (n) | limit 5 | stats count() c`,
		`* | uniq by (user) | stats count() c`,
		`* | top 2 by (level) | stats count() c`,
		`* | offset 25 | stats count() c`,
		`* | limit 5 | stats sum(n) s`,
		`* | limit 5 | stats min(n) mn`,
		`* | limit 5 | stats max(n) mx`,
		// The same prefixes with a NON-mergeable aggregate. These already took
		// the exact path and already agreed; they are the other half of the
		// asymmetry and must keep agreeing.
		`* | limit 5 | stats avg(n) a`,
		`* | top 2 by (level) | stats avg(n) a`,
		`* | uniq by (user) | stats quantile(0.5, n) p`,
		// ROW-LOCAL prefixes: the merge path has always been right about these
		// and must go on taking them, or the fix is a blanket re-route.
		`* | filter level:error | stats count() c`,
		`* | fields n, _time | stats count() c`,
		`* | stats count() c`,
	} {
		t.Run(q, func(t *testing.T) {
			sawAnswer := false
			for _, route := range []string{
				fmt.Sprintf("/select/logsql/stats_query?start=%d&end=%d&query=",
					base, base+86400),
				fmt.Sprintf("/select/logsql/stats_query_range?step=1h&start=%d&end=%d&query=",
					base, base+86400),
			} {
				p := route + urlEscape(q)
				codeS, bodyS := chaosGet(t, single.URL+p)
				codeC, bodyC := chaosGet(t, cluster.URL+p)
				if codeS != 200 {
					continue // the node cannot answer this shape on this route
				}
				if codeC != codeS {
					t.Fatalf("%s: node %d, cluster %d: %.250s",
						route[15:len(route)-7], codeS, codeC, bodyC)
				}
				setS := statsSet(t, bodyS)
				if len(setS) > 0 {
					sawAnswer = true
				}
				if !equalSets(setS, statsSet(t, bodyC)) {
					t.Fatalf("%s: a non-row-local prefix ran per shard:\n"+
						"  node:    %.200s\n  cluster: %.200s",
						route[15:len(route)-7], bodyS, bodyC)
				}
			}
			if !sawAnswer {
				t.Fatalf("the node answered nothing on either route, so this "+
					"case compares two empties and proves nothing: %s", q)
			}
		})
	}
}
