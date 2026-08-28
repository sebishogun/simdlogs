package api

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/sebishogun/simdlogs/internal/query"
)

// Exact stats across shards: ask for the rows, aggregate once, here.
//
// # What this replaces
//
// A router used to answer /select/logsql/stats_query by sending the WHOLE
// stats pipe to every shard and combining the per-shard outputs by output name
// -- sum, or min, or max. That works for an aggregate whose partial state is
// its output, and for nothing else. So five aggregates a single node answers
// were refused with 400 across a cluster:
//
//	avg()  quantile()  uniq()  count_uniq()  histogram()
//
// The refusal was right about the merge and wrong about the alternative. There
// is a second way to answer, and it is the one a single node uses: get the
// rows, run the aggregate once over all of them. A router can do that -- the
// planner already sends the row-local half to the shards and runs everything
// after it here -- and the result is not an approximation of the node's answer,
// it is the same computation on the same rows. Measured on two shards holding
// 2500 rows each, all five were byte-identical to the single node's, grouped
// and ungrouped.
//
// # What it costs, and why the merge stays for the rest
//
// Every matching row crosses the network instead of one number per group. That
// is the price of an exact quantile without a sketch.
//
// THREE THINGS send a query here, not one, and this said two.
// `NonMergeableReason` is the first: an aggregate whose partial state is not
// its output cannot be combined from per-shard numbers at all. The second is a
// non-row-local pipe BEFORE the aggregate, which leaves something other than a
// StatsPipe at the head of the coordinator half -- `* | sort by (n) | stats
// count() c` takes this path with nothing following its aggregate at all. The
// third is any pipe AFTER the aggregate --
// `| limit`, `| offset`, `| sort`, `| top`, `| uniq` -- because the federated
// path sends the whole query to every shard, so each applies that pipe to its
// own groups and the coordinator merges without applying it again. Measured on
// three shards, HTTP 200 throughout: `| stats by (_msg) count() c | limit 2`
// gave node 2 series and cluster 6; `| offset 25` gave node 5 and cluster
// none. See `needsExactStats` at the foot of this file, and docs/wrong.md
// entry 114.
//
// So a cluster DOES now get slower for shapes that merged correctly before --
// `| stats ... | sort` is the common one. Measured on a 3-shard, 30-row
// fixture: 1,385 allocs and 140 KB against the federated path's 1,162 and
// 87 KB. The material cost is not in a fixture that size; it is every matching
// row on the wire instead of one row per group per shard.
//
// This paragraph read "the choice is per query, made by NonMergeableReason, so
// a cluster does not get slower for the queries that were already correct",
// which stopped being true the moment the second condition landed at the
// bottom of the same file.
//
// The remaining approximation is not hidden by this: a sketch would let
// quantile() merge cheaply, with a documented error bound. This is exact and
// expensive. When the sketch lands the choice becomes three-way; until then
// "expensive and right" beats "cheap and refused".

// shardRows is the window's rows, merged from every shard and ready to
// aggregate: the rows themselves, their timestamps in the same order, and the
// coordinator half of the plan that has still to run over them.
//
// times is carried alongside rather than re-read from each row because the
// range endpoint slices by it once per bucket. The rows are sorted by it, so a
// bucket is a contiguous range and each slice is two binary searches rather
// than a scan of the whole window.
type shardRows struct {
	rows       []query.Row
	times      []int64
	coordPipes []query.Pipe
	q          *query.Query
}

// mergedRowsAcrossShards asks the shards for the rows a stats query selects and
// merges them in the order a single node would have produced.
//
// It stops before the aggregate: the instant endpoint runs it once over
// everything, and the range endpoint runs it once per bucket, so the split is
// where those two stop agreeing.
//
// The ResponseWriter comes back because fanOutChecked may replace it: it wraps
// the writer to lower the completeness header when a shard is missing, and a
// caller that kept the original would answer complete over an incomplete read.
func (s *Server) mergedRowsAcrossShards(
	w http.ResponseWriter, r *http.Request, from, to int64,
) (shardRows, http.ResponseWriter, bool) {
	var out shardRows
	q, err := query.ParseLogsQL(r.FormValue("query"))
	if err != nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, err.Error())
		return out, w, false
	}
	shardQuery, coordPipes, _, ok := s.planQuery(w, r)
	if !ok {
		return out, w, false
	}
	out.coordPipes, out.q = coordPipes, q

	// AN EMPTY WINDOW IS ANSWERED HERE, without asking anyone.
	//
	// No row can fall in a window that ends at or before it starts, so the
	// answer is whatever the aggregate makes of NO rows -- which is what the
	// node computes, its own scan having found none. Running the coordinator
	// half over an empty set reproduces that by construction rather than by
	// matching an envelope, and it saves a fan-out that cannot return anything.
	//
	// This used to be the whole defence against `end=0`, and it was not enough:
	// it only covers a window the ROUTER reads as empty, and `start=-1&end=0`
	// is not one. The shards read `end=0` as "no end" and answered from their
	// whole retention -- node `result: []`, cluster `{"a":"14.5"}`, both 200.
	// That is fixed where it was caused, in `parseRequest`: an `end` that is
	// given and an `end` that is absent are now different things. This guard is
	// back to being what it reads as, a short-circuit for a window with nothing
	// in it.
	if to <= from {
		return out, w, true
	}

	shardReq := r.Clone(r.Context())
	vals := shardReq.URL.Query()
	vals.Set("query", shardQuery)
	// The RESOLVED window on the shard request, in nanoseconds.
	//
	// stats_query is an instant query: `time` names the end of the window and
	// start/end extend it, and an absent end means now. The shards are being
	// asked a ROW query, which knows nothing about `time` -- so a request with
	// only `time=` set would have had its window resolved here and ignored
	// there, and every shard would have answered from its whole retention while
	// the aggregate claimed to cover one instant.
	//
	// RFC3339Nano, because a bare integer does not survive the hop.
	//
	// parseTimeParam infers the UNIT of a bare integer from its magnitude --
	// under 1e11 seconds, under 1e14 milliseconds, under 1e17 microseconds --
	// so a nanosecond count round-trips only at or above 1e17 ns, which is
	// 1973-03-03 onward. Below that the shard reads a different instant.
	// Restoring the integer form at this line and at federatedMatrix's turns
	// `TestASmallTimeWindowReachesTheShardsUnchanged` red on all four queries
	// and three routes, as node-has-rows against cluster-empty:
	//
	//	?start=100&end=200&query=*|stats avg(n) a   node 9.5,  cluster []
	//	?start=100&end=200&step=3h ... count() c    node 20,   cluster []
	//	?start=100&end=200&step=3h ... sum(n) s     node 190,  cluster []
	//
	// all at HTTP 200. An earlier version of this comment quoted a different
	// pair of numbers, node 2.5 against cluster 101.5, which came from a
	// scratch fixture that is not in this repository -- the committed corpus
	// has no rows in the window the shard mis-reads, so the cluster half is
	// empty rather than wrong. A number in a comment that the repository
	// cannot produce is a number nobody can check.
	//
	// The layout round-trips exactly for the whole int64 range, 1<<62 and
	// negative values included. Seconds would not: the window edges are where
	// an instant query's answer changes, and rounding them moves it.
	vals.Set("start", time.Unix(0, from).UTC().Format(time.RFC3339Nano))
	vals.Set("end", time.Unix(0, to).UTC().Format(time.RFC3339Nano))
	// `limit` is the caller's bound on the FINAL answer and must not reach a
	// shard when there is a coordinator half -- each shard would truncate its
	// rows before the aggregate ever saw them. A node does not apply it to a
	// stats query at all (runStats runs its own scan), so it is dropped here
	// whether or not the plan has a coordinator half.
	vals.Del("limit")
	shardReq.URL.RawQuery = vals.Encode()
	// The four keys this path OWNS, so a POST form cannot put the caller's
	// versions back over them. Without the mark, withFormInURL lets the body
	// win for a urlencoded form -- which is what a node does -- and the body
	// still holds the unplanned query and the unresolved window.
	shardReq = withPlanKeys(shardReq, "query", "limit", "start", "end")

	bodies, w, ok := s.fanOutChecked(w, shardReq, "/select/logsql/query", nil)
	if !ok {
		return out, w, false
	}
	all, badShard, badLine := collectShardRows(bodies)
	if !s.refuseNotARow(w, r, badShard, badLine) {
		return out, w, false
	}

	// ASCENDING by time, ties by (shard, position).
	//
	// Most aggregates do not care: a count, a sum, a quantile of a set is the
	// same whatever order the set arrives in. Some do. values() emits in
	// encounter order, and first/last and the row_min/row_max family resolve
	// ties by which row came first. Leaving the rows in goroutine-completion
	// order would make those answers depend on which shard's body landed first
	// -- a different answer on every request rather than a wrong one, which is
	// the harder kind to notice.
	//
	// It is a TOTAL and REPRODUCIBLE order, and it is not always the single
	// node's. An earlier version of this comment called it "the order an
	// unbounded single-node scan produces", which is true only while
	// timestamps are distinct. Give three rows one timestamp and split them
	// across shards, and a node interleaves them by its own scan order while
	// this groups them by shard: values(n) came back ["-5","0",…,"7","8"] from
	// the node and ["-5","7","8","0",…] from the cluster. Both contain the same
	// values; only an order-sensitive aggregate can see the difference, and
	// only on tied timestamps. corpus() gives every row a distinct one, so no
	// committed fixture reaches it.
	//
	// Closing it means merging by (t, then the node's own within-shard rule),
	// which the router cannot reconstruct without the node telling it -- the
	// scan order within one group is not on the wire. Recorded rather than
	// claimed away.
	sort.Slice(all, func(i, j int) bool {
		if all[i].t != all[j].t {
			return all[i].t < all[j].t
		}
		if all[i].shard != all[j].shard {
			return all[i].shard < all[j].shard
		}
		return all[i].seq < all[j].seq
	})

	out.rows = make([]query.Row, 0, len(all))
	out.times = make([]int64, 0, len(all))
	for _, rw := range all {
		out.rows = append(out.rows, jsonLineToRow(rw.line))
		out.times = append(out.times, rw.t)
	}
	return out, w, true
}

// exactVector answers /select/logsql/stats_query for an aggregate that cannot
// be merged from per-shard outputs.
//
// The window is resolved exactly as the single-node handler resolves it --
// `time` names the instant, start/end extend it, an absent end is now -- and
// then the SAME resolved window goes to the shards and stamps the samples. A
// router that resolved it twice, or resolved it here and let the shards resolve
// their own, would answer an instant query over a window each shard chose.
// NO BUCKET LOOP LIVES HERE. An instant query has one stamp, taken once at
// `instantStamp(to, nowNs)` below; the `for bs := from; bs < to` walk a record
// once put on this function is `exactMatrix`'s, further down this file.
func (s *Server) exactVector(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	if v := r.FormValue("time"); v != "" {
		if n, ok := parseTimeParam(v); ok {
			to = n
		}
	}
	nowNs := time.Now().UnixNano()
	if to == defaultWindowTo {
		to = nowNs
	}
	sr, w, ok := s.mergedRowsAcrossShards(w, r, from, to)
	if !ok {
		return
	}
	// PRICED over the window the shards SCANNED, not the one the request
	// named. A query carrying an absolute `_time:` filter is scanned over the
	// intersection, and `rate()` is a count divided by the window's seconds.
	// `/select/logsql/query` was given this and these two were not:
	//
	//	query=_time:[12:00:00Z,12:00:30Z] | sort by (n) | stats rate() r
	//	  node         0.967741935483871
	//	  stats_query  0.00000001678877628337586
	//
	// A factor of 5.8e7, both 200. Same defect as entry 112's, on the sibling
	// route, left there because the test that caught it drove only `query`.
	pf, pt := query.ResolvedWindow(r.FormValue("query"), from, to, time.Now().UnixNano())
	rows, err := s.applyCoordinatorPipesIn(r, sr.rows, sr.coordPipes, pf, pt)
	if err != nil {
		s.writeErr(w, r, readSpec(), query.HTTPStatus(err), err.Error())
		return
	}
	samples, notStats := query.StatsSamplesFromRows(sr.q, rows)
	if notStats {
		// Unreachable through needsExactStats, which returns false for a query
		// with no stats pipe. Answered rather than ignored because "no series"
		// is a real answer and an empty vector is what the node gives.
		samples = nil
	}
	result := make([]map[string]any, 0, len(samples))
	// instantStamp, not `to / 1e9`: a saturated end is +infinity and not an
	// instant. See its doc comment; the node's copy of this loop is
	// `statsQuery`, and the two must stamp one request the same.
	at := instantStamp(to, nowNs)
	for _, sm := range samples {
		result = append(result, map[string]any{
			"metric": sm.Metric,
			"value":  [2]any{at, sm.Value},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(promResponse{
		Status: "success",
		Data:   promData{ResultType: "vector", Result: result},
	})
}

// exactMatrix answers /select/logsql/stats_query_range for an aggregate that
// cannot be merged from per-shard outputs.
//
// The window's rows are fetched ONCE and then sliced per bucket, rather than
// one fan-out per bucket. A default range is 30 buckets: 30 fan-outs would be
// 30 round trips to every shard for a window each shard has to re-scan, and the
// buckets partition the same rows this already holds. The rows arrive sorted by
// time, so each bucket is a contiguous span found by two binary searches.
//
// Each bucket's aggregate runs over its OWN window, which is what makes rate()
// a per-bucket rate rather than the whole range's.
//
// THIS IS THE FUNCTION WITH THE BUCKET WALK, not `exactVector` -- a record
// once attributed the walk to that one, which is the INSTANT handler and has
// no loop at all. The walk is floored to a step multiple and each bucket is a
// whole step, which is what `query.StatsQueryRange`, `query.fillHits` and the
// reference implementation all do: see query.BucketSpan,
// docs/compatibility.md, TestTheTwoRangeSurfacesAgreeOnBuckets and
// TestTheRouterAndNodeAgreeOnRangeBuckets.
func (s *Server) exactMatrix(w http.ResponseWriter, r *http.Request) {
	from, to := timeWindow(r)
	// THE SAME NORMALISATION THE SCAN USES, and NOT `to - from`.
	//
	// This was the node's old two-step normalisation copied here: `step = to -
	// from`, then `step = 1` if that was still not positive. Both wrap. The
	// saturated window [MinInt64, MaxInt64] -- which
	// `?start=1000-01-01&end=9999-01-01` produces -- has `to - from` == -1, so
	// the router bucketed the whole int64 domain a NANOSECOND at a time while
	// holding every matching row of the cluster in memory. `parseStepNs` no
	// longer returns a non-positive step; this keeps the guard and makes it
	// agree with `query.RangeStepNs`, which is what the node's scan uses.
	step := parseStepNs(r.FormValue("step"), from, to)
	if step <= 0 {
		step = query.RangeStepNs(from, to)
	}
	// The SAME bucket ceiling the single node applies, from the same function.
	//
	// Without it `?step=1m` and no `end` is a window running to 1<<62: about
	// 77 million buckets, each one an aggregate over a slice of the merged
	// rows, with every matching row of the cluster held in memory throughout.
	// The node spins on the same request; the router spins holding the data.
	from, to, ok := s.boundRangeBuckets(w, r, from, to, step)
	if !ok {
		return
	}
	// THE FETCH COVERS WHOLE BUCKETS, because the buckets do.
	//
	// The walk below is floored to a step multiple and each bucket is a full
	// step, so the first bucket begins before `from` and the last ends after
	// `to` whenever the request's bounds are not multiples themselves. Fetching
	// [from,to) would leave those two buckets partial on a router and whole on
	// a node -- the cluster answering a smaller number than the same rows on
	// one machine, which is the class the differential exists to catch.
	sf, st := query.BucketSpan(from, to, step)
	sr, w, ok2 := s.mergedRowsAcrossShards(w, r, sf, st)
	if !ok2 {
		return
	}

	// ONE budget for the whole request, copied onto every bucket.
	//
	// applyQueryBudget stamps Deadline = now + MaxQueryDuration each time it
	// runs, so calling it per bucket gave a 30-bucket graph thirty fresh
	// deadlines and -search.maxDuration never fired. The node builds one and
	// copies it; this does the same.
	budget := &query.Query{}
	s.applyQueryBudget(r, budget)

	// The time filter resolved ONCE, over the whole range, with ONE `now`.
	//
	// Each bucket's window is this intersected with the bucket's own edges,
	// which is the same answer the per-bucket call gave:
	//
	//	resolve(whole)      = [max(from, fs), min(to, fe)]
	//	resolve(bucket)     = [max(bs, fs),   min(be, fe)]
	//	bucket n resolve(w) = [max(bs, from, fs), min(be, to, fe)]
	//
	// and the last two are equal because every bucket lies inside the range:
	// bs >= from and be <= to. PER-BUCKET NARROWING IS PRESERVED. Hoisting the
	// narrowing ITSELF -- giving every bucket the range's window -- is a
	// different change and a wrong one: a reviewer did it, and on a filter
	// spanning two buckets the node answered [1, 1] against [0.909...,
	// 0.0909...] with the whole suite green, because the only test here used a
	// filter that fits inside one bucket.
	//
	// Hoisting the RESOLUTION is also the more faithful of the two. The
	// per-bucket call ran a full ParseLogsQL -- 748 ns, 1584 B, 15 allocs for
	// the test's filter -- to compute something that does not vary: +39.9% at
	// 30 buckets, +83.9% and 3,852 more allocations at 240. And it read a fresh
	// time.Now() each time, so a RELATIVE filter drifted across the graph,
	// where `StatsQueryRange` on a node passes one `now` to every bucket.
	// Resolved over the FETCHED span, not the requested one. The identity
	// above -- bucket n's resolve equals the range's resolve intersected with
	// the bucket's edges -- needs every bucket to lie inside the window it was
	// resolved over, and the first and last buckets now reach outside
	// [from,to). Resolving over [sf,st) restores it exactly.
	rs, re := query.ResolvedWindow(r.FormValue("query"), sf, st, time.Now().UnixNano())

	var acc query.RangeAcc
	// The SAME walk `query.StatsQueryRange` takes -- floored to a step
	// multiple, each bucket a whole step, stopping at the requested `to`. See
	// the long note there for what the reference answers and why `be` is not
	// clamped. The overflow guard is the same one: `bs += step` running past
	// MaxInt64 wraps to a large negative, `bs < to` is true again, and the walk
	// never terminates -- on this path while holding every matching row of the
	// cluster.
	for bs := sf; bs < to; {
		be := bs + step
		if be < bs {
			be = math.MaxInt64
		}
		lo := sort.Search(len(sr.times), func(i int) bool { return sr.times[i] >= bs })
		hi := sort.Search(len(sr.times), func(i int) bool { return sr.times[i] >= be })
		// A fresh parse per bucket, which is what the single-node range query
		// does.
		//
		// It is NOT load-bearing today, and saying so is the point: a mutation
		// that hoisted this parse out of the loop and shared one across every
		// bucket left the differential green, over a fixture with three
		// non-empty buckets that would have caught accumulation. StatsPipe.apply
		// builds its group map per call and stampPipes rewrites the window per
		// call, so a shared parse carries nothing between buckets.
		//
		// It stays because the two range paths have to agree, and the cheapest
		// way to keep them agreeing is to do the same thing: the day a
		// coordinator pipe does hold state across runs, the node's per-bucket
		// re-parse would be right and a hoisted one here would be wrong, and
		// nothing in this file would say why.
		bq, err := query.ParseLogsQL(r.FormValue("query"))
		if err != nil {
			s.writeErr(w, r, readSpec(), http.StatusBadRequest, err.Error())
			return
		}
		// SetWindow even though nothing resolves a time filter on `bq` today.
		//
		// `bq` is used only to slice `coord` and to take `statsShape`, so the
		// missing flag is inert here. It is also the exact shape that made
		// `/select/sql` widen past its `end`, one refactor away from firing,
		// and the flag costs nothing.
		bq.SetWindow(bs, be)
		// The SAME suffix planQuery returned, taken off this bucket's parse.
		//
		// Re-deriving it with PlanDistributed drops whatever planQuery
		// WITHHELD from the shards. When `limit` is set and the leading
		// row-local pipe changes row count, planQuery sends only the head
		// filter down and returns the whole pipeline as the coordinator half;
		// PlanDistributed splits at the first non-row-local pipe and hands
		// back just the aggregate, so the filter silently vanished. Measured,
		// `?limit=5&query=* | filter level:error | stats avg(n) a`: node 14.4,
		// cluster 14.5, at HTTP 200.
		//
		// A length-suffix rather than sr.coordPipes itself, because the pipes
		// have to come from THIS bucket's parse -- they carry the bucket's
		// window and its own aggregate state.
		coord := bq.Pipes[len(bq.Pipes)-len(sr.coordPipes):]
		// PER BUCKET, narrowed. `StatsQueryRange` on a node calls SetWindow
		// with the bucket's edges and lets resolveTimePreds narrow them, so a
		// bucket outside an absolute `_time:` filter prices as empty rather
		// than as its full width. Doing it once for the whole range would
		// give every bucket the same window, which is the shape entry 112
		// records under a different cause.
		pbs, pbe := bs, be
		if rs > pbs {
			pbs = rs
		}
		if re < pbe {
			pbe = re
		}
		rows, err := s.applyCoordinatorPipesBudgeted(
			r, sr.rows[lo:hi], coord, pbs, pbe, budget)
		if err != nil {
			s.writeErr(w, r, readSpec(), query.HTTPStatus(err), err.Error())
			return
		}
		acc.Bucket(bq, bs, rows)
		bs = be
	}

	series := acc.Series()
	result := make([]map[string]any, 0, len(series))
	for _, se := range series {
		vals := make([][2]any, 0, len(se.Values))
		for _, v := range se.Values {
			ts, _ := strconv.ParseInt(v[0], 10, 64)
			vals = append(vals, [2]any{ts, v[1]})
		}
		result = append(result, map[string]any{"metric": se.Metric, "values": vals})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(promResponse{
		Status: "success",
		Data:   promData{ResultType: "matrix", Result: result},
	})
}

// needsExactStats reports whether this query's aggregates have to be computed
// over merged rows rather than combined from per-shard outputs.
//
// THREE things force it, and only the first was here at the start. This said
// two, and the file header above said two while naming a DIFFERENT pair -- one
// doc named (a) and (c), the other named (a) and (b), and neither named all
// three.
//
// # The aggregate itself
//
// NonMergeableReason read as a question rather than as a refusal. The same
// predicate decided both before -- non-mergeable meant 400 -- and it now
// decides which of two correct paths answers. Keeping one predicate is the
// point: two would drift, and the drift would be a query the router merges
// wrongly because one list was updated and the other was not.
//
// # What comes BEFORE the aggregate
//
// The merge path sends the whole pipeline to every shard and combines the
// outputs by name. That is only equal to what a node does when everything
// ahead of the aggregate is row-local -- a filter decides each row on its own,
// so running it per shard and running it once give the same rows. A `limit`,
// `offset`, `sort`, `uniq` or `top` does not: it is a decision about the SET,
// and each shard taking it over its own rows takes a different one.
//
// Measured, 3 shards, explicit window, no endpoint `limit`:
//
//	| limit 5 | stats count() c              node 5, cluster 15
//	| sort by (n) | limit 5 | stats count() c  node 5, cluster 15
//	| uniq by (user) | stats count() c        node 7, cluster 21
//	| top 2 by (level) | stats count() c      node 2, cluster 6
//	| offset 25 | stats count() c             node 5, cluster []
//
// all at HTTP 200. `/select/logsql/query` gets every one right, because it
// plans; these two surfaces did not plan at all.
//
// The worst of it was the ASYMMETRY: `| limit 5 | stats avg(n) a` agreed while
// `| limit 5 | stats count() c` did not, because avg is non-mergeable and took
// the exact path while count took the merge path. The same endpoint, the same
// prefix, right or wrong depending on which aggregate followed it -- and the
// avg half only became visible when lifting the 400 gave it an answer at all.
//
// `PlanDistributed` already computes the split: ShardPipes is the row-local
// prefix and CoordinatorPipes is everything from the first pipe that is not.
// So "is everything before the aggregate row-local" is exactly "is the
// coordinator half's head the aggregate", and no second list of pipe kinds is
// introduced to fall out of step with ClassifyPipe.
func needsExactStats(raw string) bool {
	q, err := query.ParseLogsQL(raw)
	if err != nil {
		return false // the shards will reject it with the parse error
	}
	hasStats := false
	for _, p := range q.Pipes {
		sp, isStats := p.(*query.StatsPipe)
		if !isStats {
			continue
		}
		hasStats = true
		if query.NonMergeableReason(sp.Aggs) != "" {
			return true
		}
	}
	// Only for a query that aggregates. Without a stats pipe these surfaces
	// have no aggregate to compute either way, and routing such a query here
	// would answer an empty vector where the shards' own refusal belongs.
	if !hasStats {
		return false
	}
	coord := query.PlanDistributed(q.Pipes).CoordinatorPipes
	if len(coord) == 0 {
		return false
	}
	if _, isStats := coord[0].(*query.StatsPipe); !isStats {
		return true
	}
	// ANYTHING AFTER THE AGGREGATE, too.
	//
	// This checked only what came BEFORE, so a pipe following the stats pipe
	// went down the federated path -- where the WHOLE query, that pipe
	// included, is sent to every shard. Each shard then applies it to its own
	// groups and the coordinator merges what comes back without applying it
	// again. Measured, 3 shards, explicit window, every response HTTP 200:
	//
	//	| stats by (_msg) count() c | limit 2      node 2 series   cluster 6
	//	| stats by (_msg) count() c | offset 25    node 5          cluster 0
	//	| stats by (user) count() c | limit 2      node 2          cluster 4
	//	| stats by (_msg) count() c | sort by (_msg) | limit 2
	//	                                          node 2          cluster 6
	//
	// `limit 2` becomes two groups PER SHARD; `offset 25` skips 25 groups per
	// shard and no shard has 25, so a query that answers five series on a node
	// answers none across a cluster. The exact path computes the aggregate
	// once at the coordinator over merged rows and then runs the rest of the
	// pipeline over that, which is what a single node does.
	//
	// Every pipe, not a list of the dangerous ones. A list would have to be
	// kept in step with the pipe set, and the failure mode of missing an entry
	// is this defect again, silently, at 200. The cost is push-down for shapes
	// like `| stats ... | sort`, which a list would have preserved.
	//
	// `| stats count() c | limit 1` agreed before this and still does: one
	// group makes `limit 1` a no-op. It agreed by arithmetic, not by rule,
	// which is why it made the defect look absent.
	return len(coord) > 1
}
