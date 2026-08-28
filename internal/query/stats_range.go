package query

import (
	"errors"
	"math"
	"strconv"
	"time"
)

// errNotStats is returned when a query has no stats pipe, so it has no series
// to report -- the caller answers with an empty result, not a wrong one.
var errNotStats = errors.New("simdlogs: query has no stats pipe")

// RangeSeries is one output series of a stats_query_range: the group-by label
// set and its (unix-seconds, value) points across the time buckets.
type RangeSeries struct {
	Metric map[string]string
	Values [][2]string // [tsSeconds, value]
}

// statsShape returns the group-by fields and the first aggregate's alias of a
// query's leading (or only) stats pipe -- what a range query graphs.
func statsShape(q *Query) (by []string, alias string, ok bool) {
	for _, p := range q.Pipes {
		if sp, is := p.(*StatsPipe); is {
			a := ""
			if len(sp.Aggs) > 0 {
				a = sp.Aggs[0].Alias
			}
			return sp.By, a, true
		}
	}
	return nil, "", false
}

// statsAliases returns the group-by fields and EVERY aggregate's alias. A stats
// pipe with several aggregates produces one series per aggregate, each named by
// its alias in __name__, so returning only the first would silently drop the
// rest of a `count() n, sum(x) s` query.
func statsAliases(q *Query) (by []string, aliases []string, ok bool) {
	for _, p := range q.Pipes {
		if sp, is := p.(*StatsPipe); is {
			for i := range sp.Aggs {
				aliases = append(aliases, sp.Aggs[i].Alias)
			}
			return sp.By, aliases, true
		}
	}
	return nil, nil, false
}

// InstantSample is one sample of a stats_query: the group-by labels plus
// __name__ (the aggregate's alias), and the value at the window's end.
type InstantSample struct {
	Metric map[string]string
	Value  string
}

// StatsQueryInstant runs raw as a stats query over the whole [from,to) window
// and returns one sample per (group, aggregate) -- the instant counterpart of
// StatsQueryRange, which is what /select/logsql/stats_query answers.
// budget carries the deadline, the byte bound and the stop flag onto the
// Query this parses for itself. Nil is unbounded.
func StatsQueryInstant(s Store, raw string, from, to, now int64, budget *Query) ([]InstantSample, error) {
	q, err := ParseLogsQL(raw)
	if err != nil {
		return nil, err
	}
	q.SetWindow(from, to)
	q.SetNow(now)
	applyBudget(q, budget)
	by, aliases, ok := statsAliases(q)
	if !ok {
		return nil, errNotStats
	}
	return statsSamples(by, aliases, RunPipeline(s, q)), nil
}

// StatsSamplesFromRows projects rows a stats pipe has ALREADY produced into the
// instant-vector samples /select/logsql/stats_query answers with.
//
// It exists so a ROUTER answers that endpoint's shape from this code rather
// than its own. A router has no Store: for an aggregate that cannot be built
// from per-shard partials it asks the shards for rows and runs the stats pipe
// once over the merged set -- which is what a single node does -- and is then
// holding stats rows with no way to turn them into samples. Projecting them
// there instead is how the two modes drift: __name__ is the alias, the
// by-labels are read off the row, an empty alias is skipped, and each of those
// is a decision the other copy would have to re-make the same way.
//
// notStats reports that q has no stats pipe -- the condition StatsQueryInstant
// returns errNotStats for -- so a caller can tell "no series" from "no rows".
func StatsSamplesFromRows(q *Query, rows []Row) (samples []InstantSample, notStats bool) {
	by, aliases, ok := statsAliases(q)
	if !ok {
		return nil, true
	}
	return statsSamples(by, aliases, rows), false
}

func statsSamples(by, aliases []string, rows []Row) []InstantSample {
	out := make([]InstantSample, 0, len(rows)*len(aliases))
	for _, row := range rows {
		for _, alias := range aliases {
			if alias == "" {
				continue
			}
			metric := make(map[string]string, len(by)+1)
			for _, f := range by {
				metric[f] = rowField(row, f)
			}
			metric["__name__"] = alias
			out = append(out, InstantSample{Metric: metric, Value: rowField(row, alias)})
		}
	}
	return out
}

// StatsQueryRange runs raw as a stats query over [from,to) in `step`-sized
// buckets, returning one series per group-by tuple with a point per bucket. The
// query is re-parsed per bucket so each gets a clean window (RunPipeline mutates
// the query while resolving _time). ts is the bucket start in unix seconds.
// budget carries the deadline and the stop flag onto every bucket's Query.
// A range query re-parses per bucket, so the budget has to be re-applied per
// bucket too or only the first one is bounded. Nil is unbounded.
func StatsQueryRange(s Store, raw string, from, to, step, now int64, budget *Query) ([]RangeSeries, error) {
	if step <= 0 {
		step = RangeStepNs(from, to)
	}
	var acc RangeAcc
	// THE BUCKET WALK CANNOT OVERFLOW AND CANNOT FAIL TO TERMINATE.
	//
	// It was `for bs := from; bs < to; bs += step`, and both halves of that
	// wrapped. `?step=1h&start=2262-04-11T00:00:00Z&end=3000-01-01` puts `to`
	// at MaxInt64 (the far bound is outside the domain and saturates) and
	// `from` a day under it: after the last whole bucket `bs += step` runs PAST
	// MaxInt64, wraps to a large negative, `bs < to` is true again, and the
	// walk climbs the entire int64 domain an hour at a time -- then wraps and
	// does it again, forever. No bucket ceiling can stop that: the ceiling
	// counted 23 buckets and was right.
	//
	// `be = bs + step` wrapped the same way one line in, so the last bucket's
	// window was [huge, negative) and matched nothing.
	//
	// Written this way `be` is strictly greater than `bs` and never past `to`,
	// so the walk advances monotonically to `to` and stops. Termination no
	// longer depends on the caller's step, which is what made the request
	// below an unauthenticated denial of service:
	//
	//	GET /select/logsql/stats_query_range?query=*&start=1960-01-01&end=9999-01-01
	//	  before   no answer after 20s, one core pegged, httptest Close() hangs
	//	  after    200, a matrix, immediately
	// FLOORED TO A STEP MULTIPLE, AND EACH BUCKET IS A WHOLE STEP. Both halves
	// are the reference's, ASKED rather than reasoned about: the round that
	// pinned the difference argued from Prometheus and never ran
	// `internal/bench/victoria-logs`, which is in this repository. Six rows at
	// 00:15, 00:45, 01:15, 01:45, 02:15 and 02:45 on 2026-06-01Z,
	// `start=00:30Z&end=02:30Z&step=1h`, both binaries on this machine:
	//
	//	surface              VictoriaLogs                 simdlogs, before
	//	hits                 00:00,01:00,02:00 = 2,2,2    00:00,01:00,02:00 = 1,2,1
	//	stats_query_range    00:00,01:00,02:00 = 2,2,2    00:30,01:30 = 2,2
	//
	// VictoriaLogs has no divergence between its two range surfaces, so
	// neither may this one: a caller was answered a different bucket count,
	// different labels and different per-bucket values by two routes over one
	// window. What changed is where the buckets begin, how far each one
	// reaches, AND how many there are: the count is
	// `ceil((to - alignDown(from, step)) / step)`, so the same
	// `start=00:30Z end=02:30Z step=1h` that gave 2 buckets gives 3. The
	// before/after table above says so on its own face. An earlier draft of
	// this comment claimed the count was still the caller's; a client's graph
	// gets a different number of points.
	//
	// `be` is NOT clamped to `to`. A bucket is [k*step, (k+1)*step) or it is
	// not a bucket -- clamping made the last one partial, so the value under a
	// step-aligned label was not that step's value. `query.BucketSpan` is the
	// same statement for the scan window on the /hits side.
	lo, _ := BucketSpan(from, to, step)
	for bs := lo; bs < to; {
		be := bs + step
		if be < bs {
			// Past MaxInt64: the last bucket in the domain. `bs = be` below
			// then ends the walk, whatever `to` is.
			be = math.MaxInt64
		}
		q, err := ParseLogsQL(raw)
		if err != nil {
			return nil, err
		}
		q.SetWindow(bs, be)
		q.SetNow(now)
		applyBudget(q, budget)
		acc.Bucket(q, bs, RunPipeline(s, q))
		bs = be
	}
	return acc.Series(), nil
}

// RangeWidthNs is a range window's width in nanoseconds, EXACTLY, as a uint64.
//
// `to - from` in int64 is not the width. `?start=1000-01-01&end=9999-01-01`
// resolves to [MinInt64, MaxInt64] -- both bounds are outside the
// int64-nanosecond domain and `parseTimeParam` saturates them, which is entry
// 129/130's fix working as designed -- and that window's width is 2^64-1. The
// subtraction wraps to -1, and every consumer read the -1 as "no buckets":
// the default step came out 0, `boundRangeBuckets` returned early on
// `step <= 0` WITHOUT applying its ceiling, and StatsQueryRange normalised the
// non-positive step to 1 NANOSECOND. An unauthenticated GET then spun a core
// from MinInt64 to MaxInt64 and never answered.
//
// Two's complement makes the exact width free: for to >= from,
// `uint64(to) - uint64(from)` is the true difference whatever the signs are.
// A window with `to <= from` has no buckets at all, which is 0 and not a wrap.
func RangeWidthNs(from, to int64) uint64 {
	if to <= from {
		return 0
	}
	return uint64(to) - uint64(from)
}

// RangeStepNs is the step a range query gets when it names none, or names one
// that is zero or negative: 1/30th of the window, so a graph gets ~30 points.
//
// It is never zero and never negative, and that is the load-bearing property
// rather than the ~30. A non-positive step is what made the bucket ceiling
// unreachable, and a ceiling that does not run is the only thing between this
// endpoint and an infinite loop.
//
// One definition for the CEILING, called from both places that bucket a
// `stats_query_range` -- the HTTP `step` parameter (`parseStepNs`) and this
// scan -- because a ceiling computed from one step and a walk taken at another
// is a ceiling on a different request. `internal/api` reaches it through
// `parseStepNs`; the router's exact-stats path reaches it through the same
// function.
//
// `/select/logsql/hits` is a THIRD language and never reaches here: it parses
// `step` with `hitsStepNs`, and the two disagree for `1800`, an absent step,
// `0s` and a negative step -- see docs/compatibility.md. Unifying them is its
// own change; this comment used to claim there were only two.
func RangeStepNs(from, to int64) int64 {
	w := RangeWidthNs(from, to)
	switch {
	case w == 0:
		return int64(time.Minute)
	case w/30 > 0:
		return int64(w / 30) // at most (2^64-1)/30, which fits in an int64
	default:
		return int64(w) // a window narrower than 30ns is one bucket
	}
}

// RangeAcc accumulates one bucket of stats rows at a time into range series.
//
// It is the half of a range query that is not the scan, and it is separated
// from the scan because a ROUTER has no store to scan. A router asks the shards
// for the window's rows, slices them by bucket and runs the aggregate over each
// slice; from there the two modes have exactly the same job -- key a series by
// its group-by tuple, name it with the aggregate's alias, append one point per
// bucket -- and doing that job twice is how a graph ends up with a different
// legend, a different series identity, or a different point order depending on
// which mode answered.
//
// The zero value is ready.
type RangeAcc struct {
	acc   map[string]*RangeSeries
	order []string
	by    []string
	alias string
}

// Bucket adds one bucket's stats rows. q is that bucket's query -- it names the
// group-by fields and the aggregate's alias -- and bucketStart is the bucket's
// start in nanoseconds, which becomes the point's timestamp.
func (a *RangeAcc) Bucket(q *Query, bucketStart int64, rows []Row) {
	if a.acc == nil {
		a.acc = map[string]*RangeSeries{}
	}
	if b, al, ok := statsShape(q); ok {
		a.by, a.alias = b, al
	}
	tsSec := strconv.FormatInt(bucketStart/1e9, 10)
	for _, row := range rows {
		key := ""
		metric := map[string]string{}
		for _, f := range a.by {
			v := rowField(row, f)
			metric[f] = v
			key += v + "\x00"
		}
		se := a.acc[key]
		if se == nil {
			// __name__ carries the aggregate's alias, the way a Prometheus
			// series carries its metric name -- without it a graph has no
			// legend and two aggregates are indistinguishable.
			if a.alias != "" {
				metric["__name__"] = a.alias
			}
			se = &RangeSeries{Metric: metric}
			a.acc[key] = se
			a.order = append(a.order, key)
		}
		se.Values = append(se.Values, [2]string{tsSec, rowField(row, a.alias)})
	}
}

// Series returns the accumulated series in first-seen order.
func (a *RangeAcc) Series() []RangeSeries {
	out := make([]RangeSeries, 0, len(a.order))
	for _, k := range a.order {
		out = append(out, *a.acc[k])
	}
	return out
}

// applyBudget copies the budget fields -- and only those -- onto q. The
// filter half of budget is never read.
func applyBudget(q, budget *Query) {
	if budget == nil {
		return
	}
	q.Deadline, q.MaxBytes, q.Stopped = budget.Deadline, budget.MaxBytes, budget.Stopped
	// The context and the reason, not only the flag. A subquery that stopped
	// used to set the shared bool and record its reason on a Query the caller
	// throws away, so the outer query reported the generic "time or byte
	// budget" for every cause -- including a cancelled client, which is not a
	// budget at all. Sharing the pointer means the first stop anywhere in the
	// tree is the one reported.
	q.ctx = budget.ctx
	q.stopReason = budget.stopReason
	q.maxGroups, q.maxMemory = budget.maxGroups, budget.maxMemory
	q.maxGroupKeys, q.maxPipeRows = budget.maxGroupKeys, budget.maxPipeRows
}
