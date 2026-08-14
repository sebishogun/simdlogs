package query

import (
	"errors"
	"strconv"
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
	q.From, q.To, q.Now = from, to, now
	applyBudget(q, budget)
	by, aliases, ok := statsAliases(q)
	if !ok {
		return nil, errNotStats
	}
	rows := RunPipeline(s, q)
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
	return out, nil
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
		step = to - from
	}
	if step <= 0 {
		step = 1
	}
	acc := map[string]*RangeSeries{}
	var order []string
	var by []string
	var alias string
	for bs := from; bs < to; bs += step {
		be := bs + step
		if be > to {
			be = to
		}
		q, err := ParseLogsQL(raw)
		if err != nil {
			return nil, err
		}
		q.From, q.To, q.Now = bs, be, now
		applyBudget(q, budget)
		if b, a, ok := statsShape(q); ok {
			by, alias = b, a
		}
		rows := RunPipeline(s, q)
		tsSec := strconv.FormatInt(bs/1e9, 10)
		for _, row := range rows {
			key := ""
			metric := map[string]string{}
			for _, f := range by {
				v := rowField(row, f)
				metric[f] = v
				key += v + "\x00"
			}
			se := acc[key]
			if se == nil {
				// __name__ carries the aggregate's alias, the way a Prometheus
				// series carries its metric name -- without it a graph has no
				// legend and two aggregates are indistinguishable.
				if alias != "" {
					metric["__name__"] = alias
				}
				se = &RangeSeries{Metric: metric}
				acc[key] = se
				order = append(order, key)
			}
			se.Values = append(se.Values, [2]string{tsSec, rowField(row, alias)})
		}
	}
	out := make([]RangeSeries, 0, len(order))
	for _, k := range order {
		out = append(out, *acc[k])
	}
	return out, nil
}

// applyBudget copies the budget fields -- and only those -- onto q. The
// filter half of budget is never read.
func applyBudget(q, budget *Query) {
	if budget == nil {
		return
	}
	q.Deadline, q.MaxBytes, q.Stopped = budget.Deadline, budget.MaxBytes, budget.Stopped
}
