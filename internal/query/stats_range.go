package query

import "strconv"

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

// StatsQueryRange runs raw as a stats query over [from,to) in `step`-sized
// buckets, returning one series per group-by tuple with a point per bucket. The
// query is re-parsed per bucket so each gets a clean window (RunPipeline mutates
// the query while resolving _time). ts is the bucket start in unix seconds.
func StatsQueryRange(s Store, raw string, from, to, step, now int64) ([]RangeSeries, error) {
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
