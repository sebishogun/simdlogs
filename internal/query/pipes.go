package query

import (
	"sort"
	"strconv"
)

// The pipe language: after the filter selects rows, pipes transform them --
// LogsQL's `filter | stats ... | sort ... | limit N`. stats is the heavy one
// and aggregates during the group scan (never materializing the matched rows);
// the row pipes (sort, limit, fields) run on whatever row set precedes them.

// Pipe transforms a row set. Stats is a source pipe (it consumes the scan, not
// a prior row set) and is handled specially by RunPipeline.
type Pipe interface {
	apply(rows []Row) []Row
}

// AggKind is a stats aggregation function.
type AggKind uint8

const (
	AggCount AggKind = iota
	AggSum
	AggAvg
	AggMin
	AggMax
	AggUniq
	AggCountUniq
)

// Agg is one aggregation in a stats pipe.
type Agg struct {
	Field string // "" for count()
	Alias string
	Kind  AggKind
}

// StatsPipe is `stats by (fields) agg1, agg2, ...`.
type StatsPipe struct {
	By   []string
	Aggs []Agg
}

func (p *StatsPipe) apply(rows []Row) []Row { return rows } // handled by RunPipeline

// SortPipe is `sort by (fields) [desc] [limit N]`.
type SortPipe struct {
	By    []string
	Limit int
	Desc  bool
}

// LimitPipe is `limit N` / `head N`.
type LimitPipe struct{ N int }

// FieldsPipe is `fields a, b` -- keep only these, in that order.
type FieldsPipe struct{ Keep []string }

// statSlot accumulates one aggregation for one group-by key.
type statSlot struct {
	sum, min, max float64
	cnt           int64 // numeric samples (avg denominator)
	set           map[string]struct{}
	has           bool
}

// statEntry is one group-by key's accumulators.
type statEntry struct {
	by    []string
	slots []statSlot
	rows  int64 // count()
}

// RunPipeline runs q's filter and pipes. A leading stats pipe aggregates
// during the scan; otherwise the filter's rows feed the pipe chain.
func RunPipeline(s Store, q *Query) []Row {
	var rows []Row
	pipes := q.Pipes
	if len(pipes) > 0 {
		if sp, ok := pipes[0].(*StatsPipe); ok {
			rows = runStats(s, q, sp)
			pipes = pipes[1:]
		}
	}
	if rows == nil {
		rows = Run(s, q)
	}
	for _, p := range pipes {
		rows = p.apply(rows)
	}
	return rows
}

// runStats aggregates matched rows by the group-by fields during the scan,
// accumulating each aggregation without building the matched rows.
func runStats(s Store, q *Query, sp *StatsPipe) []Row {
	acc := map[string]*statEntry{}
	var key []byte
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		sel := matchBitset(g, q)
		if sel.Count() == 0 {
			continue
		}
		byCol := make([][]uint32, len(sp.By))
		byDict := make([][]string, len(sp.By))
		for j, f := range sp.By {
			byCol[j], byDict[j] = g.DictIndices(f)
		}
		aggCol := make([][]uint32, len(sp.Aggs))
		aggDict := make([][]string, len(sp.Aggs))
		for j := range sp.Aggs {
			if sp.Aggs[j].Field != "" {
				aggCol[j], aggDict[j] = g.DictIndices(sp.Aggs[j].Field)
			}
		}
		sel.ForEach(func(i int) {
			key = key[:0]
			for j := range sp.By {
				if byCol[j] != nil {
					key = append(key, byDict[j][byCol[j][i]]...)
				}
				key = append(key, 0)
			}
			e := acc[string(key)]
			if e == nil {
				by := make([]string, len(sp.By))
				for j := range sp.By {
					if byCol[j] != nil {
						by[j] = byDict[j][byCol[j][i]]
					}
				}
				e = &statEntry{by: by, slots: make([]statSlot, len(sp.Aggs))}
				for j := range sp.Aggs {
					if sp.Aggs[j].Kind == AggUniq || sp.Aggs[j].Kind == AggCountUniq {
						e.slots[j].set = map[string]struct{}{}
					}
				}
				acc[string(key)] = e
			}
			e.rows++
			for j := range sp.Aggs {
				a := &sp.Aggs[j]
				if a.Field == "" {
					continue // count()
				}
				var val string
				if aggCol[j] != nil {
					val = aggDict[j][aggCol[j][i]]
				}
				sl := &e.slots[j]
				if a.Kind == AggUniq || a.Kind == AggCountUniq {
					sl.set[val] = struct{}{}
					continue
				}
				f, err := strconv.ParseFloat(val, 64)
				if err != nil {
					continue
				}
				if !sl.has || f < sl.min {
					sl.min = f
				}
				if !sl.has || f > sl.max {
					sl.max = f
				}
				sl.sum += f
				sl.cnt++
				sl.has = true
			}
		})
	}
	out := make([]Row, 0, len(acc))
	for _, e := range acc {
		fields := make([]Field, 0, len(sp.By)+len(sp.Aggs))
		for j, f := range sp.By {
			fields = append(fields, Field{f, e.by[j]})
		}
		for j := range sp.Aggs {
			fields = append(fields, Field{sp.Aggs[j].Alias, formatAgg(&sp.Aggs[j], &e.slots[j], e.rows)})
		}
		out = append(out, Row{Fields: fields})
	}
	return out
}

func formatAgg(a *Agg, sl *statSlot, rows int64) string {
	switch a.Kind {
	case AggCount:
		return strconv.FormatInt(rows, 10)
	case AggSum:
		return trimFloat(sl.sum)
	case AggAvg:
		if sl.cnt == 0 {
			return "0"
		}
		return trimFloat(sl.sum / float64(sl.cnt))
	case AggMin:
		return trimFloat(sl.min)
	case AggMax:
		return trimFloat(sl.max)
	case AggUniq, AggCountUniq:
		return strconv.Itoa(len(sl.set))
	}
	return ""
}

func trimFloat(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// rowField returns a row's value for key.
func rowField(r Row, key string) string {
	for _, f := range r.Fields {
		if f.Key == key {
			return f.Value
		}
	}
	return ""
}

func lessVal(a, b string) bool {
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	if ea == nil && eb == nil {
		return fa < fb
	}
	return a < b
}

func (p *SortPipe) apply(rows []Row) []Row {
	sort.SliceStable(rows, func(a, b int) bool {
		for _, f := range p.By {
			va, vb := rowField(rows[a], f), rowField(rows[b], f)
			if va == vb {
				continue
			}
			less := lessVal(va, vb)
			if p.Desc {
				return !less
			}
			return less
		}
		return false
	})
	if p.Limit > 0 && len(rows) > p.Limit {
		rows = rows[:p.Limit]
	}
	return rows
}

func (p *LimitPipe) apply(rows []Row) []Row {
	if p.N >= 0 && len(rows) > p.N {
		return rows[:p.N]
	}
	return rows
}

func (p *FieldsPipe) apply(rows []Row) []Row {
	keep := make(map[string]bool, len(p.Keep))
	for _, k := range p.Keep {
		keep[k] = true
	}
	for i := range rows {
		nf := rows[i].Fields[:0]
		for _, f := range rows[i].Fields {
			if keep[f.Key] {
				nf = append(nf, f)
			}
		}
		rows[i].Fields = nf
	}
	return rows
}
