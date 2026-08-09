package query

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// UniqPipe is `uniq by (fields) [limit N]` -- distinct rows by those fields.
type UniqPipe struct {
	By    []string
	Limit int
}

// TopPipe is `top N (fields)` -- the N most frequent field-tuples with counts.
type TopPipe struct {
	By []string
	N  int
}

// TailPipe is `tail N` -- the last N rows.
type TailPipe struct{ N int }

// OffsetPipe is `offset N` -- skip the first N rows.
type OffsetPipe struct{ N int }

// RenamePipe is `rename a as b, c as d`.
type RenamePipe struct{ From, To []string }

// DeletePipe is `delete a, b` / `drop a, b` -- remove those fields.
type DeletePipe struct{ Drop []string }

// FilterPipe is `filter <expr>` -- a filter applied mid-pipe over the row
// stream (e.g. after stats: `... | stats by(x) count() c | filter c:>10`).
type FilterPipe struct{ Expr *Expr }

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
		q.MatAll = false // pipes project their own fields; skip full-record materialize
		if sp, ok := pipes[0].(*StatsPipe); ok {
			rows = runStats(s, q, sp)
			pipes = pipes[1:]
		}
	}
	if rows == nil {
		q.Materialize = pipeFields(pipes) // so the row pipes see their fields
		rows = Run(s, q)
	}
	for _, p := range pipes {
		rows = p.apply(rows)
	}
	return rows
}

// pipeFields is the set of fields the row pipes reference, so the filter's
// materialize step includes them (a `* | top (service)` needs service on the
// rows even though nothing filtered on it).
func pipeFields(pipes []Pipe) []string {
	seen := map[string]bool{}
	var out []string
	add := func(fs ...string) {
		for _, f := range fs {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	for _, p := range pipes {
		switch t := p.(type) {
		case *SortPipe:
			add(t.By...)
		case *UniqPipe:
			add(t.By...)
		case *TopPipe:
			add(t.By...)
		case *RenamePipe:
			add(t.From...)
		case *FieldsPipe:
			add(t.Keep...)
		case *FilterPipe:
			fs := map[string]bool{}
			filterFields(t.Expr, fs)
			for f := range fs {
				add(f)
			}
		}
	}
	return out
}

// runStats aggregates matched rows by the group-by fields during the scan,
// accumulating each aggregation without building the matched rows.
func runStats(s Store, q *Query, sp *StatsPipe) []Row {
	// Fast path: `by (field) count()` is the footer posting counts --
	// StatsByField reads them from the offset table without a per-row scan
	// for whole-in-window groups (the 1078x trick). This is the common
	// top-N / group-by shape.
	if len(sp.By) == 1 && len(sp.Aggs) == 1 && sp.Aggs[0].Kind == AggCount {
		alias := sp.Aggs[0].Alias
		vcs := StatsByField(s, q, sp.By[0])
		out := make([]Row, 0, len(vcs))
		for _, vc := range vcs {
			out = append(out, Row{Fields: []Field{{sp.By[0], vc.Value}, {alias, strconv.Itoa(vc.Count)}}})
		}
		return out
	}
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

func rowKey(r Row, by []string) string {
	var b strings.Builder
	for _, f := range by {
		b.WriteString(rowField(r, f))
		b.WriteByte(0)
	}
	return b.String()
}

func (p *UniqPipe) apply(rows []Row) []Row {
	seen := make(map[string]bool, len(rows))
	out := rows[:0]
	for _, r := range rows {
		k := rowKey(r, p.By)
		if !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	if p.Limit > 0 && len(out) > p.Limit {
		out = out[:p.Limit]
	}
	return out
}

func (p *TopPipe) apply(rows []Row) []Row {
	cnt := map[string]int{}
	vals := map[string][]string{}
	for _, r := range rows {
		k := rowKey(r, p.By)
		if cnt[k] == 0 {
			v := make([]string, len(p.By))
			for i, f := range p.By {
				v[i] = rowField(r, f)
			}
			vals[k] = v
		}
		cnt[k]++
	}
	out := make([]Row, 0, len(cnt))
	for k, c := range cnt {
		fields := make([]Field, 0, len(p.By)+1)
		for i, f := range p.By {
			fields = append(fields, Field{f, vals[k][i]})
		}
		fields = append(fields, Field{"count", strconv.Itoa(c)})
		out = append(out, Row{Fields: fields})
	}
	sort.SliceStable(out, func(a, b int) bool {
		ca, _ := strconv.Atoi(rowField(out[a], "count"))
		cb, _ := strconv.Atoi(rowField(out[b], "count"))
		return ca > cb
	})
	if p.N > 0 && len(out) > p.N {
		out = out[:p.N]
	}
	return out
}

func (p *TailPipe) apply(rows []Row) []Row {
	if p.N >= 0 && len(rows) > p.N {
		return rows[len(rows)-p.N:]
	}
	return rows
}

func (p *OffsetPipe) apply(rows []Row) []Row {
	if p.N >= len(rows) {
		return rows[:0]
	}
	if p.N > 0 {
		return rows[p.N:]
	}
	return rows
}

func (p *RenamePipe) apply(rows []Row) []Row {
	m := make(map[string]string, len(p.From))
	for i := range p.From {
		m[p.From[i]] = p.To[i]
	}
	for ri := range rows {
		for fi := range rows[ri].Fields {
			if nn, ok := m[rows[ri].Fields[fi].Key]; ok {
				rows[ri].Fields[fi].Key = nn
			}
		}
	}
	return rows
}

func (p *FilterPipe) apply(rows []Row) []Row {
	out := rows[:0]
	for _, r := range rows {
		if matchRow(r, p.Expr) {
			out = append(out, r)
		}
	}
	return out
}

// matchRow evaluates a filter tree against one materialized row (string
// fields) -- the mid-pipe filter, distinct from evalExpr which runs over a
// group's columns.
func matchRow(r Row, e *Expr) bool {
	switch e.Op {
	case OpAnd:
		for _, k := range e.Kids {
			if !matchRow(r, k) {
				return false
			}
		}
		return true
	case OpOr:
		for _, k := range e.Kids {
			if matchRow(r, k) {
				return true
			}
		}
		return false
	case OpNot:
		return !matchRow(r, e.Child)
	default: // OpLeaf
		return matchPredRow(r, &e.Pred)
	}
}

func matchPredRow(r Row, p *Pred) bool {
	v := rowField(r, p.Field)
	switch p.Kind {
	case Eq:
		return v == p.Value
	case Contains:
		return strings.Contains(v, p.Value)
	case Prefix:
		return strings.HasPrefix(v, p.Value)
	case In:
		for _, x := range p.Values {
			if x == v {
				return true
			}
		}
		return false
	case Regexp:
		if p.re == nil {
			p.re = regexp.MustCompile(p.Value)
		}
		return p.re.MatchString(v)
	case Lt, Le, Gt, Ge:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
		return cmpNum(f, p.Kind, p.Num)
	}
	return false
}

func (p *DeletePipe) apply(rows []Row) []Row {
	drop := make(map[string]bool, len(p.Drop))
	for _, d := range p.Drop {
		drop[d] = true
	}
	for ri := range rows {
		nf := rows[ri].Fields[:0]
		for _, f := range rows[ri].Fields {
			if !drop[f.Key] {
				nf = append(nf, f)
			}
		}
		rows[ri].Fields = nf
	}
	return rows
}
