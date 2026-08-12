package query

import (
	"encoding/json"
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
	AggQuantile
	AggValues     // values(field): JSON array of every non-empty value in the group
	AggUniqValues // uniq_values(field): sorted JSON array of the distinct values
	AggSumLen     // sum_len(field): sum of len(value) over the group
	AggCountEmpty // count_empty(field): rows where the field is empty or missing
	AggRowAny     // row_any(field): an arbitrary value of the field in the group
)

// Agg is one aggregation in a stats pipe.
type Agg struct {
	Field string // "" for count()
	Alias string
	P     float64 // percentile for quantile() (0..1)
	Kind  AggKind
}

// StatsPipe is `stats by (fields) agg1, agg2, ...`.
type StatsPipe struct {
	By   []string
	Aggs []Agg
}

// apply aggregates a materialized row stream -- stats used mid-pipe (e.g. after
// collapse_nums or a filter). A leading stats instead runs during the scan via
// RunPipeline/runStats and never reaches here.
func (p *StatsPipe) apply(rows []Row) []Row {
	acc := map[string]*statEntry{}
	var order []string
	var key []byte
	for _, r := range rows {
		key = key[:0]
		for _, f := range p.By {
			key = append(key, rowField(r, f)...)
			key = append(key, 0)
		}
		e := acc[string(key)]
		if e == nil {
			by := make([]string, len(p.By))
			for j, f := range p.By {
				by[j] = rowField(r, f)
			}
			e = newStatEntry(by, p.Aggs)
			acc[string(key)] = e
			order = append(order, string(key))
		}
		accSample(e, p.Aggs, func(j int) string { return rowField(r, p.Aggs[j].Field) })
	}
	out := make([]Row, 0, len(order))
	for _, k := range order {
		out = append(out, statEntryRow(p, acc[k]))
	}
	return out
}

// newStatEntry allocates an entry with the sets the uniq aggregations need.
func newStatEntry(by []string, aggs []Agg) *statEntry {
	e := &statEntry{by: by, slots: make([]statSlot, len(aggs))}
	for j := range aggs {
		if aggs[j].Kind == AggUniq || aggs[j].Kind == AggCountUniq || aggs[j].Kind == AggUniqValues {
			e.slots[j].set = map[string]struct{}{}
		}
	}
	return e
}

// accSample folds one sample into an entry: valOf(j) is the value of aggregation
// j's field on this sample. Shared by the during-scan and mid-pipe stats so the
// two never drift.
func accSample(e *statEntry, aggs []Agg, valOf func(j int) string) {
	e.rows++
	for j := range aggs {
		a := &aggs[j]
		if a.Field == "" {
			continue // count()
		}
		val := valOf(j)
		sl := &e.slots[j]
		if a.Kind == AggUniq { // uniq returns the values, so it stays exact
			sl.set[val] = struct{}{}
			continue
		}
		if a.Kind == AggCountUniq {
			if sl.hll != nil {
				sl.hll.add(val)
				continue
			}
			sl.set[val] = struct{}{}
			if len(sl.set) > hllThreshold { // spill the exact set into a bounded sketch
				sl.hll = newHLL()
				for v := range sl.set {
					sl.hll.add(v)
				}
				sl.set = nil
			}
			continue
		}
		if a.Kind == AggCountEmpty {
			if val == "" {
				sl.sum++
			}
			continue
		}
		if a.Kind == AggValues {
			if val != "" && len(sl.strs) < valuesCap {
				sl.strs = append(sl.strs, val)
			}
			continue
		}
		if a.Kind == AggRowAny {
			if len(sl.strs) == 0 && val != "" {
				sl.strs = append(sl.strs, val)
			}
			continue
		}
		if a.Kind == AggUniqValues {
			if val != "" {
				sl.set[val] = struct{}{}
			}
			continue
		}
		if a.Kind == AggSumLen {
			sl.sum += float64(len(val))
			continue
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			continue
		}
		if a.Kind == AggQuantile {
			sl.vals = append(sl.vals, f)
			if len(sl.vals) >= quantileCap*2 { // bound RAM: thin the sorted samples by half
				sort.Float64s(sl.vals)
				j := 0
				for i := 0; i < len(sl.vals); i += 2 {
					sl.vals[j] = sl.vals[i]
					j++
				}
				sl.vals = sl.vals[:j]
			}
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
}

// statEntryRow renders one group-by entry as an output row.
func statEntryRow(sp *StatsPipe, e *statEntry) Row {
	fields := make([]Field, 0, len(sp.By)+len(sp.Aggs))
	for j, f := range sp.By {
		fields = append(fields, Field{f, e.by[j]})
	}
	for j := range sp.Aggs {
		fields = append(fields, Field{sp.Aggs[j].Alias, formatAgg(&sp.Aggs[j], &e.slots[j], e.rows)})
	}
	return Row{Fields: fields}
}

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
	hll           *hyperLogLog // count_uniq once the set exceeds hllThreshold (bounded RAM)
	vals          []float64    // numeric samples for quantile() (exact)
	strs          []string     // collected values for values() (bounded by valuesCap)
	has           bool
}

// valuesCap bounds how many values a single values() slot collects, so one
// group-by key over a billion rows cannot OOM. uniq_values() is bounded by
// cardinality via the set, not this.
const valuesCap = 10000

// hllThreshold is where count_uniq switches from an exact set to HyperLogLog:
// below it the answer is exact; above it RAM is bounded at ~16KB regardless of
// cardinality (vs an unbounded set that OOMs at billion-row scale).
const hllThreshold = 8192

// quantileCap bounds a quantile aggregation's sample buffer: past 2x this the
// sorted samples are thinned by half (keep every other), which preserves the
// distribution's shape -- and thus the quantile -- while keeping RAM bounded no
// matter how many rows a group-by key spans. Exact below the cap.
const quantileCap = 65536

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
		case *StatsPipe: // a mid-pipe stats needs its group-by and agg fields materialized
			add(t.By...)
			for _, a := range t.Aggs {
				if a.Field != "" {
					add(a.Field)
				}
			}
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
		case *UnpackJSONPipe:
			add(orDefault(t.From, "_msg"))
		case *ExtractPipe:
			add(orDefault(t.From, "_msg"))
		case *FormatPipe:
			add(templateFields(t.Template)...)
		case *MathPipe:
			add(t.fields...)
		case *CollapseNumsPipe:
			add(orDefault(t.Field, "_msg"))
		case *RankPipe:
			add(orDefault(t.Field, "_msg"))
		case *UnpackLogfmtPipe:
			add(orDefault(t.From, "_msg"))
		case *ReplacePipe:
			add(orDefault(t.Field, "_msg"))
		case *CopyPipe:
			add(t.From...)
		case *LenPipe:
			add(t.Field)
		case *ExtractRegexpPipe:
			add(orDefault(t.From, "_msg"))
		case *DecolorizePipe:
			add(orDefault(t.Field, "_msg"))
		case *PackPipe:
			add(t.Fields...) // explicit fields from storage; pack-all uses whatever rows already carry
		case *UnrollPipe:
			add(t.Field)
		case *UnpackSyslogPipe:
			add(orDefault(t.From, "_msg"))
		}
	}
	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// templateFields returns the <name> placeholders referenced by a format
// template, so they are materialized before the pipe runs.
func templateFields(tpl string) []string {
	var fs []string
	for i := 0; i < len(tpl); {
		if tpl[i] == '<' {
			if j := strings.IndexByte(tpl[i:], '>'); j >= 0 {
				fs = append(fs, tpl[i+1:i+j])
				i += j + 1
				continue
			}
		}
		i++
	}
	return fs
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
				e = newStatEntry(by, sp.Aggs)
				acc[string(key)] = e
			}
			accSample(e, sp.Aggs, func(j int) string {
				if aggCol[j] != nil {
					return aggDict[j][aggCol[j][i]]
				}
				return ""
			})
		})
	}
	out := make([]Row, 0, len(acc))
	for _, e := range acc {
		out = append(out, statEntryRow(sp, e))
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
	case AggUniq:
		return strconv.Itoa(len(sl.set))
	case AggCountUniq:
		if sl.hll != nil {
			return strconv.Itoa(sl.hll.count())
		}
		return strconv.Itoa(len(sl.set))
	case AggQuantile:
		return trimFloat(quantileOf(sl.vals, a.P))
	case AggValues:
		return jsonStrArray(sl.strs)
	case AggUniqValues:
		keys := make([]string, 0, len(sl.set))
		for k := range sl.set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return jsonStrArray(keys)
	case AggSumLen:
		return trimFloat(sl.sum)
	case AggCountEmpty:
		return trimFloat(sl.sum)
	case AggRowAny:
		if len(sl.strs) > 0 {
			return sl.strs[0]
		}
		return ""
	}
	return ""
}

// jsonStrArray renders values() / uniq_values() output as a JSON array string,
// matching VictoriaLogs. An empty slot is "[]", never "null".
func jsonStrArray(vals []string) string {
	if len(vals) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(vals)
	return string(b)
}

func trimFloat(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// quantileOf returns the p-quantile (0..1) of vals by linear interpolation
// between the two nearest ranks -- exact, not sketched. It sorts in place; the
// slice is the aggregation's own sample buffer, discarded after.
func quantileOf(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	if p <= 0 {
		return vals[0]
	}
	if p >= 1 {
		return vals[len(vals)-1]
	}
	pos := p * float64(len(vals)-1)
	lo := int(pos)
	if lo+1 >= len(vals) {
		return vals[lo]
	}
	return vals[lo] + (pos-float64(lo))*(vals[lo+1]-vals[lo])
}

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

// CollapseNumsPipe is `collapse_nums [at field]` (digits -> <N>, VictoriaLogs'
// semantics) or `pattern [at field]` (Full: also hex/uuid tokens -> <ID>, a
// drain-style templater). Either way `stats by (_msg) count()` after it mines
// the top log patterns.
type CollapseNumsPipe struct {
	Field string
	Full  bool
}

func (p *CollapseNumsPipe) apply(rows []Row) []Row {
	f := p.Field
	if f == "" {
		f = "_msg"
	}
	for ri := range rows {
		setRowField(&rows[ri], f, templatize(rowField(rows[ri], f), p.Full))
	}
	return rows
}

// templatize collapses variable tokens to placeholders. Full also collapses
// hex/uuid-ish identifiers (before the digit pass, so their digits do not leak).
func templatize(s string, full bool) string {
	if full {
		s = collapseHexTokens(s)
	}
	return collapseNums(s)
}

// collapseNums replaces every maximal run of ASCII digits with "<N>".
func collapseNums(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	inNum := false
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			if !inNum {
				sb.WriteString("<N>")
				inNum = true
			}
			continue
		}
		inNum = false
		sb.WriteByte(s[i])
	}
	return sb.String()
}

// collapseHexTokens replaces alphanumeric tokens that look like hex identifiers
// (>=8 chars, all hex, mixing a letter and a digit -- request ids, hashes, uuid
// segments) with <ID>. Pure-digit tokens are left for collapseNums.
func collapseHexTokens(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		if !isAlnum(s[i]) {
			sb.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for j < len(s) && isAlnum(s[j]) {
			j++
		}
		tok := s[i:j]
		if isHexID(tok) {
			sb.WriteString("<ID>")
		} else {
			sb.WriteString(tok)
		}
		i = j
	}
	return sb.String()
}

func isAlnum(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isHexID(tok string) bool {
	if len(tok) < 8 {
		return false
	}
	hasDigit, hasLetter := false, false
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F':
			hasLetter = true
		default:
			return false // non-hex char -> not an id
		}
	}
	return hasDigit && hasLetter
}

// RankPipe is `rank "term1 term2" [at field]`: score each row by how often the
// terms appear in the field (default _msg), write _score, and sort by it
// descending -- relevance ranking of full-text matches, which VictoriaLogs
// (time-ordered only) does not offer.
type RankPipe struct {
	Terms []string
	Field string
}

func (p *RankPipe) apply(rows []Row) []Row {
	f := p.Field
	if f == "" {
		f = "_msg"
	}
	for ri := range rows {
		v := rowField(rows[ri], f)
		score := 0
		for _, t := range p.Terms {
			score += strings.Count(v, t)
		}
		setRowField(&rows[ri], "_score", strconv.Itoa(score))
	}
	sort.SliceStable(rows, func(a, b int) bool {
		sa, _ := strconv.Atoi(rowField(rows[a], "_score"))
		sb, _ := strconv.Atoi(rowField(rows[b], "_score"))
		return sa > sb
	})
	return rows
}

// setRowField updates key in place, or appends it -- the mutation the
// transform pipes (unpack_json/format/extract) share.
func setRowField(r *Row, key, val string) {
	for i := range r.Fields {
		if r.Fields[i].Key == key {
			r.Fields[i].Value = val
			return
		}
	}
	r.Fields = append(r.Fields, Field{key, val})
}

// UnpackJSONPipe is `unpack_json [from field] [prefix p]`: parse the (JSON
// object) source field and add each of its keys as a field. Default source is
// _msg.
type UnpackJSONPipe struct {
	From   string
	Prefix string
}

func (p *UnpackJSONPipe) apply(rows []Row) []Row {
	from := p.From
	if from == "" {
		from = "_msg"
	}
	for ri := range rows {
		var m map[string]json.RawMessage
		if json.Unmarshal([]byte(rowField(rows[ri], from)), &m) != nil {
			continue // not a JSON object; leave the row unchanged
		}
		for k, raw := range m {
			setRowField(&rows[ri], p.Prefix+k, jsonRawScalar(raw))
		}
	}
	return rows
}

// jsonRawScalar renders a JSON value as a plain string (a string unquoted,
// anything else as its source bytes).
func jsonRawScalar(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// FormatPipe is `format "template" as field`: build a field from a template
// whose <name> placeholders are replaced by field values.
type FormatPipe struct {
	Template string
	As       string
}

func (p *FormatPipe) apply(rows []Row) []Row {
	as := p.As
	if as == "" {
		as = "_msg"
	}
	for ri := range rows {
		setRowField(&rows[ri], as, expandTemplate(p.Template, rows[ri]))
	}
	return rows
}

// expandTemplate replaces each <name> in tpl with rowField(name); literal text
// is copied through. An unclosed < is copied literally.
func expandTemplate(tpl string, r Row) string {
	var sb strings.Builder
	for i := 0; i < len(tpl); {
		if tpl[i] == '<' {
			if j := strings.IndexByte(tpl[i:], '>'); j >= 0 {
				sb.WriteString(rowField(r, tpl[i+1:i+j]))
				i += j + 1
				continue
			}
		}
		sb.WriteByte(tpl[i])
		i++
	}
	return sb.String()
}

// ExtractPipe is `extract "pattern" [from field]`: pull fields out of the
// source (default _msg) with a pattern of literal text and <name> captures,
// each capture running up to the next literal.
type ExtractPipe struct {
	From    string
	Pattern string
}

// extractTok is one pattern token: a literal (cap == "") or a capture.
type extractTok struct {
	lit string
	cap string
}

func parseExtractPattern(pat string) []extractTok {
	var toks []extractTok
	for i := 0; i < len(pat); {
		if pat[i] == '<' {
			e := strings.IndexByte(pat[i:], '>')
			if e < 0 {
				toks = append(toks, extractTok{lit: pat[i:]})
				break
			}
			toks = append(toks, extractTok{cap: pat[i+1 : i+e]})
			i += e + 1
			continue
		}
		nb := strings.IndexByte(pat[i:], '<')
		if nb < 0 {
			toks = append(toks, extractTok{lit: pat[i:]})
			break
		}
		toks = append(toks, extractTok{lit: pat[i : i+nb]})
		i += nb
	}
	return toks
}

func (p *ExtractPipe) apply(rows []Row) []Row {
	from := p.From
	if from == "" {
		from = "_msg"
	}
	toks := parseExtractPattern(p.Pattern)
	for ri := range rows {
		src := rowField(rows[ri], from)
		for ti := 0; ti < len(toks); ti++ {
			t := toks[ti]
			if t.cap == "" { // literal: skip past it, or give up on this row
				idx := strings.Index(src, t.lit)
				if idx < 0 {
					break
				}
				src = src[idx+len(t.lit):]
				continue
			}
			// capture: up to the next literal, or to the end
			var next string
			if ti+1 < len(toks) {
				next = toks[ti+1].lit
			}
			if next == "" {
				setRowField(&rows[ri], t.cap, src)
				src = ""
				continue
			}
			end := strings.Index(src, next)
			if end < 0 {
				setRowField(&rows[ri], t.cap, src)
				break
			}
			setRowField(&rows[ri], t.cap, src[:end])
			src = src[end:]
		}
	}
	return rows
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
