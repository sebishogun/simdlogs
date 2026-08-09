package query

import (
	"regexp"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Query is a selective log query: a time window and a conjunction of
// field predicates. It is the subset the planner and LogsQL parser feed;
// AND across predicates, with equality and substring the two the storage
// footer can skip on.
type Query struct {
	From, To int64
	Preds    []Pred
	Limit    int
}

// PredKind selects the comparison.
type PredKind uint8

const (
	Eq       PredKind = iota // field := value  (dict-id equality)
	Contains                 // field ~ substr  (substring, bloom-skippable)
	Regexp                   // field ~ /re/    (RE2 on survivors only)
)

// Pred is one field predicate.
type Pred struct {
	Field string
	Kind  PredKind
	Value string
	re    *regexp.Regexp
}

// Row is a materialized result: the decoded field values of a match.
type Row struct {
	Time   int64
	Fields map[string]string
}

// Store is the read surface the engine needs; storage.Store satisfies it.
type Store interface {
	Groups(from, to int64) []*storage.Reader
}

// Run executes q over the store and returns matching rows in time order,
// up to Limit. Groups outside the time window are skipped by the store;
// groups a predicate proves cannot match are skipped by the footer bloom;
// only survivors are decoded and scanned. This layered skip is where the
// orders of magnitude over a whole-block scan come from.
func Run(s Store, q *Query) []Row {
	var out []Row
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue // footer skip: no column of this group decoded
		}
		out = appendMatches(out, g, q)
		if q.Limit > 0 && len(out) >= q.Limit {
			return out[:q.Limit]
		}
	}
	return out
}

// groupCanMatch rejects a group whose footer proves a required equality
// value absent -- the bloom + dict scan, no row decode.
func groupCanMatch(g *storage.Reader, q *Query) bool {
	for i := range q.Preds {
		p := &q.Preds[i]
		if p.Kind == Eq && g.ColumnExists(p.Field) && !g.DictContains(p.Field, p.Value) {
			return false
		}
	}
	return true
}

// appendMatches decodes the survivor group ONCE per column, builds the
// match bitset with the vectorized predicates, and materializes only the
// selected rows. The earlier version decoded a column inside the per-row
// loop -- O(matches x column) -- which lost the head-to-head; each column
// is now decoded exactly once and indexed into.
func appendMatches(out []Row, g *storage.Reader, q *Query) []Row {
	n := g.Rows
	sel := NewBitset(n)
	sel.SetAll()

	// Time predicate as a bitset over the decoded timestamps.
	ts := g.Timestamps("_time", nil, nil)
	tsel := NewBitset(n)
	for i, t := range ts {
		if t >= q.From && t < q.To {
			tsel.Set(i)
		}
	}
	sel.And(tsel)

	// Decode each predicate field's indices+dict once, reused for both the
	// filter and the materialize.
	type col struct {
		idx  []uint32
		dict []string
	}
	cols := make(map[string]col, len(q.Preds))
	getCol := func(field string) col {
		if c, ok := cols[field]; ok {
			return c
		}
		idx, dict := g.DictIndices(field)
		c := col{idx: idx, dict: dict}
		cols[field] = c
		return c
	}

	for i := range q.Preds {
		p := &q.Preds[i]
		c := getCol(p.Field)
		sel.And(predBitsetCol(g, p, c.idx, c.dict, n))
	}

	sel.ForEach(func(i int) {
		row := Row{Time: ts[i], Fields: make(map[string]string, len(q.Preds))}
		for _, p := range q.Preds {
			c := cols[p.Field]
			if c.idx != nil {
				row.Fields[p.Field] = c.dict[c.idx[i]]
			}
		}
		out = append(out, row)
	})
	return out
}

// predBitsetCol is predBitset over already-decoded indices/dict.
func predBitsetCol(g *storage.Reader, p *Pred, idx []uint32, dict []string, n int) *Bitset {
	b := NewBitset(n)
	if idx == nil {
		return b
	}
	switch p.Kind {
	case Eq:
		id := g.DictID(p.Field, p.Value)
		if id < 0 {
			return b
		}
		want := uint32(id)
		for i, v := range idx {
			if v == want {
				b.Set(i)
			}
		}
	case Contains:
		hit := make([]bool, len(dict))
		for di, d := range dict {
			hit[di] = containsSubstr(d, p.Value)
		}
		for i, v := range idx {
			if hit[v] {
				b.Set(i)
			}
		}
	case Regexp:
		if p.re == nil {
			p.re = regexp.MustCompile(p.Value)
		}
		hit := make([]bool, len(dict))
		for di, d := range dict {
			hit[di] = p.re.MatchString(d)
		}
		for i, v := range idx {
			if hit[v] {
				b.Set(i)
			}
		}
	}
	return b
}

func containsSubstr(s, sub string) bool {
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Count returns how many rows match q, without materializing any -- the
// aggregation hot path. It is the design's best case: group-skip by
// footer, then popcount of the match bitset, no per-row map, no field
// decode beyond the predicate columns. This is where finer skip
// granularity turns into a real margin over a scan-and-count.
func Count(s Store, q *Query) int {
	total := 0
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		total += matchBitset(g, q).Count()
	}
	return total
}

// Histogram buckets match counts by time at the given step (nanoseconds),
// the /select/logsql/hits shape -- again without materializing rows.
func Histogram(s Store, q *Query, step int64) map[int64]int {
	out := map[int64]int{}
	raw := []uint64(nil)
	tbuf := []int64(nil)
	for _, g := range s.Groups(q.From, q.To) {
		if !groupCanMatch(g, q) {
			continue
		}
		sel := matchBitset(g, q)
		ts := g.Timestamps("_time", raw, tbuf)
		sel.ForEach(func(i int) {
			out[ts[i]/step*step]++
		})
	}
	return out
}

// matchBitset builds the selection for a group: time window AND every
// predicate, each column decoded once. Shared by Count, Histogram and the
// row path.
func matchBitset(g *storage.Reader, q *Query) *Bitset {
	n := g.Rows
	sel := NewBitset(n)
	sel.SetAll()
	// Time: skip the per-row filter entirely when the whole group is
	// inside the window (the common selective-window case) -- no _time
	// decode at all.
	if g.TimeMin < q.From || g.TimeMax >= q.To {
		ts := g.Timestamps("_time", nil, nil)
		tsel := NewBitset(n)
		for i, t := range ts {
			if t >= q.From && t < q.To {
				tsel.Set(i)
			}
		}
		sel.And(tsel)
	}
	for i := range q.Preds {
		p := &q.Preds[i]
		idx, dict := g.DictIndices(p.Field)
		sel.And(predBitsetCol(g, p, idx, dict, n))
	}
	return sel
}
