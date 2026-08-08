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

// appendMatches decodes the survivor group, builds the match bitset with
// the vectorized predicates, and materializes the selected rows.
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

	for i := range q.Preds {
		p := &q.Preds[i]
		sel.And(predBitset(g, p, n))
	}

	limit := q.Limit
	sel.ForEach(func(i int) {
		row := Row{Time: ts[i], Fields: map[string]string{}}
		row.Fields["_time"] = ""
		// Materialize the predicate fields (a real select would take a
		// projection list; here every dict column is available).
		for _, p := range q.Preds {
			if idx, dict := g.DictIndices(p.Field); idx != nil {
				row.Fields[p.Field] = dict[idx[i]]
			}
		}
		out = append(out, row)
		_ = limit
	})
	return out
}

// predBitset builds the per-row match mask for one predicate. Equality on
// a dict column is one id compare per row (no string compares) over the
// decoded indices; substring and regexp fall to the survivors.
func predBitset(g *storage.Reader, p *Pred, n int) *Bitset {
	b := NewBitset(n)
	idx, dict := g.DictIndices(p.Field)
	if idx == nil {
		return b // absent column matches nothing
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
