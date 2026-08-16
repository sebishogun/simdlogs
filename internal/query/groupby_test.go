package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A row that does not carry the group-by field is not a group.
//
// rowField returns "" for both "absent" and "present and empty", and every
// group-by pipe keyed on it -- so rows with no `svc` collected into a group
// labelled svc="" and that group could RANK FIRST. 22 rows, 10 of them without
// `svc`, gave the cluster a top-2 of {"svc":"","c":"10"} then d(5); the same
// query on a single node answers d(5), c(4), because the node reads a group's
// dictionary and a group with no such column has none. Two halves of one system
// answering differently, neither saying so.
//
// A row whose value IS the empty string is still a member, and stays its own
// group -- absent and empty are different facts and the fix must not merge them
// the other way round.

func fieldRow(pairs ...string) Row {
	r := Row{NoTime: true}
	for i := 0; i+1 < len(pairs); i += 2 {
		r.Fields = append(r.Fields, Field{pairs[i], pairs[i+1]})
	}
	return r
}

// render collapses a row set to "k=v,k=v;..." sorted, so a test failure shows
// the whole answer rather than one field of it.
func renderGroups(rows []Row) string {
	var out []string
	for _, r := range rows {
		var parts []string
		for _, f := range r.Fields {
			parts = append(parts, f.Key+"="+f.Value)
		}
		out = append(out, strings.Join(parts, ","))
	}
	return strings.Join(out, " | ")
}

func groupRows() []Row {
	var rows []Row
	// 4 with svc=c, 5 with svc=d, 2 with svc="" (explicitly empty), and 10
	// carrying no svc at all -- the phantom group's would-be membership, and the
	// largest count in the set, so a wrong answer ranks it first.
	for i := 0; i < 4; i++ {
		rows = append(rows, fieldRow("svc", "c", "_msg", fmt.Sprintf("c%d", i)))
	}
	for i := 0; i < 5; i++ {
		rows = append(rows, fieldRow("svc", "d", "_msg", fmt.Sprintf("d%d", i)))
	}
	for i := 0; i < 2; i++ {
		rows = append(rows, fieldRow("svc", "", "_msg", fmt.Sprintf("e%d", i)))
	}
	for i := 0; i < 10; i++ {
		rows = append(rows, fieldRow("_msg", fmt.Sprintf("none%d", i)))
	}
	return rows
}

func TestARowWithoutTheGroupByFieldIsNotAGroup(t *testing.T) {
	rows := groupRows()

	t.Run("stats", func(t *testing.T) {
		p := &StatsPipe{By: []string{"svc"}, Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}
		got := p.apply(rows)
		// c(4), d(5) and the explicitly-empty group (2). NOT a group of 10.
		counts := map[string]string{}
		for _, r := range got {
			counts[rowField(r, "svc")] = rowField(r, "c")
		}
		if len(got) != 3 {
			t.Fatalf("stats by svc produced %d groups, want 3 (c, d and the "+
				"explicitly-empty one): %s", len(got), renderGroups(got))
		}
		for svc, want := range map[string]string{"c": "4", "d": "5", "": "2"} {
			if counts[svc] != want {
				t.Errorf("group %q counted %s, want %s (all: %s)", svc, counts[svc], want, renderGroups(got))
			}
		}
		if counts[""] == "10" || counts[""] == "12" {
			t.Errorf("the rows carrying no svc were folded into the empty-string "+
				"group: %s", renderGroups(got))
		}
	})

	t.Run("top", func(t *testing.T) {
		p := &TopPipe{By: []string{"svc"}, N: 2}
		got := p.apply(rows)
		if len(got) == 0 {
			t.Fatal("top by svc returned nothing")
		}
		// Whatever the ranking, the phantom 10 must not be in it.
		for _, r := range got {
			if rowField(r, "hits") == "10" {
				t.Errorf("top by svc ranked the rows that carry no svc: %s", renderGroups(got))
			}
		}
	})

	t.Run("uniq", func(t *testing.T) {
		p := &UniqPipe{By: []string{"svc"}}
		got := p.apply(append([]Row(nil), rows...))
		// c, d, "" -- three distinct values, and the ten absent rows are not a
		// fourth.
		if len(got) != 3 {
			t.Errorf("uniq by svc returned %d values, want 3: %s", len(got), renderGroups(got))
		}
	})
}

// field_values returns the TOP values, not the first-seen ones.
//
// The pipe built its list in first-seen order and handed it to valueCountRows,
// which truncates whatever it is given -- so `| field_values svc limit 2` over a
// log stream returned the two values that appeared earliest, which in a stream
// are typically the rarest. Same output shape as every values endpoint, opposite
// ordering rule, nothing in the response saying so.
func TestFieldValuesReturnsTheTopValuesAndNotTheFirstSeen(t *testing.T) {
	// Deliberately first-seen-ascending and count-descending in OPPOSITE
	// orders, so the two rules cannot give the same answer.
	var rows []Row
	rows = append(rows, fieldRow("svc", "rare-first")) // seen first, 1 hit
	for i := 0; i < 3; i++ {
		rows = append(rows, fieldRow("svc", "middle"))
	}
	for i := 0; i < 9; i++ {
		rows = append(rows, fieldRow("svc", "top"))
	}

	p := &FieldValuesPipe{Field: "svc", Limit: 2}
	got := p.apply(rows)
	if len(got) != 2 {
		t.Fatalf("field_values svc limit 2 returned %d rows: %s", len(got), renderGroups(got))
	}
	for i, want := range []struct{ value, hits string }{{"top", "9"}, {"middle", "3"}} {
		if v, h := rowField(got[i], "value"), rowField(got[i], "hits"); v != want.value || h != want.hits {
			t.Errorf("row %d is %s=%s, want %s=%s (all: %s). Truncating in first-seen "+
				"order returns the RAREST values as the top ones",
				i, v, h, want.value, want.hits, renderGroups(got))
		}
	}
}

// StatsByField returns the same order every time, and ties break by value.
//
// It sorted by count with NO tie-break over a slice built by ranging a Go map,
// which Go deliberately randomizes per run: five identical requests for
// `stats by (svc) | limit 3` returned five different sets of three values, and
// an operator comparing two dashboard loads saw data change that had not.
// sortValueCounts and runTopFast both break the tie by value and both say why;
// this was the one place that did not.
//
// Asserted over repeated calls against ONE store rather than against a fixed
// expected order: a single call to a randomized function agrees with a fixed
// expectation often enough that pinning one order proves nothing.
func TestStatsByFieldIsDeterministic(t *testing.T) {
	// Every value with the SAME count, so the tie-break is the only thing
	// deciding the order and map iteration has maximum freedom to disagree.
	s, _ := storage.OpenStore(t.TempDir())
	base := int64(1_700_000_000_000_000_000)
	var ts []int64
	var sv []string
	for i := 0; i < 60; i++ {
		ts = append(ts, base+int64(i))
		sv = append(sv, fmt.Sprintf("svc-%02d", i%20)) // 20 values, 3 rows each
	}
	sd := storage.BuildDict(sv)
	s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "svc", Type: storage.ColDict, Dict: &sd},
	}})

	q := &Query{From: base, To: base + 1000}
	first := ""
	for run := 0; run < 20; run++ {
		vcs := StatsByField(s, q, "svc")
		if len(vcs) != 20 {
			t.Fatalf("run %d returned %d values, want 20", run, len(vcs))
		}
		var b strings.Builder
		for _, vc := range vcs {
			fmt.Fprintf(&b, "%s=%d,", vc.Value, vc.Count)
		}
		if run == 0 {
			first = b.String()
			continue
		}
		if b.String() != first {
			t.Fatalf("run %d ordered the same data differently:\n  run 0: %s\n  run %d: %s",
				run, first, run, b.String())
		}
	}
	// And the tie-break is by VALUE ascending, the rule every other values
	// endpoint uses -- not merely "some stable order".
	vcs := StatsByField(s, q, "svc")
	for i := 1; i < len(vcs); i++ {
		if vcs[i-1].Count == vcs[i].Count && vcs[i-1].Value > vcs[i].Value {
			t.Errorf("equal counts are not ordered by value: %q before %q",
				vcs[i-1].Value, vcs[i].Value)
			break
		}
	}
}

// The cardinality ceiling fires at a coordinator too.
//
// tooManyKeys returns false when q is nil, and ApplyPipes -- the coordinator's
// pipe runner -- never stamped it. So search.maxGroupKeys was enforced on a
// storage node and not at a router: the same query answered 413 against one node
// and 200 with every group through the cluster. That is the worse direction, and
// not by a little: the coordinator is the ONE place that holds every shard's
// groups at once, so it is exactly where a cardinality ceiling matters.
func TestTheGroupKeyCeilingFiresAtACoordinator(t *testing.T) {
	var rows []Row
	for i := 0; i < 12; i++ {
		rows = append(rows, fieldRow("svc", fmt.Sprintf("svc-%d", i)))
	}

	for _, tc := range []struct {
		name  string
		pipes []Pipe
	}{
		{"stats by", []Pipe{&StatsPipe{By: []string{"svc"}, Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}}},
		{"top by", []Pipe{&TopPipe{By: []string{"svc"}, N: 100}}},
		{"uniq by", []Pipe{&UniqPipe{By: []string{"svc"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &Query{Pipes: tc.pipes}
			// Bound the way a coordinator binds it: an UNBOUND query has nowhere
			// to record why it stopped, so it would return nil with a nil error
			// -- the silent empty answer, not the ceiling working.
			q.Bind(context.Background(), Limits{MaxGroupKeys: 3})

			got := ApplyPipes(q, rows)
			if got != nil {
				t.Errorf("MaxGroupKeys=3 let %d groups through at the coordinator", len(got))
			}
			err := q.StopErr()
			if err == nil {
				t.Fatal("the query was not stopped, so nothing tells the caller the " +
					"answer would have been truncated")
			}
			if !errors.Is(err, ErrTooManyGroupKeys) {
				t.Errorf("stopped with %v, want ErrTooManyGroupKeys", err)
			}
		})
	}

	// Under the ceiling, the same query goes through: a gate that refuses
	// everything is not a gate.
	q := &Query{Pipes: []Pipe{&StatsPipe{By: []string{"svc"}, Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}}}
	q.Bind(context.Background(), Limits{MaxGroupKeys: 100})
	if got := ApplyPipes(q, rows); len(got) != 12 {
		t.Errorf("with MaxGroupKeys=100, 12 groups produced %d rows (err %v)", len(got), q.StopErr())
	}
}
