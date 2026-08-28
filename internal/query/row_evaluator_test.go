package query

import (
	"sort"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// ONE PREDICATE, TWO SPELLINGS, ONE ANSWER.
//
// `| filter <pred>` and a top-level `<pred>` are the same filter, run by two
// different evaluators: the group scan (bitsets over the dictionary columns)
// and the per-row form. The per-row form was written TWICE -- `predMatchesRow`
// in filter.go and `matchPredRow` in pipes.go -- and each copy was missing what
// the other had, so five predicate kinds answered every row at the top level
// and ZERO rows through `| filter`, at HTTP 200 with nothing to say so.
//
// The table below is that measurement. Each row is asserted through BOTH
// spellings against the same store, so a kind either works in both or the test
// names which one it lost.
func TestAPipeFilterAnswersEveryPredicateKindTheTopLevelDoes(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	st := twoRowStore(t, []int64{base, base + int64(time.Second)}, map[string][]string{
		"_msg": {"alpha", "beta"},
		"n":    {"5", "50"},
		"ip":   {"10.0.0.5", "10.9.9.9"},
		"lvl":  {"ERROR", "info"},
	})
	for _, tc := range []struct {
		name, pred string
		want       int
	}{
		{"numeric range", `n:range(1, 10)`, 1},
		{"length range", `_msg:len_range(1, 5)`, 2},
		{"ipv4 range", `ip:ipv4_range(10.0.0.0, 10.0.0.255)`, 1},
		{"sequence", `_msg:seq("al", "ha")`, 1},
		{"string range", `_msg:string_range(a, b)`, 1},
		// The controls: the two kinds both copies already had. Without them a
		// build whose `| filter` answered every row would pass the rows above.
		{"substring (control)", `_msg:alpha`, 1},
		{"numeric comparison (control)", `n:>10`, 1},
		{"exact (control)", `lvl:=ERROR`, 1},
		{"a predicate matching nothing (control)", `_msg:zzz`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			top := countRows(t, st, tc.pred)
			piped := countRows(t, st, `* | filter `+tc.pred)
			if top != tc.want {
				t.Fatalf("top-level %s matched %d rows, want %d -- the fixture "+
					"changed, so this row proves nothing about the pipe",
					tc.pred, top, tc.want)
			}
			if piped != tc.want {
				t.Fatalf("`| filter %s` matched %d rows and the identical "+
					"top-level predicate matched %d.\nOne filter, two "+
					"evaluators, two answers -- and the pipe's is silent.",
					tc.pred, piped, tc.want)
			}
		})
	}
}

// AND A TIME PREDICATE, which only ONE of the two copies could ever answer.
//
// `_time` lives in Row.Time, not in Row.Fields, so a field lookup returns "" and
// every comparison against it fails. The unified body handles the time kinds
// before the lookup, and reports NO MATCH -- not 1970 -- for a source that has
// no timestamp, because zero would make a stats result match any range
// containing the epoch.
func TestAPipeFilterAnswersATimePredicate(t *testing.T) {
	st := twoRowStore(t, []int64{
		time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2020, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
	}, map[string][]string{"_msg": {"alpha", "beta"}})
	for _, tc := range []struct {
		name, q string
		want    int
	}{
		{"a day", `* | filter _time:2026-06-01`, 1},
		{"a lower bound", `* | filter _time:>2026-01-01`, 1},
		{"an upper bound", `* | filter _time:<2026-01-01`, 1},
		{"a range", `* | filter _time:[2019-01-01, 2021-01-01]`, 1},
		{"a range covering both", `* | filter _time:[2019-01-01, 2027-01-01]`, 2},
		{"a day range", `* | filter _time:day_range[11:00, 13:00]`, 2},
		{"a day range excluding both", `* | filter _time:day_range[01:00, 02:00]`, 0},
		// A ROW WITH NO TIMESTAMP MATCHES NO TIME RANGE. `| stats` produces
		// rows carrying no `_time`, and reading their timestamp as zero would
		// file every one of them in 1970.
		{"a stats row has no timestamp", `* | stats count() c | filter _time:[1969-01-01, 1971-01-01]`, 0},
		{"the stats row itself (control)", `* | stats count() c`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := countRows(t, st, tc.q); got != tc.want {
				t.Fatalf("%s matched %d rows, want %d", tc.q, got, tc.want)
			}
		})
	}
}

// twoRowStore is a store holding one group with the given timestamps and
// dictionary columns.
func twoRowStore(t *testing.T, ts []int64, cols map[string][]string) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	g := &storage.Group{Rows: len(ts), Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
	}}
	// Sorted, so the column order does not depend on map iteration and two runs
	// build the same group.
	names := make([]string, 0, len(cols))
	for n := range cols {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		d := storage.BuildDict(cols[n])
		g.Columns = append(g.Columns, storage.Column{Name: n, Type: storage.ColDict, Dict: &d})
	}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	return s
}

// countRows runs a query over the whole store and returns how many rows it
// answered.
func countRows(t *testing.T, s *storage.Store, q string) int {
	t.Helper()
	pq, err := ParseLogsQL(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	return len(RunPipeline(s, pq))
}
