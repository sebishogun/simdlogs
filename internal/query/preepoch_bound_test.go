package query

import (
	"math"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A SATURATED FAR-PAST BOUND IS MinInt64, AND MinInt64 IS A BOUND.
//
// Two lines in this package turned it back into the epoch:
//
//	filter.go      predMatchesRow   `if from == math.MinInt64 { from = 0 }`
//	time_filter.go timePredBitset   the same, on the columnar path
//
// so `_time:>=1000-01-01` -- whose lower bound saturates to MinInt64, which is
// what entry 129/130's fix produces -- stopped at 1970, while
// `?start=1000-01-01` on the HTTP layer reached 1677. Rows between 1677-09-21
// and the epoch are storable and this store holds them.
//
// GATED HERE BECAUSE IT WAS GATED ONLY FROM internal/api. Both clamps were
// mutated back into the tree separately and `go test ./internal/query` stayed
// GREEN under each: the whole fix was visible only from a different package's
// suite. The two rows below are the row scan and the block scan of the same
// filter, which is why the clamps had to be removed together -- a build with
// one of them answers differently depending on which path the query takes.
func TestASaturatedLowerBoundIsNotTheEpoch(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	// 1900, 1969, 2026: two before the epoch, one after, all inside the
	// int64-nanosecond domain. The pre-epoch pair is what makes this one
	// clamp rather than a general gap -- 1900-01-01 needs no saturation.
	ns := func(y int) int64 {
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	}
	msg := storage.BuildDict([]string{"y1900", "y1969", "y2026"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 3, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{ns(1900), ns(1969), ns(2026)}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
	}}); err != nil {
		t.Fatal(err)
	}

	// The widest window the HTTP layer can resolve, so the WINDOW is not what
	// any row measures: only the `_time:` filter is.
	run := func(q string) int {
		t.Helper()
		pq, perr := ParseLogsQL(q)
		if perr != nil {
			t.Fatalf("parse %q: %v", q, perr)
		}
		pq.From, pq.To = math.MinInt64, math.MaxInt64
		pq.ToSet = true
		return len(RunPipeline(s, pq))
	}

	for _, tc := range []struct {
		name, query string
		want        int
	}{
		// THE BLOCK SCAN (timePredBitset): a bare `_time:` filter with no
		// pipe goes to the group scan's bitset path.
		{"a saturating lower bound, block scan", `_time:>1000-01-01`, 3},
		{"a saturating range, block scan", `_time:[1000-01-01, 2100-01-01]`, 3},
		{"an open lower bound, block scan", `_time:<2100-01-01`, 3},
		// THE ROW SCAN (predMatchesRow): a `| filter` pipe evaluates the same
		// predicate a row at a time.
		{"a saturating lower bound, row scan", `* | filter _time:>1000-01-01`, 3},
		{"a saturating range, row scan", `* | filter _time:[1000-01-01, 2100-01-01]`, 3},
		{"an open lower bound, row scan", `* | filter _time:<2100-01-01`, 3},
		// CONTROLS. A representable pre-epoch bound never hit either clamp and
		// always answered correctly, so a fix that widened everything would be
		// invisible without these; and the exclusions must still exclude.
		{"a representable pre-epoch bound (control)", `_time:[1900-01-01, 2100-01-01]`, 3},
		{"the first representable day (control)", `_time:[1677-09-22, 2100-01-01]`, 3},
		{"a bound that excludes the pre-epoch rows (control)", `_time:[1970-01-01, 2100-01-01]`, 1},
		{"the same, row scan (control)", `* | filter _time:[1970-01-01, 2100-01-01]`, 1},
		{"a bound that excludes everything (control)", `_time:[2100-01-01, 2200-01-01]`, 0},
		{"an upper bound before the epoch (control)", `_time:<1970-01-01`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.query); got != tc.want {
				t.Fatalf("%s matched %d rows, want %d.\n"+
					"A far-past bound saturates to MinInt64 and MinInt64 is a "+
					"BOUND, not a sentinel meaning the epoch: `ts >= MinInt64` "+
					"is true for every int64, which is exactly what an absent "+
					"lower bound means.", tc.query, got, tc.want)
			}
		})
	}

	// AND THE TWO PATHS AGREE, which is the property one clamp alone breaks.
	for _, q := range []string{
		`_time:>1000-01-01`,
		`_time:[1000-01-01, 2100-01-01]`,
		`_time:<2100-01-01`,
		`_time:[1970-01-01, 2100-01-01]`,
	} {
		t.Run("the row scan and the block scan agree on "+q, func(t *testing.T) {
			block, row := run(q), run("* | filter "+q)
			if block != row {
				t.Fatalf("the block scan matched %d rows and the row scan %d for "+
					"the same filter %q: one evaluator clamps and the other does "+
					"not, so the answer depends on the spelling", block, row, q)
			}
		})
	}
}
