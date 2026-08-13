package query

import (
	"fmt"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// fastPipeStore builds a multi-group store with skewed field values, so the
// posting-count path and the row-scan path have something to disagree about:
// ties in the counts, values absent from some groups, and a predicate that
// selects part of a group.
func fastPipeStore(t *testing.T) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hosts := []string{"h1", "h2", "h3", "h4"}
	levels := []string{"error", "warn", "info"}
	ts := int64(0)
	for g := 0; g < 4; g++ {
		const n = 50
		var times []int64
		hv := make([]string, n)
		lv := make([]string, n)
		for i := 0; i < n; i++ {
			ts++
			times = append(times, ts)
			// Skew shifts per group so a value can be top overall but not locally,
			// and h4 appears only in the last group.
			hi := (i + g) % 3
			if g == 3 && i%10 == 0 {
				hi = 3
			}
			hv[i] = hosts[hi]
			lv[i] = levels[i%len(levels)]
		}
		hd := storage.BuildDict(hv)
		ld := storage.BuildDict(lv)
		if _, err := s.AppendGroup(&storage.Group{Rows: n, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "host", Type: storage.ColDict, Dict: &hd},
			{Name: "level", Type: storage.ColDict, Dict: &ld},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// rowsEqual compares two result sets field for field, order included.
func rowsEqual(a, b []Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i].Fields) != len(b[i].Fields) || a[i].NoTime != b[i].NoTime {
			return false
		}
		for j := range a[i].Fields {
			if a[i].Fields[j] != b[i].Fields[j] {
				return false
			}
		}
	}
	return true
}

func dump(rows []Row) string {
	s := ""
	for _, r := range rows {
		s += "{"
		for _, f := range r.Fields {
			s += f.Key + "=" + f.Value + " "
		}
		s += "} "
	}
	return s
}

// TestFastPipesMatchGeneric is the contract for every footer-read shortcut:
// the answer must be what materializing the rows and running the pipe would
// have produced. The generic path is the reference; a shortcut that disagrees
// is a wrong answer however fast it is.
func TestFastPipesMatchGeneric(t *testing.T) {
	s := fastPipeStore(t)
	queries := []string{
		`* | stats count() n`,
		`level:=error | stats count() n`,
		`* | top 2 by (host)`,
		`* | top 10 by (host)`,
		`level:=error | top 3 by (host)`,
		`* | uniq by (host)`,
		`level:=error | uniq by (host)`,
		`* | uniq by (host) limit 2`,
		`* | limit 7`,
		`level:=warn | limit 5`,
	}
	for _, raw := range queries {
		t.Run(raw, func(t *testing.T) {
			fast := runRaw(t, s, raw)
			generic := runRawGeneric(t, s, raw)
			// `limit` and `uniq` define no row order, so compare as sets there;
			// `top` and `stats` are ordered and compared exactly.
			if isOrdered(raw) {
				if !rowsEqual(fast, generic) {
					t.Fatalf("fast != generic\n fast:    %s\n generic: %s", dump(fast), dump(generic))
				}
				return
			}
			if len(fast) != len(generic) {
				t.Fatalf("fast %d rows, generic %d\n fast:    %s\n generic: %s",
					len(fast), len(generic), dump(fast), dump(generic))
			}
			seen := map[string]int{}
			for _, r := range generic {
				seen[rowSig(r)]++
			}
			for _, r := range fast {
				seen[rowSig(r)]--
			}
			for k, v := range seen {
				if v != 0 {
					t.Fatalf("row %q count differs by %d\n fast:    %s\n generic: %s", k, v, dump(fast), dump(generic))
				}
			}
		})
	}
}

func isOrdered(raw string) bool {
	for _, s := range []string{"stats", "top"} {
		if contains(raw, s) {
			return true
		}
	}
	return false
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func rowSig(r Row) string {
	s := ""
	for _, f := range r.Fields {
		s += f.Key + "=" + f.Value + ";"
	}
	return s
}

func runRaw(t *testing.T, s Store, raw string) []Row {
	t.Helper()
	q, err := ParseLogsQL(raw)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	return RunPipeline(s, q)
}

// runRawGeneric runs the same query with the shortcuts disabled, by executing
// the filter itself and applying the pipe to the materialized rows.
func runRawGeneric(t *testing.T, s Store, raw string) []Row {
	t.Helper()
	q, err := ParseLogsQL(raw)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	resolveTimePreds(q)
	q.Materialize = pipeFields(q.Pipes)
	rows := Run(s, q)
	for _, p := range q.Pipes {
		if sp, ok := p.(*StatsPipe); ok {
			sp.rangeSec = float64(q.To-q.From) / 1e9
		}
		rows = p.apply(rows)
	}
	return rows
}

// TestFastPipesNarrowShapes covers the forms the shortcuts must decline, so a
// future widening cannot silently answer them with the wrong plan.
func TestFastPipesNarrowShapes(t *testing.T) {
	s := fastPipeStore(t)
	for _, raw := range []string{
		`* | top 2 by (host, level)`,    // multi-field tuple: not one dictionary
		`* | uniq by (host, level)`,     // same
		`* | stats count(host) n`,       // count of a field, not of rows
		`* | stats sum(host) n`,         // not a count
		`* | stats by (host) count() n`, // grouped: the by-field path, not the popcount
	} {
		fast := runRaw(t, s, raw)
		generic := runRawGeneric(t, s, raw)
		if len(fast) != len(generic) {
			t.Errorf("%s: fast %d rows, generic %d", raw, len(fast), len(generic))
		}
	}
}

// TestLimitPushdownStopsEarly proves the `| limit N` push-down is what makes it
// cheap: the scan must stop, not materialize everything and slice.
func TestLimitPushdownStopsEarly(t *testing.T) {
	s := fastPipeStore(t)
	q, err := ParseLogsQL(`* | limit 7`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	rows := RunPipeline(s, q)
	if len(rows) != 7 {
		t.Fatalf("limit 7 returned %d rows", len(rows))
	}
	if q.Limit != 7 {
		t.Fatalf("limit was not pushed into the scan: q.Limit = %d", q.Limit)
	}
}

// TestLimitPushdownKeepsSmallerBound guards the interaction with the endpoint's
// own ?limit=: whichever bound is tighter must win.
func TestLimitPushdownKeepsSmallerBound(t *testing.T) {
	s := fastPipeStore(t)
	for _, tc := range []struct{ outer, pipe, want int }{
		{outer: 3, pipe: 7, want: 3},
		{outer: 9, pipe: 4, want: 4},
		{outer: 0, pipe: 5, want: 5},
	} {
		q, err := ParseLogsQL(fmt.Sprintf(`* | limit %d`, tc.pipe))
		if err != nil {
			t.Fatal(err)
		}
		q.From, q.To, q.Limit = 0, int64(1)<<62, tc.outer
		if rows := RunPipeline(s, q); len(rows) != tc.want {
			t.Errorf("outer=%d pipe=%d: got %d rows want %d", tc.outer, tc.pipe, len(rows), tc.want)
		}
	}
}

// TestFastPipesEmptyResult pins the zero-match case: `stats count()` over rows
// that do not exist produces no group, so it produces no row. Emitting a
// count-of-zero row instead put a phantom 0 into every empty bucket of a
// stats_query_range.
func TestFastPipesEmptyResult(t *testing.T) {
	s := fastPipeStore(t)
	for _, raw := range []string{
		`level:=nosuchlevel | stats count() n`,
		`level:=nosuchlevel | top 3 by (host)`,
		`level:=nosuchlevel | uniq by (host)`,
	} {
		fast := runRaw(t, s, raw)
		generic := runRawGeneric(t, s, raw)
		if len(fast) != len(generic) {
			t.Errorf("%s: fast %d rows, generic %d (fast: %s)", raw, len(fast), len(generic), dump(fast))
		}
	}
}

// TestKeepFirst covers the bitset trim that bounds a limited scan, including
// the word-crossing case where the bound falls inside a 64-row word.
func TestKeepFirst(t *testing.T) {
	for _, tc := range []struct{ n, keep int }{
		{n: 10, keep: 3}, {n: 64, keep: 64}, {n: 65, keep: 64}, {n: 200, keep: 70},
		{n: 200, keep: 0}, {n: 200, keep: 999}, {n: 129, keep: 65},
	} {
		b := NewBitset(tc.n)
		b.SetAll()
		want := tc.keep
		if want > tc.n {
			want = tc.n
		}
		b.KeepFirst(tc.keep)
		if got := b.Count(); got != want {
			t.Errorf("n=%d keep=%d: count = %d want %d", tc.n, tc.keep, got, want)
		}
		// The kept bits must be the FIRST ones: a limit returns the earliest rows.
		last := -1
		b.ForEach(func(i int) { last = i })
		if want > 0 && last != want-1 {
			t.Errorf("n=%d keep=%d: last set bit = %d want %d", tc.n, tc.keep, last, want-1)
		}
	}
	// Sparse: every third row set, keep 5 -> the first five of those.
	b := NewBitset(100)
	for i := 0; i < 100; i += 3 {
		b.Set(i)
	}
	b.KeepFirst(5)
	var got []int
	b.ForEach(func(i int) { got = append(got, i) })
	want := []int{0, 3, 6, 9, 12}
	if len(got) != len(want) {
		t.Fatalf("sparse KeepFirst = %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sparse KeepFirst = %v want %v", got, want)
		}
	}
}

// TestTimeFieldFromTimestampColumn covers _time read as a VALUE by a pipe. It
// is stored once, in the timestamp column; a pipe that names it must still see
// it, or dropping the duplicate string column would silently empty these.
func TestTimeFieldFromTimestampColumn(t *testing.T) {
	s := fastPipeStore(t)
	for _, tc := range []struct{ raw, want string }{
		{`* | fields _time, host | limit 1`, "1970-01-01T00:00:00.000000001Z"},
		{`* | limit 1 | fields _time`, "1970-01-01T00:00:00.000000001Z"},
	} {
		rows := runRaw(t, s, tc.raw)
		if len(rows) == 0 {
			t.Fatalf("%s: no rows", tc.raw)
		}
		if got := rowField(rows[0], "_time"); got != tc.want {
			t.Errorf("%s: _time = %q want %q", tc.raw, got, tc.want)
		}
	}
	// Grouping by _time sees one distinct timestamp per row, not one empty group.
	rows := runRaw(t, s, `* | stats by (_time) count() n`)
	if len(rows) != 200 {
		t.Errorf("stats by (_time) = %d groups, want 200 (one per row)", len(rows))
	}
	for _, r := range rows[:1] {
		if rowField(r, "_time") == "" {
			t.Errorf("stats by (_time) produced an empty group key: %v", r.Fields)
		}
	}
}

// TestKeepLast is the mirror of TestKeepFirst: a tail query keeps the LAST n
// set bits, and an off-by-one there returns the wrong rows silently.
func TestKeepLast(t *testing.T) {
	for _, tc := range []struct{ n, keep int }{
		{n: 10, keep: 3}, {n: 64, keep: 64}, {n: 65, keep: 64}, {n: 200, keep: 70},
		{n: 200, keep: 0}, {n: 200, keep: 999}, {n: 129, keep: 65},
	} {
		b := NewBitset(tc.n)
		b.SetAll()
		want := tc.keep
		if want > tc.n {
			want = tc.n
		}
		b.KeepLast(tc.keep)
		if got := b.Count(); got != want {
			t.Errorf("n=%d keep=%d: count = %d want %d", tc.n, tc.keep, got, want)
		}
		first := -1
		b.ForEach(func(i int) {
			if first < 0 {
				first = i
			}
		})
		if want > 0 && first != tc.n-want {
			t.Errorf("n=%d keep=%d: first set bit = %d want %d", tc.n, tc.keep, first, tc.n-want)
		}
	}
	// Sparse: every third row set, keep 3 -> the last three of those.
	b := NewBitset(100)
	for i := 0; i < 100; i += 3 {
		b.Set(i)
	}
	b.KeepLast(3)
	var got []int
	b.ForEach(func(i int) { got = append(got, i) })
	want := []int{93, 96, 99}
	if len(got) != len(want) {
		t.Fatalf("sparse KeepLast = %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sparse KeepLast = %v want %v", got, want)
		}
	}
}

// TestLastNIsNewestFirst pins the endpoint's `limit`: the most recent n rows,
// newest first. The pipe's `| limit n` keeps the FIRST n, and the two must not
// be conflated -- a viewer asking for the tail was shown the oldest rows.
func TestLastNIsNewestFirst(t *testing.T) {
	s := fastPipeStore(t)
	q, err := ParseLogsQL(`*`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To, q.LastN = 0, int64(1)<<62, 5
	rows := Run(s, q)
	if len(rows) != 5 {
		t.Fatalf("LastN=5 returned %d rows", len(rows))
	}
	// fastPipeStore timestamps run 1..200, so the newest five are 200..196.
	for i, want := range []int64{200, 199, 198, 197, 196} {
		if rows[i].Time != want {
			t.Fatalf("LastN rows = %v..., want newest first (200,199,...)", rows[0].Time)
		}
	}
	// The pipe still means the first n.
	first := runRaw(t, s, `* | limit 5`)
	if len(first) != 5 || first[0].Time != 1 {
		t.Fatalf("`| limit 5` first row time = %d, want 1 (the FIRST rows)", first[0].Time)
	}
}

// TestLastNAcrossOverlappingGroups covers the cutoff: with out-of-order arrivals
// an older group can still hold a newer row, so the scan must not stop at the
// first group that fills the bound.
func TestLastNAcrossOverlappingGroups(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// group 0 holds times 1..3 AND 100 (a late arrival); group 1 holds 4..6.
	for _, times := range [][]int64{{1, 2, 3, 100}, {4, 5, 6}} {
		vals := make([]string, len(times))
		for i := range vals {
			vals[i] = "v"
		}
		d := storage.BuildDict(vals)
		if _, err := st.AppendGroup(&storage.Group{Rows: len(times), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "f", Type: storage.ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	q, _ := ParseLogsQL(`*`)
	q.From, q.To, q.LastN = 0, int64(1)<<62, 2
	rows := Run(st, q)
	if len(rows) != 2 || rows[0].Time != 100 || rows[1].Time != 6 {
		var got []int64
		for _, r := range rows {
			got = append(got, r.Time)
		}
		t.Fatalf("LastN over overlapping groups = %v, want [100 6]", got)
	}
}
