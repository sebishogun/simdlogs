package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// iterStore builds a store with `groups` groups of `perGroup` rows, each row
// carrying a level and a body, with timestamps strictly increasing across the
// whole store so group order is time order.
func iterStore(t *testing.T, groups, perGroup int) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ts := int64(1)
	for g := 0; g < groups; g++ {
		times := make([]int64, perGroup)
		msgs := make([]string, perGroup)
		levels := make([]string, perGroup)
		for i := range times {
			times[i] = ts
			msgs[i] = fmt.Sprintf("group %d row %d body", g, i)
			levels[i] = []string{"info", "warn", "error"}[(g+i)%3]
			ts++
		}
		md := storage.BuildDict(msgs)
		ld := storage.BuildDict(levels)
		if _, err := s.AppendGroup(&storage.Group{Rows: perGroup, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "_msg", Type: storage.ColDict, Dict: &md},
			{Name: "level", Type: storage.ColDict, Dict: &ld},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// render is the deterministic form both paths are compared in. Rows carry
// slices into mapped memory, so comparing them as values would compare
// pointers; the wire form is what a client actually receives and what has to
// match.
// iterQuery parses q and opens its time window. From/To default to zero and
// TimeRangeMatches is `TimeMin < to`, so an unset window matches no group at
// all -- the HTTP layer fills one in and a package-level test has to too.
func iterQuery(t *testing.T, src string) *Query {
	t.Helper()
	q, err := ParseLogsQL(src)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, math.MaxInt64
	q.MatAll = true
	return q
}

func render(rows []Row) string {
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%d", r.Time)
		for _, f := range r.Fields {
			fmt.Fprintf(&b, "\t%s=%s", f.Key, f.Value)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func collect(t *testing.T, s Store, q *Query) string {
	t.Helper()
	var b strings.Builder
	if err := ScanEach(s, q, func(rows []Row) error {
		b.WriteString(render(rows))
		return nil
	}); err != nil {
		t.Fatalf("ScanEach: %v", err)
	}
	return b.String()
}

// The streamed answer and the materialized one are byte-for-byte the same.
//
// Both fan-out regimes, because they are different code: below
// parallelMinGroups Run walks serially and ScanEach walks serially; above it
// Run merges worker partials in group order and ScanEach delivers slots in
// group order. Two independent ways of preserving the same order, so the
// equivalence is worth asserting rather than assuming.
func TestStreamedAndMaterializedAnswersAreIdentical(t *testing.T) {
	for _, tc := range []struct{ groups, perGroup int }{
		{1, 10},
		{3, 7},   // serial in both paths
		{4, 5},   // exactly at parallelMinGroups
		{9, 11},  // pipelined
		{32, 64}, // more groups than workers
	} {
		t.Run(fmt.Sprintf("%dx%d", tc.groups, tc.perGroup), func(t *testing.T) {
			s := iterStore(t, tc.groups, tc.perGroup)
			for _, qs := range []string{"*", "level:error", "body"} {
				want := render(Run(s, iterQuery(t, qs)))
				got := collect(t, s, iterQuery(t, qs))

				if got != want {
					t.Fatalf("query %q, %d groups: streamed answer differs\nstreamed:\n%s\nmaterialized:\n%s",
						qs, tc.groups, got, want)
				}
				if want == "" {
					t.Fatalf("query %q matched nothing; the comparison is vacuous", qs)
				}
			}
		})
	}
}

// The whole point: more rows than any materialization budget would allow,
// consumed through a sink, with the sink never holding more than one group's
// worth. A materialized run of the same query would have every row in memory
// at once before the first one could be delivered.
func TestAStreamDeliversMoreRowsThanItEverHolds(t *testing.T) {
	const groups, perGroup = 40, 500
	s := iterStore(t, groups, perGroup)
	q := iterQuery(t, "*")

	seen, maxBatch := 0, 0
	if err := ScanEach(s, q, func(rows []Row) error {
		seen += len(rows)
		if len(rows) > maxBatch {
			maxBatch = len(rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != groups*perGroup {
		t.Fatalf("%d rows delivered, want %d", seen, groups*perGroup)
	}
	if maxBatch > perGroup {
		t.Fatalf("a batch held %d rows; one group is %d", maxBatch, perGroup)
	}
}

// A sink that fails ends the scan, and its error comes back unchanged -- so a
// hung-up client stops the query rather than filling a buffer nobody reads.
func TestASinkErrorStopsTheScan(t *testing.T) {
	s := iterStore(t, 12, 50)
	q := iterQuery(t, "*")

	boom := errors.New("client went away")
	calls := 0
	err := ScanEach(s, q, func(rows []Row) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("ScanEach = %v, want the sink's own error", err)
	}
	if calls != 1 {
		t.Errorf("the sink was called %d times after failing on the first", calls)
	}
}

// Cancellation and the byte budget stop a stream, and the stop is reported as
// its typed error rather than as a short answer.
func TestAStreamStopsOnCancellationAndOnBudget(t *testing.T) {
	s := iterStore(t, 20, 200)

	t.Run("canceled", func(t *testing.T) {
		q := iterQuery(t, "*")
		ctx, cancel := context.WithCancel(context.Background())
		q.Bind(ctx, Limits{})
		cancel()
		err := ScanEach(s, q, func(rows []Row) error { return nil })
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("ScanEach = %v, want ErrCanceled", err)
		}
	})

	t.Run("byte budget", func(t *testing.T) {
		q := iterQuery(t, "*")
		q.Bind(context.Background(), Limits{MaxBytes: 1})
		var seen int
		err := ScanEach(s, q, func(rows []Row) error { seen += len(rows); return nil })
		if !errors.Is(err, ErrByteLimit) {
			t.Fatalf("ScanEach = %v, want ErrByteLimit", err)
		}
		if seen == 20*200 {
			t.Error("the budget stopped nothing: every row was delivered")
		}
	})
}

// Streamable is the whole streaming decision, so what it refuses matters as
// much as what it allows.
func TestStreamableRefusesWhatCannotStream(t *testing.T) {
	piped := iterQuery(t, "* | stats count() n")
	bare := iterQuery(t, "*")
	lastN := iterQuery(t, "*")
	lastN.LastN = 5
	capped := iterQuery(t, "*")
	capped.MaxRows = 100

	for _, tc := range []struct {
		name string
		q    *Query
		want bool
	}{
		{"bare select", bare, true},
		{"with a stats pipe", piped, false},
		{"newest-n", lastN, false},
		{"row cap in force", capped, false},
		{"nil", nil, false},
	} {
		if got := Streamable(tc.q); got != tc.want {
			t.Errorf("Streamable(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
	// And ScanEach refuses rather than answering one of them wrongly.
	if err := ScanEach(iterStore(t, 2, 2), piped, func([]Row) error { return nil }); !errors.Is(err, ErrRejected) {
		t.Errorf("ScanEach on a piped query = %v, want ErrRejected", err)
	}
}

// Streaming allocates less than materializing the same answer.
//
// Not "allocates nothing": appendMatches builds each row's Fields slice, and
// that cost is identical on both paths and scales with rows either way. What
// streaming removes is the []Row that grows to hold the WHOLE result -- the
// serial walk reuses one group-sized backing array, so the difference is the
// answer's size minus one group's.
//
// A regression guard, not a CI gate: it asserts a direction, not a number.
func TestStreamingAllocatesLessThanMaterializing(t *testing.T) {
	// Three groups: below parallelMinGroups, so both paths walk serially and
	// the only difference measured is the buffer. The pipelined path keeps a
	// window of groups in flight and cannot reuse one buffer across them --
	// a deliberate trade for keeping Run's fan-out, and not what this covers.
	const groups, perGroup = 3, 4000
	s := iterStore(t, groups, perGroup)

	measure := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}

	mat := measure(func() {
		if n := len(Run(s, iterQuery(t, "*"))); n != groups*perGroup {
			t.Fatalf("%d rows", n)
		}
	})
	str := measure(func() {
		n := 0
		if err := ScanEach(s, iterQuery(t, "*"), func(rows []Row) error {
			n += len(rows)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n != groups*perGroup {
			t.Fatalf("%d rows", n)
		}
	})
	if str >= mat {
		t.Errorf("streamed allocated %d bytes, materialized %d; streaming should hold "+
			"one group rather than the whole answer", str, mat)
	}
	t.Logf("materialized %d B, streamed %d B for %d rows", mat, str, groups*perGroup)
}

func BenchmarkStreamedVsMaterialized(b *testing.B) {
	t := &testing.T{}
	s := iterStore(t, 16, 500)
	q := iterQuery(t, "*")

	b.Run("materialized", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows := Run(s, q)
			if len(rows) == 0 {
				b.Fatal("no rows")
			}
		}
	})
	b.Run("streamed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := ScanEach(s, q, func(rows []Row) error { n += len(rows); return nil }); err != nil {
				b.Fatal(err)
			}
			if n == 0 {
				b.Fatal("no rows")
			}
		}
	})
}
