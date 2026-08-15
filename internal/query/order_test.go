package query

import (
	"fmt"
	"math"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The three shapes that make "time order" ambiguous, pinned.
//
// Equal timestamps, groups whose time ranges overlap, and out-of-order ingest
// each leave the engine's row order to how the scan happens to walk. That is
// harmless until a caller pages through it, at which point an unstable order
// silently skips rows and repeats others. These tests pin the answer to what
// the engine does today, so a future change to the walk fails here rather than
// in someone's pagination.

// orderStore appends one group per `times` slice, in the order given, so a
// test can build overlapping and out-of-order groups directly.
func orderStore(t *testing.T, groups ...[]int64) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	for gi, times := range groups {
		msgs := make([]string, len(times))
		for i, ts := range times {
			msgs[i] = fmt.Sprintf("g%d-i%d-t%d", gi, i, ts)
		}
		d := storage.BuildDict(msgs)
		if _, err := s.AppendGroup(&storage.Group{Rows: len(times), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "_msg", Type: storage.ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func pageQuery(t *testing.T) *Query {
	t.Helper()
	q, err := ParseLogsQL("*")
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, math.MaxInt64
	q.MatAll = true
	return q
}

func msgsOf(rows []Row) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, rowField(r, "_msg"))
	}
	return out
}

// The tuple is a total order: no two distinct rows compare equal, in any of
// the ambiguous shapes.
func TestTheRowKeyIsATotalOrder(t *testing.T) {
	s := orderStore(t,
		[]int64{5, 5, 5}, // equal timestamps within a group
		[]int64{3, 7},    // overlaps the first group's range
		[]int64{9, 1, 5}, // out-of-order ingest, and a duplicate of 5
		[]int64{5},       // one more 5, in its own group
	)
	page, err := ScanPage(s, pageQuery(t), nil, Oldest, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 9 {
		t.Fatalf("%d keys, want 9", len(page.Keys))
	}
	seen := map[RowKey]bool{}
	for i, k := range page.Keys {
		if seen[k] {
			t.Fatalf("duplicate key %+v at %d", k, i)
		}
		seen[k] = true
		if i > 0 && !page.Keys[i-1].Before(k) {
			t.Fatalf("keys %d and %d are not strictly ordered: %+v then %+v",
				i-1, i, page.Keys[i-1], k)
		}
	}
}

// Paging returns every row exactly once, in both directions, at page sizes
// that do and do not divide the row count -- including a size of 1, where
// every boundary is a cursor boundary.
func TestPagingReturnsEveryRowExactlyOnce(t *testing.T) {
	s := orderStore(t,
		[]int64{5, 5, 5, 5},
		[]int64{3, 7, 7},
		[]int64{9, 1, 5},
		[]int64{2, 8},
	)
	const total = 12

	for _, dir := range []Direction{Oldest, Newest} {
		for _, size := range []int{1, 2, 5, 7, 12, 100} {
			t.Run(fmt.Sprintf("%s/size=%d", dir, size), func(t *testing.T) {
				var got []string
				seen := map[RowKey]bool{}
				var after *RowKey
				for pages := 0; ; pages++ {
					if pages > total+2 {
						t.Fatalf("paging did not terminate after %d pages", pages)
					}
					p, err := ScanPage(s, pageQuery(t), after, dir, size)
					if err != nil {
						t.Fatal(err)
					}
					for i, k := range p.Keys {
						if seen[k] {
							t.Fatalf("row %+v returned twice", k)
						}
						seen[k] = true
						got = append(got, rowField(p.Rows[i], "_msg"))
					}
					if !p.More {
						break
					}
					next := p.Next
					after = &next
				}
				if len(got) != total {
					t.Fatalf("%d rows across all pages, want %d", len(got), total)
				}
				// And the concatenation equals the unpaginated answer, so
				// paging is a way of reading the same order rather than an
				// order of its own.
				whole, err := ScanPage(s, pageQuery(t), nil, dir, 1000)
				if err != nil {
					t.Fatal(err)
				}
				want := msgsOf(whole.Rows)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("row %d differs: paged %q, whole %q", i, got[i], want[i])
					}
				}
			})
		}
	}
}

// Newest-first is the exact reverse of oldest-first. They are separate code
// paths through the same comparison, so the property is worth asserting rather
// than assuming.
func TestNewestIsTheReverseOfOldest(t *testing.T) {
	s := orderStore(t, []int64{5, 5, 3}, []int64{9, 1}, []int64{5, 7})
	oldest, err := ScanPage(s, pageQuery(t), nil, Oldest, 1000)
	if err != nil {
		t.Fatal(err)
	}
	newest, err := ScanPage(s, pageQuery(t), nil, Newest, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldest.Rows) != len(newest.Rows) || len(oldest.Rows) != 7 {
		t.Fatalf("%d oldest, %d newest, want 7 each", len(oldest.Rows), len(newest.Rows))
	}
	o, n := msgsOf(oldest.Rows), msgsOf(newest.Rows)
	for i := range o {
		if o[i] != n[len(n)-1-i] {
			t.Fatalf("position %d: oldest %q, newest reversed %q", i, o[i], n[len(n)-1-i])
		}
	}
}

// Rows appended after the first page are not in it. The snapshot is what makes
// "exactly once" true: a walk that saw new rows mid-page would return rows
// whose position in the order is before the cursor, and no cursor can express
// "I have seen these but not those".
func TestAppendsDuringAWalkAreNotInIt(t *testing.T) {
	s := orderStore(t, []int64{1, 2, 3}, []int64{4, 5, 6})

	p1, err := ScanPage(s, pageQuery(t), nil, Oldest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !p1.More {
		t.Fatal("the first page should not be the last")
	}

	// A new group whose rows sort BEFORE the cursor -- the case that would
	// corrupt a timestamp-only walk.
	d := storage.BuildDict([]string{"late-arrival"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 1, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1}},
		{Name: "_msg", Type: storage.ColDict, Dict: &d},
	}}); err != nil {
		t.Fatal(err)
	}

	next := p1.Next
	var got []string
	after := &next
	for {
		p, err := ScanPage(s, pageQuery(t), after, Oldest, 2)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, msgsOf(p.Rows)...)
		if !p.More {
			break
		}
		n := p.Next
		after = &n
	}
	// The late arrival sorts at time 1, before the cursor, so it is not in any
	// later page. Its group id is higher than every earlier group's, which is
	// what keeps the tuple ordered rather than merely the timestamp.
	for _, m := range got {
		if m == "late-arrival" {
			t.Fatal("a row appended mid-walk appeared after the cursor")
		}
	}
	if len(got) != 4 {
		t.Fatalf("%d rows after the first page, want 4: %v", len(got), got)
	}
}

// A page size that exactly equals the row count reports no more, so a caller
// does not make one extra empty request per walk.
func TestAnExactlyFullPageIsTheLastOne(t *testing.T) {
	s := orderStore(t, []int64{1, 2, 3})
	p, err := ScanPage(s, pageQuery(t), nil, Oldest, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rows) != 3 {
		t.Fatalf("%d rows", len(p.Rows))
	}
	if p.More {
		t.Error("a page holding every row reports More")
	}
	if p.Next != (RowKey{}) {
		t.Errorf("a last page handed back a cursor: %+v", p.Next)
	}
}

// A piped query cannot be paginated by row: the pipe defines its own output
// order, and a cursor into it would name a row the next page's pipe does not
// produce.
func TestPipedQueriesCannotBePaginated(t *testing.T) {
	s := orderStore(t, []int64{1, 2, 3})
	q, err := ParseLogsQL("* | stats count() n")
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, math.MaxInt64
	if _, err := ScanPage(s, q, nil, Oldest, 10); err == nil {
		t.Fatal("a stats query was paginated")
	}
	if _, err := ScanPage(s, pageQuery(t), nil, Oldest, 0); err == nil {
		t.Fatal("a zero page size was accepted")
	}
}
