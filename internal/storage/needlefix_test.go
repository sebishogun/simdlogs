package storage

import "testing"

// TestTimestampAtMatchesFullDecode verifies the checkpoint point read agrees
// with a full column decode at every block boundary -- the codec change that
// let a selective query materialize a match's time without decoding the whole
// timestamp column. A boundary off-by-one would show here and nowhere else.
func TestTimestampAtMatchesFullDecode(t *testing.T) {
	for _, n := range []int{1, tsBlock - 1, tsBlock, tsBlock + 1, 3*tsBlock + 7, 100_000} {
		g, _ := buildGroup(n)
		r, err := ReadGroup(g.Marshal())
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		full := r.Timestamps("_time", nil, nil)
		// Every boundary-adjacent row plus a stride sample across the column.
		probe := map[int]bool{0: true, n - 1: true}
		for b := 0; b*tsBlock < n; b++ {
			for _, d := range []int{-1, 0, 1} {
				if i := b*tsBlock + d; i >= 0 && i < n {
					probe[i] = true
				}
			}
		}
		for i := 0; i < n; i += 257 {
			probe[i] = true
		}
		for i := range probe {
			got, ok := r.TimestampAt("_time", i)
			if !ok {
				t.Fatalf("n=%d TimestampAt(%d) not ok", n, i)
			}
			if got != full[i] {
				t.Fatalf("n=%d TimestampAt(%d)=%d want %d", n, i, got, full[i])
			}
		}
		// Out-of-range is reported, not decoded.
		if _, ok := r.TimestampAt("_time", n); ok {
			t.Fatalf("n=%d TimestampAt(n) reported ok", n)
		}
	}
}

// TestTimeRangeMaskNonMonotonic verifies the per-block min/max range skip
// agrees with a brute-force scan even when timestamps are out of order within
// the group (the skip must not assume monotonicity), and that every matching
// row falls inside the reported window span.
func TestTimeRangeMaskNonMonotonic(t *testing.T) {
	n := 3*tsBlock + 100
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64((i * 2654435761) % 100000) // scattered, non-monotonic
	}
	svc := make([]string, n)
	for i := range svc {
		svc[i] = "x"
	}
	sd := BuildDict(svc)
	g := &Group{Rows: n, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "service", Type: ColDict, Dict: &sd},
	}}
	r, err := ReadGroup(g.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []struct{ from, to int64 }{
		{0, 100000}, {1000, 5000}, {50000, 50001}, {-10, 10}, {99999, 200000},
	} {
		mask := r.TimeRangeMaskInto("_time", w.from, w.to, nil)
		lo, hi := r.TimeWindowSpan("_time", w.from, w.to)
		for i := 0; i < n; i++ {
			want := ts[i] >= w.from && ts[i] < w.to
			if mask[i] != want {
				t.Fatalf("window [%d,%d) row %d ts=%d: mask=%v want=%v", w.from, w.to, i, ts[i], mask[i], want)
			}
			if want && (i < lo || i >= hi) {
				t.Fatalf("window [%d,%d): matching row %d outside span [%d,%d)", w.from, w.to, i, lo, hi)
			}
		}
	}
}

// TestPostingRowsSeek verifies the O(1) byte-offset seek returns exactly the
// rows a brute-force scan would, for every id including the last -- the id at
// the far end of the varint stream, which the previous single-table form
// reached only by walking every preceding list. Correctness must hold for the
// value whose lookup the fix was about.
func TestPostingRowsSeek(t *testing.T) {
	cases := []struct {
		indices []uint32
		dictLen int
	}{
		{[]uint32{3, 0, 3, 1, 3, 0}, 4},              // repeats, one empty id (2)
		{[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 10}, // each id once, in order
	}
	// A high-cardinality group like a trace column: many singleton lists, the
	// last id far into the stream.
	big := make([]uint32, 4096)
	for i := range big {
		big[i] = uint32(i)
	}
	cases = append(cases, struct {
		indices []uint32
		dictLen int
	}{big, 4096})

	for ci, c := range cases {
		blob := buildPostings(c.indices, c.dictLen).marshal(nil)
		for id := 0; id < c.dictLen; id++ {
			var want []uint32
			for row, v := range c.indices {
				if int(v) == id {
					want = append(want, uint32(row))
				}
			}
			got := postingRows(blob, id, len(c.indices))
			if len(got) != len(want) {
				t.Fatalf("case %d id %d: got %v want %v", ci, id, got, want)
			}
			for k := range want {
				if got[k] != want[k] {
					t.Fatalf("case %d id %d row %d: got %d want %d", ci, id, k, got[k], want[k])
				}
			}
		}
	}
}

// TestEqualityRowsAfterLayout exercises the full footer stack (PostOff/PostLen
// plus the two-table postings) through a Reader: the rows and count a dict
// value reports must match a brute-force scan of the source records.
func TestEqualityRowsAfterLayout(t *testing.T) {
	g, recs := buildGroup(20_000)
	r, err := ReadGroup(g.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	for _, val := range []string{"error", "info", "auth", "db"} {
		var want []uint32
		for i, rec := range recs {
			if rec.Level == val {
				want = append(want, uint32(i))
			}
		}
		// level column for level values, service column for service values.
		field := "level"
		if val == "auth" || val == "db" {
			field = "service"
			want = want[:0]
			for i, rec := range recs {
				if rec.Service == val {
					want = append(want, uint32(i))
				}
			}
		}
		rows, ok := r.EqualityRows(field, val)
		if !ok {
			t.Fatalf("%s=%s: EqualityRows not ok", field, val)
		}
		if len(rows) != len(want) {
			t.Fatalf("%s=%s: got %d rows want %d", field, val, len(rows), len(want))
		}
		for k := range want {
			if rows[k] != want[k] {
				t.Fatalf("%s=%s row %d: got %d want %d", field, val, k, rows[k], want[k])
			}
		}
		if _, cnt, ok := r.EqualityCount(field, val); !ok || cnt != len(want) {
			t.Fatalf("%s=%s: count %d want %d (ok=%v)", field, val, cnt, len(want), ok)
		}
	}
}
