package storage

import (
	"strconv"
	"testing"
)

// The A/B for ValueCountsInto: the streamed dictWalk writing into a
// caller-owned buffer, against the materialized []string into a fresh slice.
// Both arms are in this one binary and run interleaved in one session, because
// a two-build comparison would put the 8.3% code-layout noise floor between
// them.

func vcTestReader(t testing.TB, rows, card int) *Reader {
	vals := make([]string, rows)
	for i := range vals {
		vals[i] = "v" + strconv.Itoa(i%card)
	}
	d := BuildDict(vals)
	g := &Group{Rows: rows, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: make([]int64, rows)},
		{Name: "c", Type: ColDict, Dict: &d},
	}}
	r, err := ReadGroup(g.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestValueCountsIntoMatchesMaterialized is the correctness gate on the reused
// buffer. The buffer is POISONED before every call with entries that cannot
// occur, so an element the fused path fails to write shows up as the poison
// rather than as a plausible neighbouring value -- the failure mode a reused
// buffer has and a fresh allocation does not.
func TestValueCountsIntoMatchesMaterialized(t *testing.T) {
	defer func() { vcFused = true }()
	poison := func(n int) []ValueCount {
		b := make([]ValueCount, n)
		for i := range b {
			b[i] = ValueCount{Value: "\xde\xad\xbe\xefPOISON", Count: -0x0DEFACED}
		}
		return b
	}
	for _, card := range []int{1, 2, 8, 64, 65, 127, 1000, 4096} {
		rows := 8192
		if card > rows {
			rows = card
		}
		r := vcTestReader(t, rows, card)

		vcFused = false
		want := r.ValueCountsInto(nil, "c")
		if len(want) != card {
			t.Fatalf("card=%d: reference returned %d values", card, len(want))
		}

		// Fresh buffer, oversized dirty buffer, and undersized dirty buffer:
		// the three shapes a reused buffer arrives in.
		for _, dst := range [][]ValueCount{
			nil,
			poison(card * 2)[:0],
			poison(card / 2)[:0],
			poison(card)[:0],
		} {
			vcFused = true
			got := r.ValueCountsInto(dst, "c")
			if len(got) != len(want) {
				t.Fatalf("card=%d cap=%d: fused len %d, reference len %d", card, cap(dst), len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("card=%d cap=%d: entry %d: fused %+v, reference %+v", card, cap(dst), i, got[i], want[i])
				}
			}
		}

		// A column that is not there must hand the buffer back, not nil, so a
		// loop over groups keeps it.
		buf := poison(card)[:0]
		if got := r.ValueCountsInto(buf, "nosuchcolumn"); len(got) != 0 || cap(got) != cap(buf) {
			t.Fatalf("missing column: len %d cap %d, want len 0 cap %d", len(got), cap(got), cap(buf))
		}
		if got := r.ValueCountsInto(nil, "nosuchcolumn"); got != nil {
			t.Fatalf("missing column with nil buffer: got %v, want nil", got)
		}
		if got := r.ValueCounts("nosuchcolumn"); got != nil {
			t.Fatalf("ValueCounts of a missing column: got %v, want nil", got)
		}
	}
}

// TestValueCountsIntoReusedAcrossGroups is the caller's pattern: one buffer,
// many groups of different cardinality, each answer read before the next
// overwrites it. A stale entry surviving from a longer previous group is
// exactly what this catches.
func TestValueCountsIntoReusedAcrossGroups(t *testing.T) {
	cards := []int{4096, 3, 700, 1, 64, 65}
	readers := make([]*Reader, len(cards))
	for i, c := range cards {
		readers[i] = vcTestReader(t, 8192, c)
	}
	var buf []ValueCount
	for i, r := range readers {
		buf = r.ValueCountsInto(buf, "c")
		want := r.ValueCounts("c") // fresh slice, same arm
		if len(buf) != len(want) {
			t.Fatalf("group %d (card %d): reused len %d, fresh len %d", i, cards[i], len(buf), len(want))
		}
		for j := range want {
			if buf[j] != want[j] {
				t.Fatalf("group %d (card %d): entry %d: reused %+v, fresh %+v", i, cards[i], j, buf[j], want[j])
			}
		}
	}
}

func BenchmarkValueCountsInto(b *testing.B) {
	const rows = 128 * 1024
	for _, card := range []int{8, 1000, rows} {
		r := vcTestReader(b, rows, card)
		// Arms interleaved per shape, so any drift over the run hits both.
		for _, arm := range []struct {
			name  string
			fused bool
			reuse bool
		}{{"fused-reuse", true, true}, {"fused-fresh", true, false}, {"materialized", false, false}} {
			b.Run("card="+strconv.Itoa(card)+"/"+arm.name, func(b *testing.B) {
				vcFused = arm.fused
				defer func() { vcFused = true }()
				var buf []ValueCount
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if arm.reuse {
						buf = r.ValueCountsInto(buf, "c")
					} else {
						buf = r.ValueCountsInto(nil, "c")
					}
				}
				sinkVC = buf
			})
		}
	}
}
