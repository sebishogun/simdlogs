package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The A/B for pooling the per-group timestamp decode. Both arms are in this
// one binary and run interleaved in one session, because a two-build
// comparison would put the 8.3% code-layout noise floor between them.

// poisonTsPool fills the pool with buffers whose every element is a timestamp
// that cannot occur, so a decode that fails to write an element returns the
// poison rather than a plausible time. The buffers are Put back, which is what
// the next getTs draws from.
func poisonTsPool(n, size int) {
	ps := make([]*[]int64, n)
	for i := range ps {
		s := make([]int64, size)
		for j := range s {
			s[j] = -0x0DEADBEEF
		}
		ps[i] = &s
	}
	for _, p := range ps {
		tsScratch.Put(p)
	}
}

// TestPooledTimestampsMatchUnpooled is the correctness gate on the pool: the
// times a query reports with a poisoned pool must equal, exactly, the times it
// reports when every decode allocates a fresh slice.
func TestPooledTimestampsMatchUnpooled(t *testing.T) {
	s, lo, hi := mergeStore(t, 6, 20_000)
	defer func() { poolTs = true }()
	for _, q := range []*Query{
		{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}},
		{From: lo, To: hi + 1},
		{From: lo + (hi-lo)/3, To: hi - (hi-lo)/3},
		{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "nosuchservice"}}},
	} {
		poolTs = false
		want := Run(s, q)
		poolTs = true
		poisonTsPool(64, 40_000)
		got := Run(s, q)
		if len(got) != len(want) {
			t.Fatalf("pooled %d rows, unpooled %d rows", len(got), len(want))
		}
		for i := range want {
			if got[i].Time != want[i].Time {
				t.Fatalf("row %d: pooled time %d, unpooled %d", i, got[i].Time, want[i].Time)
			}
		}

		// The histogram reads the same buffer through a different call site.
		step := (hi - lo) / 17
		if step <= 0 {
			step = 1
		}
		poolTs = false
		wantH := Histogram(s, q, step)
		poolTs = true
		poisonTsPool(64, 40_000)
		gotH := Histogram(s, q, step)
		if len(gotH) != len(wantH) {
			t.Fatalf("pooled histogram %d buckets, unpooled %d", len(gotH), len(wantH))
		}
		for k, v := range wantH {
			if gotH[k] != v {
				t.Fatalf("bucket %d: pooled %d, unpooled %d", k, gotH[k], v)
			}
		}
	}
}

// TestTimestampsRangeIntoShortStream drives the tail the decoder does not
// reach -- a truncated timestamp column -- with a dirty buffer. That tail was
// zero when every call allocated, and a pooled buffer must not date the rows
// with the previous group's times.
func TestTimestampsRangeIntoShortStream(t *testing.T) {
	rows := 4096
	ts := make([]int64, rows)
	base := int64(1_700_000_000_000_000_000)
	for i := range ts {
		ts[i] = base + int64(i)*1000
	}
	g := &storage.Group{Rows: rows, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
	}}
	blob := g.Marshal()
	for _, cut := range []int{0, 1, 64, 512, 4096} {
		if cut >= len(blob) {
			continue
		}
		r, err := storage.ReadGroup(blob[:len(blob)-cut])
		if err != nil {
			continue // a cut that destroys the footer is not this test's case
		}
		want := r.TimestampsRange("_time", 0, rows)
		dirty := make([]int64, rows)
		for i := range dirty {
			dirty[i] = -0x0DEADBEEF
		}
		got := r.TimestampsRangeInto(dirty[:0], "_time", 0, rows)
		if len(got) != len(want) {
			t.Fatalf("cut=%d: into len %d, fresh len %d", cut, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("cut=%d: element %d: into %d, fresh %d", cut, i, got[i], want[i])
			}
		}
	}
}

func BenchmarkTsScratch(b *testing.B) {
	s, lo, hi := mergeStore(b, 10, 100_000)
	shapes := []struct {
		name string
		q    *Query
	}{
		{"full-scan", &Query{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}},
		{"windowed", &Query{From: lo + (hi-lo)/2, To: lo + (hi-lo)/2 + (hi-lo)/50}},
	}
	// Arms interleaved per shape, so any drift over the run hits both.
	for _, sh := range shapes {
		for _, arm := range []struct {
			name string
			pool bool
		}{{"pooled", true}, {"make", false}} {
			b.Run(sh.name+"/"+arm.name, func(b *testing.B) {
				poolTs = arm.pool
				defer func() { poolTs = true }()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					sinkRows = Run(s, sh.q)
				}
			})
		}
	}
}
