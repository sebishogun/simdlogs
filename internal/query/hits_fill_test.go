package query

import (
	"math"
	"testing"
	"time"
)

// THE HISTOGRAM'S BUCKET COUNT, GATED IN THE PACKAGE THAT COMPUTES IT.
//
// `fillHits` derived its count with `int((to - start + step - 1) / step)`,
// which wraps over a window whose bounds saturate -- and a saturated window is
// what `?end=9999-01-01` produces, because a bound past 2262 is +infinity and
// MaxInt64 is how this tree spells it. The wrap went both ways and gave two
// different wrong answers, neither of them an error:
//
//	window                 step     n before   n true
//	[0, MaxInt64)          8760h      100000      293   negative wrap, then
//	                                                    replaced by the
//	                                                    package's own 100,000
//	[MinInt64, MaxInt64)   8760h           0      585   positive wrap, small
//
// The 100,000 is this package's ceiling on `Hits`, ten times `internal/api`'s
// 10,000 -- and 10,000 is the one the HTTP 413 enforces and the one a caller
// is told about, so the first row is a route answering 200 with ten times the
// buckets its own refusal threshold allows.
//
// `internal/api` has the end-to-end gate. This one is here because the
// arithmetic is here: the clamp entry 131 records was fixed in this package
// and gated only from the API's suite, and a defect in one package that only a
// different package's tests can see is a defect nobody's `go test ./internal/query`
// will catch.
func TestFillHitsCountsTheWindowAndNeverLeavesIt(t *testing.T) {
	const year = int64(8760 * time.Hour)
	for _, tc := range []struct {
		name      string
		from, to  int64
		step      int64
		wantCount int
	}{
		{"a saturated end from the epoch", 0, math.MaxInt64, year, 293},
		{"both bounds saturated", math.MinInt64, math.MaxInt64, year, 585},
		{"a saturated end from a representable start", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), math.MaxInt64, year, 237},
		// Controls: ordinary windows, where the subtraction never wrapped and
		// the answer was always right.
		{"an ordinary day at an hour a bucket", 0, int64(24 * time.Hour), int64(time.Hour), 24},
		{"a window narrower than one step", 0, 1, year, 1},
		{"an empty window", 5, 5, year, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &Query{From: tc.from, To: tc.to, ToSet: true}
			hs := fillHits(map[int64]int{}, q, tc.step, map[string]string{})
			if len(hs.Timestamps) != tc.wantCount {
				t.Fatalf("%d buckets, want %d", len(hs.Timestamps), tc.wantCount)
			}
			if len(hs.Values) != len(hs.Timestamps) {
				t.Fatalf("%d values against %d timestamps: a client indexes the "+
					"two arrays together", len(hs.Values), len(hs.Timestamps))
			}
			for i, v := range hs.Timestamps {
				if v >= tc.to {
					t.Fatalf("bucket %d is %d, at or past the window's end %d",
						i, v, tc.to)
				}
				if i > 0 && v <= hs.Timestamps[i-1] {
					t.Fatalf("bucket %d (%d) does not follow bucket %d (%d): the "+
						"series is documented dense, ASCENDING and gap-free",
						i, v, i-1, hs.Timestamps[i-1])
				}
			}
		})
	}
}

// AND THE COUNTS ARE THE ONES THE BUCKETS ARE KEYED WITH.
//
// A series whose timestamps are right and whose lookups miss is an all-zero
// histogram at 200, which is what an alignment change would produce. histoGroup
// keys a row with `alignDown`; fillHits starts at `alignDown(from, step)` and
// walks by step, so the two agree by construction -- this pins it.
//
// The keys are built by CALLING alignDown rather than by repeating its
// arithmetic here: a test that hand-copies the loop it is measuring drifts
// away from the tree the moment the tree changes, and the truncating version
// of this line is exactly what let a pre-epoch count be lost with this test
// green.
func TestFillHitsReadsTheBucketsHistogramWrote(t *testing.T) {
	const step = int64(time.Hour)
	// Three rows, two in one bucket: the keys histoGroup would produce.
	rows := []int64{
		time.Date(2026, 6, 1, 12, 5, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 6, 1, 12, 40, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 6, 1, 14, 1, 0, 0, time.UTC).UnixNano(),
	}
	buckets := map[int64]int{}
	for _, ts := range rows {
		buckets[alignDown(ts, step)]++
	}
	q := &Query{
		From:  time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
		To:    time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC).UnixNano(),
		ToSet: true,
	}
	hs := fillHits(buckets, q, step, map[string]string{})
	if got := []int{2, 0, 1}; len(hs.Values) != 3 ||
		hs.Values[0] != got[0] || hs.Values[1] != got[1] || hs.Values[2] != got[2] {
		t.Fatalf("values %v, want %v", hs.Values, got)
	}
	if hs.Total != 3 {
		t.Fatalf("total %d, want 3", hs.Total)
	}
}

// A PRE-EPOCH BUCKET KEY MUST BE INSIDE THE WALK.
//
// `ts/step*step` and `from - from%step` both truncate TOWARD ZERO, so the
// bucket keyed 0 spanned (-step, +step) -- rows from both sides of the epoch --
// while every other bucket spanned [k*step, (k+1)*step). A window that ends
// before the epoch never reaches key 0, because the walk runs `t < to`: the
// row at -1800e9 keyed to 0, the walk stopped at -60e9, and the count vanished
// from Timestamps, Values AND Total at HTTP 200.
//
// The alignment rows are the whole property. Nothing else in this package
// distinguishes a floor from a truncation, because for t >= 0 they are the
// same value.
func TestBucketKeysFloorAndTheWalkFindsThem(t *testing.T) {
	const hour = int64(time.Hour)
	t.Run("alignDown floors", func(t *testing.T) {
		for _, tc := range []struct{ t, step, want int64 }{
			{-1800e9, hour, -3600e9},
			{-3600e9, hour, -3600e9},
			{-3599999999999, hour, -3600e9},
			{-1, hour, -3600e9},
			{0, hour, 0},
			{1, hour, 0},
			{3600e9, hour, 3600e9},
			{5400e9, hour, 3600e9},
			// The bottom of the domain has no aligned bucket below it, so
			// both callers clamp to the smallest multiple int64 can hold --
			// which is what the truncating form produced there too.
			{math.MinInt64, hour, math.MinInt64 / hour * hour},
			{math.MinInt64 + 1, hour, math.MinInt64 / hour * hour},
		} {
			if got := alignDown(tc.t, tc.step); got != tc.want {
				t.Errorf("alignDown(%d, %d) = %d, want %d", tc.t, tc.step, got, tc.want)
			}
			if got := alignDown(tc.t, tc.step); got > tc.t && tc.t > math.MinInt64/tc.step*tc.step {
				t.Errorf("alignDown(%d) = %d, which is AFTER the instant it buckets", tc.t, got)
			}
		}
	})

	t.Run("a pre-epoch window loses no count", func(t *testing.T) {
		rows := []int64{-5400e9, -1800e9} // 22:30 and 23:30 on 1969-12-31
		buckets := map[int64]int{}
		for _, ts := range rows {
			buckets[alignDown(ts, hour)]++
		}
		q := &Query{From: -86400e9, To: -60e9, ToSet: true}
		hs := fillHits(buckets, q, hour, map[string]string{})
		if hs.Total != len(rows) {
			t.Fatalf("total %d over %d rows inside the window: keys %v against buckets %v",
				hs.Total, len(rows), buckets, hs.Timestamps)
		}
		for k := range buckets {
			found := false
			for _, ts := range hs.Timestamps {
				if ts == k {
					found = true
				}
			}
			if !found {
				t.Errorf("bucket key %d was written by histoGroup and is not in the walk", k)
			}
		}
	})

	t.Run("a window straddling the epoch loses no count", func(t *testing.T) {
		rows := []int64{-1800e9, 1800e9, 5400e9}
		buckets := map[int64]int{}
		for _, ts := range rows {
			buckets[alignDown(ts, hour)]++
		}
		q := &Query{From: -3600e9, To: 7200e9, ToSet: true}
		hs := fillHits(buckets, q, hour, map[string]string{})
		if hs.Total != len(rows) {
			t.Fatalf("total %d over %d rows: buckets %v, walk %v",
				hs.Total, len(rows), buckets, hs.Timestamps)
		}
	})
}
