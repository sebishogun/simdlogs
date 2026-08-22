package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// THE HISTOGRAM'S BUCKET COUNT IS THE WINDOW'S WIDTH, AND THE WIDTH IS NOT AN
// int64 SUBTRACTION.
//
// `/select/logsql/hits` has one ceiling that a caller meets: `boundRangeBuckets`
// refuses an explicit window wider than `maxHitsBuckets` (10,000) steps with a
// 413. Under a COARSE step that ceiling is passed honestly and the engine then
// rendered a hundred thousand buckets anyway, because `fillHits` computed its
// own count with an int64 subtraction that wraps:
//
//	n := int((to - start + step - 1) / step)
//	if n < 0 || n > maxHitsBuckets { n = maxHitsBuckets }   // query's 100,000
//
// `?start=1970-01-01&end=9999-01-01` resolves to [0, MaxInt64] -- the far bound
// saturates, which is entry 129/130's fix working -- so `to - start + step - 1`
// runs past MaxInt64 and comes back NEGATIVE. The wrap was then read as "no
// buckets" and replaced with the ENGINE's ceiling, a different constant in a
// different package, ten times the HTTP one. Measured on this tree at
// `step=8760h` (one year, an ordinary dashboard value):
//
//	                                     413?   timestamps rendered
//	?start=1970-01-01&end=9999-01-01      no     100000
//	  the true count for that window      --     293
//
// and `start + int64(i)*step` wraps too, so the series was not ascending
// either: past bucket 292 the timestamps run off MaxInt64 and come back at the
// far-past end, which is a dense gap-free ascending array in name only.
//
// The 1-minute default step on the same window IS a loud 413 -- 10,000 buckets
// is passed long before the width can wrap -- so this is the coarse-step path
// alone, which is why nothing saw it.
func TestAHistogramOverASaturatedWindowRendersTheBucketsItPromised(t *testing.T) {
	node := realShard(t, []string{
		`{"_time":"2026-06-01T12:00:00Z","_msg":"a"}`,
		`{"_time":"2026-06-02T12:00:00Z","_msg":"b"}`,
	})

	// The exact window: `start` is given, so this row does not move when the
	// no-parameter default changes.
	t.Run("a coarse step over the widest expressible window", func(t *testing.T) {
		code, series := getHits(t, node, "query=*&start=1970-01-01&end=9999-01-01&step=8760h")
		if code != 200 {
			t.Fatalf("%d", code)
		}
		if len(series) != 1 {
			t.Fatalf("%d series, want 1", len(series))
		}
		ts := series[0].Timestamps
		// [0, MaxInt64) at one year a bucket: ceil((2^63-1)/8760h) = 293.
		if len(ts) != 293 {
			t.Errorf("%d buckets, want 293.\n"+
				"The count is the window's width divided by the step. An int64 "+
				"`to - start + step - 1` wraps over a saturated window and the "+
				"wrap was replaced by internal/query's own 100,000 -- a ceiling "+
				"in a different package, ten times the one that answered 200.",
				len(ts))
		}
		assertAscendingInsideTheWindow(t, ts, 0, maxInt64)
	})

	// AND THE CEILING THE DOCUMENT NAMES IS THE ONE A CALLER MEETS: never
	// more than maxHitsBuckets timestamps come back from this route, at any
	// step, with or without an explicit window.
	for _, tc := range []struct {
		name, params string
	}{
		{"no start, a coarse step", "query=*&end=9999-01-01&step=8760h"},
		{"no window at all, a coarse step", "query=*&step=8760h"},
		{"an explicit far-past start", "query=*&start=1000-01-01&end=9999-01-01&step=8760h"},
		{"a step wider than the domain", "query=*&start=1000-01-01&end=9999-01-01&step=1000000h"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, series := getHits(t, node, tc.params)
			if code == http.StatusRequestEntityTooLarge {
				return // refused, which is the other allowed answer
			}
			if code != 200 {
				t.Fatalf("%d", code)
			}
			for _, se := range series {
				if len(se.Timestamps) > maxHitsBuckets {
					t.Fatalf("%d buckets came back from a request the 413 ceiling "+
						"let through; %d is the ceiling this route documents",
						len(se.Timestamps), maxHitsBuckets)
				}
				assertAscendingInsideTheWindow(t, se.Timestamps, minInt64, maxInt64)
			}
			// AND THE ROWS ARE IN IT. Every window here contains both rows,
			// and the OTHER shape of the same wrap is an answer of zero
			// buckets: `to - start + step - 1` can also wrap back POSITIVE and
			// small, which came out as n=0 and a structurally valid empty
			// histogram. `?start=1000-01-01&end=9999-01-01&step=8760h`
			// answered exactly that on this tree.
			if got := totalOf(series); got != 2 {
				t.Fatalf("the histogram totals %d rows over %d series, want 2: "+
					"the store holds two rows and every window here contains them",
					got, len(series))
			}
		})
	}

	// THE CONTROL, which both reviewers measured: the DEFAULT step over the
	// same window is a loud 413 and always was. Without it a build that
	// refused everything would satisfy every row above.
	t.Run("the default step over the same window is a 413 (control)", func(t *testing.T) {
		code, _ := getHits(t, node, "query=*&start=1970-01-01&end=9999-01-01")
		if code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%d, want 413", code)
		}
	})

	// AND THE ANSWER IS STILL RIGHT ON AN ORDINARY WINDOW: a build that
	// returned an empty series would pass every bound above.
	t.Run("an ordinary window still counts the rows (control)", func(t *testing.T) {
		code, series := getHits(t, node, "query=*&start=2026-06-01&end=2026-06-03&step=24h")
		if code != 200 {
			t.Fatalf("%d", code)
		}
		if len(series) != 1 || series[0].Total != 2 {
			t.Fatalf("series=%d total=%d, want 1 series totalling 2 rows",
				len(series), totalOf(series))
		}
	})
}

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

type hitsSeriesOut struct {
	Timestamps []int64
	Values     []int
	Total      int
}

// getHits reads /select/logsql/hits and parses the RFC3339Nano timestamps back
// into nanoseconds, which is what makes "ascending" checkable at all: the wire
// format is text and lexicographic order is not time order.
func getHits(t *testing.T, ts *httptest.Server, params string) (int, []hitsSeriesOut) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/select/logsql/hits?" + params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil
	}
	var body struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
			Values     []int    `json:"values"`
			Total      int      `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("hits body is not the documented shape: %v\n%.300s", err, raw)
	}
	out := make([]hitsSeriesOut, 0, len(body.Hits))
	for _, h := range body.Hits {
		se := hitsSeriesOut{Values: h.Values, Total: h.Total}
		for _, s := range h.Timestamps {
			tt, perr := time.Parse(time.RFC3339Nano, s)
			if perr != nil {
				t.Fatalf("bucket timestamp %q does not parse: %v", s, perr)
			}
			se.Timestamps = append(se.Timestamps, tt.UnixNano())
		}
		if len(se.Timestamps) != len(h.Values) {
			t.Fatalf("%d timestamps against %d values: a client indexes the two "+
				"arrays together", len(se.Timestamps), len(h.Values))
		}
		out = append(out, se)
	}
	return resp.StatusCode, out
}

func assertAscendingInsideTheWindow(t *testing.T, ts []int64, from, to int64) {
	t.Helper()
	for i, v := range ts {
		if v < from || v > to {
			t.Fatalf("bucket %d is %d, outside the window [%d, %d]", i, v, from, to)
		}
		if i > 0 && v <= ts[i-1] {
			t.Fatalf("bucket %d (%d) does not follow bucket %d (%d): the series "+
				"is documented dense, ASCENDING and gap-free, and `start + i*step` "+
				"wraps past MaxInt64", i, v, i-1, ts[i-1])
		}
	}
}

func totalOf(series []hitsSeriesOut) int {
	n := 0
	for _, s := range series {
		n += s.Total
	}
	return n
}

// THE HISTOGRAM MUST TOTAL WHAT THE QUERY RETURNS, INCLUDING BEFORE THE EPOCH.
//
// `histoGroup` keyed a row at `ts/step*step` and `fillHits` started at
// `from - from%step`, both of which TRUNCATE TOWARD ZERO. Bucket 0 therefore
// spanned (-step, +step) -- rows on both sides of the epoch -- while every
// other bucket spanned [k*step, (k+1)*step), and a window that ends before the
// epoch never reaches key 0 because the walk runs `t < to`. Measured on two
// rows inside one pre-epoch window, both requests at HTTP 200:
//
//	/select/logsql/hits?query=*&start=1969-12-31T00:00:00Z
//	    &end=1969-12-31T23:59:00Z&step=1h    24 buckets, TOTAL 1
//	/select/logsql/query, the same window                  2 rows
//
// The count vanished from Timestamps, Values AND Total, so nothing in the
// response said a row had been dropped. Entry 132 recorded these two sites as
// truncating "the same way, so no count is lost to a mismatch": they do
// truncate the same way and the count is lost anyway, because the mismatch is
// between the KEY and the WINDOW rather than between the two sites.
//
// The rows straddle the epoch so a build that fixed one side and not the other
// is visible, and the post-epoch window is the control -- for t >= 0 the floor
// and the truncation are the same value and always were.
func TestAHistogramTotalsEveryRowTheQueryReturns(t *testing.T) {
	node := realShard(t, []string{
		`{"_time":"1969-12-31T22:30:00Z","_msg":"before, unaligned"}`,
		`{"_time":"1969-12-31T23:30:00Z","_msg":"before, in the bucket that keyed to 0"}`,
		`{"_time":"1970-01-01T00:30:00Z","_msg":"after, unaligned"}`,
		`{"_time":"1970-01-01T02:00:00Z","_msg":"after, aligned"}`,
	})

	for _, tc := range []struct {
		name, window string
		want         int
	}{
		{
			"a window that ends before the epoch",
			"start=1969-12-31T00:00:00Z&end=1969-12-31T23:59:00Z",
			2,
		},
		{
			"a window that straddles the epoch",
			"start=1969-12-31T00:00:00Z&end=1970-01-01T03:00:00Z",
			4,
		},
		{
			"a window that starts at an unaligned pre-epoch instant",
			"start=1969-12-31T22:15:00Z&end=1969-12-31T23:59:00Z",
			2,
		},
		// The control: entirely after the epoch, where truncation and floor
		// are the same value. A build that broke the ordinary case would pass
		// every row above.
		{
			"an ordinary post-epoch window (control)",
			"start=1970-01-01T00:00:00Z&end=1970-01-01T03:00:00Z",
			2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, series := getHits(t, node, "query=*&"+tc.window+"&step=1h")
			if code != 200 {
				t.Fatalf("hits answered %d", code)
			}
			rows := queryRowCount(t, node, "query=*&"+tc.window)
			if rows != tc.want {
				t.Fatalf("/select/logsql/query returned %d rows, want %d: the fixture "+
					"does not hold what this test is about", rows, tc.want)
			}
			if got := totalOf(series); got != tc.want {
				t.Errorf("the histogram totals %d and the query returns %d over the same "+
					"window. A row whose bucket key falls outside the walk is dropped "+
					"from Timestamps, Values AND Total, at HTTP 200.", got, rows)
			}
			// The per-bucket values must add up to the reported total, or a
			// build could report the right total from the wrong buckets.
			sum := 0
			for _, se := range series {
				for _, v := range se.Values {
					sum += v
				}
			}
			if sum != tc.want {
				t.Errorf("the bucket values add to %d, want %d", sum, tc.want)
			}
		})
	}
}

// queryRowCount is how many rows /select/logsql/query returns: the reference
// the histogram is compared against, because it is the same scan without the
// bucketing.
func queryRowCount(t *testing.T, ts *httptest.Server, params string) int {
	t.Helper()
	resp, err := http.Get(ts.URL + "/select/logsql/query?" + params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("query answered %d: %.200s", resp.StatusCode, raw)
	}
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}
