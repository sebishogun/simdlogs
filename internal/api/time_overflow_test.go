package api

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// THE OVERFLOW FAMILY, ON EVERY SURFACE THAT CONVERTS A TIME.
//
// int64 nanoseconds since the epoch reach 1677-09-21 .. 2262-04-11 and NOTHING
// outside it. Every conversion into that domain -- `t.UnixNano()`, `f * 1e9`,
// `n * int64(time.Second)` -- wraps silently past those instants, and a wrapped
// bound is not a near miss: a far-FUTURE lower bound becomes a far-PAST one, so
// a filter meaning "nothing" answers EVERYTHING, at HTTP 200, in a response
// that is structurally valid.
//
// docs/wrong.md entry 129 fixed `unixToNanos` and the ES date-math arithmetic.
// It did not reach `parseTimeParam`'s ParseFloat branch, its layout branch,
// `parseAbsTime` (the LogsQL `_time:` filter), `esTimeNumber`'s fractional
// branch, or the ingest path -- entry 130 holds those measurements. The family
// is the unit of work, so the whole family is gated here.
//
// EVERY far-future row has a far-past control in the same spelling, and both
// directions of the comparison are asserted. A bound test fed only
// out-of-window values cannot tell a correct bound from one that refuses
// everything: zero rows satisfies both.

// overflowNode is the 30-row corpus, stamped 2026-06-01, on one storage node.
// Every bound below is far outside it in one direction or the other, so the
// answer is always all 30 rows or none, never a fraction that could come from
// a bound landing somewhere plausible.
func overflowNode(t *testing.T) *httptest.Server {
	t.Helper()
	return realShard(t, corpus(1)[0])
}

// `start`/`end` ON THE HTTP SELECT ENDPOINTS.
//
// `parseTimeParam` has three branches and entry 129 saturated one of them.
// Measured on the tree before this gate, all at HTTP 200 against 30 rows:
//
//	?start=13000000000.5   30 rows   want 0    (ParseFloat: f*1e9 overflows)
//	?start=9999999999.5    30         want 0    (ParseFloat, year 2286)
//	?start=3000-01-01      30         want 0    (layout: t.UnixNano() wraps)
//	?start=9999-01-01      30         want 0
//	?start=2263-01-01      30         want 0    (the cliff: 2262 answers 0)
//	?end=3000-01-01         0         want 30   (the wrap the other way)
//	?start=1000-01-01       0         want 30   (pre-1678 wraps POSITIVE)
func TestAnOutOfRangeStartOrEndParameterSaturates(t *testing.T) {
	node := overflowNode(t)
	// A SEPARATE node for the range half, whose servers are not closed when
	// this test fails -- a range walk that never terminates makes
	// httptest.Server.Close block, and a red gate that hangs is a red gate
	// nobody reads. See spinSafeShard.
	rangeNode := spinSafeShard(t, corpus(1)[0])
	for _, tc := range []struct {
		name, param string
		want        int
	}{
		// The ParseFloat branch: fractional epoch seconds.
		{"fractional epoch seconds, year 2381", "start=13000000000.5", 0},
		{"fractional epoch seconds, year 2286", "start=9999999999.5", 0},
		{"fractional epoch seconds, in range (control)", "start=1000000000.5", 30},
		{"fractional epoch seconds as an end, year 2381 (control)", "end=13000000000.5", 30},
		{"fractional epoch seconds as an end, in range", "end=1000000000.5", 0},
		// The layout branch: a spelled-out date past 2262 or before 1678.
		{"year 3000 start", "start=3000-01-01", 0},
		{"year 9999 start", "start=9999-01-01", 0},
		{"year 2263 start", "start=2263-01-01", 0},
		{"year 2262 start (the last representable year)", "start=2262-01-01", 0},
		{"year 3000 end (control)", "end=3000-01-01", 30},
		{"year 9999 end (control)", "end=9999-01-01", 30},
		{"year 1000 start (control)", "start=1000-01-01", 30},
		{"year 1000 end", "end=1000-01-01", 0},
		// In-range controls in the same spelling: a bound that saturates
		// everything would satisfy every row above.
		{"year 2000 start (control)", "start=2000-01-01", 30},
		{"year 2000 end", "end=2000-01-01", 0},
		{"year 2100 end (control)", "end=2100-01-01", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, lines, raw := queryRowsParams(t, node, "*", tc.param)
			if code != 200 {
				t.Fatalf("%d, want 200: %.300s", code, raw)
			}
			if len(lines) != tc.want {
				t.Fatalf("?%s answered %d rows, want %d.\nA time conversion "+
					"that wraps turns a far-future bound into a far-past one, "+
					"so a filter meaning \"nothing\" answers everything.",
					tc.param, len(lines), tc.want)
			}
		})
		// THE SAME TABLE, ON THE RANGE ROUTE.
		//
		// Every spelling above was measured against /select/logsql/query and
		// nothing else. The trigger for the round's worst defect --
		// `start=1000-01-01&end=9999-01-01`, already written twice in this
		// file -- was never pointed at /select/logsql/stats_query_range, where
		// the same window made the bucket walk unbounded. The rows and the
		// wants are the same: a range response summed across its buckets is
		// the row count of the same window.
		t.Run("stats_query_range/"+tc.name, func(t *testing.T) {
			answered, code, total, body := rangeMatrix(rangeNode, "* | stats count() c", tc.param)
			if !answered {
				t.Fatalf("?%s did not answer within %s: %s", tc.param, rangeAnswerBound, body)
			}
			if code != 200 {
				t.Fatalf("?%s answered %d, want 200: %.300s", tc.param, code, body)
			}
			if total != float64(tc.want) {
				t.Fatalf("?%s summed %v across its buckets, want %d: %.300s",
					tc.param, total, tc.want, body)
			}
		})
	}
}

// THE LogsQL `_time:` FILTER, the other parser of the same instants.
//
// `parseAbsTime` returns `t.UnixNano()` raw and every caller then adds an
// interval to it (`lo + iv`, `hi + iv`), so a bound past 2262 wrapped and the
// addition wrapped again. Measured on the tree before this gate, 30 rows,
// HTTP 200 throughout:
//
//	_time:[2000-01-01, 9999-01-01]    0 rows   want 30   <- "no upper bound"
//	_time:[2000-01-01, 3000-01-01]    0        want 30
//	_time:>3000-01-01                30        want 0
//	_time:<3000-01-01                 0        want 30
//	_time:[1000-01-01, 2100-01-01]    0        want 30
//
// The cliff was exact: `2262-01-01` answered 30 and `2263-01-01` answered 0.
//
// `[a, 9999-01-01]` is the idiom for "from a, no upper bound" -- a client that
// cannot spell +infinity spells the largest year it can. It answered nothing.
func TestAnOutOfRangeTimeFilterBoundSaturates(t *testing.T) {
	node := overflowNode(t)
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"the no-upper-bound idiom", `_time:[2000-01-01, 9999-01-01]`, 30},
		{"upper bound in year 3000", `_time:[2000-01-01, 3000-01-01]`, 30},
		{"upper bound in year 2263", `_time:[2000-01-01, 2263-01-01]`, 30},
		{"upper bound in year 2262 (the last representable year)", `_time:[2000-01-01, 2262-01-01]`, 30},
		{"lower bound in year 1000", `_time:[1000-01-01, 2100-01-01]`, 30},
		{"greater than year 3000", `_time:>3000-01-01`, 0},
		{"greater or equal year 3000", `_time:>=3000-01-01`, 0},
		{"less than year 3000 (control)", `_time:<3000-01-01`, 30},
		{"less or equal year 3000 (control)", `_time:<=3000-01-01`, 30},
		{"greater than year 1000 (control)", `_time:>1000-01-01`, 30},
		{"less than year 1000", `_time:<1000-01-01`, 0},
		{"a bare year past the range", `_time:3000`, 0},
		{"a bare year before the range", `_time:1000`, 0},
		// In-range controls: the same spellings, bounds that do not overflow.
		{"in-range window (control)", `_time:[2026-01-01, 2027-01-01]`, 30},
		{"in-range window that excludes (control)", `_time:[2020-01-01, 2021-01-01]`, 0},
		{"in-range bare year (control)", `_time:2026`, 30},
		// A RELATIVE bound, where the overflow is in the DURATION rather than
		// the instant. `parseDurationNs` multiplies a count the caller wrote by
		// a unit this file chose, and `resolveTimePred` subtracts the product
		// from `now`. 18446744074 seconds is 584 years, and the product wrapped
		// to 290 MILLISECONDS -- so "everything since 584 years ago" answered
		// 0 of 30 rows at HTTP 200. 9999999999s wrapped too and came out right
		// by a second wrap in the subtraction, which is why one probe value
		// per branch is not a measurement of the branch.
		{"a 584-year relative lower bound", `_time:>18446744074s`, 30},
		{"a 317-year relative lower bound", `_time:>9999999999s`, 30},
		{"a 584-year relative upper bound", `_time:<18446744074s`, 0},
		{"an hour ago (control)", `_time:>1h`, 0},
		{"a thousand years of days (control)", `_time:>1000000000000d`, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, lines, raw := queryRows(t, node, tc.query+" *")
			if code != 200 {
				t.Fatalf("%d, want 200: %.300s", code, raw)
			}
			if len(lines) != tc.want {
				t.Fatalf("%s answered %d rows, want %d.\nA `_time:` bound past "+
					"2262 wrapped into the past and one before 1678 wrapped "+
					"into the future, both silently.", tc.query, len(lines), tc.want)
			}
		})
	}
}

// AN ELASTICSEARCH BOUND AS A FRACTIONAL JSON NUMBER.
//
// `esTimeNumber` caps the RAW float at +/-9.1e18 and then multiplies a
// fractional one by 1e9, so the PRODUCT overflows. `int64` of an out-of-range
// float64 is MinInt64 on amd64 -- so a far-FUTURE bound became minus infinity,
// and the two directions swapped:
//
//	{"gte": 13000000000.5}   4 of 4   want 0
//	{"lte": 13000000000.5}   0        want 4
func TestAFractionalESBoundSaturates(t *testing.T) {
	ts := esServer(t,
		map[string]string{"_msg": "a", "level": "error"},
		map[string]string{"_msg": "b", "level": "warn"},
		map[string]string{"_msg": "c", "level": "info"},
		map[string]string{"_msg": "d", "level": "info"},
	)
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		{"fractional gte, year 2381", `{"query":{"range":{"@timestamp":{"gte":13000000000.5}}},"size":100}`, 0},
		{"fractional lte, year 2381 (control)", `{"query":{"range":{"@timestamp":{"lte":13000000000.5}}},"size":100}`, 4},
		{"fractional gte, year 2286", `{"query":{"range":{"@timestamp":{"gte":9999999999.5}}},"size":100}`, 0},
		{"fractional lte, year 2286 (control)", `{"query":{"range":{"@timestamp":{"lte":9999999999.5}}},"size":100}`, 4},
		{"fractional gte, year 1970 (control)", `{"query":{"range":{"@timestamp":{"gte":100.5}}},"size":100}`, 4},
		{"fractional lte, year 1970", `{"query":{"range":{"@timestamp":{"lte":100.5}}},"size":100}`, 0},
		// The far-past direction: a large NEGATIVE fractional second.
		{"fractional gte, far past (control)", `{"query":{"range":{"@timestamp":{"gte":-13000000000.5}}},"size":100}`, 4},
		{"fractional lte, far past", `{"query":{"range":{"@timestamp":{"lte":-13000000000.5}}},"size":100}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %.300s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("%s answered total=%d, want %d.\n"+
					"int64 of an out-of-range float64 is MinInt64 on amd64, so "+
					"a far-future bound became minus infinity and the two "+
					"directions of the comparison swapped.", tc.body, total, tc.total)
			}
		})
	}
}

// A ROW WHOSE `_time` CANNOT BE STORED IS REFUSED AND COUNTED, NOT STORED
// INVISIBLY.
//
// `parseLayout` returned `t.UnixNano()` for whatever `time.Parse` accepted, and
// time.Parse accepts any year. A row stamped 9999-01-01 wrapped to an unrelated
// instant outside the query window, so it was ACCEPTED at HTTP 200, counted as
// ingested, written to the store -- and then unreachable by every query.
// Measured before this gate: four rows posted (one normal, year 9999, year
// 3000, year 1000), `{"ingested":4,"skipped":0}`, and `query=*` answered ONE
// row. Three rows written and invisible, with nothing in the response to say
// so.
//
// REFUSED rather than saturated, and the asymmetry with the bounds above is
// deliberate. A BOUND is a comparison, so +/-infinity is the right answer for
// an instant past the domain: `gte 9999` matches nothing and `lte 9999`
// matches everything, which is what the client meant. A row's `_time` is a
// FACT, and clamping it to 2262-04-11 files the row under a timestamp the
// client never sent -- it would then be returned by a query for 2262 and
// deleted by a retention pass for 2262. There is no clamped value that is not
// a fabrication, so the row is rejected with its ordinal and reason, and the
// count in the response is what tells the shipper.
//
// internal/api/cluster.go makes the same call on the same shape for a bucket
// timestamp arriving from a shard: out of range is refused, because
// "converting it wraps to an unrelated date".
func TestAnUnstorableIngestTimestampIsRefusedAndCounted(t *testing.T) {
	for _, tc := range []struct {
		name, path, body     string
		ingested, skipped    int
		visible              int
		wantMsgs, absentMsgs []string
	}{
		{
			name: "jsonline",
			path: "/insert/jsonline",
			body: `{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}
{"_time":"9999-01-01T00:00:00Z","_msg":"year9999"}
{"_time":"3000-01-01T00:00:00Z","_msg":"year3000"}
{"_time":"1000-01-01T00:00:00Z","_msg":"year1000"}
`,
			ingested: 1, skipped: 3, visible: 1,
			wantMsgs:   []string{"normal"},
			absentMsgs: []string{"year9999", "year3000", "year1000"},
		},
		{
			name: "logfmt",
			path: "/insert/logfmt",
			body: `_time=2026-06-01T12:00:00Z _msg=normal
_time=9999-01-01T00:00:00Z _msg=year9999
_time=1000-01-01T00:00:00Z _msg=year1000
`,
			ingested: 1, skipped: 2, visible: 1,
			wantMsgs:   []string{"normal"},
			absentMsgs: []string{"year9999", "year1000"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := NewServer(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Close() })
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(ts.Close)

			resp, err := http.Post(ts.URL+tc.path, "application/x-ndjson",
				strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("insert answered %d: %.300s", resp.StatusCode, raw)
			}
			var counts struct {
				Ingested int `json:"ingested"`
				Skipped  int `json:"skipped"`
			}
			if err := json.Unmarshal(raw, &counts); err != nil {
				t.Fatalf("insert response is not the count shape: %s", raw)
			}
			if counts.Ingested != tc.ingested || counts.Skipped != tc.skipped {
				t.Errorf("insert answered ingested=%d skipped=%d, want %d/%d.\n"+
					"A row whose timestamp cannot be stored must be COUNTED. "+
					"Accepting it stores a row at an unrelated instant that no "+
					"query can reach, with nothing in the response to say so.",
					counts.Ingested, counts.Skipped, tc.ingested, tc.skipped)
			}

			// THE STORE IS THE CONTROL. A count is a claim about the store;
			// the query is what the operator actually sees.
			code, lines, body := queryRowsParams(t, ts, "*", "start=1000-01-01&end=9999-01-01")
			if code != 200 {
				t.Fatalf("%d: %.300s", code, body)
			}
			if len(lines) != tc.visible {
				t.Errorf("the store answers %d rows for the widest window "+
					"this build can express, want %d: %.400s",
					len(lines), tc.visible, body)
			}
			for _, m := range tc.wantMsgs {
				if !strings.Contains(body, m) {
					t.Errorf("row %q is not in the store: %.400s", m, body)
				}
			}
			for _, m := range tc.absentMsgs {
				if strings.Contains(body, m) {
					t.Errorf("row %q was stored after being refused: %.400s", m, body)
				}
			}
		})
	}
}

// AN IN-RANGE TIMESTAMP IN EVERY SPELLING THE INGEST PATH ACCEPTS IS STILL
// ACCEPTED.
//
// The control for the test above. A refusal that fired on everything would
// satisfy every row of it, and the refusal is one comparison away from doing
// exactly that.
func TestAnInRangeIngestTimestampIsStillAccepted(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	stamp := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	body := strings.Join([]string{
		fmt.Sprintf(`{"_time":%q,"_msg":"rfc3339"}`, stamp.Format(time.RFC3339)),
		fmt.Sprintf(`{"_time":%q,"_msg":"rfc3339nano"}`, stamp.Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"_time":"%s","_msg":"spacesep"}`, stamp.Format("2006-01-02 15:04:05")),
		fmt.Sprintf(`{"_time":"%d","_msg":"nanosdigits"}`, stamp.UnixNano()),
		`{"_time":"not a time at all","_msg":"unparseable"}`,
		`{"_msg":"notime"}`,
	}, "\n") + "\n"

	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var counts struct {
		Ingested int `json:"ingested"`
		Skipped  int `json:"skipped"`
	}
	if err := json.Unmarshal(raw, &counts); err != nil {
		t.Fatalf("insert response is not the count shape: %s", raw)
	}
	if counts.Ingested != 6 || counts.Skipped != 0 {
		t.Fatalf("ingested=%d skipped=%d, want 6/0: %s\n"+
			"An unreadable `_time` is DATA -- it stays an ordinary field and "+
			"the row takes the fallback timestamp. Only a timestamp that "+
			"parses and cannot be stored is refused.", counts.Ingested, counts.Skipped, raw)
	}
	code, lines, out := queryRowsParams(t, ts, "*", "start=2000-01-01&end=2100-01-01")
	if code != 200 {
		t.Fatalf("%d: %.300s", code, out)
	}
	if len(lines) != 6 {
		t.Fatalf("the store answers %d rows, want 6: %.500s", len(lines), out)
	}
}

// queryRowsParams is queryRows with extra query-string parameters, which is
// how the `start`/`end` rows above reach the same endpoint.
func queryRowsParams(t *testing.T, ts *httptest.Server, q, params string) (int, []string, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/select/logsql/query?query=" + urlEscape(q) + "&" + params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return resp.StatusCode, lines, string(b)
}

// THE MERGE'S OWN TIME PARSERS, which read a peer's bytes rather than a
// client's.
//
// `rowLineTime` orders the federated merge: the router parses `"_time":"..."`
// out of every row every shard returns. Its fast path multiplies days by
// 86400e9, which overflows for year 9999 (2.5e20) and year 0000 (-6.2e19), and
// its `time.Parse` fallback called UnixNano, which wraps at the same
// boundaries. Either way the row sorted to the OPPOSITE end of the result from
// where its timestamp puts it -- a wrong order at HTTP 200, with the row set
// itself correct, which is the hardest shape to notice.
//
// A shard formats `_time` from an int64, so a well-behaved peer cannot produce
// one of these. That is an argument for the peer, not for the parser: this is
// the router reading bytes off a socket.
func TestTheMergeOrdersAnOutOfRangePeerTimestampAtTheRightEnd(t *testing.T) {
	line := func(ts string) string { return `{"_time":"` + ts + `","_msg":"x"}` }
	for _, tc := range []struct {
		name, ts string
		want     int64
	}{
		{"an ordinary instant", "2026-06-01T12:00:00Z",
			time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()},
		{"the newest storable instant", "2262-04-11T23:47:16.854775807Z", math.MaxInt64},
		{"past the newest", "9999-01-01T00:00:00Z", math.MaxInt64},
		{"past the newest, year 3000", "3000-01-01T00:00:00Z", math.MaxInt64},
		{"before the oldest", "1000-01-01T00:00:00Z", math.MinInt64},
		{"year zero", "0000-01-01T00:00:00Z", math.MinInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rowLineTime(line(tc.ts)); got != tc.want {
				t.Fatalf("rowLineTime(%q) = %d, want %d.\nA merge key that "+
					"wraps sorts the row to the opposite end of time from the "+
					"one its timestamp names.", tc.ts, got, tc.want)
			}
			// The fast path and the fallback must AGREE. The fast path is an
			// optimization of the general parser, so a bound one of them
			// enforces and the other does not is two orderings for one input.
			if ns, ok := fastRFC3339Nano(tc.ts); ok && ns != tc.want {
				t.Fatalf("fastRFC3339Nano(%q) = %d but the merge wants %d",
					tc.ts, ns, tc.want)
			}
			// THE THIRD COPY. `jsonLineToRow` lifts the same `_time` into
			// Row.Time for the row-scan merge path, through its own
			// time.Parse. Three parsers of one field is the two-copies shape
			// entries 113, 122 and 127 record, one copy worse.
			if got := jsonLineToRow([]byte(line(tc.ts))); got.NoTime || got.Time != tc.want {
				t.Fatalf("jsonLineToRow(%q).Time = %d (NoTime=%v), want %d",
					tc.ts, got.Time, got.NoTime, tc.want)
			}
		})
	}
}

// THE DATADOG INTAKE, whose timestamp is a bare JSON NUMBER in milliseconds.
//
// `ddTime` was `int64(f) * 1_000_000`. Two things go wrong and they go wrong
// differently: a value past 9.2e12 ms (the year 2262) overflows the multiply,
// and a float64 beyond int64's range converts to MinInt64 on amd64 before the
// multiply even runs. Measured on three entries -- one normal, one at
// 1.3e16 ms, one at 9.3e18:
//
//	                       before            after
//	POST /api/v2/logs      202, empty body   202 {"accepted":1,"rejected":2,...}
//	query=*                2 rows            1 row
//	query=_msg:far-future  0                 0
//	query=_msg:huge        1                 0
//
// The `huge` row is the second failure on its own: it was STORED, at an instant
// nothing sent, and `*` returned it. The `far-future` row is the first: stored
// and invisible. Neither was counted.
func TestADatadogTimestampOutsideTheStorableRangeIsRefusedAndCounted(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Post(ts.URL+"/api/v2/logs", "application/json", strings.NewReader(
		`[{"message":"normal","timestamp":1780000000000},
		  {"message":"far-future","timestamp":13000000000000000},
		  {"message":"huge","timestamp":9.3e18}]`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%d: %.300s", resp.StatusCode, raw)
	}
	// The intake reports its counts in a partial-success body; an empty body is
	// what "everything was accepted" looks like, which is what it used to say.
	for _, want := range []string{`"accepted":1`, `"rejected":2`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the intake response does not carry %s: %.300s\n"+
				"A row whose timestamp cannot be stored must be counted.",
				want, raw)
		}
	}
	code, lines, body := queryRowsParams(t, ts, "*", "start=1000-01-01&end=9999-01-01")
	if code != 200 {
		t.Fatalf("%d: %.200s", code, body)
	}
	if len(lines) != 1 || !strings.Contains(body, "normal") {
		t.Fatalf("the store holds %d rows for the widest window this build can "+
			"express, want just the storable one: %.400s", len(lines), body)
	}
}

// ------------------------------------------------------------------
// THE RANGE ROUTE: A SATURATED WINDOW MUST STILL ANSWER.
//
// The saturation above is the fix, and the fix is what made this reachable.
// `?start=1000-01-01&end=9999-01-01` resolves to [MinInt64, MaxInt64] now --
// correct, and a width of 2^64-1 that no int64 subtraction can hold:
//
//	parseStepNs("", from, to)      `to > from` is true, `to - from` is -1,
//	                               so the "1/30th of the range" default is 0
//	boundRangeBuckets              opened `if step <= 0 { return ... true }`,
//	                               one line ABOVE the overflow-safe width
//	                               written for this class, so no ceiling ran
//	StatsQueryRange                `step <= 0` -> `step = to - from` (-1) ->
//	                               `step = 1`, then
//	                               `for bs := MinInt64; bs < MaxInt64; bs += 1`
//
// Measured on the tree before this gate, an UNAUTHENTICATED GET:
//
//	                                        answered within 5s?
//	?start=1960-01-01&end=9999-01-01                no    (goroutine still in
//	?start=1000-01-01&end=9999-01-01                no     StatsQueryRange at
//	?start=1800-01-01&end=9999-01-01                no     stats_range.go:144;
//	?start=1000-01-01&end=2100-01-01                no     httptest Close()
//	?start=1970-01-01&end=9999-01-01                no     never returns)
//	?step=0s  &start=1960-01-01&end=9999-01-01      no
//	?step=-1s &start=1960-01-01&end=9999-01-01      no
//	?step=1h&start=2262-04-11T00:00:00Z&end=3000-01-01  no
//	?start=2026-01-01&end=2027-01-01               yes    (the control)
//	?step=1s&start=1970-01-01&end=9999-01-01       yes    413, ceiling reached
//
// Before the saturation this answered instantly and EMPTY: 1000-01-01 wrapped
// forward to ~2218 and 9999-01-01 wrapped back to ~1809, so `to <= from`
// short-circuited the loop. The fix for the wrap is what made the width
// overflow reachable, which is why it is this round's own defect.
//
// Two separate wraps had to close. `parseStepNs`/`boundRangeBuckets` no longer
// produce or ignore a non-positive step -- and the walk itself no longer
// depends on that, because `bs += step` past MaxInt64 wraps negative and starts
// the whole domain again, forever, at ANY step. The `?step=1h` row is that
// second one alone: its ceiling counted 23 buckets and was right.
//
// "Did it answer at all, within a bound" is a measurement that holds at any
// machine load, which is why this gate asserts wall-clock at all.

// rangeAnswerBound is the wall clock a range query gets to answer in.
//
// Generous by two orders of magnitude: every row below answers in under a
// millisecond on a green tree, on a machine with a load average of 8. It is
// not a latency assertion -- it is the difference between answering and never
// answering, and nothing between 1ms and 15s is a plausible third case.
const rangeAnswerBound = 15 * time.Second

// spinSafeShard is realShard that does not close its server when the test FAILED.
//
// `httptest.Server.Close` waits for outstanding requests, so a handler that
// never returns turns a RED gate into a hung test binary killed by `-timeout`
// -- which is both the shape this gate catches and the reason it was awkward
// to catch: the reviewer who found the defect could not close its own
// httptest.Server. On a green tree every probe answers and both servers close
// normally.
func spinSafeShard(t *testing.T, rows []string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		ts.Close()
		srv.Close()
	})
	if len(rows) > 0 {
		resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
			strings.NewReader(strings.Join(rows, "\n")+"\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	return ts
}

// spinSafeRouter is `router` with spinSafeShard's cleanup rule.
func spinSafeRouter(t *testing.T, urls ...string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv.SetBackends(urls)
	srv.SetReplicas(1)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		ts.Close()
		srv.Close()
	})
	return ts
}

// rangeMatrix runs one stats_query_range and reports whether it ANSWERED
// within the bound, its status, and the summed value of every bucket of every
// series. The client carries the bound, so the request returns even though a
// spinning handler's goroutine does not.
func rangeMatrix(ts *httptest.Server, q, params string) (answered bool, code int, total float64, body string) {
	c := &http.Client{Timeout: rangeAnswerBound}
	resp, err := c.Get(ts.URL + "/select/logsql/stats_query_range?query=" + urlEscape(q) + "&" + params)
	if err != nil {
		return false, 0, 0, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var pr struct {
		Data struct {
			Result []struct {
				Values [][2]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &pr); err == nil {
		for _, se := range pr.Data.Result {
			for _, v := range se.Values {
				var s string
				if json.Unmarshal(v[1], &s) != nil || s == "" {
					continue
				}
				var f float64
				fmt.Sscanf(s, "%g", &f)
				total += f
			}
		}
	}
	return true, resp.StatusCode, total, string(raw)
}

// A SATURATED RANGE WINDOW ANSWERS, ON A NODE AND THROUGH A ROUTER.
//
// The router half is not decoration: `federatedMatrix` resolves the window and
// writes it onto every shard request, and `exactMatrix` runs its own bucket
// walk over the merged rows while holding every matching row of the cluster in
// memory. Three separate walks, one rule; a fix applied to the node alone
// leaves the router spinning on the same GET.
func TestASaturatedRangeWindowAnswersOnNodeAndRouter(t *testing.T) {
	node := spinSafeShard(t, corpus(1)[0])
	parts := corpus(2)
	cluster := spinSafeRouter(t,
		spinSafeShard(t, parts[0]).URL,
		spinSafeShard(t, parts[1]).URL)

	for _, tc := range []struct {
		name, params string
		// total is the summed count across every bucket: 30 when the window
		// covers the 2026-06-01 corpus, 0 when it does not. A gate that only
		// asserted "answered" would pass on a server that answered an empty
		// matrix to everything.
		total float64
	}{
		{"far past to far future", "start=1000-01-01&end=9999-01-01", 30},
		{"1960 to 9999", "start=1960-01-01&end=9999-01-01", 30},
		{"1800 to 9999", "start=1800-01-01&end=9999-01-01", 30},
		{"far past to 2100", "start=1000-01-01&end=2100-01-01", 30},
		{"epoch to 9999", "start=1970-01-01&end=9999-01-01", 30},
		{"a step of zero over a saturated window", "step=0s&start=1960-01-01&end=9999-01-01", 30},
		{"a negative step over a saturated window", "step=-1s&start=1960-01-01&end=9999-01-01", 30},
		{"a bare integer step over a saturated window", "step=0&start=1960-01-01&end=9999-01-01", 30},
		// The walk's own overflow, with a step the ceiling is happy with: the
		// last bucket runs past MaxInt64 and `bs += step` wraps.
		{"an hourly step against the last representable day",
			"step=1h&start=2262-04-11T00:00:00Z&end=3000-01-01", 0},
		// Controls: a window that needs no saturation at all, and one whose
		// bucket count the ceiling refuses.
		{"an ordinary window (control)", "start=2026-01-01&end=2027-01-01", 30},
		{"a window narrower than the corpus (control)", "start=2020-01-01&end=2021-01-01", 0},
	} {
		for _, sv := range []struct {
			where string
			ts    *httptest.Server
			query string
		}{
			// count() is mergeable, so a router answers it from per-shard
			// partials (federatedMatrix). avg() is not, so the router pulls the
			// rows and buckets them itself (exactMatrix) -- a THIRD copy of the
			// walk, and the one that spins holding the data.
			{"node", node, "* | stats count() c"},
			{"router federated", cluster, "* | stats count() c"},
			{"router exact", cluster, "* | stats count() c, avg(n) a"},
		} {
			answered, code, total, body := rangeMatrix(sv.ts, sv.query, tc.params)
			if !answered {
				t.Errorf("%s: /select/logsql/stats_query_range?%s did not answer "+
					"within %s.\nA non-positive or overflowing step makes the bucket "+
					"walk unbounded, and this request needs no authentication: %s",
					sv.where, tc.params, rangeAnswerBound, body)
				// Every later probe now competes with a goroutine that will
				// never stop, so the rest of the table would measure the
				// machine rather than the server.
				return
			}
			if code != 200 {
				t.Errorf("%s: ?%s answered %d, want 200: %.300s", sv.where, tc.params, code, body)
				continue
			}
			if total != tc.total {
				t.Errorf("%s: ?%s summed %v across its buckets, want %v: %.300s\n"+
					"Answering is not enough -- an empty matrix answers instantly too.",
					sv.where, tc.params, total, tc.total, body)
			}
		}
	}
}

// ------------------------------------------------------------------
// THE ALL-DIGITS NANOSECOND SPELLING, ON EVERY INGEST PROTOCOL.
//
// Entry 130's ingest gate has six out-of-range rows and every one of them is a
// date LAYOUT -- "9999-01-01T00:00:00Z". `parseTime` reaches its layouts only
// after `allDigits`/`ParseInt`, and a 21-digit nanosecond count fails ParseInt
// with ErrRange and then FELL THROUGH to them: none matches a run of digits,
// so the value came back as outcome two, "not a timestamp at all", and the
// caller stamped the row with the RECEIVER'S CLOCK.
//
// A Loki push timestamp and an OTLP `timeUnixNano` are ALWAYS all-digits.
// Neither protocol can spell a timestamp any other way, so entry 130's gate
// tested, for both of them, a spelling they never send. Measured with
// `253402300800000000000` (year 9999 in nanoseconds), one out-of-range record
// beside one storable one:
//
//	/insert/jsonline    200 {"ingested":2,"skipped":1}  (the 1 is a LAYOUT row)
//	/insert/logfmt      200 {"ingested":2,"skipped":0}
//	/loki/api/v1/push   204, no X-Simdlogs-Rejected
//	/v1/logs            200 {}   -- full success
//	/api/v2/logs        202, empty body
//	control, the same year as a layout through OTLP:
//	                    200 {"partialSuccess":{"rejectedLogRecords":"1"}}
//
// A JSON NUMBER was a second hole in the same contract, on the routes that
// take one. jsonline read `val.Int()` with no parseTime, no nanosOf and no
// range check, so three different instants -- 9.3e18, 2534023008e11 and
// -9.3e18 -- were all stored at MinInt64, both directions collapsed onto the
// same fabricated instant.
//
// THE STORE IS THE ASSERTION, because the five protocols report differently
// (a count object, a 204 with a header, an OTLP partialSuccess body, a
// Datadog intake body) and only one thing is common to all of them: what a
// query can see afterwards. A row stamped `now` is VISIBLE, which is what
// makes this table catch the fallback as well as the wrap.
func TestAnAllDigitsUnstorableTimestampIsRefusedOnEveryProtocol(t *testing.T) {
	const ns9999 = "253402300800000000000" // year 9999 in nanoseconds: 21 digits
	for _, tc := range []struct {
		name, path, ctype, body string
	}{
		{
			"jsonline, the string spelling", "/insert/jsonline", "application/x-ndjson",
			`{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}` + "\n" +
				`{"_time":"` + ns9999 + `","_msg":"unstorable"}` + "\n",
		},
		{
			// The JSON-number arm: no parseTime at all until this round.
			"jsonline, the number spelling", "/insert/jsonline", "application/x-ndjson",
			`{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}` + "\n" +
				`{"_time":` + ns9999 + `,"_msg":"unstorable"}` + "\n",
		},
		{
			// A float, where int64() of an out-of-range float64 is MinInt64 on
			// amd64 -- both directions collapsing onto one instant.
			"jsonline, a float past the domain", "/insert/jsonline", "application/x-ndjson",
			`{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}` + "\n" +
				`{"_time":9.3e18,"_msg":"unstorable"}` + "\n",
		},
		{
			"jsonline, a float past the domain the other way", "/insert/jsonline", "application/x-ndjson",
			`{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}` + "\n" +
				`{"_time":-9.3e18,"_msg":"unstorable"}` + "\n",
		},
		{
			"elasticsearch _bulk inherits it", "/insert/elasticsearch/_bulk", "application/x-ndjson",
			`{"create":{}}` + "\n" + `{"_time":"2026-06-01T12:00:00Z","_msg":"normal"}` + "\n" +
				`{"create":{}}` + "\n" + `{"_time":` + ns9999 + `,"_msg":"unstorable"}` + "\n",
		},
		{
			"logfmt", "/insert/logfmt", "text/plain",
			"_time=2026-06-01T12:00:00Z _msg=normal\n_time=" + ns9999 + " _msg=unstorable\n",
		},
		{
			"loki push", "/loki/api/v1/push", "application/json",
			`{"streams":[{"stream":{"app":"a"},"values":[` +
				`["1780315200000000000","normal"],["` + ns9999 + `","unstorable"]]}]}`,
		},
		{
			"otlp json", "/v1/logs", "application/json",
			`{"resourceLogs":[{"scopeLogs":[{"logRecords":[` +
				`{"timeUnixNano":"1780315200000000000","body":{"stringValue":"normal"}},` +
				`{"timeUnixNano":"` + ns9999 + `","body":{"stringValue":"unstorable"}}]}]}]}`,
		},
		{
			// systemd's field is an UNSIGNED microsecond count, so `us*1000`
			// overflowed for every instant past 2262 -- a legal journal value.
			"journald", "/insert/journald", "text/plain",
			"__REALTIME_TIMESTAMP=1780315200000000\nMESSAGE=normal\n\n" +
				"__REALTIME_TIMESTAMP=253402300800000000\nMESSAGE=unstorable\n\n",
		},
		{
			// AND THE HALF OF THAT UNSIGNED DOMAIN ParseInt COULD NOT READ.
			// The guard above it was `if us, err := ParseInt(...); err == nil`,
			// so 2^63 microseconds -- inside uint64, which is what a journal
			// field IS -- failed for RANGE, the field was ignored, and the
			// entry was stamped with the RECEIVER'S CLOCK and stored. The
			// row above it never reached that arm: 2.5e17 parses as an int64
			// and was caught by the us*1000 check, so the ErrRange
			// fall-through had no probe at all.
			"journald, past int64 as an unsigned microsecond count",
			"/insert/journald", "text/plain",
			"__REALTIME_TIMESTAMP=1780315200000000\nMESSAGE=normal\n\n" +
				"__REALTIME_TIMESTAMP=9223372036854775808\nMESSAGE=unstorable\n\n",
		},
		{
			// Datadog's timestamp is milliseconds; entry 130 gated 1.3e16 and
			// 9.3e18 and not the year-9999 spelling every other row uses.
			"datadog intake", "/api/v2/logs", "application/json",
			`[{"message":"normal","timestamp":1780315200000},` +
				`{"message":"unstorable","timestamp":253402300800000}]`,
		},
		{
			// SYSLOG STAMPED THE RECEIVER'S CLOCK and said so in a comment:
			// "the datagram has no client to report a per-record rejection
			// to". True of the UDP listener; this route is an HTTP request
			// with a client on the other end, and stamping `now` files the
			// line under an instant nobody sent either way.
			"syslog over HTTP", "/insert/syslog", "text/plain",
			"<34>1 2026-06-01T12:00:00Z host app - - - normal\n" +
				"<34>1 9999-01-01T00:00:00Z host app - - - unstorable\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := NewServer(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Close() })
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(ts.Close)

			resp, err := http.Post(ts.URL+tc.path, tc.ctype, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("%s answered %d: %.300s", tc.path, resp.StatusCode, raw)
			}
			// The widest window this build can express. A row stamped with the
			// receiver's clock is INSIDE it, so "the store holds one row" is
			// the assertion that separates a refusal from a fallback.
			code, lines, body := queryRowsParams(t, ts, "*", "start=1000-01-01&end=9999-01-01")
			if code != 200 {
				t.Fatalf("%d: %.300s", code, body)
			}
			if len(lines) != 1 || !strings.Contains(body, "normal") {
				t.Errorf("the store holds %d rows over the widest expressible window, "+
					"want just the storable one: %.500s\n"+
					"An unstorable `_time` must be REFUSED and counted. Stamping it "+
					"`now` files the row under an instant nobody sent, which is what "+
					"wrapping did before it.\ningest response was: %.200s",
					len(lines), body, raw)
			}
			if strings.Contains(body, "unstorable") {
				t.Errorf("the refused row is in the store: %.500s", body)
			}
		})
	}
}

// ------------------------------------------------------------------
// A STORABLE INSTANT BEFORE THE EPOCH IS REACHABLE IN BOTH LANGUAGES.
//
// The int64-nanosecond domain starts at 1677-09-21, not at 1970, and this
// store accepts and stores every instant in it: three rows stamped 1900, 1969
// and 2026 all ingest at `{"ingested":3,"skipped":0}`. The far-past bound they
// need SATURATES to MinInt64 -- that is entry 129/130's fix -- and two lines
// one layer down turned MinInt64 straight back into 0:
//
//	internal/query/filter.go      predMatchesRow  `if from == MinInt64 { from = 0 }`
//	internal/query/time_filter.go timePredBitset  the same, on the columnar path
//
// so the ONE value the saturation produces was the one value that was undone.
// The Elasticsearch surface had the same blind spot for a third reason:
// `esToQuery` built its Query with the zero From and the lift is
// `if from > q.From`, which can only raise it. Measured on those three rows,
// every request under `?start=1000-01-01&end=9999-01-01` so the HTTP window is
// not the thing under test, all at HTTP 200:
//
//   - 3 of 3   OK
//     _time:[1000-01-01, 2100-01-01]   1        want 3
//     _time:<2100-01-01                1        want 3
//     _time:>1000-01-01                1        want 3
//   - | filter _time:<2100-01-01     1        want 3
//     _time:[1900-01-01, 2100-01-01]   3        OK
//     _time:[1677-09-22, 2100-01-01]   3        OK
//     ES {"match_all":{}}              1        want 3
//     ES range gte 1000-01-01          1        want 3
//
// The representable rows are the control and they are the reason this is one
// clamp rather than a general pre-1970 gap: 1900-01-01 needs no saturation, so
// it never hit the clamp and always answered correctly. Only the saturated
// spelling failed, which is what makes the two languages disagree about the
// same instant -- `?start=1000-01-01` reached 1677 while `_time:>=1000-01-01`
// stopped at 1970.
func preEpochNode(t *testing.T) *httptest.Server {
	t.Helper()
	return realShard(t, []string{
		`{"_time":"1900-01-01T00:00:00Z","_msg":"y1900"}`,
		`{"_time":"1969-01-01T00:00:00Z","_msg":"y1969"}`,
		`{"_time":"2026-06-01T12:00:00Z","_msg":"y2026"}`,
	})
}

func TestAStorableInstantBeforeTheEpochIsReachableInBothLanguages(t *testing.T) {
	node := preEpochNode(t)
	// BOTH HALVES ON THE SAME WINDOW, WHICH THEY WERE NOT.
	//
	// The LogsQL half ran under `start=1000-01-01&end=9999-01-01` and the
	// Elasticsearch half ran with NO QUERY STRING AT ALL -- `/_search` reads
	// its window from the body and ignores `?start`/`?end` outright, so a
	// client cannot bring the two surfaces onto one window and neither could
	// this gate. That made it a comparison of two builds' answers to two
	// different questions, and it hid a real disagreement: the LogsQL default
	// lower bound was the EPOCH and the ES default was MinInt64. Measured on
	// this same three-row store with no parameters on any request:
	//
	//	/select/logsql/query?query=*                     1
	//	/select/logsql/query?query=_time:[1900-01-01, 2100-01-01]
	//	                                                 1
	//	/select/sql?query=SELECT * FROM logs             1
	//	/_search {"match_all":{}}                        3
	//	/_count  {"match_all":{}}                        3
	//
	// Every row below now runs under BOTH windows and must answer the same,
	// which is only possible because `defaultWindowFrom` is one constant that
	// both surfaces read.
	const wide = "start=1000-01-01&end=9999-01-01"
	windows := []struct{ name, params string }{
		{"an explicit widest window", wide},
		{"the default window", ""},
	}

	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"no time filter at all (control)", `*`, 3},
		// The saturated spellings: every one of these produces T1 == MinInt64.
		{"an absolute range from before the domain", `_time:[1000-01-01, 2100-01-01]`, 3},
		{"an open lower bound", `_time:<2100-01-01`, 3},
		{"a lower bound before the domain", `_time:>1000-01-01`, 3},
		{"the same through a filter pipe", `* | filter _time:<2100-01-01`, 3},
		// The representable controls: no saturation, so these always worked
		// and a fix that widened everything would not be visible here.
		{"a representable pre-epoch bound", `_time:[1900-01-01, 2100-01-01]`, 3},
		{"the first representable day", `_time:[1677-09-22, 2100-01-01]`, 3},
		// And bounds that must still EXCLUDE: a filter that answered
		// everything would satisfy every row above.
		{"a bound that excludes the pre-epoch rows", `_time:[1970-01-01, 2100-01-01]`, 1},
		{"a bound that excludes everything", `_time:[2100-01-01, 2200-01-01]`, 0},
		{"an upper bound before the epoch", `_time:<1970-01-01`, 2},
	} {
		for _, win := range windows {
			t.Run(tc.name+", "+win.name, func(t *testing.T) {
				code, lines, body := queryRowsParams(t, node, tc.query, win.params)
				if code != 200 {
					t.Fatalf("%d: %.300s", code, body)
				}
				if len(lines) != tc.want {
					t.Fatalf("%s under %q answered %d rows, want %d: %.400s\n"+
						"A far-past bound saturates to MinInt64, and MinInt64 is a "+
						"BOUND, not a sentinel meaning the epoch. Rows between "+
						"1677-09-21 and 1970 are storable and this store holds them.",
						tc.query, win.params, len(lines), tc.want, body)
				}
			})
		}
	}

	// THE STATS PIPE, READ BY ITS COUNT AND NOT BY ITS ROW COUNT.
	//
	// This row was `{"the same through a stats pipe", "_time:<2100-01-01 |
	// stats count() c", 1}` in the row table above, and the 1 was the number
	// of RESPONSE LINES. A stats pipe emits exactly one row whether `c` is 3
	// or 1, so the assertion held for every build. Entry 131's own table
	// records this query as "1 row, c=1 want c=3" -- the value the gate did
	// not read.
	for _, tc := range []struct {
		name, query, wantC string
	}{
		{"a stats pipe under a saturated upper bound", `_time:<2100-01-01 | stats count() c`, "3"},
		{"a filter pipe into a stats pipe", `* | filter _time:>1000-01-01 | stats count() c`, "3"},
		{"a stats pipe under a representable bound (control)", `_time:>1900-01-01 | stats count() c`, "2"},
		{"a stats pipe that must still exclude (control)", `_time:>1970-01-01 | stats count() c`, "1"},
	} {
		for _, win := range windows {
			t.Run(tc.name+", "+win.name, func(t *testing.T) {
				code, lines, body := queryRowsParams(t, node, tc.query, win.params)
				if code != 200 {
					t.Fatalf("%d: %.300s", code, body)
				}
				if len(lines) != 1 {
					t.Fatalf("a stats pipe answered %d rows, want 1: %.300s", len(lines), body)
				}
				var got struct {
					C string `json:"c"`
				}
				if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
					t.Fatalf("stats row is not readable: %v\n%s", err, lines[0])
				}
				if got.C != tc.wantC {
					t.Fatalf("%s under %q counted c=%q, want %q.\n"+
						"The ROW COUNT is 1 either way -- a stats pipe emits one "+
						"row whatever it counted -- so the number that carries "+
						"the answer is the only one worth asserting.",
						tc.query, win.params, got.C, tc.wantC)
				}
			})
		}
	}

	// THE SQL SURFACE, which reads the same window through timeWindow and had
	// no row here at all. Under the clamp mutation this answers 1 for a bound
	// that must reach 3, silently, at 200.
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"a bare select", `SELECT * FROM logs`, 3},
		{"a saturating lower bound", `SELECT * FROM logs WHERE _time > '1000-01-01'`, 3},
		{"a representable lower bound (control)", `SELECT * FROM logs WHERE _time > '1900-01-01'`, 2},
		{"a bound that must still exclude (control)", `SELECT * FROM logs WHERE _time > '1970-01-01'`, 1},
	} {
		for _, win := range windows {
			t.Run("sql/"+tc.name+", "+win.name, func(t *testing.T) {
				u := node.URL + "/select/sql?query=" + urlEscape(tc.query)
				if win.params != "" {
					u += "&" + win.params
				}
				resp, err := http.Get(u)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != 200 {
					t.Fatalf("%d: %.300s", resp.StatusCode, raw)
				}
				n := 0
				for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
					if l != "" {
						n++
					}
				}
				if n != tc.want {
					t.Fatalf("%s under %q answered %d rows, want %d: %.300s",
						tc.query, win.params, n, tc.want, raw)
				}
			})
		}
	}

	// THE ELASTICSEARCH SURFACE, whose window started at the zero value. It
	// takes no `?start`/`?end`, so its window is the DEFAULT one -- which is
	// now the same window every row above ran under a second time.
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		{"match_all", `{"query":{"match_all":{}},"size":100}`, 3},
		{"a range from before the domain", `{"query":{"range":{"@timestamp":{"gte":"1000-01-01T00:00:00Z","lte":"2100-01-01T00:00:00Z"}}},"size":100}`, 3},
		{"an upper bound only", `{"query":{"range":{"@timestamp":{"lte":"2100-01-01T00:00:00Z"}}},"size":100}`, 3},
		{"a representable pre-epoch lower bound", `{"query":{"range":{"@timestamp":{"gte":"1900-01-01T00:00:00Z"}}},"size":100}`, 3},
		{"a negated future bound", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"gte":"2100-01-01T00:00:00Z"}}}]}},"size":100}`, 3},
		// A NEGATED bound THAT SATURATES. The `must_not` row above uses
		// `gte 2100-01-01`, which is inside the domain and needs no
		// saturation, so it cannot see the clamp at all: under the es.go
		// mutation it still answers 3. This one answers 2 instead of 0.
		{"a negated bound from before the domain", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"gte":"1000-01-01T00:00:00Z"}}}]}},"size":100}`, 0},
		{"a negated upper bound from before the domain", `{"query":{"bool":{"must_not":[{"range":{"@timestamp":{"lt":"1000-01-01T00:00:00Z"}}}]}},"size":100}`, 3},
		{"a saturating bound under a should", `{"query":{"bool":{"should":[{"range":{"@timestamp":{"gte":"1000-01-01T00:00:00Z"}}}]}},"size":100}`, 3},
		// Controls that must still exclude.
		{"a bound after the epoch", `{"query":{"range":{"@timestamp":{"gte":"1970-01-01T00:00:00Z"}}},"size":100}`, 1},
		{"a bound after everything", `{"query":{"range":{"@timestamp":{"gte":"2100-01-01T00:00:00Z"}}},"size":100}`, 0},
		{"an upper bound at the epoch", `{"query":{"range":{"@timestamp":{"lt":"1970-01-01T00:00:00Z"}}},"size":100}`, 2},
	} {
		t.Run("es/"+tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, node, tc.body)
			if code != 200 {
				t.Fatalf("%d: %.300s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("%s answered total=%d, want %d.\n"+
					"esToQuery built its window with the zero From, and the lift "+
					"is `if from > q.From` -- which can only ever raise it.",
					tc.body, total, tc.total)
			}
		})
	}

	// AND /_count AGREES WITH /_search, on the same default window. A
	// disagreement between the two is what entry 124 measured first.
	t.Run("es/_count agrees with _search", func(t *testing.T) {
		resp, err := http.Post(node.URL+"/_count", "application/json",
			strings.NewReader(`{"query":{"match_all":{}}}`))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var cnt struct{ Count int }
		json.Unmarshal(raw, &cnt)
		if cnt.Count != 3 {
			t.Fatalf("/_count answered %d, want 3: %.200s", cnt.Count, raw)
		}
	})
}

// A VECTOR SAMPLE'S TIMESTAMP MUST BE AN INSTANT, AND +INFINITY IS NOT ONE.
//
// `/select/logsql/stats_query` is the Prometheus instant-query envelope: each
// sample is `[<unix seconds>, "<value>"]` and the seconds are the end of the
// window. `?end=9999-01-01` resolves to MaxInt64 -- the saturation entries 129
// and 130 installed, which is correct for a BOUND -- and `to / 1e9` rendered
// that as 9223372036, the last second the int64-nanosecond domain can express.
// Measured on the three-row store below, before the fix:
//
//	"value":[9223372036,"3"]     which is 2262-04-11T23:47:16Z
//
// The count is right and the instant is a fabrication: a Grafana panel plots
// the point 236 years to the right of every other series on the dashboard.
//
// Both surfaces are here because there are two copies of the loop that stamps
// it -- `statsQuery` on a node and `exactVector` on a router -- and a node and
// the router in front of it stamping one request differently is the defect
// this repository already records twice under "one dispatch, two copies".
func TestAVectorSampleIsStampedWithAnInstantAndNotWithInfinity(t *testing.T) {
	node := preEpochNode(t)
	cluster := router(t, node.URL)

	// A non-mergeable aggregate forces the ROUTER onto exactVector rather than
	// federating; count() alone would take the mergeable path.
	for _, tc := range []struct {
		name, target, query string
	}{
		{"node, a mergeable aggregate", node.URL, `* | stats count() c`},
		{"node, a non-mergeable aggregate", node.URL, `* | stats count_uniq(_msg) u`},
		{"router, a non-mergeable aggregate", cluster.URL, `* | stats count_uniq(_msg) u`},
	} {
		for _, win := range []struct {
			name, params string
			// wantAt is the exact stamp when the request names an END this
			// build can represent: a Prometheus instant query at `time=<t>`
			// reports `t`, future or not, so an in-domain end is left alone.
			// Zero means "the request time", within a day.
			wantAt int64
		}{
			{"a saturated end", "start=1000-01-01&end=9999-01-01", 0},
			{"no window at all", "", 0},
			{"an ordinary end (control)", "start=1900-01-01&end=2100-01-01", 4102444800},
			{"an in-domain future end (control)", "start=1900-01-01&end=2262-04-11T00:00:00Z", 9223286400},
		} {
			t.Run(tc.name+", "+win.name, func(t *testing.T) {
				u := tc.target + "/select/logsql/stats_query?query=" + urlEscape(tc.query)
				if win.params != "" {
					u += "&" + win.params
				}
				resp, err := http.Get(u)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != 200 {
					t.Fatalf("%d: %.300s", resp.StatusCode, raw)
				}
				var out struct {
					Data struct {
						Result []struct {
							Value [2]any `json:"value"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(raw, &out); err != nil {
					t.Fatalf("not the vector envelope: %v\n%.300s", err, raw)
				}
				if len(out.Data.Result) == 0 {
					t.Fatalf("no samples, so this row asserts nothing: %.300s", raw)
				}
				// A day of slack each way: the stamp is a clock read.
				lo := time.Now().Add(-24 * time.Hour).Unix()
				hi := time.Now().Add(24 * time.Hour).Unix()
				for _, sm := range out.Data.Result {
					secs, ok := sm.Value[0].(float64)
					if !ok {
						t.Fatalf("sample timestamp is %T, not a number: %.300s", sm.Value[0], raw)
					}
					at := int64(secs)
					if at == int64(math.MaxInt64)/1_000_000_000 {
						t.Fatalf("the sample is stamped %d -- MaxInt64 nanoseconds "+
							"in seconds, the last instant this build can express. "+
							"A saturated `end` is +infinity, which is the right "+
							"answer for a BOUND and not a time a point can be "+
							"plotted at: %.300s", at, raw)
					}
					if win.wantAt != 0 {
						if at != win.wantAt {
							t.Fatalf("the sample is stamped %d (%s), want %d: an "+
								"`end` this build can represent is the instant the "+
								"client asked to be evaluated at, and must not be "+
								"rewritten: %.300s", at,
								time.Unix(at, 0).UTC().Format(time.RFC3339),
								win.wantAt, raw)
						}
						continue
					}
					if at < lo || at > hi {
						t.Fatalf("the sample is stamped %d (%s), which is not "+
							"within a day of now: %.300s", at,
							time.Unix(at, 0).UTC().Format(time.RFC3339), raw)
					}
				}
			})
		}
	}
}
