package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// An extremal aggregate is taken extremally, not summed.
//
// mergeableAggs has always listed AggMin and AggMax, and the comment above it
// says "Additive OR EXTREMAL only" -- but every federated stats merge added
// unconditionally. Two shards holding n = 100..105 and n = 106..111 answered
// `stats min(n)` with 206. No row has n = 206: the answer is not merely wrong,
// it is arithmetically impossible, and it came back HTTP 200.

// vectorShard answers /select/logsql/stats_query with one Prometheus instant
// vector series named `name` and the given value.
func vectorShard(t *testing.T, name, value string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"__name__":%q},"value":[1767225600,%q]}]}}`, name, value)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// rangeShard is the same for stats_query_range: one matrix series, two points.
func rangeShard(t *testing.T, name, v1, v2 string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"__name__":%q},"values":[[1767225600,%q],[1767225660,%q]]}]}}`,
			name, v1, v2)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func vectorValue(t *testing.T, ts *httptest.Server, q string) (int, string) {
	t.Helper()
	code, got, raw := getJSONFrom(t, ts, "/select/logsql/stats_query?query="+q)
	if code != 200 {
		return code, raw
	}
	data, _ := got["data"].(map[string]any)
	res, _ := data["result"].([]any)
	if len(res) != 1 {
		t.Fatalf("want one merged series, got %d: %s", len(res), raw)
	}
	se := res[0].(map[string]any)
	return code, fmt.Sprint(se["value"].([]any)[1])
}

func TestAnExtremalAggregateIsNotSummedAcrossShards(t *testing.T) {
	for _, tc := range []struct {
		agg      string
		a, b     string
		want     string
		wouldSum string
	}{
		{"min%28n%29+m", "100", "106", "100", "206"},
		{"max%28n%29+m", "105", "111", "111", "216"},
		{"count%28%29+m", "6", "6", "12", "12"},
		{"sum%28n%29+m", "615", "651", "1266", "1266"},
	} {
		t.Run(tc.agg, func(t *testing.T) {
			a := vectorShard(t, "m", tc.a)
			b := vectorShard(t, "m", tc.b)
			ts := router(t, a.URL, b.URL)

			code, got := vectorValue(t, ts, "*+%7C+stats+"+tc.agg)
			if code != 200 {
				t.Fatalf("%d: %s", code, got)
			}
			if got != tc.want {
				t.Errorf("the cluster answered %s, want %s (summing the shards' "+
					"values would give %s)", got, tc.want, tc.wouldSum)
			}
		})
	}
}

func TestARangeQueryTakesExtremesPerBucket(t *testing.T) {
	a := rangeShard(t, "m", "100", "3")
	b := rangeShard(t, "m", "106", "2")
	ts := router(t, a.URL, b.URL)

	code, got, raw := getJSONFrom(t, ts,
		"/select/logsql/stats_query_range?query=*+%7C+stats+min%28n%29+m&step=1m&start=1&end=2")
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	data := got["data"].(map[string]any)
	res := data["result"].([]any)
	if len(res) != 1 {
		t.Fatalf("want one merged series, got %d: %s", len(res), raw)
	}
	pts := res[0].(map[string]any)["values"].([]any)
	if len(pts) != 2 {
		t.Fatalf("want two buckets, got %d: %s", len(pts), raw)
	}
	for i, want := range []string{"100", "2"} {
		got := fmt.Sprint(pts[i].([]any)[1])
		if got != want {
			t.Errorf("bucket %d is %s, want %s (summing gives %s)", i, got, want,
				[]string{"206", "5"}[i])
		}
	}
}

// A series a shard names but the query's stats pipe does not is refused: the
// router has no basis for choosing an operator for it.
func TestAnUnknownSeriesNameIsRefused(t *testing.T) {
	a := vectorShard(t, "m", "1")
	b := vectorShard(t, "somethingelse", "2")
	ts := router(t, a.URL, b.URL)

	code, _, raw := getJSONFrom(t, ts,
		"/select/logsql/stats_query?query=*+%7C+stats+min%28n%29+m")
	if code/100 == 2 {
		t.Fatalf("answered %d %s, want a refusal", code, raw)
	}
	if !strings.Contains(raw, "somethingelse") {
		t.Errorf("the refusal does not name the series: %s", raw)
	}
}

// `stats_query&by=` decoded a shape the shards do not send.
//
// A storage node emits `{"stats":[...]}` only when StatsQueryInstant FAILS --
// which it does exactly when the query has no stats pipe -- and `by=` is set.
// A query that HAS one gets the Prometheus vector whatever `by=` says, and the
// router switched on `by=` alone: the vector envelope unmarshals cleanly into
// `struct{ Stats []vc }` with a nil slice, so a grouped stats query answered
// `{"stats":[]}` at HTTP 200 and a dashboard panel drew nothing.
func TestAGroupedStatsQueryIsNotDecodedAsTheByFieldShape(t *testing.T) {
	a := vectorShard(t, "c", "5")
	b := vectorShard(t, "c", "7")
	ts := router(t, a.URL, b.URL)

	code, got, raw := getJSONFrom(t, ts,
		"/select/logsql/stats_query?query=*+%7C+stats+count%28%29+c&by=level")
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	if _, wrongShape := got["stats"]; wrongShape {
		t.Fatalf("answered the by-field shape for a query with a stats pipe: %s", raw)
	}
	data, ok := got["data"].(map[string]any)
	if !ok {
		t.Fatalf("no data envelope: %s", raw)
	}
	res, _ := data["result"].([]any)
	if len(res) != 1 {
		t.Fatalf("want one merged series, got %d: %s", len(res), raw)
	}
	if v := fmt.Sprint(res[0].(map[string]any)["value"].([]any)[1]); v != "12" {
		t.Errorf("the cluster counted %s, want 12", v)
	}
}

// A matrix value or bucket start this router cannot read is refused, not
// counted as zero.
//
// matrixValue and matrixStamp both swallowed the parse error and returned 0. A
// zero term is silently missing from a sum and beats every positive value in a
// min; a zero bucket start is both the sort key and the emitted stamp, so the
// point landed at the epoch and sorted first. federatedVector refuses the same
// condition, and federatedMatrix was the odd one out.
func TestAnUnreadableMatrixPointIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"value", `{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"__name__":"m"},"values":[[1767225600,"not-a-number"]]}]}}`, "not-a-number"},
		{"bucket start", `{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"__name__":"m"},"values":[["not-a-stamp","1"]]}]}}`, "not-a-stamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := rangeShard(t, "m", "1", "2")
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeEnvelope(w.Header(), 0, 0, true, 1, "")
				w.Write([]byte(tc.body))
			}))
			t.Cleanup(bad.Close)
			ts := router(t, good.URL, bad.URL)

			code, _, raw := getJSONFrom(t, ts,
				"/select/logsql/stats_query_range?query=*+%7C+stats+sum%28n%29+m&step=1m&start=1&end=2")
			if code/100 == 2 {
				t.Fatalf("answered %d %s: the unreadable point was counted as zero", code, raw)
			}
			if !strings.Contains(raw, tc.want) {
				t.Errorf("the refusal does not quote what it could not read: %s", raw)
			}
		})
	}
}
