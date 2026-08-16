package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Merge fixtures: what a storage node ACTUALLY returns, merged by the router.
//
// The LLD has carried "the router's merge code for four endpoints is stale --
// it decodes envelopes the backends no longer send and answers bogus/empty
// results" since this campaign began. These are the fixtures that pin it. Each
// one is the exact body a storage node emits today, so a merge written against
// a remembered shape fails here rather than in a cluster.
//
// The rule this file enforces: a router's answer for a path has the same SHAPE
// as a storage node's answer for that path. A client must not have to know
// which one it is talking to.

// fixtureShard serves a fixed body per path with a complete envelope.
func fixtureShard(t *testing.T, bodies map[string]string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeEnvelope(w.Header(), 0, 0, true, 1, "gen-test", "")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts
}

func getJSONFrom(t *testing.T, ts *httptest.Server, path string) (int, map[string]any, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(b, &out)
	return resp.StatusCode, out, string(b)
}

// A single node's answer for each of these paths, captured from the handlers:
// every one of them goes through writeValues, which emits {"values": [...]}.
const (
	valuesA = `{"values":[{"value":"alpha","hits":3},{"value":"shared","hits":2}]}`
	valuesB = `{"values":[{"value":"beta","hits":5},{"value":"shared","hits":4}]}`
)

// The shared-values endpoints: the router must read the key the backend sends
// and answer with the key a client expects -- the same one.
func TestSharedValueEnvelopesMergeAcrossShards(t *testing.T) {
	for _, path := range []string{
		"/select/logsql/streams",
		"/select/logsql/stream_ids",
		"/select/logsql/field_names",
		"/select/logsql/field_values",
		"/select/logsql/stream_field_names",
		"/select/logsql/stream_field_values",
	} {
		t.Run(path, func(t *testing.T) {
			a := fixtureShard(t, map[string]string{path: valuesA})
			b := fixtureShard(t, map[string]string{path: valuesB})
			ts := router(t, a.URL, b.URL)

			q := path + "?query=*&field=level"
			code, got, raw := getJSONFrom(t, ts, q)
			if code != 200 {
				t.Fatalf("%d: %s", code, raw)
			}
			vals, ok := got["values"].([]any)
			if !ok {
				t.Fatalf("the router answers a different SHAPE than a storage node "+
					"(which emits {\"values\":...}): %s", raw)
			}
			if len(vals) != 3 {
				t.Fatalf("%d merged values, want 3 (alpha, beta, shared): %s", len(vals), raw)
			}
			// `shared` appears on both shards and must be SUMMED, not listed
			// twice: a value's count is a cluster-wide count.
			byValue := map[string]float64{}
			for _, v := range vals {
				m := v.(map[string]any)
				byValue[fmt.Sprint(m["value"])] = m["hits"].(float64)
			}
			if byValue["shared"] != 6 {
				t.Errorf("shared = %v, want 6 (2+4): %s", byValue["shared"], raw)
			}
			if byValue["alpha"] != 3 || byValue["beta"] != 5 {
				t.Errorf("per-shard values wrong: %v", byValue)
			}
		})
	}
}

// The limit is applied AFTER the merge, over the cluster-wide list.
//
// Applied per backend and then merged, `limit=2` over three shards returns up
// to six values -- and the two it keeps from each shard are that shard's top
// two, which is not the cluster's top two.
func TestValueLimitsApplyAfterTheMerge(t *testing.T) {
	const path = "/select/logsql/field_values"
	a := fixtureShard(t, map[string]string{path: valuesA})
	b := fixtureShard(t, map[string]string{path: valuesB})
	ts := router(t, a.URL, b.URL)

	code, got, raw := getJSONFrom(t, ts, path+"?query=*&field=level&limit=2")
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	vals := got["values"].([]any)
	if len(vals) != 2 {
		t.Fatalf("%d values with limit=2, want 2: %s", len(vals), raw)
	}
	// The cluster's top two by count: shared (6) then beta (5).
	first := vals[0].(map[string]any)
	if fmt.Sprint(first["value"]) != "shared" {
		t.Errorf("the top value is %v, want shared (6 cluster-wide): %s", first["value"], raw)
	}
}

// Dense hit series merge by label set and timestamp, not by concatenation.
func TestHitSeriesMergeByLabelsAndTimestamps(t *testing.T) {
	const path = "/select/logsql/hits"
	a := fixtureShard(t, map[string]string{path: `{"hits":[
	  {"fields":{"level":"error"},"timestamps":["2026-01-01T00:00:00Z","2026-01-01T00:01:00Z"],"values":[1,2],"total":3},
	  {"fields":{"level":"info"},"timestamps":["2026-01-01T00:00:00Z"],"values":[10],"total":10}]}`})
	b := fixtureShard(t, map[string]string{path: `{"hits":[
	  {"fields":{"level":"error"},"timestamps":["2026-01-01T00:01:00Z","2026-01-01T00:02:00Z"],"values":[5,7],"total":12}]}`})
	ts := router(t, a.URL, b.URL)

	code, got, raw := getJSONFrom(t, ts,
		path+"?query=*&step=1m&start=1767225600000000000&end=1767225780000000000")
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	series := got["hits"].([]any)
	if len(series) != 2 {
		t.Fatalf("%d series, want 2 (one per label set, not one per shard): %s",
			len(series), raw)
	}
	var errSeries map[string]any
	for _, s := range series {
		m := s.(map[string]any)
		if f, _ := m["fields"].(map[string]any); fmt.Sprint(f["level"]) == "error" {
			errSeries = m
		}
	}
	if errSeries == nil {
		t.Fatalf("the error series is missing: %s", raw)
	}
	ts2 := errSeries["timestamps"].([]any)
	vs := errSeries["values"].([]any)
	if len(ts2) != len(vs) {
		t.Fatalf("timestamps and values are not parallel: %d and %d", len(ts2), len(vs))
	}
	if len(ts2) != 3 {
		t.Fatalf("%d buckets, want 3 (00:00, 00:01, 00:02): %s", len(ts2), raw)
	}
	// The shared bucket 00:01 is summed: 2 from A, 5 from B.
	byTime := map[string]float64{}
	for i, tv := range ts2 {
		byTime[fmt.Sprint(tv)] = vs[i].(float64)
	}
	if byTime["2026-01-01T00:01:00Z"] != 7 {
		t.Errorf("the shared bucket is %v, want 7 (2+5): %s",
			byTime["2026-01-01T00:01:00Z"], raw)
	}
	if total, _ := errSeries["total"].(float64); total != 15 {
		t.Errorf("total = %v, want 15 (3+12)", total)
	}
}

// stats_query_range series with identical labels merge rather than appearing
// twice. Two series with the same labels is not a valid matrix: a Prometheus
// client renders them as two lines and every point is drawn twice.
func TestStatsRangeMergesIdenticalLabels(t *testing.T) {
	const path = "/select/logsql/stats_query_range"
	body := func(v1, v2 string) string {
		return fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[
		  {"metric":{"level":"error"},"values":[[1767225600,"%s"],[1767225660,"%s"]]}]}}`, v1, v2)
	}
	a := fixtureShard(t, map[string]string{path: body("1", "2")})
	b := fixtureShard(t, map[string]string{path: body("10", "20")})
	ts := router(t, a.URL, b.URL)

	code, got, raw := getJSONFrom(t, ts, path+"?query=*&step=1m&start=1&end=2")
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	data := got["data"].(map[string]any)
	result := data["result"].([]any)
	if len(result) != 1 {
		t.Fatalf("%d series with identical labels, want 1 merged: %s", len(result), raw)
	}
	vals := result[0].(map[string]any)["values"].([]any)
	if len(vals) != 2 {
		t.Fatalf("%d points, want 2: %s", len(vals), raw)
	}
	// Each timestamp appears once, with the shards summed.
	first := vals[0].([]any)
	if fmt.Sprint(first[1]) != "11" {
		t.Errorf("the first point is %v, want 11 (1+10): %s", first[1], raw)
	}
}

// An ES _search across shards reports the cluster-wide total and obeys size
// over the merged hits.
func TestESSearchMergesTotalsAndSize(t *testing.T) {
	const path = "/_search"
	a := fixtureShard(t, map[string]string{path: `{"hits":{"total":{"value":40,"relation":"eq"},
	  "hits":[{"_source":{"_msg":"a1"}},{"_source":{"_msg":"a2"}}]}}`})
	b := fixtureShard(t, map[string]string{path: `{"hits":{"total":{"value":60,"relation":"eq"},
	  "hits":[{"_source":{"_msg":"b1"}}]}}`})
	ts := router(t, a.URL, b.URL)

	resp, err := http.Post(ts.URL+"/_search", "application/json",
		strings.NewReader(`{"query":{"match_all":{}},"size":2}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var got struct {
		Hits struct {
			Total struct{ Value int } `json:"total"`
			Hits  []any               `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("not the ES shape: %s", raw)
	}
	// The total is the CLUSTER's matching documents, not one shard's and not
	// the page size.
	if got.Hits.Total.Value != 100 {
		t.Errorf("total = %d, want 100 (40+60): %s", got.Hits.Total.Value, raw)
	}
	if len(got.Hits.Hits) != 2 {
		t.Errorf("%d hits with size=2, want 2: %s", len(got.Hits.Hits), raw)
	}
}

// A router answers the same Content-Type a storage node does. A client must
// not have to know which it is talking to.
func TestRouterPreservesContentType(t *testing.T) {
	a := fixtureShard(t, map[string]string{"/select/logsql/streams": valuesA})
	ts := router(t, a.URL)
	resp, err := http.Get(ts.URL + "/select/logsql/streams?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}
