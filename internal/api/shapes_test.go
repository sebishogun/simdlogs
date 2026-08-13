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

// The response shapes below were read off the reference binary, endpoint by
// endpoint, and are asserted here because a matching status code is not
// compatibility: a differently-shaped 200 reads to a client as no data. Every
// values endpoint returns {"values":[{"value","hits"}]}; hits returns dense
// parallel arrays; stats_query returns a Prometheus vector.

func shapeServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	var body strings.Builder
	// 6 rows a minute apart: 3 error, 3 info; two services.
	for i := 0; i < 6; i++ {
		lvl, svc := "error", "api"
		if i%2 == 1 {
			lvl, svc = "info", "db"
		}
		fmt.Fprintf(&body, `{"_time":"2024-05-01T00:0%d:00Z","level":%q,"service":%q,"_msg":"m%d"}`+"\n", i, lvl, svc, i)
	}
	r, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	return ts, func() { ts.Close(); srv.Close() }
}

const shapeWindow = "&start=2024-05-01T00:00:00Z&end=2024-05-01T00:10:00Z"

type valuesResp struct {
	Values []struct {
		Value string `json:"value"`
		Hits  int    `json:"hits"`
	} `json:"values"`
}

func (v valuesResp) m() map[string]int {
	out := map[string]int{}
	for _, x := range v.Values {
		out[x.Value] = x.Hits
	}
	return out
}

func getShape(t *testing.T, url string, into any) {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("GET %s -> %d: %s", url, r.StatusCode, b)
	}
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		t.Fatalf("GET %s: decode: %v", url, err)
	}
}

// TestValuesEnvelope covers the six endpoints that share one envelope. They are
// asserted together because the whole point is that a client decodes them with
// a single type.
func TestValuesEnvelope(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()

	for _, tc := range []struct {
		name, path string
		want       map[string]int
	}{
		// _stream and _stream_id are listed because every returned record carries
		// them, even with no stream fields configured.
		{"field_names", "/select/logsql/field_names?query=*", map[string]int{
			"_msg": 6, "_time": 6, "_stream": 6, "_stream_id": 6, "level": 6, "service": 6}},
		{"field_values", "/select/logsql/field_values?query=*&field=level", map[string]int{
			"error": 3, "info": 3}},
		{"field_values filtered", "/select/logsql/field_values?query=" +
			"service%3A%3Dapi&field=level", map[string]int{"error": 3}},
		// With no stream fields configured every row is in the empty stream --
		// which is a stream, not an absence of one.
		{"streams", "/select/logsql/streams?query=*", map[string]int{"{}": 6}},
		{"stream_field_names", "/select/logsql/stream_field_names?query=*", map[string]int{}},
		{"stream_field_values", "/select/logsql/stream_field_values?query=*&field=app", map[string]int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got valuesResp
			getShape(t, ts.URL+tc.path+shapeWindow, &got)
			m := got.m()
			if len(m) != len(tc.want) {
				t.Fatalf("%s = %v, want %v", tc.path, m, tc.want)
			}
			for k, v := range tc.want {
				if m[k] != v {
					t.Fatalf("%s = %v, want %v", tc.path, m, tc.want)
				}
			}
		})
	}

	// stream_ids reports one id for the one stream, in the reference's
	// 48-hex-character form.
	var ids valuesResp
	getShape(t, ts.URL+"/select/logsql/stream_ids?query=*"+shapeWindow, &ids)
	if len(ids.Values) != 1 || ids.Values[0].Hits != 6 {
		t.Fatalf("stream_ids = %+v, want one id with 6 hits", ids.Values)
	}
	if id := ids.Values[0].Value; len(id) != 48 {
		t.Fatalf("stream id %q is %d chars, want 48", id, len(id))
	}
}

// TestHitsShape pins the dense two-array form: a client indexes timestamps and
// values together, so an empty bucket must be present with a zero, not skipped.
func TestHitsShape(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	var got struct {
		Hits []struct {
			Fields     map[string]string `json:"fields"`
			Timestamps []string          `json:"timestamps"`
			Values     []int             `json:"values"`
			Total      int               `json:"total"`
		} `json:"hits"`
	}
	getShape(t, ts.URL+"/select/logsql/hits?query=*&step=1m"+shapeWindow, &got)
	if len(got.Hits) != 1 {
		t.Fatalf("hits series = %d, want 1", len(got.Hits))
	}
	h := got.Hits[0]
	if len(h.Timestamps) != 10 || len(h.Values) != 10 {
		t.Fatalf("hits: %d timestamps, %d values, want 10 of each over a 10-minute window",
			len(h.Timestamps), len(h.Values))
	}
	if h.Total != 6 {
		t.Fatalf("hits total = %d want 6", h.Total)
	}
	for i := 0; i < 6; i++ {
		if h.Values[i] != 1 {
			t.Fatalf("hits values = %v, want 1 in each of the first six buckets", h.Values)
		}
	}
	for i := 6; i < 10; i++ {
		if h.Values[i] != 0 {
			t.Fatalf("hits values = %v, want the trailing empty buckets present as 0", h.Values)
		}
	}
	if h.Timestamps[0] != "2024-05-01T00:00:00Z" {
		t.Fatalf("hits first bucket = %q, want the window start aligned to the step", h.Timestamps[0])
	}

	// Split by a field: one series per value, each carrying its label.
	var split struct {
		Hits []struct {
			Fields map[string]string `json:"fields"`
			Values []int             `json:"values"`
			Total  int               `json:"total"`
		} `json:"hits"`
	}
	getShape(t, ts.URL+"/select/logsql/hits?query=*&step=1m&field=level"+shapeWindow, &split)
	if len(split.Hits) != 2 {
		t.Fatalf("hits by level = %d series, want 2", len(split.Hits))
	}
	for _, se := range split.Hits {
		if se.Total != 3 || se.Fields["level"] == "" {
			t.Fatalf("hits by level: series %+v, want 3 hits and a level label", se)
		}
	}
}

// TestStatsQueryVector pins the Prometheus envelope, including __name__ -- the
// label a dashboard reads as the series name.
func TestStatsQueryVector(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	var got struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	getShape(t, ts.URL+"/select/logsql/stats_query?query=*+%7C+stats+count%28%29+n"+shapeWindow, &got)
	if got.Status != "success" || got.Data.ResultType != "vector" {
		t.Fatalf("stats_query envelope = %s/%s, want success/vector", got.Status, got.Data.ResultType)
	}
	if len(got.Data.Result) != 1 {
		t.Fatalf("stats_query result = %d samples, want 1", len(got.Data.Result))
	}
	r := got.Data.Result[0]
	if r.Metric["__name__"] != "n" {
		t.Fatalf("stats_query metric = %v, want __name__=n", r.Metric)
	}
	if v, _ := r.Value[1].(string); v != "6" {
		t.Fatalf("stats_query value = %v, want \"6\"", r.Value[1])
	}

	// Grouped: one sample per group, each keeping its by-label.
	getShape(t, ts.URL+"/select/logsql/stats_query?query=*+%7C+stats+by+%28level%29+count%28%29+n"+shapeWindow, &got)
	if len(got.Data.Result) != 2 {
		t.Fatalf("grouped stats_query = %d samples, want 2", len(got.Data.Result))
	}
	for _, s := range got.Data.Result {
		if s.Metric["__name__"] != "n" || s.Metric["level"] == "" {
			t.Fatalf("grouped sample metric = %v, want __name__ and level", s.Metric)
		}
	}
}

// TestStatsQueryRangeMatrix pins __name__ on the range form and the absence of
// phantom points: a bucket with no matching rows produces no group, so it
// produces no point.
func TestStatsQueryRangeMatrix(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	var got struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	getShape(t, ts.URL+"/select/logsql/stats_query_range?query=*+%7C+stats+count%28%29+n"+
		"&start=2024-05-01T00:00:00Z&end=2024-05-01T00:10:00Z&step=1m", &got)
	if got.Data.ResultType != "matrix" || len(got.Data.Result) != 1 {
		t.Fatalf("stats_query_range = %s with %d series, want matrix with 1", got.Data.ResultType, len(got.Data.Result))
	}
	se := got.Data.Result[0]
	if se.Metric["__name__"] != "n" {
		t.Fatalf("range metric = %v, want __name__=n", se.Metric)
	}
	if len(se.Values) != 6 {
		t.Fatalf("range points = %d, want 6 (the four empty buckets carry no group)", len(se.Values))
	}
}

// TestFacetsShape pins the array-of-objects form with its own key names.
func TestFacetsShape(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	var got struct {
		Facets []struct {
			FieldName string `json:"field_name"`
			Values    []struct {
				FieldValue string `json:"field_value"`
				Hits       int    `json:"hits"`
			} `json:"values"`
		} `json:"facets"`
	}
	getShape(t, ts.URL+"/select/logsql/facets?query=*"+shapeWindow, &got)
	byField := map[string]map[string]int{}
	for _, f := range got.Facets {
		m := map[string]int{}
		for _, v := range f.Values {
			m[v.FieldValue] = v.Hits
		}
		byField[f.FieldName] = m
	}
	if byField["level"]["error"] != 3 || byField["level"]["info"] != 3 {
		t.Fatalf("facets level = %v, want error:3 info:3", byField["level"])
	}
	if byField["service"]["api"] != 3 {
		t.Fatalf("facets service = %v, want api:3", byField["service"])
	}
}

// TestQueryRowsCarryStream pins _stream and _stream_id on full records, and
// their absence from a projection -- a projection returns the fields asked for.
func TestQueryRowsCarryStream(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	rows := ndjsonRows(t, ts.URL+"/select/logsql/query?query=*&limit=2"+shapeWindow)
	if len(rows) != 2 {
		t.Fatalf("got %d rows want 2", len(rows))
	}
	for _, row := range rows {
		if row["_stream"] != "{}" {
			t.Fatalf("row _stream = %q, want {}", row["_stream"])
		}
		if len(row["_stream_id"]) != 48 {
			t.Fatalf("row _stream_id = %q, want a 48-character id", row["_stream_id"])
		}
	}
	// A projecting pipe returns exactly the projected fields.
	proj := ndjsonRows(t, ts.URL+"/select/logsql/query?query=*+%7C+fields+level"+shapeWindow)
	for _, row := range proj {
		if _, ok := row["_stream"]; ok {
			t.Fatalf("projection carried _stream: %v", row)
		}
		if len(row) != 1 || row["level"] == "" {
			t.Fatalf("projection row = %v, want only level", row)
		}
	}
}

func ndjsonRows(t *testing.T, url string) []map[string]string {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var out []map[string]string
	dec := json.NewDecoder(r.Body)
	for {
		m := map[string]string{}
		if err := dec.Decode(&m); err != nil {
			break
		}
		out = append(out, m)
	}
	return out
}

// TestFacetsFieldSelection pins the two rules that decide whether a field is a
// facet at all: a field with more distinct values than max_values_per_field is
// not one (a trace id is not a facet), and a field with a single value across
// the result is dropped unless keep_const_fields asks for it.
func TestFacetsFieldSelection(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	var body strings.Builder
	for i := 0; i < 60; i++ {
		// level: 2 values. env: 1 value (constant). trace: 60 values (unique).
		fmt.Fprintf(&body, `{"_time":"2024-05-01T00:00:%02dZ","level":%q,"env":"prod","trace":"t%d"}`+"\n",
			i, []string{"error", "info"}[i%2], i)
	}
	r, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	fields := func(query string) map[string]int {
		var got struct {
			Facets []struct {
				FieldName string `json:"field_name"`
				Values    []struct {
					FieldValue string `json:"field_value"`
					Hits       int    `json:"hits"`
				} `json:"values"`
			} `json:"facets"`
		}
		getShape(t, ts.URL+"/select/logsql/facets?query=*"+query+
			"&start=2024-05-01T00:00:00Z&end=2024-05-01T00:10:00Z", &got)
		m := map[string]int{}
		for _, f := range got.Facets {
			m[f.FieldName] = len(f.Values)
		}
		return m
	}

	// Default: level survives; env is constant and trace is unique per row, but
	// 60 distinct is under the 1000 default, so trace is kept and truncated to 10.
	def := fields("")
	if def["level"] != 2 {
		t.Fatalf("facets default = %v, want level with 2 values", def)
	}
	if _, ok := def["env"]; ok {
		t.Fatalf("facets default = %v, want the constant field env dropped", def)
	}
	if def["trace"] != 10 {
		t.Fatalf("facets default = %v, want trace truncated to the 10-value limit", def)
	}
	// keep_const_fields brings the constant back.
	if got := fields("&keep_const_fields=1"); got["env"] != 1 {
		t.Fatalf("facets keep_const_fields=1 = %v, want env present", got)
	}
	// A cardinality cap below the field's distinct count drops the field whole,
	// rather than showing an arbitrary slice of it.
	capped := fields("&max_values_per_field=5")
	if _, ok := capped["trace"]; ok {
		t.Fatalf("facets max_values_per_field=5 = %v, want trace dropped entirely", capped)
	}
	if capped["level"] != 2 {
		t.Fatalf("facets max_values_per_field=5 = %v, want level kept", capped)
	}
	// limit controls how many values are shown per surviving field.
	if got := fields("&limit=3"); got["trace"] != 3 {
		t.Fatalf("facets limit=3 = %v, want 3 trace values", got)
	}
}
