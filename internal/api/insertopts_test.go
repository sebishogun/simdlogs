package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/ingest"
)

// A shipper -- Filebeat, Vector, Fluent Bit, Promtail -- is configured against
// the reference with these query args, and sends them on every write. Ignoring
// them is not cosmetic: the agent's message lands in a field nothing searches
// and its timestamp is replaced by ingest time. Each case below is what such an
// agent actually sends.

func optServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	return ts, func() { ts.Close(); srv.Close() }
}

func postTo(t *testing.T, url, ct, body string) {
	t.Helper()
	r, err := http.Post(url, ct, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	if r.StatusCode >= 400 {
		t.Fatalf("POST %s -> %d", url, r.StatusCode)
	}
}

func TestInsertFieldMappings(t *testing.T) {
	ts, done := optServer(t)
	defer done()

	const args = "?_time_field=ts&_msg_field=message&_stream_fields=app&ignore_fields=drop_me&extra_fields=env=prod"
	postTo(t, ts.URL+"/insert/jsonline"+args, "application/x-ndjson",
		`{"ts":"2024-05-01T00:00:00Z","message":"hello world","app":"api","drop_me":"secret"}`+"\n")

	rows := ndjsonRows(t, ts.URL+"/select/logsql/query?query=*"+shapeWindow)
	if len(rows) != 1 {
		t.Fatalf("got %d rows want 1", len(rows))
	}
	row := rows[0]
	if row["_msg"] != "hello world" {
		t.Errorf("_msg_field ignored: _msg = %q, message = %q", row["_msg"], row["message"])
	}
	if _, ok := row["message"]; ok {
		t.Errorf("the renamed field was left behind: %v", row)
	}
	// _time_field: the record's own timestamp, not ingest time.
	if row["_time"] != "2024-05-01T00:00:00Z" {
		t.Errorf("_time_field ignored: _time = %q", row["_time"])
	}
	if _, ok := row["ts"]; ok {
		t.Errorf("the time field was stored as data too: %v", row)
	}
	if _, ok := row["drop_me"]; ok {
		t.Errorf("ignore_fields ignored: %v", row)
	}
	if row["env"] != "prod" {
		t.Errorf("extra_fields ignored: env = %q", row["env"])
	}
	if row["_stream"] != `{app="api"}` {
		t.Errorf("_stream_fields ignored: _stream = %q", row["_stream"])
	}

	// The stream is queryable as a stream, not just as a string on the row.
	var streams valuesResp
	getShape(t, ts.URL+"/select/logsql/streams?query=*"+shapeWindow, &streams)
	if m := streams.m(); m[`{app="api"}`] != 1 {
		t.Errorf("streams = %v, want the configured stream", m)
	}
}

// TestInsertFieldMappingsEveryProtocol checks the mappings are not a
// jsonline-only feature: the reference accepts them on every insert endpoint,
// and a shipper picks the endpoint, not the mapping.
func TestInsertFieldMappingsEveryProtocol(t *testing.T) {
	const args = "?ignore_fields=drop_me&extra_fields=env=prod"
	for _, tc := range []struct{ name, path, ct, body string }{
		{"jsonline", "/insert/jsonline", "application/x-ndjson",
			`{"_time":"2024-05-01T00:00:00Z","_msg":"m","drop_me":"x"}` + "\n"},
		{"elasticsearch", "/insert/elasticsearch/_bulk", "application/x-ndjson",
			"{\"create\":{}}\n{\"@timestamp\":\"2024-05-01T00:00:00Z\",\"_msg\":\"m\",\"drop_me\":\"x\"}\n"},
		{"logfmt", "/insert/logfmt", "text/plain",
			"_time=2024-05-01T00:00:00Z _msg=m drop_me=x\n"},
		{"loki", "/insert/loki/api/v1/push", "application/json",
			`{"streams":[{"stream":{"drop_me":"x"},"values":[["1714521600000000000","m"]]}]}`},
		{"datadog", "/insert/datadog/api/v2/logs", "application/json",
			`[{"message":"m","ddsource":"s","drop_me":"x","timestamp":1714521600000}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, done := optServer(t)
			defer done()
			postTo(t, ts.URL+tc.path+args, tc.ct, tc.body)
			rows := ndjsonRows(t, ts.URL+"/select/logsql/query?query=*")
			if len(rows) == 0 {
				t.Fatalf("%s ingested nothing", tc.name)
			}
			for _, row := range rows {
				if _, ok := row["drop_me"]; ok {
					t.Errorf("%s: ignore_fields ignored: %v", tc.name, row)
				}
				if row["env"] != "prod" {
					t.Errorf("%s: extra_fields ignored: %v", tc.name, row)
				}
			}
		})
	}
}

// TestStatsQueryInstantTime covers the reference's `time` parameter: an instant
// query names the end of its window with it, and a client that sends only
// `time` must not be answered from the whole store.
func TestStatsQueryInstantTime(t *testing.T) {
	ts, done := optServer(t)
	defer done()
	var body strings.Builder
	for i := 0; i < 6; i++ {
		body.WriteString(`{"_time":"2024-05-01T00:0` + string(rune('0'+i)) + `:00Z","level":"error"}` + "\n")
	}
	postTo(t, ts.URL+"/insert/jsonline", "application/x-ndjson", body.String())

	count := func(q string) string {
		var got struct {
			Data struct {
				Result []struct {
					Value [2]any `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		getShape(t, ts.URL+"/select/logsql/stats_query?query=*+%7C+stats+count%28%29+n"+q, &got)
		if len(got.Data.Result) == 0 {
			return "0"
		}
		v, _ := got.Data.Result[0].Value[1].(string)
		return v
	}
	// time= cuts the window at 00:02:30 -> the three rows at :00, :01, :02.
	if got := count("&time=2024-05-01T00:02:30Z"); got != "3" {
		t.Errorf("stats_query&time= counted %s, want 3 -- the instant was ignored", got)
	}
	if got := count("&time=2024-05-01T00:10:00Z"); got != "6" {
		t.Errorf("stats_query&time= counted %s, want 6", got)
	}
}

// TestValuesLimit covers the `limit` every values endpoint honours.
func TestValuesLimit(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	for _, path := range []string{
		"/select/logsql/field_names?query=*&limit=2",
		"/select/logsql/field_values?query=*&field=level&limit=1",
	} {
		var got valuesResp
		getShape(t, ts.URL+path+shapeWindow, &got)
		want := 2
		if strings.Contains(path, "limit=1") {
			want = 1
		}
		if len(got.Values) != want {
			t.Errorf("%s returned %d values, want %d", path, len(got.Values), want)
		}
	}
}

// TestQueryArgRequired pins that `query` is required. Defaulting it to match-all
// answered a client's bug -- a dropped parameter -- with the entire store.
func TestQueryArgRequired(t *testing.T) {
	ts, done := shapeServer(t)
	defer done()
	for _, ep := range []string{
		"query", "hits", "facets", "field_names", "field_values",
		"streams", "stream_ids", "stream_field_names", "stream_field_values",
		"stats_query",
	} {
		for _, q := range []string{"", "?query=", "?query=%20"} {
			r, err := http.Get(ts.URL + "/select/logsql/" + ep + q)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if r.StatusCode < 400 {
				t.Errorf("/select/logsql/%s%q -> %d, want a client error", ep, q, r.StatusCode)
			}
		}
	}
}

// TestHitsFieldsLimit pins the "other" bucket: the busiest N series are kept and
// the tail is merged into one unlabelled series, rather than a series per value.
func TestHitsFieldsLimit(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	var body strings.Builder
	// four hosts with clearly different volumes: h0 x8, h1 x4, h2 x2, h3 x1
	for i, n := range []int{8, 4, 2, 1} {
		for j := 0; j < n; j++ {
			body.WriteString(`{"_time":"2024-05-01T00:00:00Z","host":"h` + string(rune('0'+i)) + `"}` + "\n")
		}
	}
	postTo(t, ts.URL+"/insert/jsonline", "application/x-ndjson", body.String())

	var got struct {
		Hits []struct {
			Fields map[string]string `json:"fields"`
			Total  int               `json:"total"`
		} `json:"hits"`
	}
	getShape(t, ts.URL+"/select/logsql/hits?query=*&step=1m&field=host&fields_limit=2"+shapeWindow, &got)
	if len(got.Hits) != 3 {
		t.Fatalf("fields_limit=2 returned %d series, want 2 plus the remainder", len(got.Hits))
	}
	if got.Hits[0].Fields["host"] != "h0" || got.Hits[0].Total != 8 {
		t.Errorf("first series = %+v, want the busiest (h0, 8)", got.Hits[0])
	}
	last := got.Hits[2]
	if len(last.Fields) != 0 {
		t.Errorf("remainder series carries labels %v, want none", last.Fields)
	}
	if last.Total != 3 { // h2 + h3
		t.Errorf("remainder total = %d, want 3", last.Total)
	}
}

// TestVLMetricNames covers the metrics a dashboard written for the reference
// reads. Each must carry a REAL value: a metric emitted as a hardcoded zero is
// worse for an operator than a panel with no data, because it looks like an
// answer.
func TestVLMetricNames(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	postTo(t, ts.URL+"/insert/jsonline", "application/x-ndjson",
		`{"_time":"2024-05-01T00:00:00Z","level":"error"}`+"\n"+
			`{"_time":"2024-05-01T00:00:01Z","level":"info"}`+"\n"+
			"not json at all\n")
	// One request that errors, so the error counter has something to report.
	r, err := http.Get(ts.URL + "/select/logsql/query")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	r, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	body := string(b)

	for _, want := range []string{
		"vl_rows_ingested_total 2",
		"vl_rows_dropped_total 1",
		"vl_http_errors_total 1",
		"vl_live_tailing_requests 0",
		"vl_storage_rows 2",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	// These are gauges whose value depends on the machine; assert only that they
	// are present and not the fabricated zero.
	for _, name := range []string{"vl_bytes_ingested_total", "vl_data_size_bytes", "vl_free_disk_space_bytes"} {
		if !strings.Contains(body, "\n"+name+" ") {
			t.Errorf("/metrics missing %s", name)
			continue
		}
		if strings.Contains(body, "\n"+name+" 0\n") {
			t.Errorf("%s reported 0, which is not a measurement", name)
		}
	}
}

// TestOTLPProtobufOverHTTP covers the collector's DEFAULT configuration:
// protobuf posted to the OTLP path must store, and the response must mirror the
// request's encoding. The JSON parser fed this body stored nothing and answered
// 200, which is data loss a client cannot see.
func TestOTLPProtobufOverHTTP(t *testing.T) {
	ts, done := optServer(t)
	defer done()

	body := ingest.BuildTestOTLPProtoExport()
	req, _ := http.NewRequest("POST", ts.URL+"/insert/opentelemetry/v1/logs", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("protobuf OTLP -> %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "protobuf") {
		t.Errorf("response Content-Type = %q, want the request's encoding mirrored", ct)
	}
	if len(b) != 0 {
		t.Errorf("protobuf success body = %d bytes, want an empty ExportLogsServiceResponse", len(b))
	}

	rows := ndjsonRows(t, ts.URL+"/select/logsql/query?query=*&start=2023-11-14T00:00:00Z&end=2023-11-15T00:00:00Z")
	if len(rows) != 2 {
		t.Fatalf("stored %d records from the protobuf export, want 2", len(rows))
	}
	byMsg := map[string]map[string]string{}
	for _, r := range rows {
		byMsg[r["_msg"]] = r
	}
	first := byMsg["boom happened"]
	if first == nil {
		t.Fatalf("the first record's body did not arrive: %v", rows)
	}
	for k, want := range map[string]string{
		"severity": "ERROR", "service.name": "api", "host": "h1",
		"code": "500", "ratio": "0.5", "retry": "true",
	} {
		if first[k] != want {
			t.Errorf("record field %s = %q want %q", k, first[k], want)
		}
	}
	if byMsg["second"] == nil {
		t.Errorf("the record timed by observed_time_unix_nano did not arrive")
	}
}
