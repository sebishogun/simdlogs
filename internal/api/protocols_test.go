package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLogfmtIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `level=error service=api msg="timed out" _time=1700000000000000000
level=info service=db msg=ok _time=1700000000001000000
level=error service=api msg="also timed out" _time=1700000000002000000
`
	r, err := http.Post(ts.URL+"/insert/logfmt", "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by=level", &st)
	got := map[string]int{}
	for _, v := range st.Stats {
		got[v.Value] = v.Hits
	}
	if got["error"] != 2 || got["info"] != 1 {
		t.Fatalf("logfmt stats by level = %v want error:2 info:1", got)
	}
}

func TestLokiIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `{"streams":[
		{"stream":{"service":"api","level":"error"},"values":[["1700000000000000000","boom"],["1700000000001000000","boom2"]]},
		{"stream":{"service":"db","level":"info"},"values":[["1700000000002000000","ok"]]}
	]}`
	r, err := http.Post(ts.URL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("loki push status %d want 204", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by=level", &st)
	got := map[string]int{}
	for _, v := range st.Stats {
		got[v.Value] = v.Hits
	}
	if got["error"] != 2 || got["info"] != 1 {
		t.Fatalf("loki stats by level = %v want error:2 info:1", got)
	}
}

func TestDatadogIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Array of intake objects: ms timestamp, message->_msg, ddtags split to fields.
	body := `[
		{"message":"boom","service":"api","status":"error","ddtags":"env:prod,team:core","timestamp":1700000000000},
		{"message":"ok","service":"db","status":"info","ddtags":"env:prod","timestamp":1700000000001}
	]`
	r, err := http.Post(ts.URL+"/api/v2/logs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("datadog intake status %d want 202", r.StatusCode)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()

	statsBy := func(field string) map[string]int {
		var st struct {
			Stats []struct {
				Value string `json:"value"`
				Hits  int    `json:"hits"`
			}
		}
		getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by="+field, &st)
		m := map[string]int{}
		for _, v := range st.Stats {
			m[v.Value] = v.Hits
		}
		return m
	}
	if s := statsBy("status"); s["error"] != 1 || s["info"] != 1 {
		t.Fatalf("datadog stats by status = %v want error:1 info:1", s)
	}
	if e := statsBy("env"); e["prod"] != 2 { // ddtags split into a field
		t.Fatalf("datadog ddtags env = %v want prod:2", e)
	}
}

func TestESBulkIngest(t *testing.T) {
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	// Action/doc pairs with @timestamp; a delete carries no doc.
	bulk := `{"index":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:20Z","level":"error","service":"api"}
{"create":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:21Z","level":"info","service":"db"}
{"delete":{"_index":"logs","_id":"9"}}
{"index":{"_index":"logs"}}
{"@timestamp":"2023-11-14T22:13:22Z","level":"error","service":"auth"}
`
	r, err := http.Post(ts.URL+"/_bulk", "application/x-ndjson", strings.NewReader(bulk))
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Items []any `json:"items"`
	}
	json.NewDecoder(r.Body).Decode(&resp)
	r.Body.Close()
	if len(resp.Items) != 3 {
		t.Fatalf("bulk items = %d want 3 (delete has no doc)", len(resp.Items))
	}
	// The two error docs are found via the ES _count DSL (@timestamp mapped).
	cbody := `{"query":{"bool":{"filter":[{"term":{"level":"error"}}]}}}`
	r, _ = http.Post(ts.URL+"/_count", "application/json", strings.NewReader(cbody))
	var cnt struct{ Count int }
	json.NewDecoder(r.Body).Decode(&cnt)
	r.Body.Close()
	if cnt.Count != 2 {
		t.Fatalf("bulk _count level=error = %d want 2", cnt.Count)
	}
}
