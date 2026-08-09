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
