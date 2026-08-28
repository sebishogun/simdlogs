package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func ingestCorpus(t *testing.T, ts *httptest.Server, n int) int {
	var body strings.Builder
	base := int64(1_700_000_000_000_000_000)
	errs := 0
	for i := 0; i < n; i++ {
		lvl := "info"
		if i%10 == 0 {
			lvl = "error"
			errs++
		}
		svc := []string{"api", "auth", "db"}[i%3]
		fmt.Fprintf(&body, `{"_time":%d,"level":%q,"service":%q,"_msg":"m%d"}`+"\n",
			base+int64(i)*1000, lvl, svc, i)
	}
	r, _ := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	return errs
}

func TestSelectSurface(t *testing.T) {
	t.Parallel()
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	errs := ingestCorpus(t, ts, 30_000)

	// field_names includes the ingested columns, each with its hit count -- the
	// {"values":[{"value","hits"}]} envelope every values endpoint returns.
	var fn struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/field_names?query=*", &fn)
	names := map[string]int{}
	for _, v := range fn.Values {
		names[v.Value] = v.Hits
	}
	if names["level"] != 30_000 || names["service"] != 30_000 {
		t.Fatalf("field_names = %v, want level and service at 30000 hits", names)
	}
	// field_values for level has info+error.
	var fv struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/field_values?query=*&field=level", &fv)
	got := map[string]int{}
	for _, v := range fv.Values {
		got[v.Value] = v.Hits
	}
	if got["error"] != errs {
		t.Fatalf("field_values level error=%d want %d", got["error"], errs)
	}
	// stats_query by service sums to 30000.
	var st struct {
		Stats []struct {
			Value string `json:"value"`
			Hits  int    `json:"hits"`
		}
	}
	getJSON(t, ts.URL+"/select/logsql/stats_query?query=*&by=service", &st)
	total := 0
	for _, v := range st.Stats {
		total += v.Hits
	}
	if total != 30_000 {
		t.Fatalf("stats by service total=%d want 30000", total)
	}
}

func TestESSearch(t *testing.T) {
	t.Parallel()
	srv, _ := NewServer(t.TempDir())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	errs := ingestCorpus(t, ts, 20_000)

	// _count with a bool/term on level:error.
	body := `{"query":{"bool":{"filter":[{"term":{"level":"error"}}]}}}`
	r, _ := http.Post(ts.URL+"/_count", "application/json", bytes.NewReader([]byte(body)))
	var cnt struct{ Count int }
	json.NewDecoder(r.Body).Decode(&cnt)
	r.Body.Close()
	if cnt.Count != errs {
		t.Fatalf("_count error = %d want %d", cnt.Count, errs)
	}
	// _search returns hits.total matching.
	r, _ = http.Post(ts.URL+"/_search", "application/json", bytes.NewReader([]byte(body)))
	var res struct {
		Hits struct {
			Total struct{ Value int } `json:"total"`
		} `json:"hits"`
	}
	json.NewDecoder(r.Body).Decode(&res)
	r.Body.Close()
	if res.Hits.Total.Value != errs {
		t.Fatalf("_search total = %d want %d", res.Hits.Total.Value, errs)
	}
}

func getJSON(t *testing.T, url string, v any) {
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
