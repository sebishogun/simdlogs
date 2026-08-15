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

// The Elasticsearch contract.
//
// The failure this file exists to prevent is a DROPPED CLAUSE. `json.Decode`
// ignores unknown fields, so every part of the DSL this server did not
// implement became "match all" -- and a dropped filter returns MORE documents
// than the client asked for, in a response that is structurally valid. A
// client filtering `status:error` and getting every log line back cannot tell
// that from a store where everything is an error.
//
// `terms`, `must_not`, `should`, `match`, `wildcard` and every non-time
// `range` were all parsed-and-dropped. `exists` was worse: it was in the
// struct, decoded, and never read.

func esServer(t *testing.T, docs ...map[string]string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	var body strings.Builder
	for _, d := range docs {
		b, _ := json.Marshal(d)
		body.Write(b)
		body.WriteByte('\n')
	}
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return ts
}

// esSearch posts a raw DSL body and returns the status, hits.total.value and
// the number of returned hits.
func esSearchRaw(t *testing.T, ts *httptest.Server, body string) (code, total, hits int, raw string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/_search", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return resp.StatusCode, 0, 0, string(b)
	}
	var out struct {
		Hits struct {
			Total struct{ Value int } `json:"total"`
			Hits  []json.RawMessage   `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("response is not the ES shape: %s", b)
	}
	return 200, out.Hits.Total.Value, len(out.Hits.Hits), string(b)
}

func manyDocs(n int) []map[string]string {
	out := make([]map[string]string, 0, n)
	for i := 0; i < n; i++ {
		lvl := "info"
		if i%5 == 0 {
			lvl = "error"
		}
		out = append(out, map[string]string{
			"_msg": fmt.Sprintf("line %d", i), "level": lvl, "app": "svc",
		})
	}
	return out
}

// hits.total is the number of MATCHING documents, not the page size.
//
// It was len(rows) after `size` had been pushed into the scan as a Limit, so a
// search with size=10 over a hundred matches answered `"total": 10` -- and
// every ES client renders that as "10 results".
func TestESTotalIsThePreSizeTotal(t *testing.T) {
	ts := esServer(t, manyDocs(100)...)
	code, total, hits, raw := esSearchRaw(t, ts, `{"query":{"match_all":{}},"size":10}`)
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	if total != 100 {
		t.Fatalf("hits.total.value = %d, want 100 (the page held %d)", total, hits)
	}
	if hits != 10 {
		t.Fatalf("%d hits returned for size=10", hits)
	}
}

// `size` bounds the page and `from` offsets it, over the whole result.
func TestESSizeAndFromPageTheWholeResult(t *testing.T) {
	ts := esServer(t, manyDocs(50)...)
	_, total, hits, raw := esSearchRaw(t, ts, `{"query":{"match_all":{}},"size":5,"from":45}`)
	if total != 50 || hits != 5 {
		t.Fatalf("total %d hits %d, want 50 and 5: %s", total, hits, raw)
	}
	// Past the end is an empty page, not an error and not a wrapped one.
	_, total, hits, raw = esSearchRaw(t, ts, `{"query":{"match_all":{}},"size":5,"from":500}`)
	if total != 50 || hits != 0 {
		t.Fatalf("total %d hits %d past the end, want 50 and 0: %s", total, hits, raw)
	}
	// No size at all is Elasticsearch's default of 10, not every document.
	_, total, hits, raw = esSearchRaw(t, ts, `{"query":{"match_all":{}}}`)
	if total != 50 || hits != 10 {
		t.Fatalf("total %d hits %d with no size, want 50 and 10: %s", total, hits, raw)
	}
}

// The clauses that used to be silently dropped now filter.
func TestESClausesThatUsedToBeDroppedNowFilter(t *testing.T) {
	docs := []map[string]string{
		{"_msg": "a", "level": "error", "host": "h1"},
		{"_msg": "b", "level": "warn", "host": "h2"},
		{"_msg": "c", "level": "info", "host": "h1"},
		{"_msg": "d", "level": "info"}, // no host at all
	}
	ts := esServer(t, docs...)

	for _, tc := range []struct {
		name  string
		body  string
		total int
	}{
		{"term", `{"query":{"term":{"level":"error"}}}`, 1},
		{"terms", `{"query":{"terms":{"level":["error","warn"]}}}`, 2},
		{"match", `{"query":{"match":{"host":"h1"}}}`, 2},
		{"exists", `{"query":{"exists":{"field":"host"}}}`, 3},
		{"prefix", `{"query":{"prefix":{"level":{"value":"in"}}}}`, 2},
		{"must_not", `{"query":{"bool":{"must_not":[{"term":{"level":"info"}}]}}}`, 2},
		{"should", `{"query":{"bool":{"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}}}`, 2},
		{"must+must_not", `{"query":{"bool":{"must":[{"exists":{"field":"host"}}],` +
			`"must_not":[{"term":{"level":"error"}}]}}}`, 2},
		{"match_all", `{"query":{"match_all":{}}}`, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d: %s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("total = %d, want %d: %s", total, tc.total, raw)
			}
		})
	}
}

// An unsupported clause is a 400 naming it, never a filter dropped on the
// floor. This is the whole point: a client learns its query was not applied.
func TestESUnsupportedClausesAre400(t *testing.T) {
	ts := esServer(t, manyDocs(10)...)
	for _, tc := range []struct{ name, body string }{
		{"unknown top-level field", `{"query":{"match_all":{}},"aggs":{"x":{"terms":{"field":"level"}}}}`},
		{"unknown clause", `{"query":{"wildcard":{"level":"err*"}}}`},
		{"unknown bool member", `{"query":{"bool":{"must":[],"boost":2}}}`},
		{"non-time range", `{"query":{"range":{"level":{"gte":"a"}}}}`},
		{"minimum_should_match", `{"query":{"bool":{"should":[{"term":{"level":"info"}}],` +
			`"minimum_should_match":2}}}`},
		{"exists with no field", `{"query":{"exists":{"field":""}}}`},
		{"negative size", `{"query":{"match_all":{}},"size":-1}`},
		{"malformed json", `{"query":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 400 {
				t.Fatalf("%d, want 400: %s", code, raw)
			}
		})
	}
}

// A time range still drives the window, which is what makes an ES time filter
// as cheap here as a LogsQL one.
func TestESTimeRangeStillDrivesTheWindow(t *testing.T) {
	ts := esServer(t, manyDocs(20)...)
	// A window in 1970 matches nothing; the documents were stamped at ingest.
	body := `{"query":{"range":{"@timestamp":{"gte":"1970-01-01T00:00:00Z",` +
		`"lt":"1970-01-02T00:00:00Z"}}},"size":100}`
	code, total, _, raw := esSearchRaw(t, ts, body)
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	if total != 0 {
		t.Fatalf("total = %d in a 1970 window, want 0", total)
	}
	// And a window that covers now matches everything.
	body = `{"query":{"range":{"@timestamp":{"gte":"2000-01-01T00:00:00Z",` +
		`"lt":"2100-01-01T00:00:00Z"}}},"size":100}`
	_, total, _, raw = esSearchRaw(t, ts, body)
	if total != 20 {
		t.Fatalf("total = %d in a covering window, want 20: %s", total, raw)
	}
}

// _count runs the same mapping, and a malformed body is a 400 rather than a
// count of the whole store -- its decode error used to be discarded entirely.
func TestESCountRejectsWhatSearchRejects(t *testing.T) {
	ts := esServer(t, manyDocs(30)...)

	post := func(body string) (int, int, string) {
		resp, err := http.Post(ts.URL+"/_count", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return resp.StatusCode, 0, string(b)
		}
		var out struct {
			Count int `json:"count"`
		}
		json.Unmarshal(b, &out)
		return 200, out.Count, string(b)
	}

	if code, n, raw := post(`{"query":{"term":{"level":"error"}}}`); code != 200 || n != 6 {
		t.Fatalf("count = %d (%d), want 6: %s", n, code, raw)
	}
	if code, n, raw := post(`{"query":`); code != 400 {
		t.Fatalf("a malformed body counted %d with status %d: %s", n, code, raw)
	}
	if code, _, raw := post(`{"query":{"wildcard":{"level":"e*"}}}`); code != 400 {
		t.Fatalf("an unsupported clause returned %d: %s", code, raw)
	}
}
