package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// A POST-form request keeps its parameters through the fan-out.
//
// The peer client sends r.URL.RawQuery with every request, and the read fan-out
// sends no body -- askShard's `post` is nil for every read path. So a client
// that POSTs `query=...` as a form, which the reference accepts and which is how
// anything longer than a URL is sent, had its parameters dropped on the way to
// the shards. Every federated endpoint except /select/logsql/query was affected;
// that one survives because planQuery rebuilds the shard URL from the parsed
// form itself, which is this fix written once for one endpoint.
//
// And the failure pointed the wrong way: the shards answered the EMPTY query and
// the router reported that as the shards having rejected the request, so an
// operator debugging it went to the storage nodes for a fault in the router.

// echoQueryShard records the query string every request arrived with.
func echoQueryShard(t *testing.T, seen *[]string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*seen = append(*seen, r.URL.RawQuery)
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		switch {
		case strings.Contains(r.URL.Path, "hits"):
			fmt.Fprint(w, `{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":1}]}`)
		case strings.Contains(r.URL.Path, "field_values"), strings.Contains(r.URL.Path, "field_names"):
			fmt.Fprint(w, `{"values":[{"value":"a","hits":1}]}`)
		default:
			fmt.Fprint(w, "")
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestAPostFormBodySurvivesTheFanOut(t *testing.T) {
	for _, path := range []string{
		"/select/logsql/hits",
		"/select/logsql/field_values",
		"/select/logsql/query",
	} {
		t.Run(path, func(t *testing.T) {
			var seen []string
			sh := echoQueryShard(t, &seen)
			ts := router(t, sh.URL)

			form := url.Values{"query": {"unmistakable_marker"}, "step": {"1h"}, "field": {"svc"}}
			resp, err := http.Post(ts.URL+path, "application/x-www-form-urlencoded",
				strings.NewReader(form.Encode()))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if len(seen) == 0 {
				t.Fatalf("no shard was asked at all (router answered %d %.200s)",
					resp.StatusCode, body)
			}
			for i, q := range seen {
				if !strings.Contains(q, "unmistakable_marker") {
					t.Errorf("shard request %d arrived as %q -- the POST form was dropped, "+
						"so the shard answered the EMPTY query (router said %d %.200s)",
						i, q, resp.StatusCode, body)
				}
			}
		})
	}
}

// A POST form does not overwrite the query the ROUTER built.
//
// http.Request.Clone copies Form and PostForm, so ParseForm inside the fan-out
// returned immediately with the caller's parsed form still attached, and
// replacing RawQuery with it discarded every rewrite the handler had just made.
// The same question answered differently by method:
//
//   - | stats count() c   over 2 shards of 5 rows
//     GET  -> {"c":"10"}
//     POST -> {"c":"2"}     each shard ran the whole pipeline and the
//     coordinator re-ran it over the two results
//
// Both at HTTP 200. And /select/logsql/query over a POST form was CORRECT
// before the commit that introduced this -- the fix broke the one endpoint that
// already worked.
func TestAPostFormDoesNotOverwriteTheRoutersOwnQuery(t *testing.T) {
	rows := func(n int, svc string) []string {
		var out []string
		for i := 0; i < n; i++ {
			out = append(out, `{"_time":"2024-01-01T00:00:0`+string(rune('0'+i))+`Z","_msg":"m","svc":"`+svc+`"}`)
		}
		return out
	}
	a := realShard(t, rows(5, "a"))
	b := realShard(t, rows(5, "b"))
	ts := router(t, a.URL, b.URL)

	const q = `* | stats count() c`
	_, getRows, getRaw := queryRows(t, ts, q)

	form := url.Values{"query": {q}}
	resp, err := http.Post(ts.URL+"/select/logsql/query", "application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	postBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if len(getRows) == 0 {
		t.Fatalf("the GET answered nothing: %s", getRaw)
	}
	if got := strings.TrimSpace(string(postBody)); got != strings.TrimSpace(getRaw) {
		t.Errorf("the same query answers differently by method:\n  GET  %s\n  POST %s",
			strings.TrimSpace(getRaw), got)
	}
}

// A form the router cannot parse is refused, not turned into the empty query.
//
// `return r` on a ParseForm error sent the shards a request with no query at
// HTTP 200 -- the exact behaviour this function's doc comment names as the
// reason it exists, left in place on the error path.
func TestAnUnparseableFormIsRefusedRatherThanEmptied(t *testing.T) {
	var seen []string
	sh := echoQueryShard(t, &seen)
	ts := router(t, sh.URL)

	resp, err := http.Post(ts.URL+"/select/logsql/hits?step=1h",
		"application/x-www-form-urlencoded", strings.NewReader("query=%zz"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		t.Errorf("an unparseable form answered %d: the shards were asked the empty "+
			"query (%d reached them) %.200s", resp.StatusCode, len(seen), body)
	}
}
