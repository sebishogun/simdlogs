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
