package api

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// A router and a node over the SAME rows, so a difference is an answer and not
// a status code.
//
// TestEveryFederatedReadAnswersWhatASingleNodeAnswers compares status over an
// EMPTY store, which is "returns the same three digits over no data". This one
// gives both sides thirty rows and compares what they say.
func loadedPair(t *testing.T, rowsPerShard int) (node, router *httptest.Server) {
	t.Helper()
	node = singleNode(t)
	var shardURLs []string
	shards := make([]*httptest.Server, 3)
	for i := range shards {
		shards[i] = singleNode(t)
		shardURLs = append(shardURLs, shards[i].URL)
	}
	// THREE SHARDS, not three replicas of one.
	//
	// wmRouter calls SetReplicas(len(backends)), which makes them replicas --
	// so the router asks one of them and answers with a third of the data. The
	// first version of this helper used it and the "cluster" answered 10 where
	// the node answered 30, which looks exactly like the defect under test and
	// is a fixture that cannot measure it.
	rs, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() })
	rs.SetBackends(shardURLs)
	rs.SetReplicas(1)
	router = httptest.NewServer(rs.Handler())
	t.Cleanup(router.Close)

	// The node gets every row; each shard gets a third, so the cluster holds
	// the same set and a correct merge answers the same thing.
	for i := 0; i < rowsPerShard*3; i++ {
		line := fmt.Sprintf(
			`{"_time":"2026-08-16T03:00:%02dZ","_msg":"row %d","level":"%s","svc":"s%d"}`+"\n",
			i%60, i, []string{"error", "info", "warn"}[i%3], i%3)
		postRaw(t, node, "/insert/jsonline?_stream_fields=svc", "application/x-ndjson", line)
		postRaw(t, shards[i%3], "/insert/jsonline?_stream_fields=svc", "application/x-ndjson", line)
	}
	return node, router
}

// A parameter sent in BOTH the URL and the body resolves the same way on a
// router as on a node.
//
// A node's ParseForm puts PostForm before the URL query, so FormValue returns
// the BODY's value. withFormInURL merges "under the query string", so the URL's
// value reaches the shard -- a different question, HTTP 200 on both sides:
//
//	POST /select/logsql/stats_query?query=level:error | stats count() c
//	     ct urlencoded   body query=* | stats count() c
//	  node   ..."value":[…,"30"]
//	  router ..."value":[…,"10"]
//
// Under MULTIPART the two agree, because Go appends multipart values AFTER the
// URL query -- so the router implements multipart precedence for both
// encodings, and a node does not.
func TestAParameterInBothTheURLAndTheBodyResolvesTheSameWay(t *testing.T) {
	node, router := loadedPair(t, 10)
	for _, tc := range []struct{ name, path, ct, body string }{
		{
			name: "stats_query, urlencoded",
			path: "/select/logsql/stats_query?query=" + url.QueryEscape(`level:error | stats count() c`),
			ct:   "application/x-www-form-urlencoded",
			body: "query=" + url.QueryEscape(`* | stats count() c`),
		},
		{
			name: "query limit, urlencoded",
			path: "/select/logsql/query?query=" + url.QueryEscape("*") + "&limit=1",
			ct:   "application/x-www-form-urlencoded",
			body: "query=" + url.QueryEscape("*") + "&limit=3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nCode, nBody := postRaw(t, node, tc.path, tc.ct, tc.body)
			rCode, rBody := postRaw(t, router, tc.path, tc.ct, tc.body)
			if nCode != rCode {
				t.Fatalf("single node %d, router %d\n  single %.200s\n  router %.200s",
					nCode, rCode, nBody, rBody)
			}
			// The BODIES, byte for byte. A status comparison is satisfied by
			// two equally wrong sides -- both of these answered 200 while one
			// counted thirty rows and the other ten.
			if nBody != rBody {
				t.Errorf("the two resolve the duplicate parameter differently:\n"+
					"  single %.250s\n  router %.250s", nBody, rBody)
			}
		})
	}
}

// A field is faceted ONCE, and a router does not sum a node's duplicate.
//
// `_stream` and `_stream_id` are synthesized onto every record AND are stored
// columns once `_stream_fields` is configured, so FacetList emitted them twice:
// once from the column in its main loop, once from Streams/StreamIDs at the
// tail. A single node answered with two `_stream` blocks and the router summed
// the pair:
//
//	30 rows, 3 streams of 10
//	  node   "_stream" appears TWICE, 10/10/10 in each
//	  router "_stream" once, 20/20/20
//
// Both HTTP 200, and the truth is 10. The duplicate on the node is odd; the
// router's sum is a wrong number a dashboard draws.
func TestAFieldIsFacetedOnceAndTheRouterAgrees(t *testing.T) {
	node, router := loadedPair(t, 10)
	const path = "/select/logsql/facets?query=%2A"
	nCode, nBody := postRaw(t, node, path, "", "")
	rCode, rBody := postRaw(t, router, path, "", "")
	if nCode != 200 || rCode != 200 {
		t.Fatalf("node %d router %d", nCode, rCode)
	}
	for _, name := range []string{"_stream", "_stream_id", "_msg", "level", "svc"} {
		key := `"field_name":"` + name + `"`
		if n := strings.Count(nBody, key); n > 1 {
			t.Errorf("a single node facets %s %d times: %.300s", name, n, nBody)
		}
		if n := strings.Count(rBody, key); n > 1 {
			t.Errorf("the router facets %s %d times: %.300s", name, n, rBody)
		}
	}
	// 30 rows over 3 streams is 10 each, on both. A doubled block sums to 20.
	if strings.Contains(rBody, `"hits":20`) {
		t.Errorf("the router reports 20 hits where the cluster holds 10: %.400s", rBody)
	}
	if nBody != rBody {
		t.Errorf("the router and a single node disagree:\n  node   %.350s\n  router %.350s",
			nBody, rBody)
	}
}
