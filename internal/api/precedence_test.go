package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strconv"
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

// A facet's VALUE is the one the row serializes, so clicking it finds the row.
//
// TestAFieldIsFacetedOnceAndTheRouterAgrees checks that `_stream` and
// `_stream_id` appear once. That is satisfied two ways: by emitting them from
// Streams/StreamIDs and skipping the stored columns (what FacetList does), and
// by keeping the stored columns and deleting the synthesized tail. Deleting
// the tail leaves the whole suite green -- so nothing distinguished
// "authoritative, whether or not a column exists" from "whatever the column
// happens to hold", and the two differ:
//
//	`_stream_id`  from the tail: present. from the column: ABSENT, in every
//	              store shape, because nothing writes that column.
//	`_stream`     from the tail: `{svc="s0"}`. from the column: "", the raw
//	              stored value, where the row serializes `{svc="s0"}`.
//
// The contract is click-through: a facet value pasted into a filter must find
// the rows it was counted from. The empty value finds nothing, which is the
// same silent-and-wrong shape as a doubled count, one step further on.
func TestAFacetValueSelectsTheRowsItCounted(t *testing.T) {
	node, router := loadedPair(t, 10)
	for _, srv := range []struct {
		name string
		ts   *httptest.Server
	}{{"node", node}, {"router", router}} {
		code, body := postRaw(t, srv.ts, "/select/logsql/facets?query=%2A", "", "")
		if code != 200 {
			t.Fatalf("%s: facets answered %d", srv.name, code)
		}
		var got struct {
			Facets []struct {
				FieldName string `json:"field_name"`
				Values    []struct {
					FieldValue string `json:"field_value"`
					Hits       int    `json:"hits"`
				} `json:"values"`
			} `json:"facets"`
		}
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("%s: facets is not JSON: %v: %.300s", srv.name, err, body)
		}
		seen := map[string][]string{}
		for _, f := range got.Facets {
			for _, v := range f.Values {
				seen[f.FieldName] = append(seen[f.FieldName], v.FieldValue)
			}
		}
		for _, name := range []string{"_stream", "_stream_id"} {
			vals := seen[name]
			if len(vals) == 0 {
				t.Errorf("%s: %s is not faceted at all, so a client cannot filter "+
					"on a field every row carries: %.300s", srv.name, name, body)
				continue
			}
			for _, v := range vals {
				if v == "" {
					t.Errorf("%s: %s is faceted with the EMPTY value, which is the "+
						"raw stored column and not what the row serializes: %.300s",
						srv.name, name, body)
					continue
				}
				// The click-through: the value must select the rows.
				q := name + `:` + strconv.Quote(v)
				qCode, qBody := postRaw(t, srv.ts, "/select/logsql/query?query="+
					url.QueryEscape(q)+"&limit=1", "", "")
				if qCode != 200 {
					t.Errorf("%s: filtering on the faceted %s value %q answered %d: %.200s",
						srv.name, name, v, qCode, qBody)
					continue
				}
				if strings.TrimSpace(qBody) == "" {
					t.Errorf("%s: the faceted %s value %q selects NO rows, though it "+
						"was counted from some", srv.name, name, v)
				}
			}
		}
	}
}

// field_names lists a synthesized name ONCE, on a store whose rows carry it.
//
// FacetList guards `_stream` against the stored column and appends
// `_stream_id` unconditionally, and field_names had the same shape: `_stream`
// guarded, `_stream_id` not. A client may send `_stream_id` as an ordinary
// field -- nothing stops it -- and then the stored column and the synthesized
// name are both listed. On a node that is a duplicate entry; a router SUMS the
// shards' counts by name, so it becomes twice the rows there are. Measured,
// six rows each carrying `_stream_id`, one shard:
//
//	node   [… {"value":"_stream_id","hits":6},{"value":"_stream_id","hits":6} …]
//	router [{"value":"_stream_id","hits":12} …]
//
// Both at HTTP 200, and the facets endpoint over the same store was already
// correct -- entry 77's defect, still live one endpoint over.
//
// The fixture SUPPLIES the field, which loadedPair's rows do not: without it
// the stored column never exists, the guard never has anything to guard
// against, and the test passes on the broken code.
func TestFieldNamesListsASynthesizedNameOnce(t *testing.T) {
	node := singleNode(t)
	shard := singleNode(t)
	rs, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() })
	rs.SetBackends([]string{shard.URL})
	rs.SetReplicas(1)
	router := httptest.NewServer(rs.Handler())
	t.Cleanup(router.Close)

	for i := 0; i < 6; i++ {
		line := fmt.Sprintf(
			`{"_time":"2026-08-16T03:00:%02dZ","_msg":"row %d","_stream_id":"deadbeef%02d","svc":"s%d"}`+"\n",
			i, i, i, i%2)
		postRaw(t, node, "/insert/jsonline?_stream_fields=svc", "application/x-ndjson", line)
		postRaw(t, shard, "/insert/jsonline?_stream_fields=svc", "application/x-ndjson", line)
	}

	const path = "/select/logsql/field_names?query=%2A"
	nCode, nBody := postRaw(t, node, path, "", "")
	rCode, rBody := postRaw(t, router, path, "", "")
	if nCode != 200 || rCode != 200 {
		t.Fatalf("node %d router %d", nCode, rCode)
	}
	for _, name := range []string{"_stream", "_stream_id", "_msg", "svc"} {
		key := `"value":"` + name + `"`
		if n := strings.Count(nBody, key); n > 1 {
			t.Errorf("a single node lists %s %d times: %.400s", name, n, nBody)
		}
		if n := strings.Count(rBody, key); n > 1 {
			t.Errorf("the router lists %s %d times: %.400s", name, n, rBody)
		}
	}
	// Six rows carry the field, so twelve is the shape of the summed duplicate.
	if strings.Contains(rBody, `"hits":12`) {
		t.Errorf("the router reports 12 hits where the cluster holds 6: %.400s", rBody)
	}
	if nBody != rBody {
		t.Errorf("the router and a single node disagree:\n  node   %.350s\n  router %.350s",
			nBody, rBody)
	}
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
			// limit=5, NOT limit=3.
			//
			// The fixture is three shards, so a router applying the URL's
			// limit=1 per shard returns 3 rows -- exactly what the body's
			// limit=3 asks for. The two answers coincided and the subtest
			// could not tell which limit had been used. Measured on the base
			// commit, against the defect this was written for:
			//
			//	url=1 body=3   node 3 rows, router 3 rows, bodies EQUAL
			//	url=1 body=4   node 4 rows, router 3 rows, DIFFER
			//	url=1 body=5   node 5 rows, router 3 rows, DIFFER
			//
			// One digit is the difference between a test and a coincidence.
			name: "query limit, urlencoded",
			path: "/select/logsql/query?query=" + url.QueryEscape("*") + "&limit=1",
			ct:   "application/x-www-form-urlencoded",
			body: "query=" + url.QueryEscape("*") + "&limit=5",
		},
		{
			// stats_query_RANGE, for the MATRIX envelope.
			//
			// federatedVector and federatedMatrix were both changed from a map
			// to a struct so the router's key order matches a node's -- a map
			// comes out of encoding/json sorted, which makes
			// {"data":…,"status":"success"} where a node writes
			// {"status":"success","data":…}. Only the vector one was exercised:
			// reverting the matrix envelope to a map left the whole suite
			// green while the router answered
			//
			//	node   {"status":"success","data":{"resultType":"matrix",…}}
			//	router {"data":{"result":…,"resultType":"matrix"},"status":"success"}
			//
			// which is what a byte comparison is for.
			name: "stats_query_range, urlencoded",
			path: "/select/logsql/stats_query_range?start=2026-08-16T03:00:00Z" +
				"&end=2026-08-16T03:01:00Z&step=30s&query=" +
				url.QueryEscape(`level:error | stats count() c`),
			ct:   "application/x-www-form-urlencoded",
			body: "query=" + url.QueryEscape(`* | stats count() c`),
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
