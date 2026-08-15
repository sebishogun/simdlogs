package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The wrong answers reviewer B reproduced against a live two-shard cluster.
// Each one came back HTTP 200 and looked like a smaller right answer.

// One malformed, unrelated query parameter used to change the answer.
//
// withoutLimits gave up silently on a query string it could not parse and
// forwarded the caller's `limit` to every shard, so the cluster's top-N was
// rebuilt from shard-local top-Ns -- the exact failure its own doc comment
// exists to prevent. `&x=%zz` was enough.
func TestAMalformedQueryStringIsRefusedNotIgnored(t *testing.T) {
	good := goodShard(t)
	ts := router(t, good.URL, goodShard(t).URL)

	for _, path := range []string{
		"/select/logsql/field_values?query=*&field=k&limit=5&x=%zz",
		"/select/logsql/facets?query=*&limit=5&x=%zz",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", path, "GET", "",
			})
			if resp.StatusCode/100 == 2 {
				t.Fatalf("answered %d with %q: the limit was forwarded to the shards, "+
					"so the cluster's top values are built from each shard's own top "+
					"values", resp.StatusCode, truncate(body, 200))
			}
			if !strings.Contains(body, "query string") {
				t.Errorf("the refusal does not say what it could not read: %q",
					truncate(body, 200))
			}
		})
	}
}

// facetShard answers /select/logsql/facets, recording what limits it was sent.
func facetShard(t *testing.T, sawLimit *string, body string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		if strings.Contains(r.URL.Path, "facets") {
			*sawLimit = r.URL.Query().Get("limit")
			w.Write([]byte(body))
			return
		}
		w.Write([]byte(`{"values":[]}`))
	}))
	t.Cleanup(ts.Close)
	return ts
}

// The shards are asked for an unlimited distribution, which means limit=0 and
// not an absent parameter.
//
// facets reads it as intParam(r, "limit", DefaultFacetLimit), so ABSENT means
// 10. Deleting the parameter left every shard truncating to its own top 10 and
// the merge summing those, which is what withoutLimits exists to prevent.
func TestFacetsAsksTheShardsForEverything(t *testing.T) {
	var sawA, sawB string
	a := facetShard(t, &sawA, `{"facets":[]}`)
	b := facetShard(t, &sawB, `{"facets":[]}`)
	ts := router(t, a.URL, b.URL)

	if _, _, raw := getJSONFrom(t, ts, "/select/logsql/facets?query=*&limit=3"); raw == "" {
		t.Fatal("no body")
	}
	for i, saw := range []string{sawA, sawB} {
		if saw != "0" {
			t.Errorf("shard %d was sent limit=%q; it must be \"0\" (unlimited), "+
				"because absent means the shard's own default of %d", i, saw, 10)
		}
	}
}

// `limit` means the same thing on a cluster as on a node: values within a
// field, not fields.
//
// It truncated FIELDS at the coordinator, so `?limit=2` answered five fields
// of top-2 values on one node and two fields of every value on a cluster --
// and the fields anyone is actually faceting on were the ones that vanished.
func TestTheFacetLimitTruncatesValuesNotFields(t *testing.T) {
	body := `{"facets":[
		{"field_name":"level","values":[{"field_value":"a","hits":5},{"field_value":"b","hits":4},{"field_value":"c","hits":3}]},
		{"field_name":"svc","values":[{"field_value":"x","hits":9},{"field_value":"y","hits":2}]},
		{"field_name":"host","values":[{"field_value":"h1","hits":7}]}]}`
	var saw string
	a := facetShard(t, &saw, body)
	ts := router(t, a.URL)

	_, got, raw := getJSONFrom(t, ts, "/select/logsql/facets?query=*&limit=2")
	facets, _ := got["facets"].([]any)
	if len(facets) != 3 {
		t.Fatalf("limit=2 kept %d of 3 fields; it truncates VALUES, not fields: %s",
			len(facets), raw)
	}
	for _, f := range facets {
		m := f.(map[string]any)
		if vals, _ := m["values"].([]any); len(vals) > 2 {
			t.Errorf("field %v kept %d values under limit=2", m["field_name"], len(vals))
		}
	}
}

// The federated ES search validates its body the way a single node does.
//
// It decoded into `want` and discarded the error, so `{"from":-1,"size":3}` --
// which a single node rejects with 400 -- came back 200 with the WRONG
// DOCUMENTS: need = from+size = 2 made each shard return two hits, rows 2-5
// were never fetched, and "total" still said they existed.
func TestTheFederatedESSearchValidatesLikeASingleNode(t *testing.T) {
	good := goodShard(t)
	ts := router(t, good.URL, goodShard(t).URL)

	for _, body := range []string{
		`{"query":{"match_all":{}},"from":-1,"size":3}`,
		`{"query":{"match_all":{}},"size":-1}`,
		`{"query":{"match_all":{}},"nosuchfield":1}`,
	} {
		t.Run(body, func(t *testing.T) {
			resp, got := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", "/_search", "POST", body,
			})
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("answered %d for %s, want 400 -- which is what a single node "+
					"answers: %s", resp.StatusCode, body, truncate(got, 200))
			}
		})
	}
}

// The primary read path refuses a line that is not a row.
//
// mergeRows carried every non-empty line through, so a proxy's HTML error page
// in a 200 body came back to the caller AS A LOG LINE. mergeDecode covers the
// eight envelope merges; this path had nothing.
func TestARowEndpointRefusesALineThatIsNotARow(t *testing.T) {
	good := goodShard(t)
	html := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		w.Write([]byte("<html><body>502 Bad Gateway</body></html>"))
	}))
	t.Cleanup(html.Close)
	ts := router(t, good.URL, html.URL)

	for _, path := range []string{
		"/select/logsql/query?query=*",
		"/select/sql?query=SELECT+%2A+FROM+logs",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", path, "GET", "",
			})
			if resp.StatusCode/100 == 2 {
				t.Fatalf("answered %d: a shard's line was not a row and the read "+
					"continued%s", resp.StatusCode,
					map[bool]string{true: ", with its error page in the body as data"}[strings.Contains(body, "<html>")])
			}
		})
	}
}

// A bucket outside the range nanoseconds can represent is refused, not wrapped.
//
// time.Parse accepts any year; UnixNano is undefined outside 1677-09-21 ..
// 2262-04-11 and wraps silently. 2600-01-01 became 2015-06-13, and its count
// was filed on -- and summed into -- a real 2015 bucket.
func TestAnUnrepresentableBucketIsRefused(t *testing.T) {
	good := hitsShard(t, []string{"2026-01-01T00:00:00Z"}, []int{1})
	far := hitsShard(t, []string{"2600-01-01T00:00:00Z"}, []int{5})
	ts := router(t, good.URL, far.URL)

	resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
		"", "/select/logsql/hits?query=*&step=1s&start=1&end=2", "GET", "",
	})
	if resp.StatusCode/100 == 2 {
		t.Fatalf("answered %d with %q: the 2600 bucket wrapped to 2015 and was "+
			"summed into a real one", resp.StatusCode, truncate(body, 300))
	}
	if !strings.Contains(body, "2600") {
		t.Errorf("the refusal does not quote the timestamp: %q", truncate(body, 300))
	}
}

// A refused read is not counted or marked as a returned partial answer.
//
// notePartialRead fired in fanOutChecked before the merge could refuse, so a
// 502 arrived at the operator as an answer the router "knowingly returned
// incomplete", with X-Simdlogs-Partial: true on it telling the client the same.
func TestARefusedReadIsNotCountedAsPartial(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	good := goodShard(t)
	bad := garbageShard(t)
	srv.SetBackends([]string{good.URL, bad.URL, deadShard(t)})
	srv.SetReplicas(1)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	before := PartialReads()
	req, _ := http.NewRequest("GET", ts.URL+"/_count?allow_partial_response=1", strings.NewReader(""))
	req.Method = "POST"
	req.Body = io.NopCloser(strings.NewReader(`{"query":{"match_all":{}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		t.Fatalf("answered %d %q, want a refusal", resp.StatusCode, truncate(string(b), 200))
	}
	if got := resp.Header.Get(HdrPartial); got != "" {
		t.Errorf("a refused read carries %s: %q; the client is told it is holding "+
			"a partial answer that was never returned", HdrPartial, got)
	}
	if after := PartialReads(); after != before {
		t.Errorf("partial_reads went %d -> %d for a read that was REFUSED; that "+
			"counter is what an operator alerts on", before, after)
	}
}

// A cluster backup with an unreachable replica is refused, not taken short.
//
// completeReplica builds its union from reachable replicas only, so a replica
// that did not answer can never make the chosen source look short. Measured: a
// row written only to replica 2 vanished from the archive when replica 2 was
// down, at HTTP 200, in a tar that passes validation.
func TestABackupIsRefusedWhenAReplicaIsUnreachable(t *testing.T) {
	_, router, nodes := clusterOf(t, 1, 2)
	if code, _ := takeClusterBackup(t, router); code != http.StatusOK {
		t.Fatalf("a backup with both replicas up answered %d", code)
	}
	nodes[0][1].Close() // replica 1 of shard 0 is gone

	code, body := takeClusterBackup(t, router)
	if code/100 == 2 {
		t.Fatalf("answered %d with a %d-byte archive: a replica was never asked "+
			"and nothing in the archive says so", code, len(body))
	}
	if !strings.Contains(string(body), "unreachable") {
		t.Errorf("the refusal does not say a replica was unreachable: %q",
			truncate(string(body), 300))
	}
}
