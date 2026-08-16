package api

import (
	"github.com/sebishogun/simdlogs/internal/query"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		{"field_name":"host","values":[{"field_value":"h1","hits":7},{"field_value":"h2","hits":1}]}]}`
	var saw string
	a := facetShard(t, &saw, body)
	ts := router(t, a.URL)

	// Every field here has two or more distinct values, so none is dropped by
	// the const rule and what is left to measure is the limit.
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

// A row carrying a field named _stream_id does not come back with the key
// twice.
//
// _stream_id is synthesized from stream membership and is not a reserved
// ingest name, so a client can send one. appendRowJSON skipped a row's own
// _time (which it emits canonically) and had no equivalent guard for
// _stream_id, so the object carried the key twice: Go's json.Unmarshal takes
// the last, a first-wins parser takes the other, and a single node gave a
// third answer -- three results for one query, all 200.
func TestARowWithItsOwnStreamIDDoesNotDuplicateTheKey(t *testing.T) {
	row := queryRowWithStreamID()
	line := appendRowJSON(nil, row, true)

	if n := strings.Count(string(line), `"_stream_id"`); n != 1 {
		t.Fatalf("the encoded row carries _stream_id %d times: %s", n, line)
	}
	// The ROW's value survives, and the synthesis is what is skipped.
	//
	// The other way round -- skipping the field -- makes a cluster and a node
	// disagree on the VALUE rather than on the key count: a shard runs the
	// bare `*` with withStream true, so the ingested value would be dropped at
	// the shard and never cross the wire, while a node's projection (withStream
	// false, nothing synthesized) keeps it.
	if !strings.Contains(string(line), "CLIENT-SUPPLIED") {
		t.Errorf("the row's own _stream_id was dropped in favour of the "+
			"synthesized one, so a cluster and a node return different values "+
			"for the same row: %s", line)
	}
	plain := appendRowJSON(nil, row, false)
	if !strings.Contains(string(plain), "CLIENT-SUPPLIED") {
		t.Errorf("without the synthesized pair the row's own field vanished: %s", plain)
	}
	if string(plain) == string(line) {
		// Not required, but if they were identical the withStream flag would
		// be doing nothing here and this test would not be measuring it.
		t.Logf("note: withStream made no difference for this row: %s", line)
	}

	// A row with NO _stream_id of its own still gets the synthesized pair,
	// which is what the field means to a client grouping by stream.
	plainRow := query.Row{Time: 1, Fields: []query.Field{{Key: "_msg", Value: "a"}}}
	if got := appendRowJSON(nil, plainRow, true); !strings.Contains(string(got), `"_stream_id"`) {
		t.Errorf("a row with no _stream_id of its own lost the synthesized one: %s", got)
	}
}

// queryRowWithStreamID is a row carrying a client-supplied _stream_id.
func queryRowWithStreamID() query.Row {
	return query.Row{
		Time: 1767225600000000000,
		Fields: []query.Field{
			{Key: "_msg", Value: "a"},
			{Key: "_stream_id", Value: "CLIENT-SUPPLIED"},
		},
	}
}

// A field constant across the whole cluster is dropped, the way a node drops
// one constant across its store.
//
// REGRESSION introduced by the facets fix: the shards are sent
// `keep_const_fields=1` so that a field constant on ONE shard survives to be
// judged over the union -- and the coordinator applied only the cardinality
// half of facetKeep, never the const half. Measured, two nodes of six rows
// against one node of twelve, `?query=*`: the cluster returned seven fields
// and the node returned four.
func TestAFieldConstantAcrossTheClusterIsDropped(t *testing.T) {
	// Both shards see `svc` as the same single value; `level` varies.
	body := func(svc string) string {
		return `{"facets":[
			{"field_name":"level","values":[{"field_value":"a","hits":5},{"field_value":"b","hits":4}]},
			{"field_name":"svc","values":[{"field_value":"` + svc + `","hits":9}]}]}`
	}
	var s1, s2 string
	a := facetShard(t, &s1, body("api"))
	b := facetShard(t, &s2, body("api"))
	ts := router(t, a.URL, b.URL)

	_, got, raw := getJSONFrom(t, ts, "/select/logsql/facets?query=*")
	names := facetNames(got)
	if len(names) != 1 || names[0] != "level" {
		t.Errorf("cluster returned %v; svc is one value across the whole cluster "+
			"and a node would drop it: %s", names, raw)
	}

	// keep_const_fields=1 keeps it, which is what the parameter is for.
	_, got, raw = getJSONFrom(t, ts, "/select/logsql/facets?query=*&keep_const_fields=1")
	if names = facetNames(got); len(names) != 2 {
		t.Errorf("with keep_const_fields=1 the cluster returned %v, want both: %s",
			names, raw)
	}

	// A field constant on EACH shard and varied across them is kept, which is
	// why the shards are asked to keep theirs.
	var t1, t2 string
	c := facetShard(t, &t1, body("api"))
	d := facetShard(t, &t2, body("web"))
	ts2 := router(t, c.URL, d.URL)
	_, got, raw = getJSONFrom(t, ts2, "/select/logsql/facets?query=*")
	if names = facetNames(got); len(names) != 2 {
		t.Errorf("svc is constant on each shard and varied across them, so the "+
			"cluster must keep it; got %v: %s", names, raw)
	}
}

func facetNames(got map[string]any) []string {
	var out []string
	for _, f := range got["facets"].([]any) {
		out = append(out, f.(map[string]any)["field_name"].(string))
	}
	return out
}

// A truncated shard response is refused, including when the cut lands right
// after a nested value's closing brace.
//
// looksLikeJSONObject first checked only the first and last byte, so a row
// with a nested value cut after that value's `}` ended in the right character
// and went to the client as a row. It balances the braces now, string-aware.
func TestATruncatedShardLineIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		refuse     bool
	}{
		{"cut mid-string", `{"_time":"2026-01-01T00:00:00Z","_msg":"hel`, true},
		{"cut after a nested brace", `{"_time":"2026-01-01T00:00:00Z","ctx":{"a":1}`, true},
		{"brace inside a string", `{"_msg":"a } b"}`, false},
		{"nested and whole", `{"_msg":"a","ctx":{"a":1}}`, false},
		{"trailing bytes", `{"_msg":"a"} extra`, true},
		// The shape two reviewers constructed independently: a line missing
		// its OWN closing brace but ending in one belonging to a nested value.
		// A first-and-last-byte check reads that as a whole object.
		{"nested brace as the last byte", `{"_time":"2026-01-01T00:00:00Z","ctx":{"a":1}`, true},
		{"brace from a string as the last byte", `{"_msg":"a }`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := goodShard(t)
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeEnvelope(w.Header(), 0, 0, true, 1, "")
				w.Write([]byte(tc.body))
			}))
			t.Cleanup(bad.Close)
			ts := router(t, good.URL, bad.URL)

			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", "/select/logsql/query?query=*", "GET", "",
			})
			refused := resp.StatusCode/100 != 2
			if refused != tc.refuse {
				t.Errorf("answered %d for %s; want refused=%v.\n  body: %s",
					resp.StatusCode, tc.body, tc.refuse, truncate(body, 200))
			}
		})
	}
}

// A row whose _stream field is the EMPTY STRING does not get a second one.
//
// The _stream guard tested the VALUE (`if stream == ""`) and the _stream_id
// guard tested PRESENCE, four lines apart in one function. So an empty
// _stream read as "no _stream" and a second pair was synthesized: the object
// carried the key twice, json.Unmarshal took the synthesized value and a
// first-wins parser took the empty one -- the identical divergence the
// _stream_id fix closed, still open beside it.
func TestAnEmptyStreamFieldDoesNotDuplicateTheKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields []query.Field
	}{
		{"empty _stream", []query.Field{{Key: "_msg", Value: "a"}, {Key: "_stream", Value: ""}}},
		{"non-empty _stream", []query.Field{{Key: "_msg", Value: "a"}, {Key: "_stream", Value: "{h=1}"}}},
		{"no _stream at all", []query.Field{{Key: "_msg", Value: "a"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := appendRowJSON(nil, query.Row{Time: 1, Fields: tc.fields}, true)
			if n := strings.Count(string(line), `"_stream"`); n != 1 {
				t.Errorf("_stream appears %d times: %s", n, line)
			}
			if n := strings.Count(string(line), `"_stream_id"`); n != 1 {
				t.Errorf("_stream_id appears %d times: %s", n, line)
			}
		})
	}
}

// A malformed line in the MIDDLE of a shard body is refused, not only the last
// one.
//
// The check was briefly restricted to the last line, on the reasoning that a
// response is truncated at its end. Two reviewers pointed out independently
// that it is a well-formedness check rather than a truncation check, and
// malformation is not end-only -- and that truncation is caught a layer above
// anyway, because a cut HTTP response fails io.ReadAll and is classified
// PeerUnavailable before mergeRows sees it. So the restriction dropped the
// only case that could reach here.
func TestAMalformedMiddleLineIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"middle line ends in a nested brace",
			`{"_time":"2026-01-01T00:00:00Z","_msg":"ok","ctx":{"a":1}` + "\n" +
				`{"_time":"2026-01-01T00:00:01Z","_msg":"second"}` + "\n"},
		{"two objects on one middle line",
			`{"_msg":"a"}{"_msg":"b"}` + "\n" + `{"_msg":"c"}` + "\n"},
		{"malformed line then a blank line",
			`{"_time":"2026-01-01T00:00:00Z","ctx":{"a":1}` + "\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := goodShard(t)
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeEnvelope(w.Header(), 0, 0, true, 1, "")
				w.Write([]byte(tc.body))
			}))
			t.Cleanup(bad.Close)
			ts := router(t, good.URL, bad.URL)

			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", "/select/logsql/query?query=*", "GET", "",
			})
			if resp.StatusCode/100 == 2 {
				t.Errorf("answered %d: a malformed line that is not the last one "+
					"reached the client.\n  body: %s", resp.StatusCode, truncate(body, 200))
			}
		})
	}
}

// `max_values_per_field=0` means unlimited to _time as well as to every other
// field.
//
// timeFacet read `<= 0` as defaultFacetMaxValues while facetKeep's own guard
// reads it as unlimited, so `0` meant two different things in the two
// functions that consume it. The coordinator sends the shards `0` to get their
// whole distribution -- and that bounded each shard's _time scan to 1000 ROWS,
// not distinct values, so _time vanished from every cluster facet answer on
// any tenant with more than 1000 matching rows per shard. The rest of the
// answer being right made it more plausible, not less.
func TestTheTimeFacetTreatsZeroAsUnlimited(t *testing.T) {
	var saw string
	// 1200 rows over two distinct timestamps: far past the old 1000-row bound,
	// and only two values, so a distinct-value cap would keep it.
	body := `{"facets":[{"field_name":"_time","values":[
		{"field_value":"2026-06-01T12:00:00Z","hits":600},
		{"field_value":"2026-06-01T12:00:01Z","hits":600}]},
		{"field_name":"level","values":[{"field_value":"a","hits":700},{"field_value":"b","hits":500}]}]}`
	a := facetShard(t, &saw, body)
	ts := router(t, a.URL)

	_, got, raw := getJSONFrom(t, ts,
		"/select/logsql/facets?query=*&max_values_per_field=5000&limit=5000")
	names := facetNames(got)
	found := false
	for _, n := range names {
		if n == "_time" {
			found = true
		}
	}
	if !found {
		t.Errorf("_time is missing from %v for a caller asking for 5000 values "+
			"per field: %s", names, raw)
	}
	if saw != "0" {
		t.Errorf("the shard was sent max_values_per_field=%q, want \"0\"", saw)
	}
}

// The store-aware pipes are refused at a coordinator, not answered from the
// merged rows.
//
// Each of these reads a storage node's own layout or index. A coordinator has
// no store, so `apply` -- the fallback for the non-leading case -- ran over the
// merged WIRE rows and produced a plausible number:
//
//   - | blocks_count   node {"blocks_count":"2"}   router {"blocks_count":"12"}
//     len(rows), which is a ROW count
//   - | block_stats    node two block-stat rows    router all 12 log rows
//     apply returns its input unchanged, a silent no-op
//   - | field_names    node 7 names                router 8, including
//     _stream_id, which the SHARD synthesized on the wire
//
// ApplyPipes already refused join, union and stream_context with the reasoning
// that skipping a store-aware pipe "would answer the query as if the pipe were
// not there, which is the silent-wrong-answer shape this whole area is about".
// These five were not in that list and did exactly that.
func TestStoreAwarePipesAreRefusedAtACoordinator(t *testing.T) {
	good := goodShard(t)
	ts := router(t, good.URL, goodShard(t).URL)

	for _, tc := range []struct{ name, pipe, wantIn string }{
		{"blocks_count", "blocks_count", "no store"},
		{"block_stats", "block_stats", "no store"},
		{"field_names", "field_names", "endpoint"},
		{"field_values", "field_values level", "endpoint"},
		{"facets", "facets", "endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A leading filter forces a coordinator half: the pipe is not the
			// first stage, so the node's own fast path is not what answers.
			q := url.QueryEscape("* | " + tc.pipe)
			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				"", "/select/logsql/query?query=" + q, "GET", "",
			})
			if resp.StatusCode/100 == 2 {
				t.Fatalf("answered %d with %q: a coordinator has no store, so this "+
					"was computed from the merged wire rows",
					resp.StatusCode, truncate(body, 200))
			}
			if !strings.Contains(body, tc.wantIn) {
				t.Errorf("the refusal does not say why (%q missing): %q",
					tc.wantIn, truncate(body, 250))
			}
		})
	}
}
