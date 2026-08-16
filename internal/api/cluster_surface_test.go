package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Every route, in router mode: federated, router-local by design, or refused.
//
// # The failure this looks for
//
// A select-router has no data of its own. Its store is empty and stays empty.
// Only a handler that KNOWS it is a router fans out; every other handler runs
// against that empty local store and answers successfully with nothing --
// `{"values":[]}`, `{"hits":{"total":0}}`, an empty NDJSON body. A 200 with an
// empty answer is indistinguishable from a cluster that genuinely holds no
// matching data, so nothing anywhere reports a problem.
//
// Counting `len(s.backends) > 0` branches gives 14 of 46 routes. This test does
// not take that list on trust: it sends the same request to a router and to a
// storage node that HAS the data, and fails when the storage node answers with
// something and the router answers with nothing.
//
// # The three labels
//
//   - federated: the router must answer what the cluster holds.
//   - routerLocal: the answer is about this process, not the data. /health,
//     /metrics, /flags. An empty or local answer is correct.
//   - refused: not federatable in this build. It must say so with a status a
//     client can act on -- never 200 and nothing.

type surfaceKind int

const (
	federated surfaceKind = iota
	routerLocal
	refused
)

type surfaceRoute struct {
	path   string
	method string
	query  string
	body   string
	ctype  string
	kind   surfaceKind
	// needsStream marks a request that only returns anything when the rows
	// carry a stream label, so the fixture has to configure stream fields.
	// Without it the storage node answers empty too and the comparison proves
	// nothing -- the subtest skips and reads as covered.
	needsStream bool
	// write marks an ingest route. Writes are checked by
	// TestARouterForwardsWritesAndStoresNothing, not by comparing read answers.
	// Marked explicitly: they used to fall out of the read test because their
	// responses look empty, which is indistinguishable from a route nobody
	// remembered to cover.
	write bool
	// why documents a refused or router-local classification. Required for
	// both, so no route can be quietly downgraded to "not our problem".
	why string
}

// The route surface. Every path registered in server.go appears exactly once;
// TestEverySurfaceRouteIsClassified proves it.
func surfaceRoutes() []surfaceRoute {
	const q = "query=%2A"
	return []surfaceRoute{
		// Reads that must answer for the whole cluster.
		{path: "/select/logsql/query", method: "GET", query: q, kind: federated},
		{path: "/select/logsql/hits", method: "GET",
			query: q + "&step=1h&start=2026-06-01T00:00:00Z&end=2026-06-02T00:00:00Z",
			kind:  federated},
		{path: "/select/logsql/facets", method: "GET", query: q, kind: federated},
		{path: "/select/logsql/field_names", method: "GET", query: q, kind: federated},
		{path: "/select/logsql/field_values", method: "GET", query: q + "&field=level", kind: federated},
		{path: "/select/logsql/stream_field_names", method: "GET", query: q, kind: federated,
			needsStream: true},
		{path: "/select/logsql/stream_field_values", method: "GET", query: q + "&field=host",
			kind: federated, needsStream: true},
		{path: "/select/logsql/streams", method: "GET", query: q, kind: federated},
		{path: "/select/logsql/stream_ids", method: "GET", query: q, kind: federated},
		{path: "/select/logsql/stats_query", method: "GET", query: q + "%20%7C%20stats%20count%28%29%20c", kind: federated},
		{path: "/select/logsql/stats_query_range", method: "GET",
			query: q + "%20%7C%20stats%20count%28%29%20c&step=1h", kind: federated},
		{path: "/_search", method: "POST", body: `{"query":{"match_all":{}}}`,
			ctype: "application/json", kind: federated},
		{path: "/_count", method: "POST", body: `{"query":{"match_all":{}}}`,
			ctype: "application/json", kind: federated},
		{path: "/select/sql", method: "GET", query: "query=SELECT%20%2A%20FROM%20logs", kind: federated},

		// Reads whose shape does not federate in this build.
		{path: "/select/logsql/tail", method: "GET", query: q, kind: refused,
			why: "a live tail is a long-lived stream from every shard merged by " +
				"arrival time; the merge has no completeness signal and one slow " +
				"shard silently drops out of the stream"},
		{path: "/select/vector", method: "GET", query: q + "&field=v&vector=1,2", kind: refused,
			why: "a k-nearest-neighbour search over shards needs each shard's top " +
				"k merged by distance; returning one shard's neighbours or " +
				"concatenating them both answer a different question"},

		// Writes: forwarded to the shards, not stored here.
		{path: "/insert/jsonline", method: "POST", body: "{\"_msg\":\"x\"}\n",
			ctype: "application/x-ndjson", kind: federated, write: true},
		{path: "/insert/logfmt", method: "POST", body: "_msg=x\n",
			ctype: "text/plain", kind: federated, write: true},
		{path: "/insert/elasticsearch/_bulk", method: "POST",
			body: "{\"create\":{}}\n{\"_msg\":\"x\"}\n", ctype: "application/x-ndjson", kind: federated, write: true},
		{path: "/_bulk", method: "POST", body: "{\"create\":{}}\n{\"_msg\":\"x\"}\n",
			ctype: "application/x-ndjson", kind: federated, write: true},
		{path: "/insert/loki/api/v1/push", method: "POST",
			body:  `{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x"]]}]}`,
			ctype: "application/json", kind: federated, write: true},
		{path: "/loki/api/v1/push", method: "POST",
			body:  `{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x"]]}]}`,
			ctype: "application/json", kind: federated, write: true},
		{path: "/insert/datadog/api/v2/logs", method: "POST",
			body: `[{"message":"x"}]`, ctype: "application/json", kind: federated, write: true},
		{path: "/api/v2/logs", method: "POST", body: `[{"message":"x"}]`,
			ctype: "application/json", kind: federated, write: true},
		{path: "/insert/opentelemetry/v1/logs", method: "POST",
			body: `{"resourceLogs":[]}`, ctype: "application/json", kind: federated, write: true},
		{path: "/v1/logs", method: "POST", body: `{"resourceLogs":[]}`,
			ctype: "application/json", kind: federated, write: true},
		{path: "/insert/journald", method: "POST", body: "MESSAGE=x\n",
			ctype: "application/vnd.fdo.journal", kind: federated, write: true},
		{path: "/insert/syslog", method: "POST", body: "<13>1 - - - - - - x\n",
			ctype: "text/plain", kind: federated, write: true},
		{path: "/v1/input", method: "POST", body: "{\"_msg\":\"x\"}\n",
			ctype: "application/x-ndjson", kind: federated, write: true},
		{path: "/insert/datadog/api/v1/validate", method: "GET", kind: routerLocal,
			why: "a credential check; it answers about this process's auth config"},

		// About this process, not about the data.
		{path: "/health", method: "GET", kind: routerLocal,
			why: "this process's own liveness"},
		{path: "/-/healthy", method: "GET", kind: routerLocal,
			why: "this process's own liveness"},
		{path: "/-/ready", method: "GET", kind: routerLocal,
			why: "this process's own readiness; cluster completeness rides the " +
				"read path's own headers"},
		{path: "/insert/ready", method: "GET", kind: routerLocal,
			why: "whether this process will accept writes"},
		{path: "/metrics", method: "GET", kind: routerLocal,
			why: "this process's counters; a cluster view is the scraper's job"},
		{path: "/flags", method: "GET", kind: routerLocal,
			why: "this process's configuration"},
		{path: "/alerts", method: "GET", kind: routerLocal,
			why: "rules evaluated by this process"},
		{path: "/vmui", method: "GET", kind: routerLocal, why: "static assets"},
		{path: "/select/vmui", method: "GET", kind: routerLocal, why: "static assets"},
		{path: "/", method: "GET", kind: routerLocal, why: "the index page"},

		// Anti-entropy. State and group are what a PEER calls on a storage
		// node; on a router they describe the router's own empty store, which
		// is true and useless, so they are router-local rather than federated.
		{path: pathReplicaState, method: "GET", kind: routerLocal,
			why: "one replica's own inventory; the router's cluster-wide view is " +
				"/admin/cluster/repair"},
		{path: pathReplicaGroup, method: "GET", query: "digest=deadbeef", kind: routerLocal,
			why: "one replica's own group bytes, addressed by content"},
		{path: "/admin/cluster/backup", method: "POST", kind: routerLocal,
			why: "the router's own coordinated capture of every shard; it is the " +
				"cluster-wide form and has nothing further to federate to"},
		{path: "/admin/cluster/repair", method: "POST", kind: routerLocal,
			why: "the router's own cluster-wide repair pass; it has no meaning to " +
				"federate further"},

		// Admin: coordinated cluster forms are task 8.7. Until then a router
		// must refuse, because the local answer is about an empty store.
		{path: "/admin/backup", method: "POST", kind: refused,
			why: "a router's backup of its own empty store would restore as an " +
				"empty cluster; the coordinated form is task 8.7"},
		{path: "/admin/acknowledge-degraded", method: "POST", kind: refused,
			why: "acknowledging degradation on the router clears nothing on the " +
				"shards that are actually degraded"},
		{path: "/admin/storage/quarantine", method: "GET", kind: refused,
			why: "a router's own store quarantines nothing, so an empty list " +
				"there reads as `nothing is wrong` about shards it never asked"},
	}
}

// Every registered route is classified, and every classification names a real
// route.
//
// Without this, adding a handler and forgetting to classify it means the new
// route is simply not covered -- and an uncovered route is exactly one that
// reads the router's empty local store with nobody watching.
func TestEverySurfaceRouteIsClassified(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Handler() // registers the routes
	registered := map[string]bool{}
	for _, p := range srv.registeredPaths() {
		registered[p] = true
	}
	classified := map[string]bool{}
	for _, r := range surfaceRoutes() {
		if classified[r.path] {
			t.Errorf("%s is classified twice", r.path)
		}
		classified[r.path] = true
		if !registered[r.path] {
			t.Errorf("%s is classified but not registered by the server", r.path)
		}
		if r.kind != federated && strings.TrimSpace(r.why) == "" {
			t.Errorf("%s is not federated and gives no reason", r.path)
		}
	}
	for path := range registered {
		if !classified[path] {
			t.Errorf("%s is registered but not classified: it may be reading the "+
				"router's empty local store", path)
		}
	}
}

// No router-mode read answers 200-and-empty where a storage node holding the
// data answers with something.
func TestNoRouterReadSilentlyReadsTheEmptyLocalStore(t *testing.T) {
	rows := corpus(1)[0]
	shard := realShard(t, rows)
	cluster := router(t, shard.URL)
	streamShard := streamingShard(t, rows)
	streamCluster := router(t, streamShard.URL)

	for _, rt := range surfaceRoutes() {
		if rt.kind != federated || rt.write {
			continue // writes: TestARouterForwardsWritesAndStoresNothing
		}
		t.Run(rt.path, func(t *testing.T) {
			node, front := shard, cluster
			if rt.needsStream {
				node, front = streamShard, streamCluster
			}
			sCode, sBody := surfaceCall(t, node, rt)
			cCode, cBody := surfaceCall(t, front, rt)
			if sCode != 200 {
				t.Skipf("the storage node cannot answer this request either: %d %.120s",
					sCode, sBody)
			}
			if cCode != 200 {
				t.Fatalf("storage node 200, router %d: %.200s", cCode, cBody)
			}
			if looksEmpty(sBody) {
				t.Skipf("the storage node's own answer is empty, so this request "+
					"proves nothing here: %.120s", sBody)
			}
			if looksEmpty(cBody) {
				t.Fatalf("the router answered 200 with nothing while the storage node "+
					"answered with data.\n  storage: %.200s\n  router:  %.200s",
					sBody, cBody)
			}
		})
	}
}

// A refused surface says so. Never 200-and-empty, which a client reads as "the
// cluster holds nothing".
func TestARefusedSurfaceSaysSo(t *testing.T) {
	rows := corpus(1)[0]
	cluster := router(t, realShard(t, rows).URL)

	for _, rt := range surfaceRoutes() {
		if rt.kind != refused {
			continue
		}
		t.Run(rt.path, func(t *testing.T) {
			code, body := surfaceCall(t, cluster, rt)
			if code == 200 {
				t.Fatalf("answered 200 in router mode; it must refuse with the reason "+
					"(%s): %.200s", rt.why, body)
			}
			if code < 400 || code >= 600 {
				t.Fatalf("status %d is neither success nor an error a client can act on", code)
			}
			if !strings.Contains(strings.ToLower(body), "cluster") &&
				!strings.Contains(strings.ToLower(body), "router") &&
				!strings.Contains(strings.ToLower(body), "shard") {
				t.Errorf("the refusal does not say it is about running as a router: %.200s", body)
			}
		})
	}
}

func surfaceCall(t *testing.T, srv *httptest.Server, rt surfaceRoute) (int, string) {
	t.Helper()
	u := srv.URL + rt.path
	if rt.query != "" {
		u += "?" + rt.query
	}
	// Bounded. /select/logsql/tail is a live stream that never ends by itself,
	// so an unbounded client here hangs the package until `go test -timeout`
	// kills it -- which is how this test first ran for 240 seconds and reported
	// nothing.
	cl := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(rt.method, u, strings.NewReader(rt.body))
	if err != nil {
		t.Fatalf("%s %s: %v", rt.method, rt.path, err)
	}
	if rt.method == "POST" {
		ct := rt.ctype
		if ct == "" {
			ct = "application/json"
		}
		req.Header.Set("Content-Type", ct)
	}
	resp, err := cl.Do(req)
	if err != nil {
		// A timeout on a streaming endpoint is an answer: it did not refuse.
		t.Fatalf("%s %s: %v", rt.method, rt.path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b)
}

// looksEmpty reports whether a successful body carries no data.
//
// Deliberately syntactic: the point is to catch a 200 whose body is an empty
// container, whatever the endpoint's shape, without teaching this test every
// envelope in the API.
func looksEmpty(body string) bool {
	s := strings.TrimSpace(body)
	if s == "" {
		return true
	}
	for _, empty := range []string{
		`{}`, `[]`, `{"values":[]}`, `{"streams":[]}`, `{"stream_ids":[]}`,
		`{"hits":[]}`, `{"count":0}`, `{"facets":[]}`, `{"rows":[]}`,
	} {
		if strings.EqualFold(strings.ReplaceAll(s, " ", ""), empty) {
			return true
		}
	}
	// A JSON envelope whose only array is empty, e.g. {"status":"success",
	// "data":{"result":[]}} -- the Prometheus shape, which is what the matrix
	// and vector endpoints answer.
	compact := strings.ReplaceAll(s, " ", "")
	if strings.Contains(compact, `"result":[]`) || strings.Contains(compact, `"hits":{"total":{"value":0`) ||
		strings.Contains(compact, `"values":[]`) || strings.Contains(compact, `"total":0`) {
		return true
	}
	return false
}

// streamingShard is a storage node whose rows carry a real stream label.
//
// realShard configures no stream fields, so every row lands in the empty stream
// and the stream_field_* endpoints answer nothing -- on the storage node as
// well as the router. A comparison between two empty answers passes without
// touching the code it is supposed to be testing.
func streamingShard(t *testing.T, rows []string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv.SetStreamFields([]string{"host", "service"})
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	var b strings.Builder
	for i, row := range rows {
		// The stream fields have to be ON the rows, and with more than one
		// value, or "the distinct values of this stream field" is a single
		// entry that any merge produces by accident.
		b.WriteString(strings.TrimSuffix(row, "}"))
		b.WriteString(`,"host":"node-` + string(rune('0'+i%3)) + `","service":"api"}` + "\n")
	}
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return ts
}

// A router forwards writes to the shards and stores nothing itself.
//
// The claim the write rows in surfaceRoutes() rest on. Without it those rows
// are classified `federated` and never checked, which is the same as not
// classifying them: a write handler that fell through to the local store would
// accept the rows, return 200, and put them somewhere no read ever looks --
// because reads fan out to the shards, and the shards never got them.
func TestARouterForwardsWritesAndStoresNothing(t *testing.T) {
	shard := realShard(t, nil)
	cluster := router(t, shard.URL)

	const line = `{"_time":"2026-06-01T12:00:00Z","_msg":"written through the router","k":"v"}`
	resp, err := http.Post(cluster.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(line+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("the router refused the write: %d %.200s", resp.StatusCode, b)
	}

	// The shard has it, asked directly.
	code, rows, raw := queryRows(t, shard, `k:=v`)
	if code != 200 || len(rows) != 1 {
		t.Fatalf("the shard does not have the row the router accepted: %d %d rows %.200s",
			code, len(rows), raw)
	}

	// And exactly once through the router: a router that ALSO stored the row
	// locally would return it twice, once from its own store and once from the
	// shard -- which is what a write handler falling through to the local store
	// looks like from outside.
	code, rows, raw = queryRows(t, cluster, `k:=v`)
	if code != 200 || len(rows) != 1 {
		t.Fatalf("reading back through the router: %d %d rows %.200s", code, len(rows), raw)
	}
	if strings.Count(raw, `"written through the router"`) != 1 {
		t.Fatalf("the row came back %d times, so the router stored a copy of its own: %.300s",
			strings.Count(raw, `"written through the router"`), raw)
	}
}

// Every write route forwards to the shards in router mode, and the router
// keeps nothing.
//
// One route proving it is not enough: each ingest handler decides for itself
// whether to forward or to write locally, so the property has to be checked per
// route. A handler that wrote locally would answer 200 and put the rows where
// no read will ever look, because reads fan out to the shards.
//
// "Kept nothing" is observed the only way a client could: the backends are
// removed, which makes the same process answer from its own store, and it must
// have nothing to answer with. Reading the store directly would prove the same
// thing about internals that no user can see.
func TestEveryWriteRouteForwardsToTheShards(t *testing.T) {
	for _, rt := range surfaceRoutes() {
		if !rt.write {
			continue
		}
		t.Run(rt.path, func(t *testing.T) {
			shard := realShard(t, nil)
			srv, cluster := routerServer(t, shard.URL)

			code, body := surfaceCall(t, cluster, rt)
			if code >= 300 {
				t.Fatalf("the router refused the write: %d %.200s", code, body)
			}

			// Now it is not a router. Whatever it answers comes from its own
			// store, and its own store must be empty.
			srv.SetBackends(nil)
			rCode, rows, raw := queryRows(t, cluster, "*")
			if rCode != 200 {
				t.Fatalf("reading the router's own store: %d %.200s", rCode, raw)
			}
			if len(rows) != 0 {
				t.Fatalf("the router kept %d rows in its own store; every read fans "+
					"out to the shards, so those rows are invisible forever: %.300s",
					len(rows), raw)
			}
		})
	}
}

// routerServer is router() with the Server handed back, for tests that need to
// change its configuration after it is serving.
func routerServer(t *testing.T, urls ...string) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(1)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// No classified route answers with a server error in router mode.
//
// The router-local routes had no coverage at all: the read comparison skips
// them by definition and the refusal test only looks at refused ones. So a
// route could panic on every request and the surface tests would stay green --
// which is what happened when the two anti-entropy endpoints were added without
// being named in the tenant allowlist. They 500'd with a panic, and the only
// thing that noticed was a test written for something else.
//
// A 4xx is fine here: a request with no digest, or one this fixture is not
// authorized for, is the endpoint working. A 5xx is the endpoint failing.
func TestNoRouteAnswersWithAServerErrorInRouterMode(t *testing.T) {
	shard := realShard(t, corpus(1)[0])
	cluster := router(t, shard.URL)

	for _, rt := range surfaceRoutes() {
		if rt.write {
			continue // covered, with real payloads, by the write test
		}
		t.Run(rt.path, func(t *testing.T) {
			code, body := surfaceCall(t, cluster, rt)
			if code >= 500 && code != http.StatusNotImplemented {
				t.Fatalf("%s answered %d in router mode: %.300s", rt.path, code, body)
			}
		})
	}
}

// And the same on a storage node, where the tenant allowlist is what decides
// whether a handler has a store to talk to.
func TestNoRouteAnswersWithAServerErrorOnAStorageNode(t *testing.T) {
	node := realShard(t, corpus(1)[0])
	for _, rt := range surfaceRoutes() {
		if rt.write {
			continue
		}
		t.Run(rt.path, func(t *testing.T) {
			code, body := surfaceCall(t, node, rt)
			// 501 is a deliberate refusal, not a failure: /admin/cluster/repair
			// reconciles replicas and a storage node has none to reconcile.
			if code >= 500 && code != http.StatusNotImplemented {
				t.Fatalf("%s answered %d on a storage node: %.300s", rt.path, code, body)
			}
		})
	}
}

// The quarantine listing answers an empty LIST, not a null, and refuses on a
// router.
//
// `[]` and `null` are different answers: a client that distinguishes them reads
// null as "this node cannot say" and an empty list as "nothing is quarantined".
// A router refuses rather than answering empty, because its own store
// quarantines nothing and an empty list there reads as "nothing is wrong" about
// shards it never asked.
func TestTheQuarantineListingSaysNothingRatherThanNull(t *testing.T) {
	node := realShard(t, nil)
	code, _, body := getJSONFrom(t, node, "/admin/storage/quarantine")
	if code != http.StatusOK {
		t.Fatalf("a storage node answered %d: %s", code, body)
	}
	if !strings.Contains(body, `"groups":[]`) {
		t.Errorf("a node with nothing quarantined answered %q; `null` and `[]` "+
			"are different answers and this must be the empty list", body)
	}
	if !strings.Contains(body, `"count":0`) {
		t.Errorf("the body carries no count: %q", body)
	}

	// A router refuses.
	ts := router(t, node.URL)
	code, _, body = getJSONFrom(t, ts, "/admin/storage/quarantine")
	if code == http.StatusOK {
		t.Errorf("a router answered 200 for its own empty store: %q -- that reads "+
			"as `nothing is wrong` about shards it never asked", body)
	}
}
