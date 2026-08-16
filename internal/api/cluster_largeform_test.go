package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// A POST query larger than a request line is ANSWERED, not called a version
// mismatch.
//
// withFormInURL folds a POST form into the peer's URL, which is right for the
// ordinary case and takes away the reason POST exists once the form is large:
// net/http bounds a request line and its headers together at MaxHeaderBytes,
// 1 MiB by default, and a peer over it answers 431 from the SERVER -- before
// the handler, so with no protocol-version header and no error class. The
// client checked the version first and read that silence as a version
// mismatch. Measured on a ONE-NODE cluster running ONE build:
//
//	503 ... 1 of 1 shards could not answer completely (0(version_mismatch))
//
// on 1.2 MB of `level:in(...)`, the shape a dashboard templating variable
// expands to. The same query against a non-router node was answered, so
// clustered mode lost a capability single-node has, and the message pointed the
// operator at node versions -- where there was nothing to find.

// recordingShard answers every request and remembers the query it was asked.
type recordingShard struct {
	ts *httptest.Server
	mu sync.Mutex
	q  []string
	lm []string
	bd []string
	// esq is the Elasticsearch spelling of the query parameter. /_count and
	// /_search send `q`, not `query`, so a shard asked nothing at all on those
	// routes is indistinguishable from a shard asked correctly if only `query`
	// is recorded -- both are "".
	esq []string
}

// askedES is the last `q` the shard was asked, and whether it was asked at all.
func (sh *recordingShard) askedES() (string, bool) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.esq) == 0 {
		return "", false
	}
	return sh.esq[len(sh.esq)-1], true
}

// bounds is the pair of result-shaping parameters the shard was asked for, as
// one string, so a difference is visible in the failure message.
func (sh *recordingShard) bounds() string {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.bd) == 0 {
		return "<the shard was never asked>"
	}
	return sh.bd[len(sh.bd)-1]
}

func newRecordingShard(t *testing.T) *recordingShard {
	t.Helper()
	sh := &recordingShard{}
	sh.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A shard reads its parameters from the form, which is the union of the
		// URL query and a form body -- so this handler sees the same thing a
		// real shard does whichever way the router sent them.
		_ = r.ParseForm()
		sh.mu.Lock()
		sh.q = append(sh.q, r.FormValue("query"))
		sh.esq = append(sh.esq, r.FormValue("q"))
		sh.lm = append(sh.lm, r.FormValue("limit"))
		sh.bd = append(sh.bd, "limit="+r.FormValue("limit")+
			" max_values_per_field="+r.FormValue("max_values_per_field"))
		sh.mu.Unlock()
		writeEnvelope(w.Header(), 0, 0, true, 0, true, "gen-test", "")
		w.Write([]byte(`{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":1}]}`))
	}))
	t.Cleanup(sh.ts.Close)
	return sh
}

func (sh *recordingShard) asked() (string, string) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.q) == 0 {
		return "", ""
	}
	return sh.q[len(sh.q)-1], sh.lm[len(sh.lm)-1]
}

// bigQuery builds an `in(...)` list of about n bytes.
func bigQuery(n int) string {
	var b strings.Builder
	b.WriteString(`_stream:{app="x"} AND level:in(`)
	for i := 0; b.Len() < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("v")
	}
	b.WriteString(")")
	return b.String()
}

func postForm(t *testing.T, ts *httptest.Server, path string, form url.Values) (int, string) {
	t.Helper()
	resp, err := http.Post(ts.URL+path, "application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// EVERY federated read, DERIVED from surfaceRoutes rather than hand-kept.
//
// The first version listed two endpoints and passed while /select/logsql/query
// -- the endpoint the change was written for -- was still capped. The second
// listed ten, one of which (/select/logsql/hits_count) is not a route at all
// and skipped forever on its 404, so it covered nine of the FOURTEEN federated
// reads and missed /select/logsql/stats_query and stats_query_range: the
// Grafana-dashboard shape this whole change is about.
//
// This repository had already learned that lesson and written it down:
// TestEveryFederatedReadIsInTheCompletenessSuite exists because
// "federatedEndpoints was a hand-kept list that had drifted to nine of the
// fourteen federated reads". A hand-kept list drifted to nine of fourteen
// again, in a test written to prove coverage.
//
// So the set comes from surfaceRoutes(), which TestEverySurfaceRouteIsClassified
// proves is every path the mux registers. A new federated endpoint is covered
// the day it is classified, and one that disappears takes its case with it.
func TestEveryFederatedReadCarriesALargeQuery(t *testing.T) {
	q := bigQuery(1_200_000)
	var carried atomic.Int64
	for _, rt := range surfaceRoutes() {
		if rt.kind != federated || rt.write {
			continue
		}
		t.Run(rt.path, func(t *testing.T) {
			sh := newRecordingShard(t)
			ts := wmRouter(t, sh.ts.URL)

			// The route's own parameters, with `query` replaced by the large
			// one. Taken from the classification so a route that needs
			// `field=` or a time window is exercised as it is really called.
			form, err := url.ParseQuery(rt.query)
			if err != nil {
				t.Fatalf("route query %q does not parse: %v", rt.query, err)
			}

			// The large input in the route's OWN query language.
			//
			// A route with a JSON body does not take a form at all, so
			// withFormInURL returns before it looks at anything -- those are
			// out of scope for this change and skipped with the reason, not
			// fed LogsQL and counted as covered.
			switch {
			case rt.body != "":
				t.Skipf("%s carries a JSON body, so the form path this test covers "+
					"is never entered: withFormInURL returns at the content type",
					rt.path)
			case strings.HasPrefix(form.Get("query"), "SELECT"):
				form.Set("query", bigSQL(1_200_000))
			default:
				form.Set("query", q)
			}
			want := form.Get("query")

			code, body := postForm(t, ts, rt.path, form)
			if code == 503 {
				t.Fatalf("%s refused a %d-byte POST query (%d): the request line is "+
					"still carrying it. %s", rt.path, len(q), code, body)
			}
			if code != 200 {
				t.Fatalf("%s answered %d: %.300s", rt.path, code, body)
			}
			got, _ := sh.asked()
			if got == "" {
				t.Fatalf("%s answered 200 without asking the shard anything", rt.path)
			}
			if got != want {
				// return, not a bare Errorf. t.Errorf does not stop the
				// subtest, so the count below ran anyway: a mutation that
				// appended one byte to every shard query failed all twelve
				// AND reported "12 of 14 carried the query", asserting the
				// thing that had just failed.
				t.Errorf("the shard was asked %d bytes, the caller sent %d",
					len(got), len(want))
				return
			}
			// Counted only on the routes that actually carried it, unchanged.
			// `n++` before the skip made n == 14 always, so the guard below
			// could never fire and the log line said fourteen carried a query
			// that twelve carried.
			carried.Add(1)
		})
	}
	// The count is part of the assertion: a classification change that empties
	// this loop would otherwise pass in silence.
	//
	// Against the FEDERATED count, not len(surfaceRoutes()) -- that is every
	// path the mux registers, so the line read "12 of 46" and invited the
	// reader to conclude thirty-four federated reads were skipped.
	nFederated := 0
	for _, rt := range surfaceRoutes() {
		if rt.kind == federated && !rt.write {
			nFederated++
		}
	}
	if n := carried.Load(); n < 12 {
		t.Errorf("only %d of %d federated reads carried the query; two have a JSON "+
			"body and are skipped with the reason. A hand-kept list drifting to "+
			"nine is what this test replaced, and a count that includes the skips "+
			"-- or the failures -- cannot see that happen again", n, nFederated)
	} else {
		t.Logf("%d of %d federated reads carried a %d-byte query; the rest have a "+
			"JSON body", n, nFederated, len(q))
	}
}

func TestALargePostQueryReachesTheShardsWhole(t *testing.T) {
	sh := newRecordingShard(t)
	ts := wmRouter(t, sh.ts.URL)

	for _, size := range []int{
		1 << 10,               // comfortably in the URL: the path that already worked
		maxPeerQueryBytes / 2, // still in the URL
		maxPeerQueryBytes * 2, // over the budget: travels as a body
		1_200_000,             // over net/http's MaxHeaderBytes, the measured case
	} {
		q := bigQuery(size)
		code, body := postForm(t, ts, "/select/logsql/hits",
			url.Values{"query": {q}, "step": {"1h"}})
		if code != 200 {
			t.Fatalf("a %d-byte POST query answered %d: %s", len(q), code, body)
		}
		// The shard must have been asked the WHOLE query. A router that sent a
		// truncated or empty one would answer 200 with a smaller number, which
		// is the failure mode that cannot be seen from the response.
		got, _ := sh.asked()
		if got != q {
			t.Errorf("the shard was asked %d bytes, the caller sent %d", len(got), len(q))
		}
	}
}

// The router's plan still wins over the caller's form at the large size.
//
// On the small path the form is merged UNDER the query string, because a
// federated handler rewrites the shard URL and Request.Clone copies Form -- the
// defect that made `stats count()` answer 10 by GET and 2 by POST. The large
// path must not reintroduce it, and Go's own precedence runs the OTHER way:
// r.Form copies PostForm first, so on the peer a body value beats a query one.
//
// What holds it together is that the two sets are DISJOINT -- a key already in
// the query is never put in the body -- so precedence never arbitrates. This
// test is a guard on that disjointness and on nothing else: sending the body
// unconditionally, at every size, leaves it green, and only dropping the skip
// turns it red (measured, both ways). The threshold is guarded by
// TestALargePostQueryReachesTheShardsWhole instead.
func TestTheRouterPlanStillWinsOverALargeForm(t *testing.T) {
	sh := newRecordingShard(t)
	ts := wmRouter(t, sh.ts.URL)

	// The facets path sets limit=0 on the shard request so the coordinator can
	// merge; a caller's limit=3 must not replace it.
	q := bigQuery(maxPeerQueryBytes * 2)
	code, body := postForm(t, ts, "/select/logsql/facets",
		url.Values{"query": {q}, "limit": {"3"}})
	if code != 200 {
		t.Fatalf("answered %d: %s", code, body)
	}
	gotQ, gotLimit := sh.asked()
	if gotQ != q {
		t.Errorf("the shard was asked %d bytes, the caller sent %d", len(gotQ), len(q))
	}
	if gotLimit == "3" {
		t.Errorf("the shard received the caller's limit=3 in place of the router's "+
			"plan: shard-local top-3s merged into a wrong answer. limit=%q", gotLimit)
	}
}

// A pre-handler status is not a version statement.
//
// Asserted directly on the classifier, because the statuses that produce it are
// emitted by net/http and by proxies rather than by anything this code calls.
func TestAPreHandlerStatusIsNotAVersionMismatch(t *testing.T) {
	for _, code := range []int{
		http.StatusRequestHeaderFieldsTooLarge, http.StatusRequestEntityTooLarge,
		http.StatusBadRequest, http.StatusRequestURITooLong,
		http.StatusHTTPVersionNotSupported,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout,
	} {
		if !preHandlerStatus(code) {
			t.Errorf("HTTP %d is emitted before a handler runs, so a missing "+
				"protocol-version header on it is not a version mismatch", code)
		}
	}
	// A status a HANDLER produces still goes through the version check: there
	// the missing header IS the evidence, and a body that parses and means
	// something else is the risk that ordering exists for.
	for _, code := range []int{200, 206, 401, 403, 404, 409, 422, 429, 500} {
		if preHandlerStatus(code) {
			t.Errorf("HTTP %d comes from a handler; exempting it from the version "+
				"check would merge a body from a node speaking another protocol", code)
		}
	}
}

// A peer that answers 431 with no version header is reported as unavailable,
// end to end -- the class whose remedy (another replica) is the one that helps.
func TestAPeerRefusingBeforeItsHandlerIsNotReportedAsAVersionMismatch(t *testing.T) {
	// A server that refuses everything the way net/http's own header-limit
	// path does: a bare status, no application headers at all.
	refuser := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestHeaderFieldsTooLarge)
	}))
	t.Cleanup(refuser.Close)

	ts := wmRouter(t, refuser.URL)
	code, body := postForm(t, ts, "/select/logsql/hits",
		url.Values{"query": {"*"}, "step": {"1h"}})
	if code == 200 {
		t.Fatalf("a shard that refused every request answered 200: %s", body)
	}
	if strings.Contains(body, "version_mismatch") {
		t.Errorf("a peer that refused before its handler ran is reported as a "+
			"version mismatch, so the operator is sent to check node versions "+
			"on a cluster where every node runs the same build: %s", body)
	}
	if !strings.Contains(body, string(PeerUnavailable)) {
		t.Errorf("want the answer to name %s: %s", PeerUnavailable, body)
	}
}

// bigSQL is a SELECT whose IN list is about n bytes -- /select/sql's own query
// language, so that route is exercised rather than fed LogsQL and skipped.
func bigSQL(n int) string {
	var b strings.Builder
	b.WriteString(`SELECT * FROM logs WHERE level='v0'`)
	for i := 1; b.Len() < n; i++ {
		fmt.Fprintf(&b, " OR level='v%d'", i)
	}
	return b.String()
}
