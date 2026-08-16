package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
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

// A limit in a POST FORM does not reach the shards, any more than one in the
// query string does.
//
// withoutLimits deletes `limit` and `max_values_per_field` from the shard
// request so each shard answers unbounded and the coordinator applies the bound
// once. It deleted them from r.URL.RawQuery only. On a POST they are in the
// BODY, so the deletion removed nothing and withFormInURL merged them straight
// back -- each shard truncated to its own top N and the router summed the
// truncated lists. Measured, three shards, 30 rows, `field=user&limit=2`:
//
//	GET  200 {"values":[{"value":"u0","hits":5},{"value":"u1","hits":5}]}
//	POST 200 {"values":[{"value":"u0","hits":4},{"value":"u1","hits":4},
//	                    {"value":"u2","hits":2},{"value":"u3","hits":2}]}
//
// u0 has five hits across the cluster and the POST answer said four. HTTP 200,
// a smaller number, and nothing in the response to tell it from a correct one.
//
// Asserted on WHAT THE SHARD RECEIVES, not on the two final answers. The first
// version of this compared the GET and POST bodies and stayed green with the
// fix reverted: with ties in the fixture, two differently-truncated shard-local
// lists can sum to the same visible answer. The bound reaching the shard at all
// is the defect, whether or not this particular data makes it show.
func TestALimitInAPostFormDoesNotReachTheShards(t *testing.T) {
	for _, ep := range []struct {
		path, params string
		// forwarded names the parameters this endpoint's plan deliberately
		// passes through rather than strips. facets forwards
		// max_values_per_field on purpose: `limit` is a RESULT shape and
		// unlimited is cheap, but max_values_per_field is a CARDINALITY bound,
		// and removing it makes timeFacet materialize every matching row --
		// measured at 640,000 rows / 54.9 MB / 482 ms / +496 MiB on one shard.
		forwarded []string
		// limitImmune marks a row whose `limit` half cannot fail, because this
		// endpoint's plan SETS the key rather than deleting it.
		limitImmune bool
	}{
		{path: "/select/logsql/field_values", params: "field=user&limit=2"},
		{path: "/select/logsql/field_names", params: "limit=2"},
		{path: "/select/logsql/streams", params: "limit=2"},
		{path: "/select/logsql/stream_ids", params: "limit=2"},
		{path: "/select/logsql/stream_field_names", params: "limit=2"},
		{path: "/select/logsql/stream_field_values", params: "field=app&limit=2"},
		// facets sets `limit=0` through `unlimited` rather than deleting it, so
		// `limit` is ALWAYS already a URL key and never enters `extra`. These
		// two rows were immune to the defect before the fix and stay green
		// through both mutations -- measured, not assumed. Kept for the
		// max_values_per_field half, which is real, and labelled so the pass
		// is not read as coverage of the limit half.
		{path: "/select/logsql/facets", params: "limit=2",
			forwarded: []string{"max_values_per_field"}, limitImmune: true},
		{path: "/select/logsql/facets", params: "limit=2&max_values_per_field=2",
			forwarded: []string{"max_values_per_field"}, limitImmune: true},
		{path: "/select/logsql/field_values", params: "field=user&max_values_per_field=7"},
	} {
		t.Run(ep.path+"?"+ep.params, func(t *testing.T) {
			form := url.Values{"query": {"*"}}
			for _, kv := range strings.Split(ep.params, "&") {
				k, v, _ := strings.Cut(kv, "=")
				form.Set(k, v)
			}

			// The same request twice, each against its own shard, so the two
			// recordings cannot interfere.
			viaGet := newRecordingShard(t)
			gts := wmRouter(t, viaGet.ts.URL)
			if code, _, raw := getJSONFrom(t, gts, ep.path+"?"+form.Encode()); code != 200 {
				t.Skipf("GET answered %d: %s", code, raw)
			}
			gotGet := viaGet.bounds()

			viaPost := newRecordingShard(t)
			pts := wmRouter(t, viaPost.ts.URL)
			if code, body := postForm(t, pts, ep.path, form); code != 200 {
				t.Skipf("POST answered %d: %s", code, body)
			}
			gotPost := viaPost.bounds()

			if gotGet != gotPost {
				t.Errorf("the shard is asked for a different bound depending on the "+
					"caller's method, so a limit the router strips over GET reaches "+
					"the shards over POST and each one truncates to its own top N:\n"+
					"  via GET : %s\n  via POST: %s", gotGet, gotPost)
			}
			// And a bound the plan STRIPS is not passed through. Equality alone
			// would be satisfied by both methods being wrong together.
			for _, k := range []string{"limit", "max_values_per_field"} {
				want := form.Get(k)
				if want == "" || want == "0" || slices.Contains(ep.forwarded, k) {
					continue
				}
				if strings.Contains(gotPost, k+"="+want) {
					t.Errorf("the shard received the caller's %s=%s: shard-local top-N "+
						"lists merged into a wrong total. shard saw %s", k, want, gotPost)
				}
			}
		})
	}
}

// A limit the PLAN DELETED does not come back over a POST form.
//
// shardQueryURL removes `limit` from the shard request when the plan has a
// coordinator half: the shards must return everything that matches and the
// bound is applied once, over the merged rows. withFormInURL then merged form
// keys "not already in the shard URL" -- and a key the plan DELETED is not in
// the URL, so the caller's limit went straight back. Measured, three shards of
// ten rows, `&limit=5`, HTTP 200 on every one:
//
//   - | stats count() c            single 30   GET 30      POST 15
//   - | stats by (level) count() c 10/10/10    10/10/10    5/5/5
//
// The endpoint is /select/logsql/query, which is the one the large-POST change
// was named after, and the comment added with it asserted the opposite: "with
// RawQuery cleared there is one source and the plan wins because the plan is
// what was merged in". A union merge does not preserve a deletion.
//
// Fixed by recording which parameters the plan OWNS rather than by deleting
// this one from r.Form as well -- that would fix `limit` and leave the next
// deleted parameter to be found the same way.
func TestALimitThePlanDeletedDoesNotComeBackOverAPostForm(t *testing.T) {
	// DISTINCT timestamps across shards, so `sort by (_time) | limit 5` has one
	// right answer. With all three shards using 00:00:00..09 the ties break
	// arbitrarily and a single node and a cluster can both be correct while
	// disagreeing -- which is a fixture that cannot tell the defect from the
	// tie.
	rows := func(level string, base int) []string {
		var out []string
		for i := 0; i < 10; i++ {
			out = append(out, fmt.Sprintf(
				`{"_time":"2024-01-01T00:%02d:%02dZ","_msg":"m","level":%q,"user":"u%d"}`,
				base, i, level, i%7))
		}
		return out
	}
	a := realShard(t, rows("info", 0))
	b := realShard(t, rows("warn", 1))
	c := realShard(t, rows("error", 2))
	ts := router(t, a.URL, b.URL, c.URL)
	solo := realShard(t, append(append(rows("info", 0), rows("warn", 1)...), rows("error", 2)...))

	for _, tc := range []struct {
		q string
		// vsSolo compares the cluster against a single node holding the same
		// rows. Off for `sort ... | limit`, where the two genuinely disagree on
		// WHICH rows survive -- both cluster methods agree with each other, so
		// it is not this defect, and it is task #438 rather than a case
		// quietly dropped from here.
		vsSolo bool
	}{
		{`* | stats count() c`, true},
		{`* | stats by (level) count() c`, true},
		{`* | sort by (_time) | limit 5`, false},
	} {
		q := tc.q
		t.Run(q, func(t *testing.T) {
			form := url.Values{"query": {q}, "limit": {"5"}}

			soloRaw := rawGet(t, solo, "/select/logsql/query?"+form.Encode())
			getRaw := rawGet(t, ts, "/select/logsql/query?"+form.Encode())
			resp, err := http.Post(ts.URL+"/select/logsql/query",
				"application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
			if err != nil {
				t.Fatal(err)
			}
			postBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			postRaw := strings.TrimSpace(string(postBody))

			if strings.TrimSpace(soloRaw) == "" {
				t.Fatalf("the single node answered nothing, so this comparison is "+
					"three empty answers agreeing: %q", soloRaw)
			}
			if g := strings.TrimSpace(getRaw); g != postRaw {
				t.Errorf("the same query answers differently by method, so the bound "+
					"the plan deleted reached the shards over POST and each one "+
					"truncated its own rows:\n  GET  %s\n  POST %s", g, postRaw)
			}
			// Against a single node holding the same rows, as a SET of lines.
			//
			// Not byte-for-byte: group order across a stats merge is a map
			// iteration on both sides, so two correct answers can differ in
			// order. What must not differ is which rows are in them.
			if tc.vsSolo && !sameLineSet(soloRaw, postRaw) {
				t.Errorf("the cluster POST returns a different SET of rows than a "+
					"single node holding the same data:\n  single %s\n  POST   %s",
					strings.TrimSpace(soloRaw), postRaw)
			}
		})
	}
}

// rawGet returns the body of a GET as a string.
func rawGet(t *testing.T, ts *httptest.Server, path string) string {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// sameLineSet reports whether two NDJSON bodies carry the same lines, in any
// order.
func sameLineSet(a, b string) bool {
	norm := func(s string) []string {
		var out []string
		for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				out = append(out, l)
			}
		}
		sort.Strings(out)
		return out
	}
	x, y := norm(a), norm(b)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// The 400 names the half that is actually unreadable.
//
// withoutLimits calls r.ParseForm, which fails on a malformed Content-Type as
// well as on a malformed body -- parsePostForm runs mime.ParseMediaType first.
// The refusal said "this request's query string could not be parsed" for all of
// them, so an operator with `Content-Type: text/plain; charset` was sent to
// inspect a query string that was perfectly correct.
func TestTheRefusalNamesTheHalfThatIsUnreadable(t *testing.T) {
	sh := newRecordingShard(t)
	ts := wmRouter(t, sh.ts.URL)

	for _, tc := range []struct {
		name, ct, body, path, want string
	}{
		{"a malformed Content-Type", "text/plain; charset", "query=*",
			"/select/logsql/field_values?query=*&field=user", "Content-Type"},
		{"a malformed form body", "application/x-www-form-urlencoded", "%zz=1",
			"/select/logsql/field_values?query=*&field=user", "request body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", ts.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", tc.ct)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 400 {
				t.Fatalf("answered %d, want 400: %.200s", resp.StatusCode, b)
			}
			if !strings.Contains(string(b), tc.want) {
				t.Errorf("the refusal does not name %q, so the operator is sent to "+
					"the wrong half: %s", tc.want, b)
			}
			if tc.want != "query string" && strings.Contains(string(b), "query string") {
				t.Errorf("the refusal blames the query string, which parses: %s", b)
			}
		})
	}
}
