package api

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A shard that answers 200 with a body the coordinator cannot read has not
// contributed its rows, and every merge used to `continue` past it.
//
// fanOutChecked refuses a read when a shard did not answer or answered from an
// incomplete store. It cannot see this one: as far as the fan-out is concerned
// the shard answered 200 with a complete envelope. The merge is the only layer
// that knows, and eight of them dropped the shard and returned the rest with no
// marker of any kind -- the same short-answer-looking-complete that the
// completeness rule exists to prevent, one layer down.

// garbageShard answers every read 200, with a complete envelope, and a body
// that is not JSON.
func garbageShard(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
		w.Write([]byte(`{"hits":`)) // truncated: invalid for every shape
	}))
	t.Cleanup(ts.Close)
	return ts
}

// mergeDecodeEndpoints are the reads whose merge decodes a shard body, one per
// HANDLER that calls mergeDecode.
//
// `handler` is the function each entry actually reaches, and
// TestEveryMergeDecodeCallerIsCovered checks that set against the mergeDecode
// call sites in the source. The list is hand-kept and the gate is not: the
// first version had seven entries for eight handlers, because
// `stats_query` without `by=` delegates to federatedVector, so
// federatedStatsQuery's own loop was never entered. That is the same drift as
// federatedEndpoints, in the commit that fixed federatedEndpoints.
var mergeDecodeEndpoints = []struct {
	name, path, method, body string
	handler                  string
}{
	{"hits", "/select/logsql/hits?query=*&step=1m&start=1&end=2", "GET", "", "federatedHits"},
	{"stats_query", "/select/logsql/stats_query?query=*+%7C+stats+count%28%29+n", "GET", "", "federatedVector"},
	{"stats_query_by", "/select/logsql/stats_query?query=*&by=level", "GET", "", "federatedStatsQuery"},
	{"stats_query_range", "/select/logsql/stats_query_range?query=*+%7C+stats+count%28%29+n&step=1m", "GET", "", "federatedMatrix"},
	{"field_values", "/select/logsql/field_values?query=*&field=level", "GET", "", "federatedValueCounts"},
	{"facets", "/select/logsql/facets?query=*", "GET", "", "federatedFacets"},
	{"es_count", "/_count", "POST", `{"query":{"match_all":{}}}`, "federatedESCount"},
	{"es_search", "/_search", "POST", `{"query":{"match_all":{}}}`, "federatedESSearch"},
	{"quarantine", "/admin/storage/quarantine", "GET", "", "federatedQuarantine"},
}

// Every function that calls mergeDecode is exercised by the table above.
//
// Taken from the source rather than listed twice: a merge added later with a
// `continue` restored in it would otherwise pass this file unchallenged.
func TestEveryMergeDecodeCallerIsCovered(t *testing.T) {
	covered := map[string]bool{}
	for _, e := range mergeDecodeEndpoints {
		covered[e.handler] = true
	}
	callers := mergeDecodeCallers(t)
	if len(callers) == 0 {
		t.Fatal("no mergeDecode call sites were found, so this gate measures nothing")
	}
	for fn := range callers {
		if !covered[fn] {
			t.Errorf("%s calls mergeDecode and no entry in mergeDecodeEndpoints "+
				"reaches it, so nothing says what it does with an unreadable shard "+
				"answer", fn)
		}
	}
	for fn := range covered {
		if !callers[fn] {
			t.Errorf("mergeDecodeEndpoints names %s, which does not call mergeDecode", fn)
		}
	}
	t.Logf("%d functions call mergeDecode; %d named in the table", len(callers), len(covered))
}

// mergeDecodeCallers is the set of functions in this package containing a call
// to s.mergeDecode.
func mergeDecodeCallers(t *testing.T) map[string]bool {
	t.Helper()
	fset := gotoken.NewFileSet()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		for _, d := range f.Decls {
			fn, isFunc := d.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == "mergeDecode" {
					out[fn.Name.Name] = true
				}
				return true
			})
		}
	}
	return out
}

func TestAShardAnsweringUnreadableIsRefusedNotSkipped(t *testing.T) {
	good := goodShard(t)
	bad := garbageShard(t)
	ts := router(t, good.URL, bad.URL)

	for _, e := range mergeDecodeEndpoints {
		t.Run(e.name, func(t *testing.T) {
			resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
				e.name, e.path, e.method, e.body,
			})
			if resp.StatusCode/100 == 2 {
				t.Fatalf("answered %d with %q: one shard's body was unreadable and its "+
					"rows are missing from this answer, which the caller has no way to see",
					resp.StatusCode, truncate(body, 200))
			}
			if !strings.Contains(body, "could not") && !strings.Contains(body, "refused") {
				t.Errorf("answered %d %q; the message has to say the answer was refused "+
					"and why, or an operator reads it as an ordinary upstream error",
					resp.StatusCode, truncate(body, 200))
			}
		})
	}
}

// The shard that is unreadable is named, so an operator knows which node to
// look at rather than which endpoint.
func TestAnUnreadableAnswerNamesTheShard(t *testing.T) {
	good := goodShard(t)
	bad := garbageShard(t)
	// bad is shard 1: SetBackends order is shard order at one replica each.
	ts := router(t, good.URL, bad.URL)

	e := mergeDecodeEndpoints[6] // _count
	resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
		e.name, e.path, e.method, e.body,
	})
	if resp.StatusCode/100 == 2 {
		t.Fatalf("answered %d, want a refusal", resp.StatusCode)
	}
	if !strings.Contains(body, "shard 1") {
		t.Errorf("the refusal does not name the shard: %q", truncate(body, 300))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// hitsShard answers /select/logsql/hits with the given dense series and
// everything else the way goodShard does.
func hitsShard(t *testing.T, stamps []string, vals []int) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
		if !strings.Contains(r.URL.Path, "hits") {
			w.Write([]byte(`{"values":[]}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"hits": []map[string]any{{
				"fields":     map[string]string{},
				"timestamps": stamps,
				"values":     vals,
				"total":      len(vals),
			}},
		})
	}))
	t.Cleanup(ts.Close)
	return ts
}

// Buckets come back in TIME order, which is not the text order of RFC3339Nano.
//
// The format drops trailing zeros in the fractional second, so it is not
// fixed-width: '.' is 0x2E and 'Z' is 0x5A, which puts "00:00:00.5Z" before
// "00:00:00Z" under sort.Strings. `step` is a time.Duration off the query
// string, so `step=500ms` produces exactly that mix. The merged arrays are
// indexed together by the client, so an out-of-order timestamp array draws the
// graph wrong.
func TestClusterHitsBucketsAreInTimeOrderNotTextOrder(t *testing.T) {
	// One second and one half-second bucket, split across two shards so the
	// merge has to order them itself.
	whole := "2026-01-01T00:00:01Z"
	half := "2026-01-01T00:00:00.5Z"
	zero := "2026-01-01T00:00:00Z"

	a := hitsShard(t, []string{zero, whole}, []int{1, 3})
	b := hitsShard(t, []string{half}, []int{2})
	ts := router(t, a.URL, b.URL)

	resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
		"hits", "/select/logsql/hits?query=*&step=500ms&start=1&end=2", "GET", "",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("hits answered %d: %s", resp.StatusCode, truncate(body, 300))
	}
	var got struct {
		Hits []struct {
			Timestamps []string `json:"timestamps"`
			Values     []int    `json:"values"`
		} `json:"hits"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("unmarshalling %q: %v", truncate(body, 300), err)
	}
	if len(got.Hits) != 1 {
		t.Fatalf("want one merged series, got %d: %s", len(got.Hits), truncate(body, 300))
	}
	stamps := got.Hits[0].Timestamps
	if len(stamps) != 3 {
		t.Fatalf("want three buckets, got %v", stamps)
	}
	var prev time.Time
	for i, s := range stamps {
		p, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("bucket %d is %q, which is not RFC3339: %v", i, s, err)
		}
		if i > 0 && !p.After(prev) {
			t.Errorf("bucket %d (%s) is not after bucket %d (%s); the arrays are "+
				"indexed together, so the client draws these points out of order.\n"+
				"got: %v", i, s, i-1, prev.Format(time.RFC3339Nano), stamps)
		}
		prev = p
	}
	// The counts stay with their own bucket.
	want := map[string]int{zero: 1, half: 2, whole: 3}
	for i, s := range stamps {
		p, _ := time.Parse(time.RFC3339Nano, s)
		var w int
		for k, v := range want {
			if kp, _ := time.Parse(time.RFC3339Nano, k); kp.Equal(p) {
				w = v
			}
		}
		if got.Hits[0].Values[i] != w {
			t.Errorf("bucket %s carries %d, want %d", s, got.Hits[0].Values[i], w)
		}
	}
}

// A shard whose two arrays are different lengths is a protocol violation, not
// a short read to absorb: truncating to the shorter one drops counts silently.
func TestClusterHitsRefusesUnequalDenseArrays(t *testing.T) {
	good := hitsShard(t, []string{"2026-01-01T00:00:00Z"}, []int{1})
	short := hitsShard(t, []string{"2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"}, []int{5})
	ts := router(t, good.URL, short.URL)

	resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
		"hits", "/select/logsql/hits?query=*&step=1s&start=1&end=2", "GET", "",
	})
	if resp.StatusCode/100 == 2 {
		t.Fatalf("answered %d with %q: a series with 2 timestamps and 1 value was "+
			"absorbed, so a bucket's count is silently gone", resp.StatusCode, truncate(body, 300))
	}
	if !strings.Contains(body, "timestamps") {
		t.Errorf("the refusal does not say what was wrong: %q", truncate(body, 300))
	}
}

// A bucket timestamp that is not RFC3339 cannot be ordered against the other
// shards' buckets, so it is refused rather than sorted somewhere arbitrary.
func TestClusterHitsRefusesAnUnorderableBucket(t *testing.T) {
	good := hitsShard(t, []string{"2026-01-01T00:00:00Z"}, []int{1})
	junk := hitsShard(t, []string{"not-a-time"}, []int{5})
	ts := router(t, good.URL, junk.URL)

	resp, body := callEndpoint(t, ts, struct{ name, path, method, body string }{
		"hits", "/select/logsql/hits?query=*&step=1s&start=1&end=2", "GET", "",
	})
	if resp.StatusCode/100 == 2 {
		t.Fatalf("answered %d with %q, want a refusal", resp.StatusCode, truncate(body, 300))
	}
	if !strings.Contains(body, "not-a-time") {
		t.Errorf("the refusal does not quote the timestamp it could not read: %q",
			truncate(body, 300))
	}
}
