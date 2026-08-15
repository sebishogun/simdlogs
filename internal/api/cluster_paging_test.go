package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ES paging must be applied ONCE, to the merged and ordered hits.
//
// The client's body went to every shard verbatim, so each skipped `from` of its
// own hits and the coordinator skipped `from` again over the concatenation:
// {"from":4,"size":4} over 2 shards of 6 answered 0 hits with "total":12 and
// HTTP 200, and rows 0-3 of every shard were unreachable from any page.
func TestESPagingIsAppliedOnceOverTheWholeCluster(t *testing.T) {
	single := realShard(t, esCorpus(1)[0])
	parts := esCorpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, tc := range []struct{ from, size int }{
		{0, 4}, {4, 4}, {2, 3}, {0, 12}, {8, 4}, {10, 5},
	} {
		t.Run(fmt.Sprintf("from=%d size=%d", tc.from, tc.size), func(t *testing.T) {
			body := fmt.Sprintf(`{"query":{"match_all":{}},"from":%d,"size":%d}`,
				tc.from, tc.size)
			sTotal, sIDs := esSearch(t, single, body)
			cTotal, cIDs := esSearch(t, cluster, body)

			if sTotal != cTotal {
				t.Fatalf("total %d on one node, %d across two", sTotal, cTotal)
			}
			if len(sIDs) != len(cIDs) {
				t.Fatalf("%d hits on one node, %d across two (from=%d size=%d):\n"+
					"  single:  %v\n  cluster: %v",
					len(sIDs), len(cIDs), tc.from, tc.size, sIDs, cIDs)
			}
			for i := range sIDs {
				if sIDs[i] != cIDs[i] {
					t.Fatalf("hit %d differs: single %q, cluster %q\n  single:  %v\n"+
						"  cluster: %v", i, sIDs[i], cIDs[i], sIDs, cIDs)
				}
			}
		})
	}
}

// Every document must be reachable by paging through the cluster.
//
// The sharpest consequence of the double skip: rows 0-3 of each shard could not
// be reached from ANY page, so a client walking from=0,4,8,... never saw them
// and had no way to know.
func TestPagingReachesEveryDocument(t *testing.T) {
	parts := esCorpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	seen := map[string]bool{}
	const page = 3
	for from := 0; from < 24; from += page {
		body := fmt.Sprintf(`{"query":{"match_all":{}},"from":%d,"size":%d}`, from, page)
		_, ids := esSearch(t, cluster, body)
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("document %q appeared on two pages", id)
			}
			seen[id] = true
		}
		if len(ids) == 0 {
			break
		}
	}
	if len(seen) != 12 {
		t.Fatalf("paging reached %d of 12 documents: %v", len(seen), seen)
	}
}

// esCorpus is 12 documents split across n shards, with distinct messages so a
// hit can be named.
func esCorpus(n int) [][]string {
	out := make([][]string, n)
	for i := 0; i < 12; i++ {
		out[i%n] = append(out[i%n], fmt.Sprintf(
			`{"_time":"2026-06-01T12:00:%02dZ","_msg":"doc-%02d"}`, i, i))
	}
	return out
}

// esSearch posts a search body and returns the total and each hit's _msg.
func esSearch(t *testing.T, ts *httptest.Server, body string) (int, []string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/_search", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("_search: %d %.200s", resp.StatusCode, b)
	}
	var v struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source struct {
					Msg string `json:"_msg"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("response: %v: %.200s", err, b)
	}
	ids := make([]string, 0, len(v.Hits.Hits))
	for _, h := range v.Hits.Hits {
		ids = append(ids, h.Source.Msg)
	}
	return v.Hits.Total.Value, ids
}
