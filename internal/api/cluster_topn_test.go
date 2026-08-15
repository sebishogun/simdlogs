package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The cluster's top-N must be the cluster's, not a merge of shard-local top-Ns.
//
// `limit` and `max_values_per_field` reached the shards, so each returned only
// its own top N and the merge combined truncated lists. A value that is popular
// across the cluster but unremarkable on every shard could never appear,
// however many hits it had -- and re-applying the limit after the merge looked
// like it fixed that.
//
// The fixture is built so exactly that happens: `spread` is top on every shard,
// `big1` is second on one shard only, and `second` is third everywhere while
// out-scoring big1 cluster-wide.
func TestTheClusterTopNIsNotBuiltFromShardLocalTopNs(t *testing.T) {
	shards := topNCorpus()
	nodes := make([]string, len(shards))
	for i, rows := range shards {
		nodes[i] = realShard(t, rows).URL
	}
	cluster := router(t, nodes...)

	var all []string
	for _, rows := range shards {
		all = append(all, rows...)
	}
	single := realShard(t, all)

	for _, limit := range []int{1, 2, 3, 5} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			path := fmt.Sprintf(
				"/select/logsql/field_values?query=%%2A&field=svc&limit=%d", limit)
			sVals := valueCounts(t, single, path)
			cVals := valueCounts(t, cluster, path)
			if len(sVals) != len(cVals) {
				t.Fatalf("%d values on one node, %d across shards:\n  single:  %v\n"+
					"  cluster: %v", len(sVals), len(cVals), sVals, cVals)
			}
			for i := range sVals {
				if sVals[i] != cVals[i] {
					t.Fatalf("value %d differs: single %v, cluster %v\n  single:  %v\n"+
						"  cluster: %v", i, sVals[i], cVals[i], sVals, cVals)
				}
			}
		})
	}
}

// topNCorpus: three shards where the cluster ranking differs from every
// shard's own ranking.
func topNCorpus() [][]string {
	var out [][]string
	for sh := 0; sh < 3; sh++ {
		var rows []string
		add := func(svc string, n int) {
			for i := 0; i < n; i++ {
				rows = append(rows, fmt.Sprintf(
					`{"_time":"2026-06-01T12:%02d:%02dZ","_msg":"m","svc":%q}`,
					sh, len(rows), svc))
			}
		}
		add("spread", 3) // 9 cluster-wide, top on every shard
		if sh == 0 {
			add("big1", 4) // 4 cluster-wide, top on shard 0 only
		}
		add("second", 2) // 6 cluster-wide, never top-2 on any shard
		out = append(out, rows)
	}
	return out
}

// valueCounts reads a {"values":[{value,hits}]} response.
func valueCounts(t *testing.T, ts *httptest.Server, path string) []query_ValueCount {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %.200s", path, resp.StatusCode, b)
	}
	var v struct {
		Values []query_ValueCount `json:"values"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v: %.200s", path, err, b)
	}
	return v.Values
}

type query_ValueCount struct {
	Value string `json:"value"`
	Hits  int    `json:"hits"`
}
