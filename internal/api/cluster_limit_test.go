package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// rowsAt fetches an NDJSON path and returns its status and non-empty lines.
func rowsAt(t *testing.T, ts *httptest.Server, path string) (int, []string) {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return resp.StatusCode, out
}

// The endpoint's `limit` bounds the CLUSTER's answer, not each shard's.
//
// It was forwarded to every shard and never applied at the coordinator, so both
// halves were wrong at once: a stats input was truncated to each shard's first
// N rows before aggregation, and a sorted select returned N rows PER SHARD.
// TestSingleNodeAndClusterAgree never sends `limit=`, which is why it stayed
// green through all of it.
func TestTheEndpointLimitIsTheClustersNotEachShards(t *testing.T) {
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL)
	single := realShard(t, corpus(1)[0])

	for _, tc := range []struct {
		q     string
		limit int
	}{
		{`*`, 5},
		{`* | stats count() c`, 5},
		{`* | stats by (level) count() c`, 2},
		{`* | sort by (_msg)`, 5},
		{`* | sort by (_msg) desc`, 3},
		{`* | uniq by (user)`, 4},
		{`* | fields level`, 7},
	} {
		name := tc.q + "&limit=" + strconv.Itoa(tc.limit)
		t.Run(name, func(t *testing.T) {
			path := "/select/logsql/query?query=" + urlEscape(tc.q) +
				"&limit=" + strconv.Itoa(tc.limit)
			sCode, sRows := rowsAt(t, single, path)
			cCode, cRows := rowsAt(t, cluster, path)
			if sCode != 200 {
				t.Skipf("the single node cannot answer it: %d", sCode)
			}
			if cCode != sCode {
				t.Fatalf("single %d, cluster %d", sCode, cCode)
			}
			// The invariant is EQUALITY WITH THE SINGLE NODE, not "at most
			// limit". A single node answers `| stats by (level)` with three
			// rows at limit=2 and all seven distinct users at limit=4 -- the
			// bound reaches its scan, not its aggregate -- so a cluster that
			// truncated to the limit would be wrong in the other direction.
			// The first version of this test asserted the tidier rule and
			// failed the cluster for matching the server it is a cluster of.
			if len(sRows) != len(cRows) {
				t.Fatalf("%d rows on one node, %d across three, at limit %d",
					len(sRows), len(cRows), tc.limit)
			}
			// THE ROWS, not only how many. A count comparison passes when the
			// cluster returns the right NUMBER of the wrong rows -- five rows
			// where the node's five are different five -- which is verbatim the
			// defect cluster_order_test.go's header documents. The node's
			// answer is the reference, so the multiset has to match it.
			if len(sRows) == 0 {
				t.Fatalf("the node returned no rows at limit %d, so this "+
					"compares two empty answers", tc.limit)
			}
			if !equalSets(sortedCopy(sRows), sortedCopy(cRows)) {
				t.Errorf("same row COUNT, different rows, at limit %d:\n"+
					"  node:    %v\n  cluster: %v", tc.limit, sRows, cRows)
			}
		})
	}
}

// A stats query's INPUT must not be truncated by the endpoint's limit.
//
// The count has to be over every matching row; `limit` bounds how many result
// rows come back, and a single-row aggregate is already under any limit above
// zero. Truncating the input made three shards each count their own first five.
func TestALimitDoesNotTruncateTheStatsInput(t *testing.T) {
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL)

	_, rows := rowsAt(t, cluster,
		"/select/logsql/query?query="+urlEscape(`* | stats count() c`)+"&limit=5")
	if len(rows) != 1 {
		t.Fatalf("%d rows for a whole-cluster count: %v", len(rows), rows)
	}
	if want := `"c":"30"`; !strings.Contains(rows[0], want) {
		t.Fatalf("the count is %s, want %s over all 30 rows -- the limit truncated "+
			"each shard's input before it was aggregated", rows[0], want)
	}
}
