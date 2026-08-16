package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// The caller's cardinality bound reaches the shards.
//
// 883508c's entire subject, and it had NO test: a reviewer reverted
// `maxValuesParam(r)` to the literal `"0"` it replaced and the whole
// internal/api package stayed green.
//
// The two halves both matter and fail differently:
//
//   - Sending the shards `0` means unlimited, and _time has roughly one distinct
//     value per row, so the shard body grows 85.8 bytes per row -- 54.9 MB at
//     640,000 rows, and above ~3.1M rows in the window it exceeds
//     peerMaxBodyBytes and EVERY cluster facets request fails after each shard
//     has allocated ~2.4 GiB building a body the router throws away.
//   - Sending the shards nothing at all caps each of them at its own default,
//     so a caller asking for 5000 got `{"facets":[]}` -- which is the defect
//     883508c was written for.
//
// So the assertion is on what the SHARD RECEIVES, not on the merged answer: the
// merged answer is the same either way for a small fixture, which is exactly why
// the reverted fix stayed green.

// paramShard records the query string of every request and answers a facets body.
func paramShard(t *testing.T, seen *[]string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*seen = append(*seen, r.URL.RawQuery)
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
		fmt.Fprint(w, `{"facets":[{"field":"svc","values":[{"value":"a","hits":1}]}]}`)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestTheCallersCardinalityBoundReachesTheShards(t *testing.T) {
	for _, tc := range []struct {
		name, query, want string
	}{
		// Asked for explicitly: that number, not the shard's default and not 0.
		{"explicit", "&max_values_per_field=5000", "max_values_per_field=5000"},
		// Not asked for: the DEFAULT, not 0. This is the half whose absence
		// turns a dashboard request into a multi-gigabyte shard body.
		{"default", "", fmt.Sprintf("max_values_per_field=%d", query.DefaultFacetMaxValues)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			sh := paramShard(t, &seen)
			ts := router(t, sh.URL)

			code, _, raw := getJSONFrom(t, ts, "/select/logsql/facets?query=*"+tc.query)
			if code != 200 {
				t.Fatalf("%d: %s", code, raw)
			}
			if len(seen) == 0 {
				t.Fatal("no shard was asked")
			}
			for i, q := range seen {
				if !strings.Contains(q, tc.want) {
					t.Errorf("shard request %d was %q, want it to carry %q -- `0` means "+
						"unlimited and _time has one distinct value per row, so the shard "+
						"body grows 85.8 B/row and fails outright above ~3.1M rows",
						i, q, tc.want)
				}
			}
		})
	}
}
