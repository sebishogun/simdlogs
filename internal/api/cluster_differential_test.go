package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

// One node versus N shards: the same data must give the same answer.
//
// This is the test the router never had. It sent the whole query to every
// shard and concatenated the final rows, so `| stats count()` answered once
// per shard and `| sort | limit 10` returned each shard's top ten -- and every
// one of those answers is a plausible number, which is why nothing caught it.
//
// The fixture is real: storage nodes with real stores, real ingest, and a
// router in front of them. A mocked shard would prove the merge merges what a
// mock returns.

// realShard is a storage node with rows in it.
func realShard(t testing.TB, rows []string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	if len(rows) > 0 {
		resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
			strings.NewReader(strings.Join(rows, "\n")+"\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	return ts
}

// corpus is deterministic rows, split evenly across n shards.
//
// The group-by key must NOT be a function of the shard index. With `level` as
// i%3 and the shard also i%3, each level lived entirely on one shard -- so a
// router that aggregated per shard and concatenated produced exactly the right
// answer, and `| stats by (level)` passed against the very defect it was
// written to catch. (i/3+i)%3 puts all three levels on all three shards while
// keeping ten rows per level, so the assertion now needs a real cross-shard
// merge to hold.
func corpus(n int) [][]string {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	out := make([][]string, n)
	for i := 0; i < 30; i++ {
		lvl := []string{"error", "warn", "info"}[(i/3+i)%3]
		user := fmt.Sprintf("u%d", i%7)
		row := fmt.Sprintf(`{"_time":%d,"_msg":"line %02d","level":%q,"user":%q,"n":"%d"}`,
			base.Add(time.Duration(i)*time.Second).UnixNano(), i, lvl, user, i)
		out[i%n] = append(out[i%n], row)
	}
	return out
}

func queryRows(t *testing.T, ts *httptest.Server, q string) (int, []string, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/select/logsql/query?query=" + urlEscape(q))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return resp.StatusCode, lines, string(b)
}

func urlEscape(s string) string {
	r := strings.NewReplacer(" ", "%20", "|", "%7C", "(", "%28", ")", "%29",
		",", "%2C", "*", "%2A", ":", "%3A", "=", "%3D", "\"", "%22", ">", "%3E")
	return r.Replace(s)
}

// Every committed pipe: the cluster's answer equals the single node's.
func TestSingleNodeAndClusterAgree(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])

	parts := corpus(3)
	shards := []string{
		realShard(t, parts[0]).URL,
		realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL,
	}
	cluster := router(t, shards...)

	for _, q := range []string{
		`*`,
		`level:=error`,
		`* | fields level, user`,
		`* | rename user as who`,
		`* | delete n`,
		`* | sort by (_msg)`,
		`* | sort by (_msg) | limit 5`,
		`* | sort by (_msg) desc | limit 3`,
		`* | offset 5 | limit 5`,
		`* | uniq by (user)`,
		`* | stats count() c`,
		`* | stats by (level) count() c`,
		`* | stats sum(n) s`,
		`* | stats min(n) lo, max(n) hi`,
		`* | fields level | sort by (level) | limit 4`,
	} {
		t.Run(q, func(t *testing.T) {
			codeS, rowsS, rawS := queryRows(t, single, q)
			codeC, rowsC, rawC := queryRows(t, cluster, q)
			if codeS != 200 {
				t.Skipf("the single node cannot answer this query: %d %.150s", codeS, rawS)
			}
			if codeC != codeS {
				t.Fatalf("single node %d, cluster %d: %.200s", codeS, codeC, rawC)
			}
			// Row ORDER is only defined where the query defines it, so the
			// comparison is a multiset unless a sort pipe is present.
			gotS, gotC := append([]string(nil), rowsS...), append([]string(nil), rowsC...)
			if !strings.Contains(q, "sort") {
				sort.Strings(gotS)
				sort.Strings(gotC)
			}
			if len(gotS) != len(gotC) {
				t.Fatalf("%d rows on one node, %d across three:\nsingle: %v\ncluster: %v",
					len(gotS), len(gotC), gotS, gotC)
			}
			for i := range gotS {
				if gotS[i] != gotC[i] {
					t.Fatalf("row %d differs:\nsingle:  %s\ncluster: %s", i, gotS[i], gotC[i])
				}
			}
		})
	}
}

// The three shapes that were silently wrong, named individually so a
// regression says which one came back.
func TestTheThreeShapesThatWereSilentlyWrong(t *testing.T) {
	parts := corpus(3)
	cluster := router(t,
		realShard(t, parts[0]).URL,
		realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL,
	)

	t.Run("stats answers once, not once per shard", func(t *testing.T) {
		code, rows, raw := queryRows(t, cluster, `* | stats count() c`)
		if code != 200 {
			t.Fatalf("%d: %s", code, raw)
		}
		if len(rows) != 1 {
			t.Fatalf("%d rows for a whole-cluster count, want 1 (one per shard is the "+
				"defect): %v", len(rows), rows)
		}
		if !strings.Contains(rows[0], `"c":"30"`) {
			t.Errorf("the count is not the cluster's 30: %s", rows[0])
		}
	})

	t.Run("limit is the cluster's, not each shard's", func(t *testing.T) {
		_, rows, raw := queryRows(t, cluster, `* | sort by (_msg) | limit 5`)
		if len(rows) != 5 {
			t.Fatalf("%d rows for limit 5 across three shards (15 is the defect): %s",
				len(rows), raw)
		}
	})

	t.Run("uniq is cluster-wide", func(t *testing.T) {
		_, rows, raw := queryRows(t, cluster, `* | uniq by (user)`)
		// Seven distinct users in the corpus, several present on more than one
		// shard.
		if len(rows) != 7 {
			t.Fatalf("%d distinct users, want 7 (per-shard duplicates are the defect): %s",
				len(rows), raw)
		}
	})
}

// A query that cannot be answered correctly across shards is refused with the
// reason, not answered with a plausible number.
func TestANonMergeableQueryIsRefusedByTheRouter(t *testing.T) {
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, q := range []string{
		`* | stats avg(n) a`,
		`* | stats count_uniq(user) u`,
	} {
		t.Run(q, func(t *testing.T) {
			code, _, raw := queryRows(t, cluster, q)
			if code != http.StatusBadRequest {
				t.Fatalf("%q returned %d, want 400: %.300s", q, code, raw)
			}
			if !strings.Contains(raw, "shards") {
				t.Errorf("the refusal does not explain itself: %.300s", raw)
			}
		})
	}
}
