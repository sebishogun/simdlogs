package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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

// urlEscape is `url.QueryEscape`, and nothing else.
//
// It used to be a `strings.NewReplacer` over ten characters, which left `+`,
// `%`, `&` and `#` alone -- so a test query containing one of those measured a
// DIFFERENT query than the one written in the source. Measured through both:
//
//	query=* | math "n + 1" as m             400 `math: unexpected "1"` vs 200
//	query=* | extract_regexp "(?P<n>[0-9]+)"    200 both ways, two regexps
//
// The `+` decodes as a space, so the first query reached the parser as
// `math "n  1" as m` and the second ran `[0-9]` where the source says `[0-9]+`.
// Nothing in the suite was red, because no query in it carried one of the four
// -- a property of today's table rather than of the helper, and the next test
// to write `+` would have debugged the server instead.
func urlEscape(s string) string { return url.QueryEscape(s) }

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
		// The five that were refused across shards until the router learned to
		// aggregate merged rows instead of merging aggregates.
		`* | stats avg(n) a`,
		`* | stats quantile(0.5, n) p50`,
		`* | stats quantile(0.9, n) p90`,
		`* | stats uniq(user) u`,
		`* | stats count_uniq(user) cu`,
		`* | stats histogram(n) h`,
		`* | stats rate() r`,
		`* | stats by (level) avg(n) a`,
		`* | stats by (level) quantile(0.5, n) p50`,
		`* | stats by (user) count_uniq(level) cu`,
		`* | sort by (n) | stats avg(n) a`,
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
			// THE NODE MUST HAVE PRODUCED ROWS. Without this, zero rows on
			// both sides compares 0 == 0, the loop below never runs, and the
			// whole table passes -- a differential suite green against a
			// binary that answers nothing. (An earlier version of this
			// comment counted "all 27 queries"; the table held 26. A number
			// nothing checks is a number that is wrong.) The guard already
			// existed three files over, in cluster_stats_refusal_test.go.
			if len(gotS) == 0 {
				t.Fatalf("the single node returned no rows for %q, so this "+
					"compares two empty answers and the cluster half is "+
					"unchecked", q)
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

// A non-mergeable aggregate is ANSWERED by the router, exactly.
//
// This asserted a 400. The aggregate cannot be merged from per-shard outputs --
// that part was and is true -- but the router does not merge it: it asks the
// shards for rows and runs the aggregate once, the way the single node this
// compares against does. So the assertion becomes the number rather than the
// status.
func TestANonMergeableQueryIsAnsweredExactly(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, q := range []string{
		`* | stats avg(n) a`,
		`* | stats count_uniq(user) u`,
	} {
		t.Run(q, func(t *testing.T) {
			codeS, rowsS, rawS := queryRows(t, single, q)
			if codeS != http.StatusOK {
				t.Fatalf("the single node refused %q: %d %.200s", q, codeS, rawS)
			}
			codeC, rowsC, rawC := queryRows(t, cluster, q)
			if codeC != http.StatusOK {
				t.Fatalf("%q returned %d across shards, want the node's 200: %.300s",
					q, codeC, rawC)
			}
			if !equalSets(sortedCopy(rowsS), sortedCopy(rowsC)) {
				t.Fatalf("cluster and single node disagree on %q:\n  single:  %v\n"+
					"  cluster: %v", q, rowsS, rowsC)
			}
		})
	}
}

// The LLD's count of the differential table is the table's real length.
//
// `docs/lld/cluster.md` says the table "has grown from fifteen to 26". The
// fifteen is history and stays prose; the 26 is a live count of the table in
// TestSingleNodeAndClusterAgree and rots on the commit that adds a query --
// which is exactly how this file's own guard comment came to say "all 27
// queries" over a table of 26. The count is read from the test's SOURCE, so
// there is no second list to fall out of step, and from the DOCUMENT, so a
// constant here and a number there cannot disagree.
func TestTheLLDsDifferentialCountIsTheTables(t *testing.T) {
	src, err := os.ReadFile("cluster_differential_test.go")
	if err != nil {
		t.Fatal(err)
	}
	fn := strings.Index(string(src), "func TestSingleNodeAndClusterAgree")
	if fn < 0 {
		t.Fatal("TestSingleNodeAndClusterAgree is not in this file any more; " +
			"point this gate at wherever its table went")
	}
	rest := string(src)[fn:]
	open := strings.Index(rest, "range []string{")
	if open < 0 {
		t.Fatal("the query table is no longer a `range []string{` literal")
	}
	rest = rest[open:]
	closing := strings.Index(rest, "\n\t} {")
	if closing < 0 {
		t.Fatal("cannot find the end of the query table")
	}
	table := rest[:closing]

	// COUNTED BY ROW, not by backticks/2.
	//
	// It was `strings.Count(table, "`+'"`"'+`") / 2`, which is a count of
	// RAW-STRING literals rather than of queries: a row written as an ordinary
	// double-quoted Go string -- the natural spelling for a query containing a
	// backtick -- counted as ZERO, so adding one made the gate demand that the
	// document's number go DOWN. Every non-blank, non-comment line in the table
	// must be exactly one query literal ending in a comma, and a line that is
	// not is a failure rather than a silent miscount.
	got := 0
	for _, line := range strings.Split(table, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") || strings.HasPrefix(line, "range ") {
			continue
		}
		if !strings.HasSuffix(line, ",") || (line[0] != '`' && line[0] != '"') {
			t.Fatalf("the table row %q is not a single quoted query ending in "+
				"a comma. This gate counts rows; a row it cannot recognise is "+
				"a row it would silently not count", line)
		}
		got++
	}
	if got == 0 {
		t.Fatal("counted no queries in the table, which is the shape of a gate " +
			"that has lost its table rather than a table that is empty")
	}

	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "lld", "cluster.md"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`grown from fifteen to (\d+)`).FindStringSubmatch(string(doc))
	if m == nil {
		t.Fatal("docs/lld/cluster.md no longer says how large the differential " +
			"table has grown; if the sentence was reworded, update this gate in " +
			"the same commit")
	}
	want, _ := strconv.Atoi(m[1])
	if got != want {
		t.Fatalf("docs/lld/cluster.md says the differential table holds %d pipe "+
			"shapes; TestSingleNodeAndClusterAgree's table holds %d", want, got)
	}
}
