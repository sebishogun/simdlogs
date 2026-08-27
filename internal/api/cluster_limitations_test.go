package api

import (
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// THE STATED CLUSTER LIMITATIONS ARE TRUE OF THE TREE.
//
// `docs/release-readiness.md` lists four cluster limitations under "Not
// blockers, but stated", and `CHANGELOG.md` repeats them under known
// limitations. They were prose. A limitation nobody re-checks is how a stale
// one survives -- and a stated limitation that is no longer true is worse than
// the limitation, because a caller designs around a restriction that has been
// lifted, or relies on a refusal that has quietly become an answer.
//
// Measured, and this is what the gate pins:
//
//	                          router      storage node
//	/select/logsql/tail       501         200
//	/select/vector            501         (a query error, not a refusal)
//	/admin/cluster/repair     200         501
//
// The two REFUSALS are the interesting direction. `tail` and `vector` refuse
// on a router because a router has no local store to stream from; `repair`
// refuses on a storage node because it reconciles replicas and a storage node
// has none. Each is a deliberate 501 and each is documented as one.
//
// The two ANSWERS are the control. Without them a server that answered 501 to
// everything would pass.
func TestTheStatedClusterLimitationsAreTrue(t *testing.T) {
	storeSrv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storeSrv.Close()
	store := httptest.NewServer(storeSrv.Handler())
	defer store.Close()

	routerSrv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer routerSrv.Close()
	routerSrv.SetBackends([]string{store.URL})
	router := httptest.NewServer(routerSrv.Handler())
	defer router.Close()

	for _, tc := range []struct {
		name, path string
		on         *httptest.Server
		want       int
		why        string
	}{
		{
			"tail refuses on a router", "/select/logsql/tail?query=%2A", router,
			http.StatusNotImplemented,
			"a router has no local store to stream from, and answering from " +
				"one shard would be a tail of part of the cluster",
		},
		{
			"vector refuses on a router", "/select/vector?query=%2A", router,
			http.StatusNotImplemented,
			"same reason as tail",
		},
		{
			"repair refuses on a storage node", "/admin/cluster/repair", store,
			http.StatusNotImplemented,
			"repair reconciles replicas and a storage node has none to reconcile",
		},
		// THE CONTROLS. A server refusing everything would satisfy the three
		// above; these say the refusals are specific.
		{
			// COSTS 8 SECONDS, deliberately. A tail is a stream: it does not
			// end, so any GET against it runs to `chaosGet`'s client timeout.
			// The status arrives with the headers long before that, and the
			// wait is the body read. It is the only control that proves the
			// 501 above is about being a ROUTER rather than about `tail`
			// itself, so the eight seconds buy something.
			"tail ANSWERS on a storage node", "/select/logsql/tail?query=%2A", store,
			http.StatusOK,
			"the refusal above is about being a router, not about tail",
		},
		{
			"repair ANSWERS on a router", "/admin/cluster/repair", router,
			http.StatusOK,
			"the refusal above is about being a storage node, not about repair",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, tc.on.URL+tc.path)
			if code != tc.want {
				t.Errorf("%s answered %d, want %d — %s.\nA stated limitation "+
					"that is no longer true is worse than the limitation: a "+
					"caller designs around a restriction that was lifted, or "+
					"relies on a refusal that quietly became an answer.\n%.200s",
					tc.path, code, tc.want, tc.why, body)
			}
		})
	}
}

// AND THE ONE THIS FILE WAS MISSING: the non-mergeable aggregates are
// ANSWERED across shards, not refused.
//
// The header above says a stated limitation that is no longer true is worse
// than the limitation, and the gate it introduced covered four bullets. The
// fifth said:
//
//	Non-mergeable aggregates (quantile, avg, uniq, count_uniq, histogram,
//	rate) are refused across shards rather than answered, on every stats
//	surface.
//
// `cluster_stats_exact.go` answers all six -- exactly, by fetching the rows
// and aggregating once -- and `docs/lld/cluster.md` has said so since
// 2026-08-16 ("This was a REFUSAL until 2026-08-16"). So the release document
// and the changelog told a caller to design around a restriction that had
// been lifted two rounds earlier, in the section whose entire job is to say
// what the release cannot do.
//
// TWO HALVES, and neither is enough alone. The behaviour half measures the
// router; the document half reads the two files, because a claim can go stale
// without the code changing at all -- which is what happened here.
func TestTheNonMergeableAggregatesAreAnsweredNotRefused(t *testing.T) {
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)
	single := realShard(t, corpus(1)[0])

	// An explicit window on both, so `rate()` -- a count over the window's
	// seconds -- is not two processes disagreeing about what time it is.
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
	win := fmt.Sprintf("&start=%d&end=%d", base, base+86400)

	for _, q := range []string{
		`* | stats quantile(0.5, n) v`,
		`* | stats avg(n) v`,
		`* | uniq by (user)`,
		`* | stats count_uniq(user) v`,
		`* | stats histogram(n) v`,
		`* | stats rate() v`,
	} {
		t.Run(q, func(t *testing.T) {
			nodeCode, nodeRows, nodeRaw := queryRowsParams(t, single, q, win[1:])
			clCode, clRows, clRaw := queryRowsParams(t, cluster, q, win[1:])
			if nodeCode != 200 {
				t.Fatalf("the single node answered %d, so this row proves "+
					"nothing about a cluster: %.200s", nodeCode, nodeRaw)
			}
			if clCode != 200 {
				t.Fatalf("a router answered %d for %q.\nThe release document "+
					"and CHANGELOG.md say this aggregate is REFUSED across "+
					"shards. If that became true again, the documents are "+
					"right and this gate is what has to change -- but say so "+
					"deliberately: %.200s", clCode, q, clRaw)
			}
			if strings.Join(clRows, "\n") != strings.Join(nodeRows, "\n") {
				t.Errorf("%q: the cluster answered\n%s\nand the node answered\n%s",
					q, clRaw, nodeRaw)
			}
		})
	}

	// THE DOCUMENT HALF. The behaviour above is what the two files describe,
	// so a bullet claiming a refusal is a bullet describing a different build.
	t.Run("the documents do not claim a refusal", func(t *testing.T) {
		for _, path := range []string{"../../docs/release-readiness.md", "../../CHANGELOG.md"} {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading the document whose claim this gates: %v", err)
			}
			for _, line := range strings.Split(string(b), "\n") {
				low := strings.ToLower(line)
				if !strings.Contains(low, "non-mergeable aggregate") {
					continue
				}
				// The bullet may wrap, so the sentence is checked across the
				// whole paragraph it starts.
				para := paragraphFrom(string(b), line)
				if strings.Contains(strings.ToLower(para), "refused across shards") {
					t.Errorf("%s still says the non-mergeable aggregates are "+
						"refused across shards:\n\t%s\nThe router answers all "+
						"six above, and docs/lld/cluster.md has recorded the "+
						"lift since 2026-08-16. A stated limitation that is no "+
						"longer true is worse than the limitation.", path, para)
				}
			}
		}
	})
}

// paragraphFrom returns the blank-line-delimited block of doc containing line,
// so a claim that wraps across two source lines is read as one sentence.
func paragraphFrom(doc, line string) string {
	for _, para := range strings.Split(doc, "\n\n") {
		if strings.Contains(para, line) {
			return strings.Join(strings.Fields(para), " ")
		}
	}
	return line
}

// FOUR MORE STATED FACTS THAT THE TREE CONTRADICTED, AND TWO COUNTS.
//
// Same shape as the entry above and a different set of documents: README.md,
// CHANGELOG.md, docs/lld/cluster.md and docs/release-readiness.md each stated
// something the code had stopped doing, or a number the code had moved past.
// Every one of them was ungated prose, and one was contradicted by another
// sentence in the SAME FILE ten lines up.
//
// What was measured, on this tree:
//
//	claim                                          what the tree does
//	README: ES `exists` "changes no answer"        es.go maps it to
//	                                               NOT (field == ""), and
//	                                               es_contract_test.go asserts
//	                                               3 of 4 documents
//	CHANGELOG: `ValidateClusterBackup` "has no     cmd/simdlogs/restore.go
//	  caller"                                      calls it, and
//	                                               unwired_test.go's ratchet
//	                                               already says so in prose
//	cluster.md: a downed shard "contributes        fanOutChecked REFUSES: 503
//	  nothing (a partial answer, not an error)"    unless the caller sets
//	                                               allow_partial_response=1
//	active docs: "22 targets"                      23 `func Fuzz` in the tree
//	cluster.md: "14 of 46"                         47, said correctly ten
//	                                               lines above the wrong one
//
// AND THE COVERAGE FACT WORTH RECORDING: only three tests in the whole
// repository read docs/release-readiness.md or CHANGELOG.md at all -- the two
// halves above and the changelog gate in internal/tests/docs. Every other
// prose claim in either file is ungated, so this table is a sample, not a
// sweep.
func TestTheStatedFactsAboutTheCodeAreTrueOfTheCode(t *testing.T) {
	t.Run("README does not say ES exists changes no answer", func(t *testing.T) {
		// The behaviour half FIRST, so the document is checked against a
		// measurement rather than against another document. Four documents,
		// three with a `host` field.
		ts := esServer(t,
			map[string]string{"_msg": "a", "level": "error", "host": "h1"},
			map[string]string{"_msg": "b", "level": "warn", "host": "h2"},
			map[string]string{"_msg": "c", "level": "info", "host": "h1"},
			map[string]string{"_msg": "d", "level": "info"}) // no host at all
		_, withHost, _, raw := esSearchRaw(t, ts, `{"query":{"exists":{"field":"host"}},"size":100}`)
		_, all, _, _ := esSearchRaw(t, ts, `{"query":{"match_all":{}},"size":100}`)
		if withHost == all {
			t.Fatalf("`exists` answered the same %d documents as match_all, so "+
				"the README's claim would be TRUE and this gate is what has to "+
				"change: %.300s", all, raw)
		}
		mustNotSay(t, "../../README.md", "exists", []string{
			"changes no answer",
			"accepted and changes no answer",
		})
		// AND THE LLD, WHICH SAID THE SAME THING AND WAS NOT READ.
		//
		// Round 18 corrected README.md's copy of this claim and left
		// docs/lld/api.md's, which was longer and more specific about being
		// wrong: it listed `exists` among the clauses esToQuery decodes and
		// discards and said an exists-only search returns the whole window.
		// A gate pointed at one copy of a claim is a gate on that copy.
		mustNotSay(t, "../../docs/lld/api.md", "exists", []string{
			"decoded but ignored",
			"an exists-only search matches the whole window",
			"changes no answer",
		})
		mustNotSay(t, "../../docs/architecture.md", "exists", []string{
			"accepted on the wire but changes no answer",
			"decoded, never mapped to a predicate",
		})
	})

	t.Run("the LLD does not say ES terms is unsupported", func(t *testing.T) {
		// The behaviour half: `terms` narrows, so it is applied.
		ts := esServer(t,
			map[string]string{"_msg": "a", "level": "error"},
			map[string]string{"_msg": "b", "level": "warn"},
			map[string]string{"_msg": "c", "level": "info"},
			map[string]string{"_msg": "d", "level": "info"})
		_, two, _, raw := esSearchRaw(t, ts,
			`{"query":{"terms":{"level":["error","warn"]}},"size":100}`)
		_, all, _, _ := esSearchRaw(t, ts, `{"query":{"match_all":{}},"size":100}`)
		if two != 2 || all != 4 {
			t.Fatalf("`terms` answered %d of %d documents, want 2 of 4. If it "+
				"really were unsupported the LLD would be right and this gate "+
				"is what has to change: %.300s", two, all, raw)
		}
		mustNotSay(t, "../../docs/lld/api.md", "terms", []string{
			"`terms` is not supported",
			"terms is not supported",
		})
	})

	t.Run("CHANGELOG does not say ValidateClusterBackup has no caller", func(t *testing.T) {
		// The behaviour half: a production (non-test) file calls it.
		if !calledOutsideTests(t, "ValidateClusterBackup") {
			t.Fatal("no non-test file calls ValidateClusterBackup, so the " +
				"CHANGELOG's claim would be TRUE and this gate is what has to change")
		}
		mustNotSay(t, "../../CHANGELOG.md", "ValidateClusterBackup", []string{"no caller"})
	})

	t.Run("the LLD does not call a fully-down shard a partial answer", func(t *testing.T) {
		// The behaviour half: two shards, one dead, and the default is a
		// refusal rather than a short answer.
		live := realShard(t, corpus(1)[0])
		dead := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", 500) }))
		t.Cleanup(dead.Close)
		cluster := router(t, live.URL, dead.URL)
		code, _, raw := queryRowsParams(t, cluster, "*", "")
		if code == 200 {
			t.Fatalf("a router with one shard fully down answered 200, so the "+
				"LLD's \"contributes nothing (a partial answer, not an error)\" "+
				"would be TRUE and this gate is what has to change: %.200s", raw)
		}
		codeP, _, rawP := queryRowsParams(t, cluster, "*", partialParam+"=1")
		if codeP != 206 {
			t.Errorf("with %s=1 the router answered %d, want 206: %.200s",
				partialParam, codeP, rawP)
		}
		mustNotSay(t, "../../docs/lld/cluster.md", "downed replica",
			[]string{"a partial answer, not an error"})
	})

	t.Run("the documented fuzz-target counts are the tree's", func(t *testing.T) {
		got := countFuncPrefix(t, "../..", "func Fuzz")
		re := regexp.MustCompile(`\b(\d+) targets\b`)
		for _, path := range []string{
			"../../docs/release-readiness.md",
			"../../docs/security.md",
			"../../docs/verification.md",
		} {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			matches := re.FindAllStringSubmatch(string(b), -1)
			if len(matches) == 0 {
				t.Errorf("%s no longer states a fuzz-target count in the form `N targets`", path)
				continue
			}
			for _, m := range matches {
				n, _ := strconv.Atoi(m[1])
				if n != got {
					t.Errorf("%s says %q and the tree has %d `func Fuzz` targets", path, m[0], got)
				}
			}
		}
	})
}

// AND THE FEDERATION-BRANCH SENTENCE, whose two numbers disagreed with the
// paragraph ten lines above it.
//
// docs/lld/cluster.md says "47 routes" and then, further down, said "14 of
// 46". `TestTheDocumentedRouteCountIsTheRealOne` matches `(\d+) routes` and so
// never saw the second number at all -- the claim slipped past a gate written
// for exactly this, because it was spelled without the word the gate looks
// for. Both numbers are read from the source here.
func TestTheFederationBranchCountIsTheSource(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Handler()
	routes := srv.routeCountForTest()

	branches := 0
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		branches += strings.Count(string(b), "len(s.backendList()) > 0")
	}
	if branches == 0 {
		t.Fatal("no federation branches found; the expression this counts has " +
			"been renamed and the gate is counting nothing")
	}

	b, err := os.ReadFile("../../docs/lld/cluster.md")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(\d+) branches across (\d+) routes`)
	m := re.FindStringSubmatch(string(b))
	if m == nil {
		t.Fatalf("docs/lld/cluster.md no longer states the federation-branch "+
			"count in the form this gate reads (`N branches across M routes`); "+
			"the tree has %d branches across %d routes", branches, routes)
	}
	gotB, _ := strconv.Atoi(m[1])
	gotR, _ := strconv.Atoi(m[2])
	if gotB != branches || gotR != routes {
		t.Errorf("docs/lld/cluster.md says %q; the tree has %d branches across %d routes",
			m[0], branches, routes)
	}
}

// mustNotSay fails when any paragraph of doc mentioning `subject` also carries
// one of the stale phrases. Paragraph-scoped so a claim that wraps across
// source lines is read as one sentence, and subject-scoped so an unrelated
// paragraph using the same words is not a false positive.
func mustNotSay(t *testing.T, path, subject string, stale []string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the document whose claim this gates: %v", err)
	}
	seen := false
	for _, para := range strings.Split(string(b), "\n\n") {
		flat := strings.Join(strings.Fields(para), " ")
		if !strings.Contains(strings.ToLower(flat), strings.ToLower(subject)) {
			continue
		}
		seen = true
		for _, phrase := range stale {
			if strings.Contains(strings.ToLower(flat), strings.ToLower(phrase)) {
				t.Errorf("%s still says %q about %q:\n\t%s", path, phrase, subject, flat)
			}
		}
	}
	if !seen {
		t.Errorf("%s no longer mentions %q, so this gate reads nothing. Either "+
			"the subject was renamed or the claim was deleted; say which.",
			path, subject)
	}
}

// calledOutsideTests reports whether any non-test .go file in the repository
// calls name. It is the behaviour half of the "has no caller" claim.
func calledOutsideTests(t *testing.T, name string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || found {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, line := range strings.Split(string(b), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue // a comment naming it is not a caller
			}
			if strings.Contains(line, name+"(") {
				found = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// countFuncPrefix counts test declarations starting with prefix, matching the
// workflow's discovery of fuzz targets from *_test.go files.
func countFuncPrefix(t *testing.T, root, prefix string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, prefix) {
				n++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}
