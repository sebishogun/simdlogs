package api

import (
	"bytes"
	"encoding/json"
	"github.com/sebishogun/simdlogs/internal/storage"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Repair, end to end: a replica that missed writes catches up.
//
// The fixture is the real failure. Two replicas of one shard; the router writes
// to both; one replica goes away; more writes land on the survivor only; the
// replica comes back holding less than its peer. Before repair existed that
// state was permanent, and a read that happened to land on the short replica
// returned fewer rows with nothing to say so.

// replicatedShard is one shard with n replicas behind a router.
func replicatedShard(t *testing.T, n int) (*Server, *httptest.Server, []*httptest.Server) {
	t.Helper()
	nodes := make([]*httptest.Server, n)
	urls := make([]string, n)
	for i := range nodes {
		nodes[i] = realShard(t, nil)
		urls[i] = nodes[i].URL
	}
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(n) // one shard, n replicas
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts, nodes
}

func postLines(t *testing.T, target string, lines ...string) (int, string) {
	t.Helper()
	resp, err := http.Post(target+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(strings.Join(lines, "\n")+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func line(ts, msg string) string {
	return `{"_time":"` + ts + `","_msg":"` + msg + `"}`
}

func rowCount(t *testing.T, ts *httptest.Server) int {
	t.Helper()
	code, rows, raw := queryRows(t, ts, "*")
	if code != 200 {
		t.Fatalf("query: %d %.200s", code, raw)
	}
	return len(rows)
}

func runRepair(t *testing.T, router *httptest.Server) RepairReport {
	t.Helper()
	resp, err := http.Post(router.URL+"/admin/cluster/repair", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("repair: %d %.300s", resp.StatusCode, b)
	}
	var rep RepairReport
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("repair report: %v: %.300s", err, b)
	}
	return rep
}

func TestRepairBringsAMissedWriteBackToAReplica(t *testing.T) {
	_, router, nodes := replicatedShard(t, 2)

	// Both replicas take the first write.
	if code, body := postLines(t, router.URL,
		line("2026-06-01T12:00:00Z", "before")); code >= 300 {
		t.Fatalf("first write: %d %s", code, body)
	}
	for i, n := range nodes {
		if got := rowCount(t, n); got != 1 {
			t.Fatalf("replica %d has %d rows after the first write, want 1", i, got)
		}
	}

	// Replica 1 misses the next two: written straight to replica 0, which is
	// what a router does when a replica is unreachable and the level allows it.
	postLines(t, nodes[0].URL, line("2026-06-01T12:00:01Z", "missed-a"))
	postLines(t, nodes[0].URL, line("2026-06-01T12:00:02Z", "missed-b"))
	if got := rowCount(t, nodes[0]); got != 3 {
		t.Fatalf("replica 0 has %d rows, want 3", got)
	}
	if got := rowCount(t, nodes[1]); got != 1 {
		t.Fatalf("replica 1 has %d rows, want the 1 it started with", got)
	}

	rep := runRepair(t, router)
	if !rep.Complete {
		t.Fatalf("the pass reported itself incomplete: %+v", rep)
	}
	if rep.Copied != 2 {
		t.Fatalf("copied %d groups, want the 2 replica 1 was missing: %+v", rep.Copied, rep)
	}
	if got := rowCount(t, nodes[1]); got != 3 {
		t.Fatalf("replica 1 has %d rows after repair, want 3", got)
	}
	if got := rowCount(t, nodes[0]); got != 3 {
		t.Fatalf("repair changed replica 0 to %d rows; it must only ever add to the "+
			"replica that is behind", got)
	}
}

// Repair converges: a second pass over an in-step shard copies nothing.
//
// Without this, a repair that re-copied every group each pass would look
// identical to a working one from the first pass alone -- and would double the
// shard's data on every run.
func TestASecondRepairPassCopiesNothing(t *testing.T) {
	_, router, nodes := replicatedShard(t, 2)
	postLines(t, router.URL, line("2026-06-01T12:00:00Z", "shared"))
	postLines(t, nodes[0].URL, line("2026-06-01T12:00:01Z", "only-on-0"))

	first := runRepair(t, router)
	if first.Copied != 1 {
		t.Fatalf("first pass copied %d, want 1", first.Copied)
	}
	rowsAfterFirst := rowCount(t, nodes[1])

	second := runRepair(t, router)
	if second.Copied != 0 {
		t.Fatalf("second pass copied %d groups over an in-step shard; repair is not "+
			"idempotent and every run grows the shard", second.Copied)
	}
	if got := rowCount(t, nodes[1]); got != rowsAfterFirst {
		t.Fatalf("replica 1 went from %d to %d rows on a no-op pass",
			rowsAfterFirst, got)
	}
}

// Repair is symmetric: a group only replica 1 has goes to replica 0.
//
// Not a nicety. If repair only ever pushed from a designated leader, the rows a
// non-leader took while the leader was down would be lost the moment repair ran
// -- which turns a healing process into a data-destroying one.
func TestRepairMovesDataInBothDirections(t *testing.T) {
	_, router, nodes := replicatedShard(t, 2)
	postLines(t, nodes[0].URL, line("2026-06-01T12:00:00Z", "only-on-0"))
	postLines(t, nodes[1].URL, line("2026-06-01T12:00:01Z", "only-on-1"))

	rep := runRepair(t, router)
	if rep.Copied != 2 {
		t.Fatalf("copied %d, want one group in each direction: %+v", rep.Copied, rep)
	}
	for i, n := range nodes {
		if got := rowCount(t, n); got != 2 {
			t.Fatalf("replica %d has %d rows after repair, want both", i, got)
		}
		_, rows, raw := queryRows(t, n, "*")
		joined := strings.Join(rows, " ")
		if !strings.Contains(joined, "only-on-0") || !strings.Contains(joined, "only-on-1") {
			t.Fatalf("replica %d does not hold both writes: %.300s", i, raw)
		}
	}
}

// An unreachable replica is not treated as an empty one.
//
// The distinction matters more than it looks: a replica that cannot be asked
// might hold groups nobody else has. Reading its silence as "holds nothing"
// would make the union wrong in both directions -- repair would copy the whole
// shard into a node that already has it, and would drop the groups only that
// node holds out of the union entirely.
func TestAnUnreachableReplicaIsNotTreatedAsEmpty(t *testing.T) {
	srv, router, nodes := replicatedShard(t, 2)
	postLines(t, router.URL, line("2026-06-01T12:00:00Z", "shared"))

	// Point replica 1 at a port nothing is listening on.
	dead := "http://127.0.0.1:1"
	srv.SetBackends([]string{nodes[0].URL, dead})

	rep := runRepair(t, router)
	if rep.Complete {
		t.Fatal("a pass that could not reach a replica reported itself complete")
	}
	if len(rep.Errors) != 1 {
		// Exactly one. With both replicas unreachable every assertion below
		// still holds -- nothing is copied, the pass is incomplete -- and the
		// test would pass without ever exercising the union logic it is about.
		t.Fatalf("%d errors, want exactly the one unreachable replica: %v",
			len(rep.Errors), rep.Errors)
	}
	if len(rep.Shards) != 1 || len(rep.Shards[0].Replicas) != 2 {
		t.Fatalf("unexpected shard shape: %+v", rep.Shards)
	}
	live := rep.Shards[0].Replicas[0]
	if live.Err != "" {
		t.Fatalf("the replica that should be reachable reported %q", live.Err)
	}
	if len(live.Groups) != 1 || live.rows() != 1 {
		t.Fatalf("the reachable replica reported %d groups / %d rows, want the one "+
			"group it holds", len(live.Groups), live.rows())
	}
	if rep.Copied != 0 {
		t.Fatalf("copied %d groups while a replica was unreachable; the reachable "+
			"one already had everything the union knows about", rep.Copied)
	}
	if got := rowCount(t, nodes[0]); got != 1 {
		t.Fatalf("the reachable replica has %d rows, want the 1 it started with", got)
	}
}

// Repair on a node that is not a router is refused, not silently a no-op.
func TestRepairOnAStorageNodeIsRefused(t *testing.T) {
	node := realShard(t, nil)
	resp, err := http.Post(node.URL+"/admin/cluster/repair", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("%d, want 501: %.200s", resp.StatusCode, b)
	}
	if !strings.Contains(string(b), "router") {
		t.Errorf("the refusal does not say why: %.200s", b)
	}
}

// A peer cannot push arbitrary bytes into a storage node through the adopt
// endpoint.
func TestTheAdoptEndpointValidatesWhatItIsGiven(t *testing.T) {
	node := realShard(t, nil)
	post := func(digest string, body string) (int, string) {
		t.Helper()
		resp, err := http.Post(node.URL+pathReplicaGroup+"?digest="+digest,
			"application/octet-stream", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := post("", "anything"); code != http.StatusBadRequest {
		t.Errorf("no digest: %d %.150s", code, body)
	}
	if code, body := post("deadbeef", "not a group"); code != http.StatusBadRequest {
		t.Errorf("bytes that do not match the digest: %d %.150s", code, body)
	}
	if got := rowCount(t, node); got != 0 {
		t.Fatalf("%d rows landed in the store from refused adoptions", got)
	}
}

// Three replicas: the union's own members are checked against each other, not
// only against what the destination already had.
//
// The overlap guard read the destination's PRE-PASS inventory. With two
// replicas that is symmetric and enough; with three it is not:
//
//	A = {g1[0,10], g2[10,20]}   uncompacted
//	B = {G[0,20]}               compacted
//	C = {}                      missed the range, or restored empty
//
// All three of g1, g2 and G overlap nothing in C's empty inventory, so all
// three were copied and C held the range twice -- reported as copied: 3,
// blocked: 0, complete: true, which is exactly the duplication the guard's own
// comment says it prevents.
func TestRepairDoesNotCopyOverlappingMembersOfOneUnion(t *testing.T) {
	have := []storage.GroupDigest{}
	union := []storage.GroupDigest{
		{Digest: "g1", TimeMin: 0, TimeMax: 10},
		{Digest: "g2", TimeMin: 11, TimeMax: 20},
		{Digest: "G", TimeMin: 0, TimeMax: 20},
	}

	// A holds the two pieces, B holds the compacted group, C holds nothing.
	holders := map[string]map[int]bool{
		"g1": {0: true}, "g2": {0: true}, "G": {1: true},
	}

	// What the pass does now: the inventory grows as it copies, and a group
	// sharing a holder with one already there is not a candidate for overlap.
	spans := append([]storage.GroupDigest(nil), have...)
	var copied, blocked []string
	for _, g := range union {
		if overlappingFrom(spans, g, holders) != "" {
			blocked = append(blocked, g.Digest)
			continue
		}
		copied = append(copied, g.Digest)
		spans = append(spans, g)
	}
	if len(copied) != 2 || len(blocked) != 1 {
		t.Fatalf("copied %v, blocked %v; want two disjoint groups copied and the "+
			"one covering both blocked", copied, blocked)
	}
	if blocked[0] != "G" {
		t.Errorf("blocked %q, want G -- the compacted group covering the two "+
			"already copied", blocked[0])
	}

	// And the old shape, kept here so the difference is visible rather than
	// asserted: checking only the pre-pass inventory copies all three.
	var wouldCopy int
	for _, g := range union {
		if overlapping(have, g) == "" {
			wouldCopy++
		}
	}
	if wouldCopy != 3 {
		t.Errorf("checking only the pre-pass inventory copies %d of 3; this test "+
			"is asserting a difference that does not exist", wouldCopy)
	}
}

// Two ordinary adjacent groups from ONE replica are both copied, even when
// they share a boundary timestamp.
//
// overlapping uses a CLOSED interval on purpose -- a group whose TimeMax
// equals the next one's TimeMin shares a timestamp, and a duplicate row at the
// boundary is still a duplicate. That is right for a compacted group against
// its pieces and wrong for two pieces of one store, which by this file's own
// invariant cannot contain the same rows.
//
// It only became reachable when the three-replica fix started growing `spans`
// mid-pass, and it was PERMANENT: copy g1, block g2, and every later pass
// rebuilds the same state and blocks it again, so the destination stays short
// forever while reporting a healthy store -- and a read that lands on it
// answers fewer rows at HTTP 200 with Complete true, because Complete reports
// local quarantine and not divergence from peers.
//
// Rows sharing one nanosecond split across a size-triggered flush produce
// exactly this, on any client timestamping at second or millisecond
// granularity.
func TestAdjacentGroupsFromOneReplicaAreNotBlocked(t *testing.T) {
	const T = 1767225600000000000
	g1 := storage.GroupDigest{Digest: "g1", TimeMin: T - 100, TimeMax: T}
	g2 := storage.GroupDigest{Digest: "g2", TimeMin: T, TimeMax: T + 100} // touching
	holders := map[string]map[int]bool{"g1": {0: true}, "g2": {0: true}}

	// Destination C is empty; the pass copies g1 then meets g2.
	spans := []storage.GroupDigest{g1}
	if blocked := overlappingFrom(spans, g2, holders); blocked != "" {
		t.Errorf("g2[%d,%d] blocked by %s after g1[%d,%d] was copied; both come "+
			"from replica 0, which cannot hold the same rows twice -- and this "+
			"block never clears, so the destination is short forever",
			g2.TimeMin, g2.TimeMax, blocked, g1.TimeMin, g1.TimeMax)
	}

	// And the guard still fires when they come from DIFFERENT stores, which is
	// the compaction case it exists for.
	other := map[string]map[int]bool{"g1": {0: true}, "g2": {1: true}}
	if overlappingFrom(spans, g2, other) == "" {
		t.Error("two touching groups from different replicas were not blocked; " +
			"that is the compaction divergence the guard is for")
	}

	// The raw predicate is unchanged: it is the closed interval, deliberately.
	if overlapping(spans, g2) == "" {
		t.Error("overlapping no longer treats a shared boundary as an overlap; " +
			"the closed interval is the point and this test would then be " +
			"asserting nothing")
	}
}

// A peer answering something that parses but is not a replica state is refused.
//
// The guard used to be `bytes.Contains(body, []byte("\"groups\""))`, which
// matches the quoted token anywhere -- including inside a VALUE. Two reviewers
// bypassed it with `{"note":"groups"}`, one on the first attempt, and a third
// found that the key check had been described in a commit message and never
// written. A peer that passes reads as an EMPTY replica, and repair then
// copies the whole shard into a node that never reported a state, which is the
// outcome ReplicaState.Err's own doc exists to prevent.
func TestABodyThatParsesButIsNotAStateIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		refuse     bool
	}{
		{"a real empty state", `{"groups":[],"highWatermark":0}`, false},
		{"a real state with groups", `{"groups":[{"digest":"a","timeMin":1,"timeMax":2}]}`, false},
		{"null", `null`, true},
		{"empty object", `{}`, true},
		{"groups as a VALUE", `{"note":"groups"}`, true},
		{"groups inside a longer value", `{"msg":"no groups here"}`, true},
		{"groups as a nested key", `{"inner":{"groups":[]}}`, true},
		{"groups is null", `{"groups":null}`, true},
		{"groups is null with space", `{"groups": null }`, true},
		{"an array", `[]`, true},
		{"a bare string", `"groups"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
				w.Write([]byte(tc.body))
			}))
			t.Cleanup(peer.Close)

			srv, err := NewServer(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Close() })
			srv.SetBackends([]string{peer.URL})
			srv.SetReplicas(1)

			req := httptest.NewRequest("GET", "/", nil)
			st := srv.askReplicaState(req, 0, 0, peer.URL)
			if refused := st.Err != ""; refused != tc.refuse {
				t.Errorf("body %s gave Err=%q; want refused=%v.\n"+
					"A body that is not a state must not read as an EMPTY one: "+
					"repair would copy the whole shard into it",
					tc.body, st.Err, tc.refuse)
			}
		})
	}
}

// A replica holding overlapping groups of its own cannot certify a pair.
//
// The holder-sharing exemption rests on "one store cannot hold two groups with
// the same rows", and that is FALSE: ingesting one time range twice without a
// write id leaves a store holding [T0,T0], [T0,T9] and [T1,T9] at once. A
// reviewer built it with an ordinary retried ingest, and the result was worse
// than the stall the exemption replaced -- every pair exempted, a CLEAN replica
// copied onto, all of its rows duplicated, and the pass reporting
// complete: true. Loud-and-intact became silent-and-destroyed.
func TestASelfOverlappingReplicaCannotCertifyAPair(t *testing.T) {
	clean := []storage.GroupDigest{
		{Digest: "c1", TimeMin: 0, TimeMax: 10},
		{Digest: "c2", TimeMin: 11, TimeMax: 20},
	}
	doubled := []storage.GroupDigest{
		{Digest: "d1", TimeMin: 0, TimeMax: 0},
		{Digest: "d2", TimeMin: 0, TimeMax: 9}, // the re-ingest, covering d1 and d3
		{Digest: "d3", TimeMin: 1, TimeMax: 9},
	}

	if got := selfOverlap(clean); got != "" {
		t.Errorf("two disjoint groups reported as overlapping: %s", got)
	}
	if got := selfOverlap(doubled); got == "" {
		t.Fatal("a store holding [0,0], [0,9] and [1,9] was not reported as " +
			"self-overlapping; those three cover the same rows twice")
	}
	if got := selfOverlap(nil); got != "" {
		t.Errorf("an empty inventory reported as overlapping: %s", got)
	}

	// A single group can never overlap itself, however wide.
	if got := selfOverlap(doubled[:1]); got != "" {
		t.Errorf("one group reported as overlapping: %s", got)
	}
}

// Two ordinary flushes touching at one timestamp DO NOT converge, and the
// stall is reported rather than silent. Through the real handler.
//
// This test asserts a limitation, not a feature, and it is here so the
// limitation cannot be lost by accident.
//
// The router decides whether two groups may hold the same rows from their
// [TimeMin,TimeMax] spans alone, and those two shapes are indistinguishable
// that way:
//
//	two adjacent flushes           [T0,T1] [T1,T2]   different rows
//	a re-ingest of one instant     [T1,T1] [T1,T1]   the same rows twice
//
// Three variants were tried, each broken by a reviewer who broke the previous
// one: no check at all duplicated a clean replica's every row at
// complete: true; a half-open test did the same, because a single-instant
// re-ingest shares exactly one endpoint like an ordinary adjacency; the closed
// test here duplicates nothing and stalls on ordinary adjacency.
//
// Repair's promise is that it never makes a replica worse, so this is the
// right one of the three. It is not the right answer: that needs the
// DESTINATION to compare the rows at the overlapping instant, which is
// evidence it has and the router does not.
func TestTouchingFlushesStallLoudlyRatherThanDuplicate(t *testing.T) {
	_, router, nodes := replicatedShard(t, 3)

	postLines(t, nodes[0].URL,
		line("2026-06-01T12:00:00Z", "a"), line("2026-06-01T12:00:10Z", "b"))
	postLines(t, nodes[0].URL,
		line("2026-06-01T12:00:10Z", "c"), line("2026-06-01T12:00:20Z", "d"))
	if got := rowCount(t, nodes[0]); got != 4 {
		t.Fatalf("replica 0 has %d rows, want 4", got)
	}

	rep := runRepair(t, router)

	// The part that must never regress: nothing is duplicated.
	if got := rowCount(t, nodes[0]); got != 4 {
		t.Errorf("repair changed the SOURCE to %d rows; it must only ever add to "+
			"a replica that is behind", got)
	}
	for i, n := range nodes[1:] {
		if got := rowCount(t, n); got > 4 {
			t.Errorf("replica %d has %d rows, more than the source's 4: repair "+
				"duplicated rows, which is the outcome this whole guard exists "+
				"to prevent", i+1, got)
		}
	}

	// The part that is a limitation, asserted so it cannot vanish quietly.
	if rep.SelfOverlapping == 0 {
		t.Error("two touching flushes were not reported as an inconclusive span " +
			"pair; if this now passes, the router has learned to tell an " +
			"adjacency from a duplicate and this test should be replaced by the " +
			"convergence one")
	}
	if rep.Complete {
		t.Error("a pass that could not converge reported itself complete")
	}
	if rep.Blocked == 0 {
		t.Error("nothing was reported blocked, so an operator reading the report " +
			"cannot see that a replica is still short")
	}
	if len(rep.Errors) == 0 {
		t.Error("no error names the replicas an operator has to look at")
	}
	// The message has to say what the operator must actually check.
	joined := strings.Join(rep.Errors, " ")
	if !strings.Contains(joined, "same rows") {
		t.Errorf("the error does not say what to check: %q", joined)
	}
}

// A replica that really does hold duplicate rows is reported and cannot
// certify a pair -- through the real handler, and without stalling the others.
func TestASelfOverlappingReplicaIsReportedThroughTheHandler(t *testing.T) {
	_, router, nodes := replicatedShard(t, 2)

	// Replica 0 ingests one range, then ingests an overlapping range again --
	// a retried batch with no write id, which is what a client library does
	// after a timeout.
	postLines(t, nodes[0].URL,
		line("2026-06-01T12:00:00Z", "a"), line("2026-06-01T12:00:09Z", "b"))
	postLines(t, nodes[0].URL,
		line("2026-06-01T12:00:01Z", "a2"), line("2026-06-01T12:00:09Z", "b2"))

	rep := runRepair(t, router)
	if rep.SelfOverlapping == 0 {
		t.Errorf("a replica holding two groups over the same span was not "+
			"reported; it must not be trusted to certify that two groups "+
			"differ: %+v", rep)
	}
	if rep.Complete {
		t.Errorf("a pass that found a self-overlapping replica reported itself "+
			"complete: %+v", rep)
	}
	if len(rep.Errors) == 0 {
		t.Error("nothing in Errors names the replica an operator has to look at")
	}
}

// A group above the peer client's in-memory response ceiling is still copied.
//
// copyGroup fetched through peers.do, whose maxBody ceiling (256 MiB in
// production) discards any larger answer as malformed -- so a group in
// (256 MiB, maxRepairBytes] could never be repaired, and a replica holding it
// stayed short forever. That ceiling bounds QUERY answers, which a router
// controls through limits it set; a group transfer is bounded by
// maxRepairBytes instead, and the regression is that repair never consults
// the in-memory ceiling at all.
//
// The fixture shrinks the CEILING rather than building a 300 MiB group: a
// group above ANY shrinkable ceiling must still cross, which is the same
// property at a size a test can afford.
func TestRepairCopiesAGroupAboveThePeerResponseCeiling(t *testing.T) {
	const ceiling = 256 << 10 // the router's in-memory peer cap, shrunk
	const rows = storage.MaxRows

	// One store holding a single group above the ceiling, written directly so
	// no ingest flush limits decide the fixture's shape.
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(repairFixtureGroup(rows)); err != nil {
		t.Fatal(err)
	}
	ds, err := st.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	// The fixture must actually be above the ceiling, or the test asserts
	// nothing: the timestamp column delta+varints away to nothing, so the size
	// is checked against the FILE, not guessed from the rows.
	if len(ds) != 1 || ds[0].Bytes <= ceiling {
		t.Fatalf("fixture group is %d bytes, want one group above the %d-byte "+
			"ceiling; the test would pass without ever crossing the cap",
			ds[0].Bytes, ceiling)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Node 0 serves that store; node 1 starts empty. The rest is the ordinary
	// two-replica fixture.
	node0 := serverOverStore(t, dir)
	node1 := realShard(t, nil)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends([]string{node0.URL, node1.URL})
	srv.SetReplicas(2) // one shard, two replicas
	// THE REGRESSION: the router's peer client refuses responses past this
	// ceiling, and the group above it used to be unrepairable through that
	// path. The ceiling is deliberately below the group.
	srv.peers.maxBody = ceiling
	router := httptest.NewServer(srv.Handler())
	t.Cleanup(router.Close)

	if got := rowCount(t, node0); got != rows {
		t.Fatalf("node 0 serves %d rows, want %d -- the fixture is not what "+
			"the test thinks it is", got, rows)
	}

	rep := runRepair(t, router)
	if !rep.Complete {
		t.Fatalf("the pass reported itself incomplete: %+v", rep)
	}
	if rep.Copied != 1 {
		t.Fatalf("copied %d groups, want the 1 the empty replica was missing: %+v",
			rep.Copied, rep)
	}
	if got := rowCount(t, node1); got != rows {
		t.Fatalf("replica 1 holds %d rows after repair, want %d -- the group "+
			"did not cross", got, rows)
	}
}

// repairFixtureGroup builds one group of rows rows: a timestamp column plus a
// single-value message column and a dense vector column. The vector is what
// keeps the fixture above any small ceiling -- it is dense float32 and does
// not delta-encode -- so the group's size is not an accident of varint luck.
func repairFixtureGroup(rows int) *storage.Group {
	ts := make([]int64, rows)
	vals := make([]string, rows)
	vec := make([]float32, rows)
	for i := range ts {
		ts[i] = 1780315200000000000 + int64(i) // 2026-06-01T12:00:00Z + i ns
		vals[i] = "big"
		vec[i] = float32(i)
	}
	d := storage.BuildDict(vals)
	return &storage.Group{Rows: rows, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "_msg", Type: storage.ColDict, Dict: &d},
		{Name: "vec", Type: storage.ColVector, Vec: vec, Dim: 1},
	}}
}

// The digest query must reach BOTH hops of a copy: the fetch names what the
// source must serve, and the adopt names what the body must hash to. Dropping
// it on either hop turns the transfer into a refusal. The two hops also carry
// independent timeouts now, so the adopt cannot inherit a deadline the fetch
// already ate.
func TestCopyGroupCarriesTheDigestQueryOnBothHops(t *testing.T) {
	var srcQuery, dstQuery string
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srcQuery = r.URL.RawQuery
		writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
		w.Write([]byte("group bytes"))
	}))
	t.Cleanup(src.Close)
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dstQuery = r.URL.RawQuery
		writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
		json.NewEncoder(w).Encode(map[string]any{"adopted": true, "digest": "deadbeef"})
	}))
	t.Cleanup(dst.Close)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })

	req := httptest.NewRequest(http.MethodPost, "/admin/cluster/repair", nil)
	moved, err := srv.copyGroup(req, src.URL, dst.URL, "deadbeef")
	if err != nil {
		t.Fatalf("copyGroup: %v", err)
	}
	if moved != int64(len("group bytes")) {
		t.Errorf("moved %d bytes, want the %d the source served", moved, len("group bytes"))
	}
	if srcQuery != "digest=deadbeef" {
		t.Errorf("the fetch asked for %q, want digest=deadbeef", srcQuery)
	}
	if dstQuery != "digest=deadbeef" {
		t.Errorf("the adopt named %q, want digest=deadbeef", dstQuery)
	}
}

// The adopt body bound is exact: a group of exactly replicaGroupLimit bytes is
// adopted, and one byte more is refused with 413. The limit is shrinkable so
// the boundary is exercised without a gigabyte fixture.
func TestTheAdoptBoundIsExact(t *testing.T) {
	// A real group, small.
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(repairFixtureGroup(8)); err != nil {
		t.Fatal(err)
	}
	ds, err := st.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	f, err := st.OpenGroupBytes(ds[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := io.ReadAll(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	// The shrinkable limit: the route's ceiling is this, not maxRepairBytes.
	srv.replicaGroupLimit = int64(len(blob))
	node := httptest.NewServer(srv.Handler())
	t.Cleanup(node.Close)

	post := func(body []byte, digest string) (int, string) {
		t.Helper()
		resp, err := http.Post(node.URL+pathReplicaGroup+"?digest="+digest,
			"application/octet-stream", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	// Exactly at the limit: accepted.
	code, body := post(blob, storage.DigestForTest(blob))
	if code != 200 {
		t.Fatalf("a group of exactly the limit answered %d: %.300s", code, body)
	}
	if !strings.Contains(body, `"adopted":true`) {
		t.Errorf("the exact-limit group was not adopted: %.300s", body)
	}

	// One byte past the limit: refused, and refused BEFORE the body is read.
	padded := append(blob, 'x')
	code, body = post(padded, storage.DigestForTest(padded))
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("a group one byte over the limit answered %d, want 413: %.300s", code, body)
	}
	if got := rowCount(t, node); got != 8 {
		t.Errorf("the refused body left %d rows, want the 8 the exact-limit group stored", got)
	}
}
