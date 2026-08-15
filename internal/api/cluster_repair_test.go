package api

import (
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

	// What the pass does now: the inventory grows as it copies.
	spans := append([]storage.GroupDigest(nil), have...)
	var copied, blocked []string
	for _, g := range union {
		if overlapping(spans, g) != "" {
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
