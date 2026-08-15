package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

// Chaos: what the cluster does when a part of it is broken.
//
// One rule across every scenario, and it is the only rule that matters: an
// EXACT answer or an EXPLICIT failure. Never a smaller number with HTTP 200.
// A distributed read that quietly drops a shard is the failure mode that
// survives every test suite, because a plausible number is what a test that
// checks "did it work" sees.
//
// Every request here is bounded. An unbounded client against a stalled peer
// hangs the package until `go test -timeout` kills it, which reports nothing
// about the code and takes the whole run's budget with it.

const chaosTimeout = 8 * time.Second

func chaosGet(t *testing.T, target string) (int, string) {
	t.Helper()
	cl := &http.Client{Timeout: chaosTimeout}
	resp, err := cl.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b)
}

// countRows is the cluster's answer to `*`, or the status if it refused.
func countRows(t *testing.T, front *httptest.Server) (int, int) {
	t.Helper()
	code, body := chaosGet(t, front.URL+"/select/logsql/query?query=%2A")
	if code != 200 {
		return code, -1
	}
	n := 0
	for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
		if l != "" {
			n++
		}
	}
	return code, n
}

// seedShards writes n rows into each shard's every replica, so the cluster
// holds shards*n rows and every replica is complete.
func seedShards(t *testing.T, nodes [][]*httptest.Server, n int) int {
	t.Helper()
	total := 0
	for i := range nodes {
		for _, node := range nodes[i] {
			for k := 0; k < n; k++ {
				postLines(t, node.URL, line(
					fmt.Sprintf("2026-06-0%dT12:00:%02dZ", i+1, k),
					fmt.Sprintf("shard%d-row%d", i, k)))
			}
		}
		total += n
	}
	return total
}

// 1. Replica kill. A shard with a live replica still answers exactly.
func TestChaosReplicaKill(t *testing.T) {
	srv, front, nodes := clusterOf(t, 2, 2)
	want := seedShards(t, nodes, 3)

	code, got := countRows(t, front)
	if code != 200 || got != want {
		t.Fatalf("healthy cluster: %d, %d rows, want %d", code, got, want)
	}

	// Kill one replica of shard 0 and repoint the router at the survivors.
	nodes[0][1].Close()
	srv.SetBackends([]string{nodes[0][0].URL, nodes[1][0].URL, nodes[1][1].URL})
	srv.SetReplicas(1)
	// Shard layout changed, so re-point at exactly the live replicas.
	srv.SetBackends([]string{nodes[0][0].URL, nodes[1][0].URL})

	code, got = countRows(t, front)
	if code != 200 {
		t.Fatalf("a cluster with a live replica per shard refused the read: %d", code)
	}
	if got != want {
		t.Fatalf("%d rows with one replica of shard 0 dead, want %d: a surviving "+
			"replica holds the whole shard", got, want)
	}
}

// 2. A whole shard down. The read must FAIL, not return the other shard's rows.
func TestChaosAWholeShardDownFailsTheRead(t *testing.T) {
	srv, front, nodes := clusterOf(t, 2, 1)
	seedShards(t, nodes, 3)

	nodes[1][0].Close()
	srv.SetBackends([]string{nodes[0][0].URL, "http://127.0.0.1:1"})

	code, body := chaosGet(t, front.URL+"/select/logsql/query?query=%2A")
	if code == 200 {
		t.Fatalf("answered 200 with a shard down; the body is one shard's rows "+
			"presented as the cluster's: %.200s", body)
	}
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", code)
	}
}

// 3. Network timeout. A peer that accepts and then stalls must not hang the
// read past the client's patience, and must not be merged as an empty answer.
func TestChaosAStalledPeerDoesNotHangOrVanish(t *testing.T) {
	block := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block // held until the test releases it
	}))
	// Order matters and it is the reverse of what reads naturally. Cleanups run
	// LIFO, and httptest's Close waits for every in-flight handler -- including
	// the one blocked on this channel. So the unblock has to be registered
	// LAST, which makes it run FIRST. Registered the other way round, Close
	// waits forever on a handler nothing will ever release, and the test
	// deadlocks in cleanup: the package runs to its -timeout and reports
	// nothing about the code.
	t.Cleanup(stalled.Close)
	t.Cleanup(func() { close(block) })

	live := realShard(t, nil)
	postLines(t, live.URL, line("2026-06-01T12:00:00Z", "live"))

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends([]string{live.URL, stalled.URL})
	srv.SetReplicas(1)
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)

	// The router bounds a stalled peer with ResponseHeaderTimeout (10s), so the
	// client has to be more patient than that or the test measures its own
	// impatience instead. This costs the suite ten seconds and there is no
	// shorter way to prove a ten-second bound exists.
	const patient = peerResponseHeaderTimeut + 10*time.Second
	done := make(chan int, 1)
	go func() {
		cl := &http.Client{Timeout: patient}
		resp, err := cl.Get(front.URL + "/select/logsql/query?query=%2A")
		if err != nil {
			done <- -1
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- resp.StatusCode
	}()

	select {
	case code := <-done:
		if code == 200 {
			t.Fatal("answered 200 while a shard was stalled: the stalled shard " +
				"contributed nothing and nothing said so")
		}
		if code == -1 {
			t.Fatal("the client gave up before the router answered; a stalled peer " +
				"must be bounded by the router, not by the caller")
		}
	case <-time.After(patient + 5*time.Second):
		t.Fatal("the router never answered with a shard stalled")
	}
}

// 4. Stale replica. A read served from a shard whose replicas disagree must be
// exact, because the router reads ONE replica per shard -- so the answer
// depends on which one it picks, and that is what repair exists to remove.
func TestChaosAStaleReplicaIsHealedRatherThanRead(t *testing.T) {
	_, front, nodes := clusterOf(t, 1, 2)
	for _, n := range nodes[0] {
		postLines(t, n.URL, line("2026-06-01T12:00:00Z", "shared"))
	}
	// Replica 0 takes two writes replica 1 misses.
	postLines(t, nodes[0][0].URL, line("2026-06-01T12:00:01Z", "missed-a"))
	postLines(t, nodes[0][0].URL, line("2026-06-01T12:00:02Z", "missed-b"))

	// Reading now gives whichever replica the router picked: 3 rows or 1. Both
	// are HTTP 200 and only one is right, which is the whole problem.
	before0, before1 := rowCount(t, nodes[0][0]), rowCount(t, nodes[0][1])
	if before0 == before1 {
		t.Fatalf("the fixture did not create a stale replica: %d and %d", before0, before1)
	}

	rep := runRepair(t, front)
	if !rep.Complete || rep.Copied != 2 {
		t.Fatalf("repair: %+v", rep)
	}
	if a, b := rowCount(t, nodes[0][0]), rowCount(t, nodes[0][1]); a != 3 || b != 3 {
		t.Fatalf("after repair the replicas hold %d and %d rows, want 3 each", a, b)
	}
	// Now the answer no longer depends on which replica is read.
	code, got := countRows(t, front)
	if code != 200 || got != 3 {
		t.Fatalf("cluster read after repair: %d, %d rows, want 3", code, got)
	}
}

// 5. Corrupt replica. A group whose bytes were damaged must not be served as
// data, and must not be handed to a peer by repair.
func TestChaosACorruptGroupIsNotServedOrCopied(t *testing.T) {
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(srv.Handler())
	t.Cleanup(node.Close)
	postLines(t, node.URL, line("2026-06-01T12:00:00Z", "will-be-corrupted"))
	srv.Close()
	node.Close()

	// Damage every group file on disk.
	damaged := 0
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() || !strings.HasPrefix(filepath.Base(p), "group-") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil || len(b) < 32 {
			return nil
		}
		b[len(b)/2] ^= 0xff
		if os.WriteFile(p, b, 0o600) == nil {
			damaged++
		}
		return nil
	})
	if damaged == 0 {
		t.Skip("no group file to damage; the write had not been flushed")
	}

	// Reopen over the damaged directory.
	srv2, err := NewServer(dir)
	if err != nil {
		// Refusing to open a corrupt store is an explicit failure, which
		// satisfies the rule.
		t.Logf("the store refused to open over damaged groups: %v", err)
		return
	}
	t.Cleanup(func() { srv2.Close() })
	node2 := httptest.NewServer(srv2.Handler())
	t.Cleanup(node2.Close)

	// Whatever it does, it must not serve the damaged rows as good data.
	code, body := chaosGet(t, node2.URL+"/select/logsql/query?query=%2A")
	if code == 200 && strings.Contains(body, "will-be-corrupted") {
		t.Fatalf("served rows out of a damaged group as though they were intact: %.200s", body)
	}
	// And the inventory a peer would repair from must not offer it either: a
	// digest is over the file's bytes, so a damaged file simply has a different
	// digest and no peer asks for it. What must not happen is the endpoint
	// answering with a 5xx panic.
	stCode, stBody := chaosGet(t, node2.URL+pathReplicaState)
	if stCode >= 500 {
		t.Fatalf("the replica state endpoint failed over a damaged store: %d %.200s",
			stCode, stBody)
	}
}

// 6. Rolling restart. Answers stay exact while replicas restart one at a time.
func TestChaosRollingRestartKeepsAnswersExact(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir()}
	servers := make([]*Server, 2)
	nodes := make([]*httptest.Server, 2)
	start := func(i int) {
		s, err := NewServer(dirs[i])
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = s
		nodes[i] = httptest.NewServer(s.Handler())
	}
	for i := range dirs {
		start(i)
	}
	t.Cleanup(func() {
		for i := range nodes {
			if nodes[i] != nil {
				nodes[i].Close()
			}
			if servers[i] != nil {
				servers[i].Close()
			}
		}
	})

	// Both replicas of one shard hold the same three rows.
	for _, n := range nodes {
		for k := 0; k < 3; k++ {
			postLines(t, n.URL, line(fmt.Sprintf("2026-06-01T12:00:0%dZ", k),
				fmt.Sprintf("row%d", k)))
		}
	}
	rt, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	rt.SetBackends([]string{nodes[0].URL, nodes[1].URL})
	rt.SetReplicas(2)
	front := httptest.NewServer(rt.Handler())
	t.Cleanup(front.Close)

	if code, got := countRows(t, front); code != 200 || got != 3 {
		t.Fatalf("before the restart: %d, %d rows", code, got)
	}
	for i := range nodes {
		nodes[i].Close()
		servers[i].Close()
		// The router still lists the closed address; the shard's other replica
		// serves. That is the property a rolling restart depends on.
		code, got := countRows(t, front)
		if code != 200 || got != 3 {
			t.Fatalf("while replica %d was down: %d, %d rows, want 3 from its peer",
				i, code, got)
		}
		start(i)
		rt.SetBackends([]string{nodes[0].URL, nodes[1].URL})
		if code, got := countRows(t, front); code != 200 || got != 3 {
			t.Fatalf("after replica %d came back: %d, %d rows", i, code, got)
		}
	}
}

// 7. Disk full. A refused write is reported as refused, never as stored.
//
// The first write into an empty store lands whatever the budget says -- the
// store really was empty when it was checked, and a bound smaller than one
// group cannot be enforced before the group exists. Every write after it must
// be refused, and that is the part that was broken: the store's size was
// sampled once and believed for ten seconds, so a store with a 1-byte budget
// took four writes in a row.
func TestChaosAFullDiskRefusesTheWriteVisibly(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Storage.MaxTenantBytes = 1 // one group already exceeds it
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	full := httptest.NewServer(srv.Handler())
	t.Cleanup(full.Close)

	if code, body := postLines(t, full.URL, line("2026-06-01T12:00:00Z", "first")); code != 200 {
		t.Fatalf("the first write into an empty store was refused: %d %.200s", code, body)
	}
	code, body := postLines(t, full.URL, line("2026-06-01T12:00:01Z", "second"))
	if code < 400 {
		t.Fatalf("a write into a store already over its budget answered %d: %.200s. "+
			"The size cache is being believed across writes", code, body)
	}
	if code != http.StatusInsufficientStorage {
		t.Errorf("status %d, want 507", code)
	}
	if !strings.Contains(body, "quota") {
		t.Errorf("the refusal does not name the budget: %.200s", body)
	}

	// Through a router, the refusal must reach the client rather than being
	// swallowed into a 200 -- a write reported as stored and refused by every
	// shard is the worst outcome available here.
	rt, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	rt.SetBackends([]string{full.URL})
	rt.SetReplicas(1)
	front := httptest.NewServer(rt.Handler())
	t.Cleanup(front.Close)

	code, body = postLines(t, front.URL, line("2026-06-01T12:00:02Z", "through-router"))
	if code < 400 {
		t.Fatalf("the router reported %d for a write every shard refused: %.200s",
			code, body)
	}
}
