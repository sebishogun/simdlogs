package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// Two repairs do not run at once on one router.
//
// Repair reads every replica's group digests, decides what is missing, and
// copies it. Two overlapping passes both read the same missing set before either
// has written any of it, then both copy it: 5 of 5 runs duplicated rows, and
// BOTH reports said complete:true, blocked:0 -- the exact output the runbook
// tells an operator to repair until they see. The digest read and the copy that
// acts on it are separated by every peer round trip in the shard, so the window
// is as wide as this code gets.
//
// The backup path has admitted one at a time since it was written. Repair, which
// MUTATES, had nothing at all.

// blockingShard wraps a real storage node and holds every request until
// released, so a repair can be parked mid-pass with a second one arriving.
type blockingShard struct {
	ts      *httptest.Server
	gate    chan struct{}
	arrived chan struct{}
	once    sync.Once
}

func newBlockingShard(t *testing.T, inner *httptest.Server) *blockingShard {
	t.Helper()
	b := &blockingShard{gate: make(chan struct{}), arrived: make(chan struct{})}
	b.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only the repair-state read is gated: gating everything would stall the
		// ingest this test does first.
		if r.URL.Path == pathReplicaState {
			b.once.Do(func() { close(b.arrived) })
			<-b.gate
		}
		proxyTo(w, r, inner.URL)
	}))
	t.Cleanup(b.ts.Close)
	return b
}

// proxyTo forwards one request to base and copies the answer back verbatim,
// headers included -- the protocol envelope is carried in them.
func proxyTo(w http.ResponseWriter, r *http.Request, base string) {
	req, err := http.NewRequest(r.Method, base+r.URL.RequestURI(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func TestTwoRepairsDoNotRunAtOnceOnOneRouter(t *testing.T) {
	inner := realShard(t, nil)
	blocked := newBlockingShard(t, inner)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends([]string{blocked.ts.URL})
	srv.SetReplicas(1)
	router := httptest.NewServer(srv.Handler())
	t.Cleanup(router.Close)

	codes := make(chan int, 2)
	post := func() {
		resp, err := http.Post(router.URL+"/admin/cluster/repair", "application/json", nil)
		if err != nil {
			codes <- -1
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		codes <- resp.StatusCode
	}

	go post()
	<-blocked.arrived // the first pass is inside, holding the latch
	post()            // the second arrives while it is held
	close(blocked.gate)

	first, second := <-codes, <-codes
	ok, conflict := 0, 0
	for _, c := range []int{first, second} {
		switch {
		case c == http.StatusConflict:
			conflict++
		case c/100 == 2:
			ok++
		}
	}
	if ok != 1 || conflict != 1 {
		t.Errorf("two overlapping repairs answered %d and %d; want exactly one 2xx and "+
			"one 409. Both succeeding is how the same missing set gets copied twice "+
			"while both reports say complete", first, second)
	}

	// The latch is RELEASED, not leaked: a repair that finishes must not lock
	// the endpoint out for the life of the process.
	resp, err := http.Post(router.URL+"/admin/cluster/repair", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Errorf("a repair after both finished answered %d: the latch was not released",
			resp.StatusCode)
	}
}

// TWO ROUTERS repairing one cluster do not duplicate its rows.
//
// The latch above is per PROCESS. Two routers pointed at the same shards both
// read the same missing set, both fetch the group, and both POST it to the
// replica that lacks it -- and neither can see the other. That case was
// reproduced and left open as task #428, with the note that closing it needs
// the decision to move to the destination, the only participant that can see it
// already holds the group.
//
// It has. AdoptGroup now holds one lock across its "do I have this?" and its
// append, so the second POST is a no-op however many routers send it. Before,
// concurrent adopts of one four-row group left 2 groups and 8 rows at four
// callers, and 3 groups and 12 rows at eight -- with every loser returning
// adopted=false, so the duplication was invisible to whoever counted.
//
// THE ROWS ARE THE ASSERTION. Both passes may legitimately report copied>0:
// each decided from a state that was true when it read it, and a report is
// about the decision. What must not happen is the replica holding the group
// twice.
func TestTwoRoutersRepairingOneClusterDoNotDuplicateIt(t *testing.T) {
	nodes := []*httptest.Server{realShard(t, nil), realShard(t, nil)}
	urls := []string{nodes[0].URL, nodes[1].URL}

	newRouter := func() *httptest.Server {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Close() })
		srv.SetBackends(urls)
		srv.SetReplicas(2) // one shard, two replicas
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(ts.Close)
		return ts
	}
	// Two SEPARATE routers, so neither sees the other's latch.
	r1, r2 := newRouter(), newRouter()

	// A group only replica 0 holds, which both routers will decide to copy.
	postLines(t, nodes[0].URL, line("2026-06-01T12:00:00Z", "only-on-0"))
	before0, before1 := rowCount(t, nodes[0]), rowCount(t, nodes[1])
	if before0 != 1 || before1 != 0 {
		t.Fatalf("the fixture starts at %d and %d rows, want 1 and 0", before0, before1)
	}

	var wg sync.WaitGroup
	codes := make([]int, 2)
	start := make(chan struct{})
	for i, r := range []*httptest.Server{r1, r2} {
		wg.Add(1)
		go func(i int, r *httptest.Server) {
			defer wg.Done()
			<-start
			resp, err := http.Post(r.URL+"/admin/cluster/repair", "application/json", nil)
			if err != nil {
				codes[i] = -1
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			codes[i] = resp.StatusCode
		}(i, r)
	}
	close(start)
	wg.Wait()

	for i, c := range codes {
		if c/100 != 2 {
			t.Fatalf("router %d answered %d; both are separate processes and "+
				"neither may refuse the other", i, c)
		}
	}
	if got := rowCount(t, nodes[1]); got != 1 {
		t.Errorf("replica 1 holds %d rows after two routers repaired the same "+
			"shard, want the 1 it was missing -- the group was adopted twice",
			got)
	}
	if got := rowCount(t, nodes[0]); got != 1 {
		t.Errorf("replica 0 went to %d rows; repair may only add to the replica "+
			"that is behind", got)
	}
}
