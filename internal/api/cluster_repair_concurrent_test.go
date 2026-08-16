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
