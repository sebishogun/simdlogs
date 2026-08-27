package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Replicated writes: what a client is told, and what a retry does.
//
// forwardWrite replicated to every member of the shard and relayed the LAST
// response's status. Replica A refusing on its own quota and replica B
// accepting answered whichever finished last -- so the same write was reported
// stored or refused depending on scheduling, and in the other order a retry
// duplicated into the replica that had already taken it.

// writeRouter returns a router in front of the given peer handlers.
func writeRouter(t *testing.T, peers ...http.HandlerFunc) (*Server, *httptest.Server, []string) {
	t.Helper()
	var urls []string
	for _, h := range peers {
		p := httptest.NewServer(h)
		t.Cleanup(p.Close)
		urls = append(urls, p.URL)
	}
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(len(urls)) // one shard, every peer a replica
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts, urls
}

// okPeer accepts every write and records the ids it saw.
func okPeer(seen *[]string, mu *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*seen = append(*seen, r.Header.Get(HdrWriteID))
		mu.Unlock()
		w.Header().Set(HdrProtocolVersion, "1")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ingested":1}`))
	}
}

// failPeer refuses with the given status.
func failPeer(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HdrProtocolVersion, "1")
		w.WriteHeader(status)
	}
}

func postWrite(t *testing.T, ts *httptest.Server, writeID, consistency string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader(`{"_msg":"x"}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if writeID != "" {
		req.Header.Set(HdrWriteID, writeID)
	}
	if consistency != "" {
		req.Header.Set(HdrConsistency, consistency)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]any{"raw": string(b)}
	}
	if id := resp.Header.Get(HdrWriteID); id != "" {
		out["_header_write_id"] = id
	}
	return resp.StatusCode, out
}

// One replica refusing is a failed write under `all`, whatever order the
// answers arrive in.
//
// The old code relayed the LAST response, so this returned 200 or 507
// depending on scheduling -- the same write reported as stored or refused by a
// coin flip.
func TestOneRefusingReplicaFailsTheWriteUnderAll(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	_, ts, _ := writeRouter(t,
		okPeer(&seen, &mu),
		failPeer(http.StatusInsufficientStorage),
	)
	code, body := postWrite(t, ts, "", "all")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("%d (%v), want 503: one replica refused and the level is `all`", code, body)
	}
	if body["write_id"] == nil {
		t.Fatal("the failure does not carry the write id, so a retry cannot be safe")
	}
	if n, _ := body["acked"].(float64); n != 1 {
		t.Errorf("acked = %v, want 1", body["acked"])
	}
}

// The same shape succeeds at `one`, and the level is the client's choice.
func TestOneRefusingReplicaSucceedsAtLevelOne(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	_, ts, _ := writeRouter(t,
		okPeer(&seen, &mu),
		failPeer(http.StatusInsufficientStorage),
	)
	code, body := postWrite(t, ts, "", "one")
	if code != 200 {
		t.Fatalf("%d (%v), want 200 at level one", code, body)
	}
}

// Quorum needs more than half.
func TestQuorumNeedsMoreThanHalf(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	_, ts, _ := writeRouter(t,
		okPeer(&seen, &mu),
		failPeer(http.StatusInsufficientStorage),
		failPeer(http.StatusInsufficientStorage),
	)
	if code, body := postWrite(t, ts, "", "quorum"); code != http.StatusServiceUnavailable {
		t.Fatalf("1 of 3 acked returned %d (%v), want 503", code, body)
	}

	var mu2 sync.Mutex
	var seen2 []string
	_, ts2, _ := writeRouter(t,
		okPeer(&seen2, &mu2),
		okPeer(&seen2, &mu2),
		failPeer(http.StatusInsufficientStorage),
	)
	if code, body := postWrite(t, ts2, "", "quorum"); code != 200 {
		t.Fatalf("2 of 3 acked returned %d (%v), want 200", code, body)
	}
}

// The default is `all`. Repair is operator-triggered rather than automatic,
// so quorum can leave a replica stale until the next repair pass.
func TestTheDefaultConsistencyIsAll(t *testing.T) {
	if got, err := ParseConsistency(""); err != nil || got != ConsistencyAll {
		t.Fatalf("default = %q (%v), want all", got, err)
	}
	for _, bad := range []string{"two", "ALL", "majority", "0"} {
		if _, err := ParseConsistency(bad); err == nil {
			t.Errorf("%q was accepted as a consistency level", bad)
		}
	}
	if got := ConsistencyQuorum.required(4); got != 3 {
		t.Errorf("quorum of 4 = %d, want 3", got)
	}
	if got := ConsistencyAll.required(3); got != 3 {
		t.Errorf("all of 3 = %d", got)
	}
	if got := ConsistencyOne.required(3); got != 1 {
		t.Errorf("one of 3 = %d", got)
	}
}

// Every replica gets the SAME write id, which is what makes the retry safe.
func TestEveryReplicaGetsTheSameWriteID(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	_, ts, _ := writeRouter(t, okPeer(&seen, &mu), okPeer(&seen, &mu))

	code, body := postWrite(t, ts, "", "all")
	if code != 200 {
		t.Fatalf("%d (%v)", code, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("%d replicas saw the write, want 2", len(seen))
	}
	if seen[0] == "" || seen[0] != seen[1] {
		t.Fatalf("replicas saw different write ids: %q and %q", seen[0], seen[1])
	}
	if body["_header_write_id"] != seen[0] {
		t.Errorf("the client was told %v, the replicas got %q",
			body["_header_write_id"], seen[0])
	}
}

// Per-request field mappings are query parameters. A router must carry them
// to storage nodes with the body; dropping them stores a different schema and
// can replace the sender's timestamp with ingest time while still answering
// success.
func TestReplicatedWriteForwardsFieldMappingQuery(t *testing.T) {
	seen := make(chan string, 1)
	_, ts, _ := writeRouter(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.RawQuery
		w.Header().Set(HdrProtocolVersion, "1")
		w.WriteHeader(http.StatusOK)
	})

	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/insert/jsonline?_time_field=ts&_msg_field=message",
		strings.NewReader(`{"ts":"2026-06-01T12:00:00Z","message":"x"}`+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("router answered %d, want 200", resp.StatusCode)
	}
	if got := <-seen; got != "_time_field=ts&_msg_field=message" {
		t.Fatalf("replica saw query %q, want the client's field mappings", got)
	}
}

// A client-supplied write id is used, so a retry names the write it repeats.
// An unusable one is refused rather than written into the manifest.
func TestAClientWriteIDIsUsedAndValidated(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	_, ts, _ := writeRouter(t, okPeer(&seen, &mu))

	const id = "0123456789abcdef"
	if code, body := postWrite(t, ts, id, "all"); code != 200 {
		t.Fatalf("%d (%v)", code, body)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 1 || got[0] != id {
		t.Fatalf("replica saw %v, want the client's id %q", got, id)
	}

	for _, bad := range []string{"short", strings.Repeat("a", 65), "not-hex-!!", "zzzz1234"} {
		if code, _ := postWrite(t, ts, bad, "all"); code != http.StatusBadRequest {
			t.Errorf("write id %q returned %d, want 400", bad, code)
		}
	}
}

func TestARetryWithTheSameWriteIDReturnsToTheSameShard(t *testing.T) {
	var hits [2]atomic.Int64
	peers := make([]*httptest.Server, len(hits))
	urls := make([]string, len(hits))
	for i := range peers {
		i := i
		peers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits[i].Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer peers[i].Close()
		urls[i] = peers[i].URL
	}

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetBackends(urls)
	srv.SetReplicas(1) // two shards, one replica each
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const id = "0123456789abcdef"
	for i := 0; i < 2; i++ {
		if code, _ := postWrite(t, ts, id, "all"); code != http.StatusServiceUnavailable {
			t.Fatalf("attempt %d returned %d, want 503", i+1, code)
		}
	}
	if a, b := hits[0].Load(), hits[1].Load(); !((a == 2 && b == 0) || (a == 0 && b == 2)) {
		t.Fatalf("two attempts with one write id hit shard counts %d and %d, want 2 and 0", a, b)
	}
}

// A storage node answers "duplicate" to a write id it already committed, and
// stores nothing. This is the property the whole mechanism exists for: without
// it every retry duplicates every row on the replicas that did commit.
func TestARetriedWriteIsNotStoredTwice(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const id = "aaaabbbbccccdddd"
	post := func() (int, bool) {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
			strings.NewReader(`{"_msg":"x"}`+"\n"))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set(HdrWriteID, id)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get(HdrDuplicate) == "true"
	}

	if code, dup := post(); code/100 != 2 || dup {
		t.Fatalf("first write: %d duplicate=%v", code, dup)
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}
	before := srv.defaultTenantRows()

	// The retry: same id, same body.
	code, dup := post()
	if code/100 != 2 {
		t.Fatalf("a retry returned %d; a duplicate is a success, the data is there", code)
	}
	if !dup {
		t.Fatal("the retry was not reported as a duplicate")
	}
	srv.def.w.Flush()
	if after := srv.defaultTenantRows(); after != before {
		t.Fatalf("rows went %d -> %d on a retry of the same write id", before, after)
	}
}

// Per-replica detail is for authorized callers.
//
// Replica URLs and their individual failures are the cluster's topology; an
// ordinary ingest client needs to know whether the write reached its level,
// not which machines exist and which are down.
//
// Only when authentication is CONFIGURED. Without -auth.config the server is
// unauthenticated by configuration and every surface is open -- /metrics
// included -- so redacting here alone would be a lock on one door of an open
// building. That is the same rule healthDetailAllowed applies to the health
// report.
func TestReplicaDetailIsRedactedForOrdinaryClients(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	peerOK := httptest.NewServer(okPeer(&seen, &mu))
	defer peerOK.Close()
	peerBad := httptest.NewServer(failPeer(http.StatusInsufficientStorage))
	defer peerBad.Close()

	srv, ts := authedServer(t)
	srv.SetBackends([]string{peerOK.URL, peerBad.URL})
	srv.SetReplicas(2)

	post := func(token string) string {
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", token,
			map[string]string{"Content-Type": "application/x-ndjson"},
			`{"_msg":"x"}`+"\n")
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(b)
	}

	// An ingest credential: the counts, not the topology.
	body := post(tokIngest)
	for _, u := range []string{peerOK.URL, peerBad.URL} {
		if strings.Contains(body, u) {
			t.Fatalf("an ingest client was told a replica URL: %s", body)
		}
	}
	if !strings.Contains(body, "acked") || !strings.Contains(body, "required") {
		t.Errorf("the client cannot tell how far the write got: %s", body)
	}

	// An admin credential: everything, because an operator has to know which
	// machine to look at.
	body = post(tokAdmin)
	if !strings.Contains(body, peerBad.URL) {
		t.Errorf("an operator was not told which replica failed: %s", body)
	}
}

var _ = atomic.AddInt64
var _ = fmt.Sprint
