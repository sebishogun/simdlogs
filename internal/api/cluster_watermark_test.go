package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A read served by a replica that has fallen behind is REPORTED.
//
// askShard returns the FIRST replica that answers, and PeerResponse.Complete is
// that peer's report on its OWN store -- true, and useless here, because a
// lagging replica's store is complete as far as it knows. Replica 0 holding 12
// rows and replica 1 holding 8: kill replica 0 and the same query answers 8
// instead of 12, HTTP 200, with nothing in the response to tell them apart.
//
// PeerResponse.HighWatermark exists for exactly this -- "what lets a caller tell
// no results from no results yet" -- and was populated on the wire, parsed by
// the client, and READ BY NO READ PATH.
//
// It is read now, and it LOGS. It does not refuse: a watermark going backwards
// turned out to have three benign causes, each of which made the refusing
// version outage a healthy cluster permanently. See checkWatermark and
// TestABenignWatermarkDropDoesNotRefuseTheRead.

// wmShard answers /select/logsql/hits with `hits` and the given watermark, and
// stops answering once its `dead` flag is set.
type wmShard struct {
	ts   *httptest.Server
	dead atomic.Bool
	hits atomic.Int64
	wm   atomic.Int64
}

func newWMShard(t *testing.T, hits, watermark int64) *wmShard {
	t.Helper()
	sh := &wmShard{}
	sh.hits.Store(hits)
	sh.wm.Store(watermark)
	sh.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sh.dead.Load() {
			// Answered, but not with an answer: the class a router retries on.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// complete=true, because that is the whole point: this peer's own store
		// IS complete. Only the watermark says it is behind.
		writeEnvelope(w.Header(), 0, 0, true, sh.wm.Load(), "")
		fmt.Fprintf(w, `{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":%d}]}`, sh.hits.Load())
	}))
	t.Cleanup(sh.ts.Close)
	return sh
}

// wmRouter builds a router over ONE shard with the given replicas, in order.
func wmRouter(t *testing.T, replicas ...string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(replicas)
	srv.SetReplicas(len(replicas)) // all of them are one shard
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func hitsTotal(t *testing.T, ts *httptest.Server, query string) (int, int64, string) {
	t.Helper()
	code, got, raw := getJSONFrom(t, ts, "/select/logsql/hits?query="+query+"&step=1h")
	if code != 200 {
		return code, 0, raw
	}
	hits, _ := got["hits"].([]any)
	var total int64
	for _, h := range hits {
		m, _ := h.(map[string]any)
		switch v := m["total"].(type) {
		case float64:
			total += int64(v)
		case string:
			n, _ := strconv.ParseInt(v, 10, 64)
			total += n
		}
	}
	return code, total, raw
}

func TestALaggingReplicaDoesNotServeAShortAnswerAsComplete(t *testing.T) {
	leader := newWMShard(t, 12, 2000)
	lagging := newWMShard(t, 8, 1000)
	ts := wmRouter(t, leader.ts.URL, lagging.ts.URL)

	// The leader answers first and its watermark is recorded.
	code, total, raw := hitsTotal(t, ts, "*")
	if code != 200 || total != 12 {
		t.Fatalf("with the leader up: %d total=%d %s", code, total, raw)
	}

	// The leader goes away. The lagging replica answers, complete=true about its
	// own store, with an older watermark.
	//
	// The answer is SERVED, and the lag is logged. An earlier version of this
	// refused with 503, and that was wrong: a watermark going backwards is not
	// reliable evidence of a lagging replica. Tenant eviction lowers it (the
	// node reports the max over OPEN tenants), a topology change lowers it (the
	// history is keyed by shard index), and retention lowers it -- each on a
	// healthy cluster, each an unrecoverable 503. See checkWatermark.
	leader.dead.Store(true)
	code, total, raw = hitsTotal(t, ts, "*")
	if code != 200 {
		t.Fatalf("the cluster refused a read from a lagging replica (%d): a watermark "+
			"going backwards has three benign causes and refusing on it outages a "+
			"healthy cluster. %s", code, raw)
	}
	if total != 8 {
		t.Errorf("the lagging replica's answer is total=%d, want 8: %s", total, raw)
	}
}

// A watermark that goes backwards for a BENIGN reason does not refuse the read.
//
// Three ways to lower a node's reported watermark with nothing wrong: evicting a
// tenant (highWatermark() is the max over OPEN tenants), repointing a shard
// index at another machine, and retention deleting the newest data. Each one
// produced a permanent 503 from the version of checkWatermark that refused.
func TestABenignWatermarkDropDoesNotRefuseTheRead(t *testing.T) {
	sh := newWMShard(t, 7, 5000)
	ts := wmRouter(t, sh.ts.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 7 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// The shard's watermark drops, as it does when a tenant is evicted.
	sh.wm.Store(1000)
	for i := 0; i < 3; i++ {
		code, total, raw := hitsTotal(t, ts, "*")
		if code != 200 {
			t.Fatalf("read %d after a benign watermark drop answered %d -- and the "+
				"version that refused never recovered, because the floor only rises: %s",
				i, code, raw)
		}
		if total != 7 {
			t.Errorf("read %d is total=%d, want 7: %s", i, total, raw)
		}
	}
}

// The watermark only ever moves forward, and equality is not lag.
//
// Without this, a shard answering the same watermark twice -- which is what an
// idle shard does on every read -- would look like it had fallen behind itself.
func TestAnUnchangedWatermarkIsNotLag(t *testing.T) {
	sh := newWMShard(t, 5, 1000)
	ts := wmRouter(t, sh.ts.URL)
	for i := 0; i < 3; i++ {
		code, total, raw := hitsTotal(t, ts, "*")
		if code != 200 || total != 5 {
			t.Fatalf("read %d: %d total=%d %s", i, code, total, raw)
		}
	}
	// It moves forward: still complete, and the new high is remembered.
	sh.wm.Store(3000)
	sh.hits.Store(9)
	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 9 {
		t.Fatalf("after the watermark advanced: %d total=%d %s", code, total, raw)
	}
	// And going back is REPORTED, not refused: the read is still served. See
	// TestABenignWatermarkDropDoesNotRefuseTheRead for why.
	sh.wm.Store(2000)
	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 9 {
		t.Errorf("a watermark that went backwards (3000 -> 2000) answered %d total=%d: %s",
			code, total, raw)
	}
}

// A peer that sends no watermark at all is an older protocol version, not a
// maximally-lagging one. Treating silence as the epoch would fail every read
// against it, which is a worse failure than the one being prevented.
func TestAPeerThatSendsNoWatermarkIsNotTreatedAsLagging(t *testing.T) {
	quiet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
		w.Header().Set(HdrComplete, "true")
		// No HdrHighWatermark.
		fmt.Fprint(w, `{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":4}]}`)
	}))
	t.Cleanup(quiet.Close)
	ts := wmRouter(t, quiet.URL)
	for i := 0; i < 2; i++ {
		if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 4 {
			t.Fatalf("read %d against a peer with no watermark header: %d total=%d %s",
				i, code, total, raw)
		}
	}
}

// Evicting a tenant does not lower what this node reports.
//
// highWatermark() was the max over the tenants that are OPEN, so a one-node
// cluster reading tenant 2 and then tenant 1 reported itself going backwards --
// one of the three healthy-cluster causes that made the reader's refusal
// unsafe. It is a running maximum now, and the maximum is the node's.
func TestATenantEvictionDoesNotLowerTheWatermark(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	hi := time.Unix(1700000000, 0).UTC()
	seed := func(tenant string, at time.Time) {
		t.Helper()
		body := fmt.Sprintf(`{"_time":%q,"_msg":"m"}`, at.Format(time.RFC3339Nano))
		r, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
			strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("AccountID", tenant)
		resp, err := ts.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			t.Fatalf("seeding tenant %s answered %d", tenant, resp.StatusCode)
		}
	}
	seed("1", hi.Add(-time.Hour))
	seed("2", hi)

	both := srv.highWatermark()
	if both == 0 {
		t.Fatal("the node reports no watermark after two writes, so this test " +
			"would pass against anything")
	}

	// Close is the eviction: it shuts every open tenant and empties the map, so
	// the scan inside highWatermark now sees nothing at all.
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if got := srv.highWatermark(); got != both {
		t.Errorf("with every tenant evicted the node reports %d, was %d -- a "+
			"watermark that falls for a reason unrelated to replication cannot "+
			"be used to refuse a read", got, both)
	}
}

// A shard index repointed at a new machine does not inherit the old one's floor.
//
// The history was a bare number keyed by shard INDEX, so SetBackends pointing
// index 0 at a different machine handed that machine the previous one's floor
// and read it as lagging while it was healthy and legitimately empty. The entry
// records which peer set the high.
func TestAReplacedMachineDoesNotInheritTheOldFloor(t *testing.T) {
	h := &shardHigh{}

	// The original machine sets a high.
	if behind, _ := h.observe("http://old:1", 100, []string{"http://old:1", "http://sib:1"}); behind {
		t.Fatal("the first report was called behind")
	}
	// Its sibling, genuinely behind, is reported.
	behind, prev := h.observe("http://sib:1", 50, []string{"http://old:1", "http://sib:1"})
	if !behind || prev != 100 {
		t.Errorf("a sibling at 50 against a high of 100 gave behind=%v prev=%d, "+
			"want true and 100 -- this is the cross-replica lag the check exists for",
			behind, prev)
	}
	// Now the topology repoints the index: `old` is gone, `new` is empty.
	behind, prev = h.observe("http://new:1", 0, []string{"http://new:1", "http://sib:1"})
	if behind {
		t.Error("a machine that replaced the one which set the floor was called " +
			"lagging; it has never been asked before and cannot be behind anything")
	}
	if prev != 100 {
		t.Errorf("the discarded floor was %d, want 100", prev)
	}
	// And the new machine's own history takes over: a peer below IT is behind.
	if behind, _ := h.observe("http://new:1", 200, []string{"http://new:1", "http://sib:1"}); behind {
		t.Fatal("the new machine's own advance was called behind")
	}
	if behind, prev := h.observe("http://sib:1", 150, []string{"http://new:1", "http://sib:1"}); !behind || prev != 200 {
		t.Errorf("after the replacement a sibling at 150 against 200 gave "+
			"behind=%v prev=%d, want true and 200", behind, prev)
	}
}

// The remaining cause, recorded rather than claimed closed.
//
// A node restart re-derives the watermark from the stores that load, so a
// replica whose newest data retention already deleted comes back below its
// sibling with nothing wrong. That is why checkWatermark still reports instead
// of refusing, and this test states the shape so the claim is not a comment
// nobody checked.
func TestARestartStillLowersTheWatermark(t *testing.T) {
	h := &shardHigh{}
	members := []string{"http://a:1", "http://b:1"}
	if behind, _ := h.observe("http://a:1", 100, members); behind {
		t.Fatal("the first report was called behind")
	}
	// The SAME machine, back after a restart with less data. It is still in the
	// topology, so the floor stands and it reads as lagging -- which is exactly
	// the false positive that keeps this a log line and not a 503.
	behind, prev := h.observe("http://a:1", 40, members)
	if !behind || prev != 100 {
		t.Fatalf("a restarted peer reporting 40 against its own 100 gave "+
			"behind=%v prev=%d; if this ever becomes false the refusal can be "+
			"turned back on and task #434 is finished", behind, prev)
	}
	t.Log("a restarted peer still reads as lagging: the watermark is monotonic " +
		"within a process and not across one, so the reader reports and does " +
		"not refuse")
}
