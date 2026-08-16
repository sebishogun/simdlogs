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
	// gen is this simulated MACHINE's process generation. Two shards must not
	// share one, or a router cannot tell them apart; and restart() changes it,
	// which is how a fixture models the one way a real node's watermark falls.
	gen atomic.Value
}

var wmGen atomic.Int64

// restart gives this shard a new generation, as a process does when it starts.
func (sh *wmShard) restart(wm int64) {
	sh.gen.Store("gen-" + strconv.FormatInt(wmGen.Add(1), 10))
	sh.wm.Store(wm)
}

func newWMShard(t *testing.T, hits, watermark int64) *wmShard {
	t.Helper()
	sh := &wmShard{}
	sh.hits.Store(hits)
	sh.wm.Store(watermark)
	sh.gen.Store("gen-" + strconv.FormatInt(wmGen.Add(1), 10))
	sh.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sh.dead.Load() {
			// Answered, but not with an answer: the class a router retries on.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// complete=true, because that is the whole point: this peer's own store
		// IS complete. Only the watermark says it is behind.
		writeEnvelope(w.Header(), 0, 0, true, sh.wm.Load(), sh.gen.Load().(string), "")
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

	// The leader goes away. The lagging replica answers, complete=true about
	// its own store, with an older watermark -- and it is a DIFFERENT machine,
	// carrying its own generation, so its shortfall is not explained by a
	// restart.
	//
	// This is refused now. The first version refused unconditionally and was
	// reverted because three ways to lower a watermark on a healthy cluster
	// were found; all three are closed, and the fourth guard is that both
	// sides of the comparison must carry a generation. 8 of 12 rows served at
	// 200 is the confident-and-wrong answer the envelope exists to prevent.
	leader.dead.Store(true)
	code, total, raw = hitsTotal(t, ts, "*")
	if code == 200 {
		t.Fatalf("served total=%d at 200 from a replica behind the highest "+
			"watermark seen, from a different machine: %s", total, raw)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("answered %d, want 503: %s", code, raw)
	}
	// And the escape hatch still works, which is what makes the refusal
	// recoverable rather than an outage: a caller that would rather have the
	// short answer can ask for it.
	// hitsTotal returns early on any non-200, so the body is read directly:
	// 206 IS the answer here, not a failure to parse.
	code, _, raw = getJSONFrom(t, ts,
		"/select/logsql/hits?query=*&step=1h&allow_partial_response=1")
	if code != http.StatusPartialContent {
		t.Errorf("with allow_partial_response=1: %d, want 206: %s", code, raw)
	}
	if !strings.Contains(raw, `"total":8`) && !strings.Contains(raw, `"total":"8"`) {
		t.Errorf("the partial answer does not carry the lagging replica's 8: %s", raw)
	}
}

// A watermark that falls because the peer RESTARTED does not refuse the read.
//
// This is the only way a real node's report falls now. Tenant eviction and
// retention cannot: highWatermark is a running maximum, node-level, that moves
// only when a write arrives. A restart re-derives it from the stores that load,
// so a replica whose newest data retention already deleted comes back lower --
// and it says which it is by carrying a new generation.
//
// The version that refused unconditionally never recovered from this, because
// the floor only rises.
func TestARestartedPeerDoesNotRefuseTheRead(t *testing.T) {
	sh := newWMShard(t, 7, 5000)
	ts := wmRouter(t, sh.ts.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 7 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// The process restarts with less data than it had.
	sh.restart(1000)
	for i := 0; i < 3; i++ {
		code, total, raw := hitsTotal(t, ts, "*")
		if code != 200 {
			t.Fatalf("read %d after a restart answered %d -- a new generation "+
				"says the fall is the process and not the data, and the floor "+
				"only rises, so refusing here never recovers: %s", i, code, raw)
		}
		if total != 7 {
			t.Errorf("read %d: total=%d, want 7", i, total)
		}
	}
	// And within the NEW generation the check is live again: a fall with no
	// restart behind it is refused.
	sh.wm.Store(500)
	if code, _, raw := hitsTotal(t, ts, "*"); code == 200 {
		t.Errorf("a fall inside one generation was served at 200; a store that "+
			"goes backwards within one process is an anomaly whatever caused "+
			"it: %s", raw)
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
	// And going back INSIDE one generation is refused: nothing a live process
	// does lowers its own running maximum, so this is a peer that lost data.
	sh.wm.Store(2000)
	if code, _, raw := hitsTotal(t, ts, "*"); code == 200 {
		t.Errorf("a watermark that went backwards (3000 -> 2000) with no restart "+
			"was served at 200: %s", raw)
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
	if behind, _, _ := h.observe("http://old:1", "g1", 100, []string{"http://old:1", "http://sib:1"}); behind {
		t.Fatal("the first report was called behind")
	}
	// Its sibling, genuinely behind, is reported.
	behind, prev, _ := h.observe("http://sib:1", "g1", 50, []string{"http://old:1", "http://sib:1"})
	if !behind || prev != 100 {
		t.Errorf("a sibling at 50 against a high of 100 gave behind=%v prev=%d, "+
			"want true and 100 -- this is the cross-replica lag the check exists for",
			behind, prev)
	}
	// Now the topology repoints the index: `old` is gone, `new` is empty.
	behind, prev, _ = h.observe("http://new:1", "g1", 0, []string{"http://new:1", "http://sib:1"})
	if behind {
		t.Error("a machine that replaced the one which set the floor was called " +
			"lagging; it has never been asked before and cannot be behind anything")
	}
	if prev != 100 {
		t.Errorf("the discarded floor was %d, want 100", prev)
	}
	// And the new machine's own history takes over: a peer below IT is behind.
	if behind, _, _ := h.observe("http://new:1", "g1", 200, []string{"http://new:1", "http://sib:1"}); behind {
		t.Fatal("the new machine's own advance was called behind")
	}
	if behind, prev, _ := h.observe("http://sib:1", "g1", 150, []string{"http://new:1", "http://sib:1"}); !behind || prev != 200 {
		t.Errorf("after the replacement a sibling at 150 against 200 gave "+
			"behind=%v prev=%d, want true and 200", behind, prev)
	}
}

// A peer that RESTARTED is not a peer that fell behind.
//
// This was the third of the three healthy-cluster causes and the one that kept
// checkWatermark a log line: a node re-derives its watermark from the stores
// that load, so a replica whose newest data retention already deleted comes
// back below its own previous report with nothing wrong.
//
// It needs no durable watermark, only the ability to tell the two apart, and a
// process that restarted says so by carrying a different generation.
func TestARestartIsNotLag(t *testing.T) {
	members := []string{"http://a:1", "http://b:1"}

	t.Run("the same peer, a new generation", func(t *testing.T) {
		h := &shardHigh{}
		if behind, _, _ := h.observe("http://a:1", "gen1", 100, members); behind {
			t.Fatal("the first report was called behind")
		}
		behind, prev, _ := h.observe("http://a:1", "gen2", 40, members)
		if behind {
			t.Error("a restarted peer was called lagging; its watermark fell " +
				"because the process is new, which is not evidence about its data")
		}
		if prev != 100 {
			t.Errorf("the discarded floor was %d, want 100", prev)
		}
		// And the new generation's own floor takes over.
		if behind, prev, _ := h.observe("http://a:1", "gen2", 20, members); !behind || prev != 40 {
			t.Errorf("within ONE generation a fall gave behind=%v prev=%d, want "+
				"true and 40: a store that goes backwards inside one process is "+
				"an anomaly whatever caused it", behind, prev)
		}
	})

	t.Run("the same peer, the same generation", func(t *testing.T) {
		h := &shardHigh{}
		h.observe("http://a:1", "gen1", 100, members)
		if behind, prev, _ := h.observe("http://a:1", "gen1", 40, members); !behind || prev != 100 {
			t.Errorf("a peer that did NOT restart reporting 40 against its own "+
				"100 gave behind=%v prev=%d, want true and 100", behind, prev)
		}
	})

	t.Run("a restart does not excuse a SIBLING", func(t *testing.T) {
		h := &shardHigh{}
		// b sets the floor and does not restart.
		h.observe("http://b:1", "genB", 100, members)
		// a restarts and is genuinely behind b. The floor is b's, so a's new
		// generation is beside the point: it really is missing b's data.
		if behind, prev, _ := h.observe("http://a:1", "genA2", 40, members); !behind || prev != 100 {
			t.Errorf("a restarted peer behind a HEALTHY sibling gave behind=%v "+
				"prev=%d, want true and 100 -- a restart explains a peer's own "+
				"fall, not a gap against a peer that did not restart",
				behind, prev)
		}
	})

	t.Run("a peer that sends no generation is not excused", func(t *testing.T) {
		h := &shardHigh{}
		h.observe("http://a:1", "", 100, members)
		if behind, _, _ := h.observe("http://a:1", "", 40, members); !behind {
			t.Error("an empty generation excused a fall; absent is not `I " +
				"restarted`, it is an older peer that cannot say")
		}
	})
}

// A peer that sends a watermark but NO generation is reported, never refused.
//
// That is a node on a build older than the generation header, which is what
// every node is during the first half of a rolling upgrade. Its watermark
// falling is genuinely ambiguous -- restart or lag, and nothing on the wire
// says which -- and a false 503 is the worse of the two failures.
//
// This is the fourth guard, and the only one the other tests cannot exercise:
// every fixture there carries a generation, so `certain` is always true and
// deleting it changes nothing they can see.
func TestAPeerWithNoGenerationIsReportedNotRefused(t *testing.T) {
	var wm atomic.Int64
	wm.Store(5000)
	var hits atomic.Int64
	hits.Store(6)
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The envelope an older build writes: everything except the generation.
		w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
		w.Header().Set(HdrShardID, "0")
		w.Header().Set(HdrReplicaID, "0")
		w.Header().Set(HdrComplete, "true")
		w.Header().Set(HdrHighWatermark, strconv.FormatInt(wm.Load(), 10))
		fmt.Fprintf(w, `{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":%d}]}`, hits.Load())
	}))
	defer old.Close()
	ts := wmRouter(t, old.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 6 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// Its watermark falls. Whether it restarted cannot be known.
	wm.Store(1000)
	for i := 0; i < 3; i++ {
		code, total, raw := hitsTotal(t, ts, "*")
		if code != 200 {
			t.Fatalf("read %d from a peer that sends no generation answered %d: "+
				"during a rolling upgrade every peer looks like this, and its "+
				"restart is indistinguishable from lag -- so this must be a log "+
				"line and not a refusal: %s", i, code, raw)
		}
		if total != 6 {
			t.Errorf("read %d: total=%d, want 6", i, total)
		}
	}
}
