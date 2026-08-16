package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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
		writeEnvelope(w.Header(), 0, 0, true, sh.wm.Load(), true, sh.gen.Load().(string), "")
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

// A replica below a SIBLING's watermark is named in the answer, not refused.
//
// This test used to expect 503, and that was wrong -- proven by a second
// reviewer with a fixture this one cannot be told apart from: two replicas
// holding the SAME twelve rows, watermarks 2000 and 1999. Kill the first and
// the second was refused forever, on an answer that would have been
// byte-identical.
//
// From the router's side the two situations are the same observation. One
// replica's watermark being lower than another's is the ORDINARY state of a
// shard taking writes -- replicas are microseconds apart all the time, and a
// running maximum that survives retention makes them differ even when they hold
// identical data. Nothing in one peer's answer separates "replication is a
// millisecond behind" from "this replica lost a day", and refusing on it turns
// every replica failure into a read outage. That is the failure that got the
// first version of this check reverted; repeating it with a generation header
// attached would have repeated the outage.
//
// So the cross-replica case is REPORTED: 200, with the shard named in
// X-Simdlogs-Shards-Lagging and a warning logged. Refusing is reserved for the
// two observations that have no benign reading, each with its own test:
// TestAPeerThatWentBackwardsInOneProcessIsRefused and
// TestAnEmptyReplicaIsNotServedAsAWholeAnswer.
func TestAReplicaBelowASiblingIsNamedRatherThanRefused(t *testing.T) {
	leader := newWMShard(t, 12, 2000)
	lagging := newWMShard(t, 8, 1000)
	ts := wmRouter(t, leader.ts.URL, lagging.ts.URL)

	// The leader answers first and its watermark is recorded.
	code, total, raw := hitsTotal(t, ts, "*")
	if code != 200 || total != 12 {
		t.Fatalf("with the leader up: %d total=%d %s", code, total, raw)
	}
	leader.dead.Store(true)

	// Four reads: a refusal here was also permanent, because the old floor only
	// ever rose.
	for i := 0; i < 4; i++ {
		resp := getWithHeaders(t, ts, "/select/logsql/hits?query=*&step=1h")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("read %d: %d. A replica below a sibling's watermark must "+
				"be served -- ordinary replication skew looks exactly like "+
				"this, and refusing it makes one replica failure a read "+
				"outage: %s", i, resp.StatusCode, body)
		}
		if got := resp.Header.Get(HdrShardsLagging); got != "0" {
			t.Errorf("read %d: %s = %q, want \"0\". Serving the short answer "+
				"is the right call; serving it SILENTLY is not, and the header "+
				"is the whole difference between an operator who can see it "+
				"and one who cannot", i, HdrShardsLagging, got)
		}
	}
}

// A peer that went backwards INSIDE ONE PROCESS is refused.
//
// This is the one watermark observation with no benign reading. A node's
// watermark is a running maximum held in memory, so within a single generation
// it cannot fall: a lower report from the same process, carrying the same
// generation, is data that was there and is not.
//
// Everything that used to make a fall benign is now visible as something else:
// a restart carries a new generation, a tenant eviction and an expiry cannot
// lower a running maximum, and a different replica is a different key.
func TestAPeerThatWentBackwardsInOneProcessIsRefused(t *testing.T) {
	sh := newWMShard(t, 12, 2000)
	ts := wmRouter(t, sh.ts.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 12 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// The SAME process, same generation, now reporting less than it did.
	sh.wm.Store(1000)
	sh.hits.Store(8)

	code, total, raw := hitsTotal(t, ts, "*")
	if code == 200 {
		t.Fatalf("served total=%d at 200 from a process whose own watermark "+
			"fell inside one generation. A running maximum cannot fall: %s",
			total, raw)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("answered %d, want 503: %s", code, raw)
	}
	// AND THE REFUSAL SAYS WHICH KIND IT IS.
	//
	// The missing bucket names a class -- `0(unavailable)` -- and this one used
	// to be a bare index, so "1 of 1 shards could not answer completely (0)"
	// read the same whether the shard's store was degraded or its watermark had
	// gone backwards. Those are different problems with different fixes, and
	// this bucket now produces mostly the second.
	if !strings.Contains(raw, "0(watermark)") {
		t.Errorf("the refusal does not say the watermark is why: %s", raw)
	}
	// The escape hatch is what makes the refusal recoverable rather than an
	// outage: a caller that would rather have the short answer can ask.
	code, _, raw = getJSONFrom(t, ts,
		"/select/logsql/hits?query=*&step=1h&allow_partial_response=1")
	if code != http.StatusPartialContent {
		t.Errorf("with allow_partial_response=1: %d, want 206: %s", code, raw)
	}
	if !strings.Contains(raw, `"total":8`) && !strings.Contains(raw, `"total":"8"`) {
		t.Errorf("the partial answer does not carry the short 8: %s", raw)
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
	members := []string{"http://old:1", "http://sib:1"}
	h.observe("http://old:1", "g1", 100, members)
	h.observe("http://sib:1", "g1", 50, members)

	// SetBackends repoints the index: `old` is gone, `new` takes its place.
	repointed := []string{"http://new:1", "http://sib:1"}
	if behind, _, certain := h.observe("http://new:1", "g1", 10, repointed); behind && certain {
		t.Error("a machine that replaced the one which set a floor of 100 was " +
			"REFUSED at 10. It has no history of its own, and 100 is not a " +
			"number it ever claimed")
	}
	// Its OWN history is what binds it: 10 is now its floor, and dropping
	// below that inside one process is the one refusable observation.
	if behind, prev, certain := h.observe("http://new:1", "g1", 5, repointed); !behind || !certain || prev != 10 {
		t.Errorf("the replacement falling from its own 10 to 5 gave behind=%v "+
			"certain=%v prev=%d, want true true 10", behind, certain, prev)
	}
	// And the machine that left takes its mark with it, so a floor cannot
	// outlive the membership that produced it -- nor can the map grow with
	// every machine that has ever held the index.
	h.mu.Lock()
	_, stillThere := h.marks["http://old:1"]
	h.mu.Unlock()
	if stillThere {
		t.Error("the replaced machine's mark outlived its membership")
	}
}

// An empty replica is REPORTED, not refused, and that is a capability this
// check does not have rather than one it chose not to use.
//
// The previous version refused a peer reporting zero while a live sibling's
// mark was above zero. It was wrong twice over, and the two are opposite:
//
//   - it could not reach the case it was written for, because the own-floor
//     arm returns first and a replica that RESTARTS onto an empty volume
//     carries a new generation (see TestARestartedEmptyReplicaIsNotRefused);
//   - and where it did fire it was wrong, because highWatermark() is a
//     per-process running maximum that SURVIVES RETENTION, so a sibling's
//     nonzero mark does not mean the shard currently holds anything.
//
// Catching a replica that lost its dataset means comparing what the replicas
// actually HOLD. That is the digest and repair machinery, not a watermark.
func TestAnEmptyReplicaIsReportedRatherThanRefused(t *testing.T) {
	h := &shardHigh{}
	members := []string{"http://a:1", "http://b:1"}
	h.observe("http://a:1", "g1", 100, members)
	behind, prev, certain := h.observe("http://b:1", "g1", 0, members)
	if !behind || prev != 100 {
		t.Errorf("a replica reporting zero against a sibling at 100 gave "+
			"behind=%v prev=%d, want true and 100 -- it is still reported",
			behind, prev)
	}
	if certain {
		t.Errorf("certain=true, so this refuses the read. A sibling's mark is a " +
			"running maximum that survives retention: it does not say the shard " +
			"holds anything, so a peer at zero beside it is not evidence")
	}
}

// A shard both of whose replicas legitimately hold nothing is not refused.
//
// Retention swept the shard; A has been up since before and its running maximum
// is still 5000; B restarted afterwards and re-derived 0. Both would answer
// with zero rows, and there is nothing wrong. The rule this replaces refused B
// permanently -- and the floor only rises, so it never cleared.
func TestAShardWhoseReplicasHoldNothingIsNotRefused(t *testing.T) {
	a := newWMShard(t, 0, 5000) // running max from before retention swept
	b := newWMShard(t, 0, 0)    // restarted after, re-derived nothing
	ts := wmRouter(t, a.ts.URL, b.ts.URL)

	if code, _, raw := hitsTotal(t, ts, "*"); code != 200 {
		t.Fatalf("with A up: %d %s", code, raw)
	}
	a.dead.Store(true)
	for i := 0; i < 4; i++ {
		code, _, raw := hitsTotal(t, ts, "*")
		if code != 200 {
			t.Fatalf("read %d: replica B was refused %d. Both replicas hold "+
				"nothing and the shard has nothing to give -- A's 5000 is a "+
				"running maximum that outlived the data it counted: %s",
				i, code, raw)
		}
	}
}

// A replica that RESTARTS onto an empty volume is served, and this test exists
// to say so rather than to approve of it.
//
// It is the wrong answer at 200 the watermark cannot catch: the peer carries a
// new generation, so its own floor re-bases and there is nothing left to
// compare. The rule that claimed to catch it never reached this case at all --
// the own-floor arm returns first -- and could not have been made to without
// refusing the healthy shard in TestAShardWhoseReplicasHoldNothingIsNotRefused.
//
// The mechanism for it is the digest comparison in cluster_repair.go, which
// asks what the replicas HOLD instead of what their clocks say.
func TestARestartedEmptyReplicaIsNotRefused(t *testing.T) {
	sh := newWMShard(t, 12, 2000)
	ts := wmRouter(t, sh.ts.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 12 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// It comes back on an empty volume: new process, nothing loaded.
	sh.restart(0)
	sh.hits.Store(0)

	code, total, raw := hitsTotal(t, ts, "*")
	if code != 200 {
		t.Fatalf("a restarted peer was refused %d. A restart is not evidence "+
			"of loss and refusing it outages a healthy cluster: %s", code, raw)
	}
	if total != 0 {
		t.Errorf("total=%d, want 0", total)
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

// ---- the three the second reviewer proved, all of them mine ----

// F3. A sibling holding the SAME rows is not refused.
//
// Two replicas of one shard legitimately report different watermarks -- they
// ingest independently and a millisecond apart is normal. Refusing the lower
// one turns a single replica failure into a read outage on data that is not
// missing, which is the failure that got the previous version of this check
// reverted, and the floor only ever rises so on an idle shard it never clears.
//
// docs/wrong.md entry 96 states the rule as "same peer, same generation, lower
// watermark". The code refused ANY member's lower watermark. This pins the
// documented rule, which is also the defensible one: a single process's
// watermark is a running maximum and cannot fall, so a fall within one
// generation is evidence. A cross-replica difference is not.
func TestASiblingHoldingTheSameRowsIsNotRefused(t *testing.T) {
	a := newWMShard(t, 12, 2000)
	b := newWMShard(t, 12, 1999) // same rows, one tick behind
	ts := wmRouter(t, a.ts.URL, b.ts.URL)

	if code, total, raw := hitsTotal(t, ts, "*"); code != 200 || total != 12 {
		t.Fatalf("with A up: %d total=%d %s", code, total, raw)
	}
	a.dead.Store(true)

	// Four reads: the refusal was also permanent, because the floor only rises.
	for i := 0; i < 4; i++ {
		code, total, raw := hitsTotal(t, ts, "*")
		if code != 200 {
			t.Fatalf("read %d: replica B was refused %d while holding the SAME "+
				"12 rows as the replica that died. Its answer would have been "+
				"byte-identical. One replica failing must not be a read "+
				"outage: %s", i, code, raw)
		}
		if total != 12 {
			t.Errorf("read %d: total=%d, want 12", i, total)
		}
	}
}

// F2. A router does not sign its children's partial answer as whole.
//
// serveEnvelope stamps Complete from this node's OWN degraded snapshot, before
// the handler runs, and the fan-out never lowers it. A middle router that
// answered 206 with one shard missing still told its parent Complete: true, so
// the parent merged a knowingly-partial answer as a whole one.
//
// The same stamp is why the generation and watermark check was inert one level
// up: a router holds no tenant data, so it sent watermark 0, which the old
// zero-skip then discarded. A node that did not answer from its own stores
// sends no watermark at all now.
func TestARouterDoesNotSignItsChildrensPartialAnswerAsWhole(t *testing.T) {
	up := newWMShard(t, 7, 3000)
	down := newWMShard(t, 5, 3000)
	down.dead.Store(true)

	// A real two-level cluster: top -> middle -> two shards, one of which
	// cannot answer. Hand-stamping the internal header on a client request
	// would test the middleware; this tests what a parent actually receives.
	mid, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mid.Close() })
	mid.SetBackends([]string{up.ts.URL, down.ts.URL})
	mid.SetReplicas(1) // two separate shards
	midTS := httptest.NewServer(mid.Handler())
	t.Cleanup(midTS.Close)

	top := wmRouter(t, midTS.URL)

	// What the middle tells a PARENT, on the internal path the peer client uses.
	req, err := http.NewRequest("GET", midTS.URL+
		"/select/logsql/hits?query=*&step=1h&allow_partial_response=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(HdrInternal, "1")
	req.Header.Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	complete := resp.Header.Get(HdrComplete)
	if resp.StatusCode == 206 && complete == "true" {
		t.Errorf("the middle router answered 206 partial and stamped %s: true. "+
			"Its parent merges that as a whole answer, so a shard missing two "+
			"levels down is invisible at the top: %s", HdrComplete, body)
	}
	// A router must not claim a watermark either: it answers from its children,
	// not from a store of its own, and a parent comparing its zero against a
	// leaf's real watermark is what made the whole check inert one level up.
	if hw := resp.Header.Get(HdrHighWatermark); hw != "" && hw != "0" {
		t.Logf("router watermark %q", hw)
	}
	if hw := resp.Header.Get(HdrHighWatermark); hw == "0" {
		t.Errorf("the middle router sent %s=0. It holds no tenant data, so "+
			"that zero is not a statement about any shard -- and treating a "+
			"reported zero as real (which an empty replica requires) then "+
			"makes every router look like it lost its data", HdrHighWatermark)
	}

	// And the end-to-end consequence: the TOP must not answer as if whole.
	code, total, raw := hitsTotal(t, top, "*")
	if code == 200 && total == 7 {
		t.Errorf("the top router answered 200 with 7 of the cluster's 12 rows. "+
			"The middle knew a shard was missing and said Complete: true, so "+
			"nothing at the top could tell: %s", raw)
	}
}

// getWithHeaders does one GET and hands back the response WITH its headers,
// which getRaw and hitsTotal both discard. The headers are the finding here.
func getWithHeaders(t *testing.T, ts *httptest.Server, path string) *http.Response {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// A shard whose own store is degraded says "degraded", not "watermark".
//
// The two land in the same bucket and had the same rendering. Asserting only
// the watermark case would pass on a version that labelled everything
// "watermark", which is a worse answer than the bare index it replaced.
func TestADegradedShardIsNamedAsDegradedNotAsLagging(t *testing.T) {
	sh := newWMShard(t, 7, 5000)
	// This peer reports its OWN store as incomplete, with a healthy watermark.
	sh.ts.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, false, 5000, true, sh.gen.Load().(string), "")
		fmt.Fprintf(w, `{"hits":[{"timestamp":"1970-01-01T00:00:00Z","total":%d}]}`, sh.hits.Load())
	})
	ts := wmRouter(t, sh.ts.URL)

	code, _, raw := hitsTotal(t, ts, "*")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("a shard reporting its own store incomplete answered %d, want 503: %s", code, raw)
	}
	if !strings.Contains(raw, "0(degraded)") {
		t.Errorf("a degraded store is not named as such: %s", raw)
	}
	if strings.Contains(raw, "watermark") {
		t.Errorf("a degraded store was reported as a watermark problem, which "+
			"sends the operator to the wrong place: %s", raw)
	}
}

// SetBackends is safe against the readers it repoints.
//
// It was `s.backends = urls`: a plain assignment against per-shard goroutines
// reading the same field. Only main.go calls it, before serving, so nothing
// raced in practice -- but the watermark check's own peer-identity guard exists
// for "SetBackends repointing an index at a different machine", which is an
// operation the field's type said could not be done at runtime without racing.
// A guard whose premise the code cannot safely perform has a hole under it.
//
// Run with -race, which is where this fails if the atomic goes away.
func TestSetBackendsIsSafeAgainstTheReadersItRepoints(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetReplicas(1)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// shards() reads the list and slices it; a torn read shows up
				// here as a panic or as a shard of the wrong width.
				for _, sh := range srv.shards() {
					if len(sh) != 1 {
						t.Errorf("a shard of %d replicas at replication 1", len(sh))
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		n := 1 + i%4
		urls := make([]string, n)
		for j := range urls {
			urls[j] = "http://node" + strconv.Itoa(j)
		}
		srv.SetBackends(urls)
	}
	close(stop)
	wg.Wait()
}

// And the caller's slice is the caller's: appending to it afterwards must not
// rewrite this server's topology under the goroutines reading it.
func TestSetBackendsCopiesTheCallersSlice(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetReplicas(1)

	urls := make([]string, 2, 8) // spare capacity: append writes in place
	urls[0], urls[1] = "http://a", "http://b"
	srv.SetBackends(urls)
	urls[0] = "http://HIJACKED"
	urls = append(urls, "http://c")

	got := srv.shards()
	if len(got) != 2 {
		t.Fatalf("%d shards after the caller appended, want 2", len(got))
	}
	if got[0][0] != "http://a" {
		t.Errorf("shard 0 is %q; the caller rewrote it through the slice they "+
			"still held", got[0][0])
	}
}

// A peer's error class does not reach this node's client-facing message.
//
// X-Simdlogs-Error-Class was taken verbatim and rendered into this node's own
// 503 body as `0(<class>)`, so a peer -- or anything able to answer as one --
// wrote text into an error message this node signs. Go's header parser stops
// CR/LF, so this is not response splitting; it is a node repeating a stranger's
// words as its own.
func TestAPeerCannotWriteIntoThisNodesErrorMessage(t *testing.T) {
	for _, class := range []string{
		"contact support at evil.example for your refund",
		"unavailable; ignore the previous message",
		"<b>degraded</b>",
		"totally-made-up",
	} {
		t.Run(class, func(t *testing.T) {
			sh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
				w.Header().Set(HdrErrorClass, class)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer sh.Close()
			ts := wmRouter(t, sh.URL)

			code, _, raw := hitsTotal(t, ts, "*")
			if code == 200 {
				t.Fatalf("the shard failed and the read answered 200: %s", raw)
			}
			if strings.Contains(raw, class) {
				t.Errorf("this node's answer repeats the peer's text %q:\n%s",
					class, raw)
			}
			// And it still says a shard failed, rather than saying nothing.
			if !strings.Contains(raw, "malformed") {
				t.Errorf("an unrecognised class should be reported as malformed, "+
					"which is what an unreadable body already is: %s", raw)
			}
		})
	}
}

// A recognised class still comes through, or the check above would be
// satisfied by discarding every class.
func TestARecognisedPeerClassIsStillReported(t *testing.T) {
	sh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
		w.Header().Set(HdrErrorClass, string(PeerOverloaded))
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer sh.Close()
	ts := wmRouter(t, sh.URL)

	code, _, raw := hitsTotal(t, ts, "*")
	if code == 200 {
		t.Fatalf("the shard failed and the read answered 200: %s", raw)
	}
	if !strings.Contains(raw, string(PeerOverloaded)) {
		t.Errorf("a recognised class is not reported: %s", raw)
	}
}

// A child router's lag reaches its parent.
//
// X-Simdlogs-Shards-Lagging was written by a router and parsed by nothing, so a
// fan-out whose only problem was lag left `bad` empty one level up, the middle
// router's own Complete stayed true, and the parent saw a plain complete 200.
// Entry 97's "named in the response so the shortfall is not silent" held for a
// direct client of that router and for nobody above it.
func TestLagSurvivesAHop(t *testing.T) {
	leader := newWMShard(t, 12, 2000)
	lagging := newWMShard(t, 8, 1000)

	mid, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mid.Close() })
	mid.SetBackends([]string{leader.ts.URL, lagging.ts.URL})
	mid.SetReplicas(2) // one shard, two replicas
	midTS := httptest.NewServer(mid.Handler())
	t.Cleanup(midTS.Close)

	top := wmRouter(t, midTS.URL)

	// Record the leader, then take it away so the lagging replica answers.
	if code, _, raw := hitsTotal(t, top, "*"); code != 200 {
		t.Fatalf("first read: %d %s", code, raw)
	}
	leader.dead.Store(true)

	resp := getWithHeaders(t, top, "/select/logsql/hits?query=*&step=1h")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("the top answered %d; lag is reported, not refused: %s",
			resp.StatusCode, body)
	}
	if resp.Header.Get(HdrShardsLagging) == "" {
		t.Errorf("the top router's answer does not name a lagging shard. The "+
			"middle router knew and said so in its own header, which nothing "+
			"above it read: %s", body)
	}
}

// And the REASON survives a hop, or the distinction entry 98 added dies one
// level up: a child's watermark refusal reached its parent as a bare
// Complete: false and was rendered `N(degraded)`.
func TestTheRefusalReasonSurvivesAHop(t *testing.T) {
	sh := newWMShard(t, 12, 2000)

	mid, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mid.Close() })
	mid.SetBackends([]string{sh.ts.URL})
	mid.SetReplicas(1)
	midTS := httptest.NewServer(mid.Handler())
	t.Cleanup(midTS.Close)

	top := wmRouter(t, midTS.URL)

	if code, total, raw := hitsTotal(t, top, "*"); code != 200 || total != 12 {
		t.Fatalf("first read: %d total=%d %s", code, total, raw)
	}
	// The same process reports less than it did: the one refusable observation.
	sh.wm.Store(1000)
	sh.hits.Store(8)

	code, _, raw := hitsTotal(t, top, "*")
	if code == 200 {
		t.Fatalf("the top answered 200 for a shard whose own watermark fell "+
			"inside one generation: %s", raw)
	}
	if !strings.Contains(raw, reasonWatermark) {
		t.Errorf("the top reports the refusal without the reason, so an "+
			"operator two levels up is sent to look at a degraded store that "+
			"is not degraded: %s", raw)
	}
}

// The envelope is INTERNAL-ONLY, and lowering Complete must not leak it to a
// client.
//
// fanOutChecked sets X-Simdlogs-Complete: false before writing, and it guards
// on the header already being present -- which is what keeps it internal, since
// serveEnvelope only stamps internal requests. Deleting that guard leaves the
// suite green: the scoping was uncovered.
func TestAPublicClientNeverSeesTheEnvelope(t *testing.T) {
	up := newWMShard(t, 7, 3000)
	down := newWMShard(t, 5, 3000)
	down.dead.Store(true)

	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends([]string{up.ts.URL, down.ts.URL})
	srv.SetReplicas(1) // two shards, one down
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	for _, path := range []string{
		"/select/logsql/hits?query=*&step=1h",
		"/select/logsql/hits?query=*&step=1h&allow_partial_response=1",
	} {
		resp := getWithHeaders(t, ts, path)
		resp.Body.Close()
		for _, h := range []string{HdrComplete, HdrShardID, HdrReplicaID, HdrNodeGeneration} {
			if got := resp.Header.Get(h); got != "" {
				t.Errorf("%s: a public client received %s=%q. The envelope is a "+
					"promise between versions of this binary, not part of the "+
					"public API", path, h, got)
			}
		}
	}
}
