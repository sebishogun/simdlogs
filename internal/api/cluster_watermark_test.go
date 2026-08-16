package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

// A read served by a replica that has fallen behind is not reported as complete.
//
// askShard returns the FIRST replica that answers, and PeerResponse.Complete is
// that peer's report on its OWN store -- true, and useless here, because a
// lagging replica's store is complete as far as it knows. Replica 0 holding 12
// rows and replica 1 holding 8: kill replica 0 and the same query answers 8
// instead of 12, HTTP 200, no header, nothing in the body to tell them apart.
//
// PeerResponse.HighWatermark exists for exactly this. Its own doc comment says
// so: "what lets a caller tell no results from no results yet". It was populated
// on the wire, parsed at cluster_client.go:250, and READ BY NO READ PATH -- the
// mechanism was built, documented, and wired to nothing.

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
	// own store, with an older watermark. This used to be 12 -> 8 at HTTP 200.
	leader.dead.Store(true)
	code, total, raw = hitsTotal(t, ts, "*")
	if code/100 == 2 {
		t.Fatalf("the cluster answered %d total=%d from a replica behind the highest "+
			"watermark it has seen (2000 -> 1000); the previous answer was 12. %s",
			code, total, raw)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("refused with %d, want 503 (the completeness rule's status): %s", code, raw)
	}

	// Asked for explicitly, it comes back as a 206 that names the shard -- the
	// same escape hatch every other incomplete read has, not a separate one.
	resp, err := http.Get(ts.URL + "/select/logsql/hits?query=*&step=1h&" + partialParam + "=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("with %s=1 the answer is %d, want 206", partialParam, resp.StatusCode)
	}
	if resp.Header.Get(HdrShardsMissing) == "" {
		t.Errorf("the 206 does not name the shard in %s", HdrShardsMissing)
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
	// And now going back IS lag.
	sh.wm.Store(2000)
	if code, _, raw := hitsTotal(t, ts, "*"); code/100 == 2 {
		t.Errorf("a watermark that went backwards (3000 -> 2000) answered %d: %s", code, raw)
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
