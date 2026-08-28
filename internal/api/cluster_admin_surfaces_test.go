package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// quarantinedShard is a storage node whose store holds one quarantined group.
//
// Built the way health_test.go builds one -- write a row, close, corrupt the
// group on disk, reopen under the quarantine policy -- because that is the
// only path that produces a real QuarantineRecord. A fabricated one would test
// the JSON and not the endpoint.
func quarantinedShard(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	postBody(t, ts, `{"_time":1,"service":"a"}`+"\n")
	ts.Close()
	srv.Close()

	corruptOneGroupOfTheTenant(t, dir)

	srv2, err := newQuarantineServer(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv2.Close() })
	ts2 := httptest.NewServer(srv2.Handler())
	t.Cleanup(ts2.Close)
	// The store quarantines on the read that finds the group unreadable, so a
	// write is what makes the tenant open and the policy fire.
	postBody(t, ts2, `{"_time":2,"service":"b"}`+"\n")
	return ts2
}

type quarantineAnswer struct {
	Count  int `json:"count"`
	Shards []struct {
		Shard  int                        `json:"shard"`
		Count  int                        `json:"count"`
		Groups []storage.QuarantineRecord `json:"groups"`
	} `json:"shards"`
}

// A router reports what its SHARDS have quarantined.
//
// It answered 501 before, on the reasoning that a router's own store
// quarantines nothing so the listing would be an empty answer about shards it
// never asked. True of the local store, and the wrong conclusion: the router
// is the only node that knows the whole shard list, and an operator asking a
// cluster's admin endpoint is asking about the cluster.
//
// The test compares against the SHARD's own listing rather than a hardcoded
// count, so it cannot pass by the router inventing records.
func TestARouterListsWhatItsShardsQuarantined(t *testing.T) {
	bad := quarantinedShard(t)
	good := realShard(t, corpus(1)[0])
	cluster := router(t, bad.URL, good.URL)

	code, body := chaosGet(t, bad.URL+"/admin/storage/quarantine")
	if code != 200 {
		t.Fatalf("the shard's own listing answered %d: %.200s", code, body)
	}
	var direct struct {
		Count  int                        `json:"count"`
		Groups []storage.QuarantineRecord `json:"groups"`
	}
	if err := json.Unmarshal([]byte(body), &direct); err != nil {
		t.Fatal(err)
	}
	if direct.Count == 0 {
		t.Skipf("the fixture did not quarantine anything, so this cannot tell a "+
			"federated listing from an empty one: %.200s", body)
	}

	code, body = chaosGet(t, cluster.URL+"/admin/storage/quarantine")
	if code != 200 {
		t.Fatalf("the router answered %d, want 200 with every shard reachable: %.300s",
			code, body)
	}
	var got quarantineAnswer
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("the router's answer is not readable: %v: %.300s", err, body)
	}
	if got.Count != direct.Count {
		t.Fatalf("the router reports %d quarantined group(s), the shard itself "+
			"reports %d", got.Count, direct.Count)
	}
	if len(got.Shards) != 2 {
		t.Fatalf("%d shards in the answer, want 2: %.300s", len(got.Shards), body)
	}
	// The RECORD, not just the count: a router that answered the right number
	// with empty records would be no use to the operator it is for.
	var found bool
	for _, sh := range got.Shards {
		for _, g := range sh.Groups {
			if g.Reason == "" || g.QuarantinedName == "" {
				t.Errorf("shard %d record has no reason or file name: %+v", sh.Shard, g)
			}
			if g.GroupID == direct.Groups[0].GroupID {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("the shard's group %d is not in the router's answer: %.400s",
			direct.Groups[0].GroupID, body)
	}
}

// A shard that cannot be asked makes the listing INCOMPLETE, and says which.
//
// This is the failure the 501 was protecting against, one level out: "nothing
// is quarantined" and "I could not reach two of five shards" are the same
// answer if the unreachable ones are dropped. The status carries it and the
// body still carries what was learned, because a shard being down is exactly
// when an operator wants the other shards' records.
func TestAnUnreachableShardMakesTheQuarantineListingIncomplete(t *testing.T) {
	bad := quarantinedShard(t)
	dead := realShard(t, nil)
	deadURL := dead.URL
	dead.Close() // nothing is listening now

	cluster := router(t, bad.URL, deadURL)

	// The SAME contract every federated read follows: 503 that names the
	// missing shard and says how to opt in. Asserted here as well as in the
	// shared suite because the shared suite drives a table, and a route that
	// answered its own bespoke 503 would satisfy the table's status check
	// while telling the client something different.
	resp, err := http.Get(cluster.URL + "/admin/storage/quarantine")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	body := string(raw)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("answered %d with one shard down, want 503: a listing missing a "+
			"shard reads as 'nothing is wrong' about it: %.300s", resp.StatusCode, body)
	}
	if v := resp.Header.Get(HdrShardsMissing); !strings.Contains(v, "1") {
		t.Errorf("%s = %q, does not name the dead shard 1", HdrShardsMissing, v)
	}
	if !strings.Contains(body, partialParam) {
		t.Errorf("the refusal does not say how to accept a partial answer: %.300s", body)
	}

	// And the opt-in gives the live shard's records, marked partial.
	resp2, err := http.Get(cluster.URL + "/admin/storage/quarantine?" + partialParam + "=1")
	if err != nil {
		t.Fatal(err)
	}
	raw2, _ := io.ReadAll(io.LimitReader(resp2.Body, 1<<20))
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("with %s=1 the router answered %d, want 206: %.300s",
			partialParam, resp2.StatusCode, raw2)
	}
	// The 206's HEADERS still say it is incomplete.
	//
	// This handler stamps the completeness headers itself, because an admin
	// route carries no cluster envelope and a successful listing would
	// otherwise arrive with none. Stamping them UNCONDITIONALLY overwrote what
	// fanOutChecked had already set on the partial path -- so a knowingly
	// partial answer went out as 206 with Complete: true and
	// Shards-Answered == Shards-Total, which is the exact confusion between
	// "nothing is quarantined" and "some shards were not asked" that this
	// endpoint used to be refused to avoid.
	if v := resp2.Header.Get(HdrComplete); v != "false" {
		t.Errorf("%s = %q on a partial answer, want false", HdrComplete, v)
	}
	if v := resp2.Header.Get(HdrPartial); v != "true" {
		t.Errorf("%s = %q on a partial answer, want true", HdrPartial, v)
	}
	if tot, ans := resp2.Header.Get(HdrShardsTotal), resp2.Header.Get(HdrShardsAnswered); tot == ans {
		t.Errorf("%s == %s == %q on a partial answer: the missing shard is not "+
			"visible in the counts", HdrShardsTotal, HdrShardsAnswered, tot)
	}
	var got quarantineAnswer
	if err := json.Unmarshal(raw2, &got); err != nil {
		t.Fatalf("the 206 body is not readable: %v: %.300s", err, raw2)
	}
	if got.Count == 0 {
		t.Errorf("the partial answer dropped the shard that DID answer; its "+
			"records are what an operator opened this endpoint for: %.300s", raw2)
	}
}

// Acknowledging through a router clears the SHARDS.
//
// It answered 501 before, correctly observing that acknowledging on a router
// clears nothing on the shards that are degraded. Forwarding it clears them,
// and the proof is the shard's own readiness probe going from 503 to 200 --
// not the router's summary, which a router could print without doing anything.
func TestAcknowledgingThroughARouterClearsTheShards(t *testing.T) {
	bad := quarantinedShard(t)
	good := realShard(t, corpus(1)[0])
	cluster := router(t, bad.URL, good.URL)

	if code, body := chaosGet(t, bad.URL+"/-/ready"); code != http.StatusServiceUnavailable {
		t.Skipf("the fixture shard is not degraded (ready=%d), so acknowledging "+
			"it cannot be observed: %.200s", code, body)
	}

	resp, err := http.Post(cluster.URL+"/admin/acknowledge-degraded", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	b := string(raw)
	if resp.StatusCode != 200 {
		t.Fatalf("the router answered %d with every shard up: %.300s", resp.StatusCode, b)
	}
	if !strings.Contains(b, "shard 0") || !strings.Contains(b, "shard 1") {
		t.Errorf("the summary does not report per shard, so an operator cannot see "+
			"what they just took responsibility for: %.300s", b)
	}

	if code, body := chaosGet(t, bad.URL+"/-/ready"); code != 200 {
		t.Fatalf("the shard is still degraded (ready=%d) after the router said it "+
			"acknowledged: the request did not reach it: %.200s", code, body)
	}
}

// A shard that did not accept makes the acknowledgement 503 and says so.
//
// The refusal it replaces warned that acknowledging on a router "would report
// success for an operation that did nothing". A partial acknowledgement is the
// same hazard: this is the one action whose meaning is a person accepting data
// loss, and a summed count with a shard silently missing is a person accepting
// something that did not happen.
func TestAPartialAcknowledgementIsNotReportedAsSuccess(t *testing.T) {
	bad := quarantinedShard(t)
	dead := realShard(t, nil)
	deadURL := dead.URL
	dead.Close()

	cluster := router(t, bad.URL, deadURL)
	resp, err := http.Post(cluster.URL+"/admin/acknowledge-degraded", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	b := string(raw)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("answered %d with one shard unreachable, want 503: this is the "+
			"one action whose meaning is a person accepting data loss, and a "+
			"summed count with a shard silently missing is a person accepting "+
			"something that did not happen: %.300s", resp.StatusCode, b)
	}
	if v := resp.Header.Get(HdrShardsMissing); !strings.Contains(v, "1") {
		t.Errorf("%s = %q, does not name the shard that did not accept",
			HdrShardsMissing, v)
	}
	if v := resp.Header.Get(HdrComplete); v != "false" {
		t.Errorf("%s = %q, want false", HdrComplete, v)
	}
	// The BODY says what was applied.
	//
	// The reachable shard HAS been acknowledged by the time the unreachable one
	// is found -- the fan-out is concurrent and there is nothing to roll back.
	// So the refusal a read would give ("this answer ... is refused") would be
	// false here, and an operator reading it would believe nothing happened.
	if !strings.Contains(b, "PARTIALLY acknowledged") {
		t.Errorf("the body does not say the acknowledgement was partial, so it "+
			"reads as though nothing was applied: %.300s", b)
	}
	if !strings.Contains(b, "UNCHANGED") {
		t.Errorf("the body does not say the unreached shard is unchanged: %.300s", b)
	}
	// And the reachable shard really was acknowledged, which is what makes the
	// wording above true rather than merely reassuring.
	if code, _ := chaosGet(t, bad.URL+"/-/ready"); code != 200 {
		t.Errorf("the reachable shard's readiness is %d: the body claims the rest "+
			"was applied and it was not", code)
	}
}
