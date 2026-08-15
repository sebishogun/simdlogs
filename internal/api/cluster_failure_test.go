package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// What a cluster read does when a shard cannot answer.
//
// Every merge consumed a [][]byte with a nil entry for a shard that did not
// answer, and merged the rest. So a read with one shard down returned the
// other shards' rows, HTTP 200, with nothing anywhere in the response to say a
// third of the data was missing -- indistinguishable from a query that
// genuinely matched fewer rows. That is the worst kind of wrong answer:
// confident, plausible and silent.

// federatedEndpoints is every read a router fans out. Each is exercised with
// one shard down and with all of them down.
var federatedEndpoints = []struct {
	name, path, method, body string
}{
	{"select", "/select/logsql/query?query=*", "GET", ""},
	{"hits", "/select/logsql/hits?query=*&step=1m&start=1&end=2", "GET", ""},
	{"stats_query", "/select/logsql/stats_query?query=*+%7C+stats+count%28%29+n", "GET", ""},
	{"stats_query_range", "/select/logsql/stats_query_range?query=*+%7C+stats+count%28%29+n&step=1m", "GET", ""},
	{"field_names", "/select/logsql/field_names?query=*", "GET", ""},
	{"field_values", "/select/logsql/field_values?query=*&field=level", "GET", ""},
	{"streams", "/select/logsql/streams?query=*", "GET", ""},
	{"es_count", "/_count", "POST", `{"query":{"match_all":{}}}`},
	{"es_search", "/_search", "POST", `{"query":{"match_all":{}}}`},
}

// goodShard answers every read with a plausible empty-but-valid body and a
// complete envelope.
func goodShard(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, "")
		switch {
		case strings.Contains(r.URL.Path, "hits"):
			w.Write([]byte(`{"hits":[]}`))
		case strings.Contains(r.URL.Path, "stats_query"):
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		case r.URL.Path == "/_count":
			w.Write([]byte(`{"count":0}`))
		case r.URL.Path == "/_search":
			w.Write([]byte(`{"hits":{"total":{"value":0},"hits":[]}}`))
		case strings.Contains(r.URL.Path, "field_names"),
			strings.Contains(r.URL.Path, "field_values"),
			strings.Contains(r.URL.Path, "streams"):
			w.Write([]byte(`{"values":[]}`))
		default:
			w.Write([]byte(""))
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// deadShard is not listening: the URL points at a closed port.
func deadShard(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := ts.URL
	ts.Close() // now nothing is listening there
	return url
}

// router builds a select-router over the given backend URLs, one shard each.
func router(t *testing.T, urls ...string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(1) // one replica per shard: a dead backend is a dead shard
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func callEndpoint(t *testing.T, ts *httptest.Server, e struct{ name, path, method, body string }) (*http.Response, string) {
	t.Helper()
	var req *http.Request
	var err error
	if e.method == "POST" {
		req, err = http.NewRequest("POST", ts.URL+e.path, strings.NewReader(e.body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequest("GET", ts.URL+e.path, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// One shard down fails the read on every federated endpoint.
func TestOneDeadShardFailsEveryFederatedRead(t *testing.T) {
	good := goodShard(t)
	dead := deadShard(t)
	ts := router(t, good.URL, dead)

	for _, e := range federatedEndpoints {
		t.Run(e.name, func(t *testing.T) {
			resp, body := callEndpoint(t, ts, e)
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("%s returned %d with one shard down, want 503: %.200s",
					e.path, resp.StatusCode, body)
			}
			if resp.Header.Get(HdrShardsMissing) == "" {
				t.Errorf("the response does not name the missing shard")
			}
			if got := resp.Header.Get(HdrShardsTotal); got != "2" {
				t.Errorf("shards_total = %q, want 2", got)
			}
			if !strings.Contains(body, "allow_partial_response") {
				t.Errorf("the refusal does not say how to opt in: %.200s", body)
			}
		})
	}
}

// Every shard down fails too, and says so.
func TestAllShardsDownFailsEveryFederatedRead(t *testing.T) {
	ts := router(t, deadShard(t), deadShard(t))
	for _, e := range federatedEndpoints {
		t.Run(e.name, func(t *testing.T) {
			resp, body := callEndpoint(t, ts, e)
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("%s returned %d with every shard down: %.200s",
					e.path, resp.StatusCode, body)
			}
			if got := resp.Header.Get(HdrShardsAnswered); got != "0" {
				t.Errorf("shards_answered = %q, want 0", got)
			}
		})
	}
}

// allow_partial_response=1 is answered 206 with the missing shards named.
func TestPartialIsOptInAndMarked(t *testing.T) {
	good := goodShard(t)
	ts := router(t, good.URL, deadShard(t))

	for _, e := range federatedEndpoints {
		t.Run(e.name, func(t *testing.T) {
			sep := "?"
			if strings.Contains(e.path, "?") {
				sep = "&"
			}
			p := e
			p.path = e.path + sep + "allow_partial_response=1"
			resp, body := callEndpoint(t, ts, p)
			if resp.StatusCode != http.StatusPartialContent {
				t.Fatalf("%s with the opt-in returned %d, want 206: %.200s",
					p.path, resp.StatusCode, body)
			}
			if resp.Header.Get(HdrPartial) != "true" {
				t.Errorf("a partial answer is not marked partial")
			}
			if resp.Header.Get(HdrShardsMissing) == "" {
				t.Errorf("a partial answer does not name what is missing")
			}
		})
	}
}

// A shard that ANSWERS from a degraded store is missing data too, and the only
// difference is that it looks fine.
func TestADegradedShardCountsAsIncomplete(t *testing.T) {
	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A valid answer, explicitly not complete.
		writeEnvelope(w.Header(), 0, 0, false, 1, "")
		w.Write([]byte(""))
	}))
	defer degraded.Close()
	good := goodShard(t)
	ts := router(t, good.URL, degraded.URL)

	resp, body := callEndpoint(t, ts, federatedEndpoints[0])
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a degraded shard answered 200: %d %.200s", resp.StatusCode, body)
	}
	if !strings.Contains(resp.Header.Get(HdrShardsMissing), "1") {
		t.Errorf("the degraded shard is not named: %q", resp.Header.Get(HdrShardsMissing))
	}
}

// A client that hangs up cancels the peer requests, rather than leaving the
// router scanning for an answer nobody will read.
func TestClientCancellationReachesThePeers(t *testing.T) {
	started := make(chan struct{}, 1)
	var cancelled sync.WaitGroup
	cancelled.Add(1)
	var once sync.Once

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done() // the peer sees the cancellation
		once.Do(cancelled.Done)
	}))
	defer slow.Close()

	ts := router(t, slow.URL)
	req, _ := http.NewRequest("GET", ts.URL+"/select/logsql/query?query=*", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)

	go func() {
		<-started
		cancel()
	}()
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}

	done := make(chan struct{})
	go func() { cancelled.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer request was still running after the client hung up")
	}
}

var _ = fmt.Sprint
