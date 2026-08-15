package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Fuzzing the cluster envelope and the Elasticsearch query surface.
//
// The envelope is what a router believes about a peer: version, completeness,
// watermark. A router that misreads it merges a partial answer as a whole one,
// which is the failure the envelope exists to prevent -- so the parse has to
// hold against a peer that is hostile, buggy, or simply on another version.
//
// The ES surface is a public parser reached before any query executes.

// A peer's headers are attacker-controlled if any peer is. Absent, malformed
// and hostile values must all resolve to "do not trust this answer" rather than
// to a default that happens to mean "trust it".
func FuzzPeerEnvelope(f *testing.F) {
	f.Add("1", "true", "0", "")
	f.Add("", "", "", "")
	f.Add("99999999999999999999", "true", "1", "x")
	f.Add("1", "TRUE", "-9223372036854775808", "trace")
	f.Add("1e5", "yes", "not-a-number", strings.Repeat("x", 300))
	f.Fuzz(func(t *testing.T, version, complete, watermark, trace string) {
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			if version != "" {
				h.Set(HdrProtocolVersion, version)
			}
			if complete != "" {
				h.Set(HdrComplete, complete)
			}
			if watermark != "" {
				h.Set(HdrHighWatermark, watermark)
			}
			if trace != "" {
				h.Set(HdrTraceID, trace)
			}
			w.Write([]byte("{}"))
		}))
		defer peer.Close()

		req := httptest.NewRequest("GET", "http://router/select/logsql/query?query=*", nil)
		cl := newClusterClient(nil)
		resp := cl.do(req, 0, 0, peer.URL, http.MethodGet, "/select/logsql/query", nil)

		// Only the exact protocol version may be trusted. Anything else --
		// absent, unparseable, a different number -- is a peer this router
		// cannot merge, and merging it anyway is how a field that moved
		// produces a wrong answer instead of an error.
		if resp.OK() && version != "1" {
			t.Fatalf("accepted an answer from a peer announcing version %q", version)
		}
		// Completeness is never assumed. A missing or unparseable header means
		// the peer did not say, and "did not say" must not read as "complete".
		if resp.Complete && complete != "true" {
			t.Fatalf("read %q as a complete answer", complete)
		}
	})
}

// The ES query surface, reached before anything executes.
func FuzzESSearchBody(f *testing.F) {
	f.Add(`{"query":{"match_all":{}}}`)
	f.Add(`{"query":{"bool":{"must":[{"term":{"level":"error"}}]}}}`)
	f.Add(`{"size":-1}`)
	f.Add(`{"size":99999999999999999999}`)
	f.Add(`{"from":-5,"size":10}`)
	f.Add(`{"query":` + strings.Repeat(`{"bool":{"must":[`, 40) + `{}` + strings.Repeat(`]}}`, 40) + `}`)
	f.Add(`{`)
	// ONE server for the whole run. A server per iteration exhausts the
	// ephemeral port range within seconds -- Go fuzzes with one worker per CPU,
	// and each iteration was opening a listener and a store.
	node := realShard(f, nil)
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 1<<16 {
			return
		}
		resp, err := http.Post(node.URL+"/_search", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /_search: %v", err)
		}
		resp.Body.Close()
		// Any status is acceptable except a server error: a body a client
		// controls must not be able to make this a 5xx, which is the shape a
		// panic or an unbounded allocation takes from outside.
		if resp.StatusCode >= 500 {
			t.Fatalf("body %.120s produced HTTP %d", body, resp.StatusCode)
		}
	})
}

// Cursors are HMAC-signed and bound to a tenant, a query hash, a window and a
// direction. A forged or corrupted one must be refused, never decoded into a
// position that pages someone else's results.
func FuzzCursor(f *testing.F) {
	f.Add("")
	f.Add("not-base64")
	f.Add(strings.Repeat("A", 200))
	node := realShard(f, corpus(1)[0])
	f.Fuzz(func(t *testing.T, cursor string) {
		if len(cursor) > 4096 {
			return
		}
		// Escaped properly. Splicing the raw value in made the CLIENT refuse
		// the URL for a control character, which says nothing about the server.
		resp, err := http.Get(node.URL + "/select/logsql/query?query=%2A&cursor=" +
			url.QueryEscape(cursor))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			t.Fatalf("cursor %.80q produced HTTP %d", cursor, resp.StatusCode)
		}
	})
}
