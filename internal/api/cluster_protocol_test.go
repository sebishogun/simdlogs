package api

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The internal protocol, against peers that misbehave in each of the ways a
// real one can.
//
// Every peer call used http.DefaultClient with no timeout, read the body with
// an unbounded io.ReadAll, and merged whatever came back with no version
// check. A peer that hung, that returned a gigabyte, or that ran a different
// release each produced a different flavour of wrong -- and the router could
// not report any of them, because every failure was a bare `continue` and the
// whole shard a `return nil, false`.

// fakePeer is a storage node the test controls.
func fakePeer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// envelopePeer answers with a correct envelope and the given body.
func envelopePeer(t *testing.T, body string) *httptest.Server {
	return fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, time.Now().UnixNano(), true, "gen-test", r.Header.Get(HdrTraceID))
		w.Write([]byte(body))
	})
}

// askPeer runs one request through the peer client.
func askPeer(t *testing.T, c *clusterClient, url string) PeerResponse {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?query=*", nil)
	return c.do(r, 0, 0, url, http.MethodGet, "/select/logsql/query", nil, "")
}

// A well-behaved peer is accepted, and its envelope is read.
func TestAWellFormedPeerIsAccepted(t *testing.T) {
	peer := envelopePeer(t, `{"_msg":"x"}`+"\n")
	got := askPeer(t, newClusterClient(nil), peer.URL)
	if !got.OK() {
		t.Fatalf("%s", got)
	}
	if got.Version != ProtocolVersion {
		t.Errorf("version = %d", got.Version)
	}
	if !got.Complete {
		t.Error("a peer that said complete=true was read as incomplete")
	}
	if got.HighWatermark == 0 {
		t.Error("the high watermark was not read")
	}
	if string(got.Body) != `{"_msg":"x"}`+"\n" {
		t.Errorf("body = %q", got.Body)
	}
}

// A peer speaking another protocol version is refused, not merged.
//
// Silently merging it is the worst outcome: a field that moved between
// releases produces a plausible wrong answer rather than an error.
func TestAVersionMismatchIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, version string }{
		{"newer", strconv.Itoa(ProtocolVersion + 1)},
		{"older", strconv.Itoa(ProtocolVersion - 1)},
		{"garbage", "banana"},
		{"absent", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.version != "" {
					w.Header().Set(HdrProtocolVersion, tc.version)
				}
				w.Header().Set(HdrComplete, "true")
				w.Write([]byte(`{"_msg":"x"}` + "\n"))
			})
			got := askPeer(t, newClusterClient(nil), peer.URL)
			if got.Class != PeerVersionMismatch {
				t.Fatalf("class = %q, want version_mismatch (%s)", got.Class, got)
			}
			if got.Body != nil {
				t.Error("a body from an unknown protocol version was kept")
			}
			// Worth trying another replica: the rest of the shard may be on
			// the right version mid-upgrade.
			if !got.Class.retryAnotherReplica() {
				t.Error("a version mismatch does not try another replica")
			}
		})
	}
}

// A peer that never answers does not hold the router forever.
func TestAHangingPeerTimesOut(t *testing.T) {
	block := make(chan struct{})
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		<-block // never answers
	})
	// Registered AFTER fakePeer's own Close, because cleanups run LIFO: this
	// one must therefore run FIRST. httptest's Close waits for outstanding
	// handlers, so unblocking after it is a deadlock -- which is how the first
	// version of this test hung until the -timeout killed it.
	t.Cleanup(func() { close(block) })

	c := newClusterClient(nil)
	// The response-header timeout is what bounds this: the connection is
	// accepted, so a dial timeout never fires. http.DefaultClient has neither.
	c.http.Transport.(*http.Transport).ResponseHeaderTimeout = 100 * time.Millisecond

	start := time.Now()
	got := askPeer(t, c, peer.URL)
	if got.Class != PeerUnavailable {
		t.Fatalf("class = %q, want unavailable (%s)", got.Class, got)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("waited %s for a peer that never answered", d)
	}
}

// A peer that is not listening is unavailable, and the router says which one.
func TestADeadPeerIsUnavailable(t *testing.T) {
	got := askPeer(t, newClusterClient(nil), "http://127.0.0.1:1")
	if got.Class != PeerUnavailable {
		t.Fatalf("class = %q (%s)", got.Class, got)
	}
	if got.Err == nil {
		t.Error("no error recorded")
	}
	if !strings.Contains(got.String(), "127.0.0.1:1") {
		t.Errorf("the failure does not name the peer: %s", got)
	}
}

// An oversized response is discarded, not truncated.
//
// A truncated JSON document is unparseable and a truncated NDJSON stream is a
// partial answer that looks complete -- so the router must not keep either.
func TestAnOversizedPeerResponseIsDiscarded(t *testing.T) {
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
		big := strings.Repeat("x", 4096)
		for i := 0; i < 64; i++ {
			w.Write([]byte(big))
		}
	})
	c := newClusterClient(nil)
	c.maxBody = 1024

	got := askPeer(t, c, peer.URL)
	if got.Class != PeerMalformed {
		t.Fatalf("class = %q, want malformed (%s)", got.Class, got)
	}
	if got.Body != nil {
		t.Errorf("%d bytes of an oversized response were kept", len(got.Body))
	}
	if got.Class.retryAnotherReplica() {
		t.Error("a malformed response retries another replica; it will return the same thing")
	}
}

// An authorization failure is not retried across replicas: the credential is
// the ROUTER's, so every replica refuses it identically and retrying turns one
// 401 into N while delaying the report by the timeout.
func TestAnUnauthorizedPeerIsNotRetried(t *testing.T) {
	var hits int
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
		w.WriteHeader(http.StatusUnauthorized)
	})
	got := askPeer(t, newClusterClient(nil), peer.URL)
	if got.Class != PeerUnauthorized {
		t.Fatalf("class = %q (%s)", got.Class, got)
	}
	if got.Class.retryAnotherReplica() {
		t.Error("an unauthorized peer retries another replica")
	}
}

// A TLS failure is a peer that did not answer, not a crash.
func TestATLSFailureIsUnavailable(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", "")
		w.Write([]byte("{}"))
	}))
	defer peer.Close()

	// A client that does not trust the peer's certificate.
	got := askPeer(t, newClusterClient(&tls.Config{}), peer.URL)
	if got.Class != PeerUnavailable {
		t.Fatalf("class = %q, want unavailable (%s)", got.Class, got)
	}

	// And one that does.
	trusting := newClusterClient(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test peer
	if got := askPeer(t, trusting, peer.URL); !got.OK() {
		t.Fatalf("a trusted TLS peer was refused: %s", got)
	}
}

// The client forwards only the headers it names, and never the caller's
// credential: the router authenticates to peers as itself, and a client
// credential travelling further than the node it was presented to is how one
// node's compromise becomes the cluster's.
func TestThePeerClientForwardsOnlyWhatItNames(t *testing.T) {
	seen := make(chan http.Header, 1)
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		writeEnvelope(w.Header(), 0, 0, true, 1, true, "gen-test", r.Header.Get(HdrTraceID))
		w.Write([]byte("{}"))
	})

	r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?query=*", nil)
	r.Header.Set("Authorization", "Bearer super-secret")
	r.Header.Set("Cookie", "session=abc")
	r.Header.Set("AccountID", "7")
	r.Header.Set("ProjectID", "3")
	r.Header.Set("X-Request-Id", "trace-123")
	newClusterClient(nil).do(r, 0, 0, peer.URL, http.MethodGet, "/select/logsql/query", nil, "")

	h := <-seen
	if h.Get("Authorization") != "" {
		t.Error("the caller's credential was forwarded to a storage node")
	}
	if h.Get("Cookie") != "" {
		t.Error("the caller's cookies were forwarded to a storage node")
	}
	if h.Get("AccountID") != "7" || h.Get("ProjectID") != "3" {
		t.Errorf("the resolved tenant was not forwarded: %q/%q",
			h.Get("AccountID"), h.Get("ProjectID"))
	}
	if h.Get("X-Request-Id") != "trace-123" {
		t.Error("the trace id was not forwarded; one request is not one trace")
	}
	if h.Get(HdrInternal) == "" {
		t.Error("the request was not marked internal, so a peer answers the public shape")
	}
}

// A storage node stamps the envelope on internal requests and does NOT on
// public ones: the envelope is a promise between versions of this binary, not
// part of the public API.
func TestAStorageNodeStampsOnlyInternalResponses(t *testing.T) {
	_, ts := uiServer(t)

	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get(HdrProtocolVersion) != "" {
		t.Error("a public response carries the internal envelope")
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/query?query=*", nil)
	req.Header.Set(HdrInternal, "1")
	req.Header.Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get(HdrProtocolVersion) != strconv.Itoa(ProtocolVersion) {
		t.Fatalf("an internal response carries no version: %v", resp.Header)
	}
	if resp.Header.Get(HdrComplete) != "true" {
		t.Errorf("a healthy node did not report a complete answer: %q",
			resp.Header.Get(HdrComplete))
	}
	if resp.Header.Get(HdrShardID) == "" || resp.Header.Get(HdrReplicaID) == "" {
		t.Error("the response does not say which shard and replica answered")
	}
}

// A peer speaking an unknown version is refused BY the storage node too, so a
// mismatch is caught in both directions.
func TestAStorageNodeRefusesAnUnknownProtocol(t *testing.T) {
	_, ts := uiServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/query?query=*", nil)
	req.Header.Set(HdrInternal, "1")
	req.Header.Set(HdrProtocolVersion, fmt.Sprint(ProtocolVersion+1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a caller on protocol %d got %d", ProtocolVersion+1, resp.StatusCode)
	}
	if resp.Header.Get(HdrErrorClass) != string(PeerVersionMismatch) {
		t.Errorf("the refusal is not classed: %q", resp.Header.Get(HdrErrorClass))
	}
}

// spool discards a response past its bound as malformed, the way do discards
// one past maxBody -- the bound that makes a REPAIR fetch safe: a group past
// maxRepairBytes is refused at the fetch, with the file discarded, rather
// than half-moved.
func TestSpoolRefusesAResponsePastItsBound(t *testing.T) {
	const bound = 1 << 20
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
		w.Write(make([]byte, 2*bound))
	})

	c := newClusterClient(nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	f, n, resp, cleanup := c.spool(req, 0, 0, peer.URL, "/x", "", bound)
	cleanup()

	if resp.Class != PeerMalformed {
		t.Fatalf("class %q, want malformed: %v", resp.Class, resp.Err)
	}
	if !strings.Contains(resp.Err.Error(), "exceeds") {
		t.Errorf("the refusal does not say why: %v", resp.Err)
	}
	if f != nil {
		t.Error("a discarded response left a file behind")
	}
	if n > bound {
		t.Errorf("read %d bytes past the %d-byte bound", n, bound)
	}

	// The same response under the same bound at zero is accepted and read
	// whole -- the bound is what the caller chooses, not what the transport
	// imposes.
	f, n, resp, cleanup = c.spool(req, 0, 0, peer.URL, "/x", "", 0)
	cleanup()
	if !resp.OK() || f == nil {
		t.Fatalf("unbounded spool: %s", resp)
	}
	if n != 2*bound {
		t.Errorf("unbounded spool read %d bytes, want %d", n, 2*bound)
	}
}

// spool creates its temp file in the client's configured directory, not the
// process temp dir -- which is often tmpfs, the one place a gigabyte repair
// transfer must not land -- and leaves nothing named behind in either place.
func TestSpoolUsesTheConfiguredDirectory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the fd-name check reads /proc/self/fd")
	}
	dir := t.TempDir()
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
		w.Write(make([]byte, 4096))
	})
	c := newClusterClient(nil)
	c.spoolDir = dir

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	f, n, resp, cleanup := c.spool(req, 0, 0, peer.URL, "/x", "", 0)
	defer cleanup()
	if !resp.OK() || f == nil {
		t.Fatalf("spool: %s", resp)
	}
	if n != 4096 {
		t.Fatalf("spooled %d bytes, want 4096", n)
	}
	// The file is unlinked, but its fd still names the place it was created.
	link, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", f.Fd()))
	if err != nil {
		t.Skipf("cannot readlink /proc/self/fd: %v", err)
	}
	if !strings.Contains(link, dir) {
		t.Errorf("the spool file was created at %q, want it under the configured %q", link, dir)
	}
	// Nothing named may remain in this test's isolated spool directory.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "simdlogs-spool-") {
			t.Errorf("a spool left %q behind in %s", e.Name(), dir)
		}
	}
}

// A spoolDir that cannot host a temp file is a loud failure, not a silent
// fallback to the process temp dir: the point of the directory is that the
// file is on the configured filesystem, and falling back would put it back on
// whichever one the process temp dir happens to be.
func TestSpoolRefusesAnUnusableConfiguredDirectory(t *testing.T) {
	peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
		w.Write(make([]byte, 64))
	})
	notADir := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newClusterClient(nil)
	c.spoolDir = notADir
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	f, _, resp, cleanup := c.spool(req, 0, 0, peer.URL, "/x", "", 0)
	cleanup()
	if resp.Class != PeerUnavailable {
		t.Fatalf("class %q, want unavailable: %v", resp.Class, resp.Err)
	}
	if f != nil {
		t.Error("a failed spool returned a file")
	}
}

// The spool bound is exact: a response of exactly maxBytes passes, one byte
// more is refused.
func TestSpoolBoundIsExact(t *testing.T) {
	const bound = 1 << 20
	for _, n := range []int64{bound, bound + 1} {
		peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w.Header(), 0, 0, true, 0, false, "gen-test", "")
			w.Write(make([]byte, n))
		})
		c := newClusterClient(nil)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		f, got, resp, cleanup := c.spool(req, 0, 0, peer.URL, "/x", "", bound)
		cleanup()
		if n == bound {
			if !resp.OK() || f == nil {
				t.Fatalf("a response of exactly the bound was refused: %s", resp)
			}
			if got != bound {
				t.Errorf("read %d bytes, want the %d served", got, bound)
			}
		} else if resp.Class != PeerMalformed || f != nil {
			t.Fatalf("a response one byte over the bound: class %q, file %v (%v)",
				resp.Class, f != nil, resp.Err)
		}
	}
}

// spool classifies a refusal exactly as doReader does: 401/403 is the
// router's credential, 429/503 is load, 5xx is an unavailable peer, and a 4xx
// carries the peer's own words. The repair fetch must not collapse a source's
// 404 -- "this digest is no longer on disk, and here is why" -- into a
// bodyless "HTTP 404".
func TestSpoolClassifiesRefusalsLikeDoReader(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body        string
		wantClass   PeerErrorClass
		wantInError string
	}{
		{"unauthorized", http.StatusUnauthorized, "nope", PeerUnauthorized, ""},
		{"forbidden", http.StatusForbidden, "nope", PeerUnauthorized, ""},
		{"overloaded", http.StatusTooManyRequests, "slow down", PeerOverloaded, ""},
		{"unavailable", http.StatusServiceUnavailable, "draining", PeerOverloaded, ""},
		{"server error", http.StatusInternalServerError, "boom", PeerUnavailable, ""},
		{"refused with a diagnostic body", http.StatusNotFound,
			"storage: group 3 changed while being read", PeerRejected, "changed while being read"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			peer := fakePeer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			c := newClusterClient(nil)
			req := httptest.NewRequest(http.MethodGet, "/", nil)

			f, _, spooled, cleanup := c.spool(req, 0, 0, peer.URL, "/x", "", 1<<20)
			cleanup()
			if spooled.Class != tc.wantClass {
				t.Fatalf("spool class %q, want %q: %v", spooled.Class, tc.wantClass, spooled.Err)
			}
			if tc.wantInError != "" && !strings.Contains(spooled.Err.Error(), tc.wantInError) {
				t.Errorf("the refusal does not carry the peer's words: %v", spooled.Err)
			}
			if f != nil {
				t.Error("a refused response left a spool file")
			}

			read := c.doReader(req, 0, 0, peer.URL, http.MethodGet, "/x", nil, "")
			if read.Class != spooled.Class {
				t.Errorf("doReader class %q differs from spool's %q on the same response",
					read.Class, spooled.Class)
			}
		})
	}
}
