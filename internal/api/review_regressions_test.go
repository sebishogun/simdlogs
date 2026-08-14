package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/query"
)

// Regressions found by review, each one reproduced before it was fixed.

// A router applies the same checks the local path does -- but it did not
// resolve the tenant, and forwardWrite clones the client's headers verbatim.
// A storage node normally runs with no -auth.config at all, so the router is
// the only place AccountID is ever checked: a token scoped to one tenant
// wrote into any other by setting a header.
func TestRouterForwardsTheResolvedTenantNotTheRequestedOne(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("AccountID")+":"+r.Header.Get("ProjectID"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	// Scoped to 0:0 only -- the shipped test used "*", which is why it
	// missed this.
	const tok = "scoped-shipper-token"
	if err := srv.SetAuth(&config.AuthConfig{Tokens: []config.TokenSpec{
		{SHA256: config.HashToken(tok), Subject: "shipper", Roles: []string{"ingest"}, Tenants: []string{"0:0"}},
	}}); err != nil {
		t.Fatal(err)
	}
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	line := `{"_time":1700000000000000000,"level":"info"}` + "\n"

	// A tenant the token does not hold must be refused, and nothing may
	// reach the backend.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", tok,
		map[string]string{"AccountID": "9"}, line)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("a 0:0-scoped token wrote to AccountID 9 through the router (status %d)", resp.StatusCode)
	}
	mu.Lock()
	n := len(seen)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("a cross-tenant write reached the backend as %v", seen)
	}

	// Its own tenant goes through, carrying the RESOLVED key.
	resp = do(t, ts, http.MethodPost, "/insert/jsonline", tok, nil, line)
	resp.Body.Close()
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "0:0" {
		t.Fatalf("backend saw %v, want one call as 0:0", got)
	}
}

// The default tenant sits in the map with no in-flight reference and the
// oldest lastUse, so it was the first thing eviction chose -- and s.def is
// never re-pointed, so every syslog listener, /alerts and every
// metrics-from-logs rule was left holding a closed writer, silently and for
// the life of the process.
func TestDefaultTenantIsNeverEvicted(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxOpenTenants = 2
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// Open and release more tenants than the limit, so eviction runs.
	for i := 1; i <= 5; i++ {
		tn, err := srv.tenant(fmt.Sprint(i), "0")
		if err != nil {
			t.Fatal(err)
		}
		tn.inFlight.Add(-1)
	}

	srv.mu.Lock()
	_, present := srv.tenants["0:0"]
	srv.mu.Unlock()
	if !present {
		t.Fatal("the default tenant was evicted")
	}
	// And its writer still works, which is what the syslog listeners and the
	// rule evaluators depend on.
	if err := srv.def.w.Flush(); err != nil {
		t.Fatalf("the default writer is unusable after eviction pressure: %v", err)
	}
}

// A live tail held its tenant claim for the life of the connection, and a
// busy tenant is never evicted. A handful of anonymous tails filled the
// tenant table and every other tenant got 503.
func TestTailsDoNotPinTenantSlots(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxOpenTenants = 3
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for i := 1; i <= 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/tail?query=*", nil)
		req.Header.Set("AccountID", fmt.Sprint(i))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tail %d -> %d", i, resp.StatusCode)
		}
	}
	// Give the handlers a moment to reach the point where they release.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		busy := 0
		srv.mu.Lock()
		for _, tn := range srv.tenants {
			if tn.inFlight.Load() > 0 {
				busy++
			}
		}
		srv.mu.Unlock()
		if busy == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A fresh tenant must still be able to write.
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "",
		map[string]string{"AccountID": "99"}, `{"_time":1700000000000000000,"a":"b"}`+"\n")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a write for a fresh tenant -> %d with three tails open", resp.StatusCode)
	}
}

// Tails are exempt from the query budget -- charging them meant a few open
// tails returned 429 for every other read -- but exempt is not unbounded.
func TestTailsAreBoundedByTheirOwnBudget(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxConcurrentTail = 2
	c.Limits.MaxQueryDuration = 0
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/tail?query=*", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tail %d -> %d", i, resp.StatusCode)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/tail?query=*", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("the third tail -> %d with a budget of 2, want 429", resp.StatusCode)
	}

	// An ordinary query is unaffected: it is a different budget.
	got, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("query -> %d with the tail budget full", got.StatusCode)
	}
}

// Close unmaps every store. A handler still inside a query is reading that
// mapping, so returning from Close while a request is in flight is a
// use-after-unmap waiting for the timing to line up. The shipped test
// released its request BEFORE calling Close, so it did not test its name.
func TestCloseWaitsForARequestThatIsStillRunning(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}

	// Hold a tenant the way an in-flight request does.
	tn, err := srv.tenant("7", "0")
	if err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()

	select {
	case <-closed:
		t.Fatal("Close returned while a request was still in flight")
	case <-time.After(150 * time.Millisecond):
	}

	tn.inFlight.Add(-1)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the request finished")
	}
}

// The Datadog Agent validates its API key with GET /api/v1/validate. Wrapping
// the route in the ingest spec restricted it to POST/PUT, so the agent was
// answered 405 and reported the key as invalid.
func TestDatadogValidateAnswersGet(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, m := range []string{http.MethodGet, http.MethodPost} {
		resp := do(t, ts, m, "/insert/datadog/api/v1/validate", "", nil, "")
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s /insert/datadog/api/v1/validate -> %d, want 200", m, resp.StatusCode)
		}
	}
}

// OTLP counted its rows before the flush and never subtracted them when the
// flush reported a closed writer, so /metrics claimed ingest that a 503 had
// already refused. Every sibling path either counts after the flush or
// subtracts here.
func TestOTLPDoesNotCountRowsAClosedWriterDropped(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	before := atomic.LoadInt64(&srv.nRowsIn)
	srv.def.w.Close() // the writer a shutting-down or evicted tenant has

	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[` +
		`{"timeUnixNano":"1700000000000000000","body":{"stringValue":"x"}}]}]}]}`
	resp := do(t, ts, http.MethodPost, "/insert/opentelemetry/v1/logs", "",
		map[string]string{"Content-Type": "application/json"}, body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("OTLP into a closed writer -> %d, want 503", resp.StatusCode)
	}
	if after := atomic.LoadInt64(&srv.nRowsIn); after != before {
		t.Fatalf("rows counted %d -> %d for a write the writer refused", before, after)
	}
}

// A column shorter than Rows is unrecoverable on the read side: the block
// count pins Rows only to block granularity, so a timestamp column up to
// blockSize-1 short was accepted and the missing rows decoded a fabricated
// time. Marshal refuses to write one.
func TestQueryStillWorksAfterTheColumnLengthAssertion(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Enough rows to cross a block boundary in both directions.
	for i := 0; i < 3000; i++ {
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
			fmt.Sprintf(`{"_time":%d,"level":"info","i":"%d"}`+"\n",
				1700000000000000000+int64(i), i))
		resp.Body.Close()
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}
	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 3000 {
		t.Fatalf("%d rows back, want 3000", len(rows))
	}
}

// Round 9. The write path stamped the resolved tenant; the read path did not,
// and every federated helper copies AccountID/ProjectID straight out of the
// request. A principal whose default tenant is 9:0, sending no header at all,
// forwarded no header at all -- and every backend answered out of ITS default.
func TestFederatedReadsCarryTheResolvedTenant(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("AccountID")+":"+r.Header.Get("ProjectID"))
		mu.Unlock()
		w.Write([]byte("{}\n"))
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	const tok = "reader-scoped-to-nine"
	if err := srv.SetAuth(&config.AuthConfig{Tokens: []config.TokenSpec{
		{SHA256: config.HashToken(tok), Subject: "reader", Roles: []string{"query"}, Tenants: []string{"9:0"}},
	}}); err != nil {
		t.Fatal(err)
	}
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=*", tok, nil, "")
	resp.Body.Close()

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("nothing was forwarded")
	}
	for _, k := range got {
		if k != "9:0" {
			t.Fatalf("a backend was asked for tenant %q by a 9:0-scoped principal", k)
		}
	}
}

// routeWrites hardcoded ndjsonSpec() for every write path, so in router mode
// a collector's default protobuf OTLP got 415, journald got 415, and the
// Datadog key probe got 405 -- the same failures the per-route specs exist to
// prevent, one layer up. One specForPath both sides call is what stops the
// mux and the router disagreeing again.
func TestRouterUsesEachRoutesOwnSpec(t *testing.T) {
	var mu sync.Mutex
	forwarded := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		forwarded++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, c := range []struct {
		method, path, ctype string
	}{
		{http.MethodPost, "/v1/logs", "application/x-protobuf"},
		{http.MethodPost, "/insert/opentelemetry/v1/logs", "application/x-protobuf"},
		{http.MethodPost, "/insert/journald", "application/vnd.fdo.journal"},
		{http.MethodGet, "/insert/datadog/api/v1/validate", ""},
	} {
		hdr := map[string]string{}
		if c.ctype != "" {
			hdr["Content-Type"] = c.ctype
		}
		resp := do(t, ts, c.method, c.path, "", hdr, "x")
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnsupportedMediaType ||
			resp.StatusCode == http.StatusMethodNotAllowed {
			t.Errorf("router %s %s (%s) -> %d", c.method, c.path, c.ctype, resp.StatusCode)
		}
	}
	mu.Lock()
	n := forwarded
	mu.Unlock()
	if n == 0 {
		t.Fatal("the router forwarded nothing at all")
	}
}

// Admission lives in guard(), which forwarding returns before, so a router
// applied no ingest budget: N concurrent posts each read a whole body into
// memory with no bound.
func TestRouterAppliesTheIngestBudget(t *testing.T) {
	// The backend blocks until released. Release BEFORE any deferred
	// shutdown runs: ts.Close() waits for outstanding requests, and a
	// deferred close(release) would run after it -- a deadlock, not a test.
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxConcurrentWrite = 1
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	line := `{"_time":1700000000000000000,"a":"b"}` + "\n"
	started := make(chan struct{})
	go func() {
		close(started)
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, line)
		resp.Body.Close()
	}()
	<-started
	// Let the first request reach the backend and block there.
	time.Sleep(100 * time.Millisecond)

	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, line)
	resp.Body.Close()
	code := resp.StatusCode
	unblock() // let the held request finish before anything shuts down
	if code != http.StatusTooManyRequests {
		t.Fatalf("a second forwarded write with a budget of 1 -> %d, want 429", code)
	}
}

// countRequest lives in withTenant, which forwarding also returns before, so
// /metrics read zero ingest on a router under full load.
func TestRouterCountsForwardedWrites(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetBackends([]string{backend.URL})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	before := atomic.LoadInt64(&srv.nIngestReq)
	for i := 0; i < 5; i++ {
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
			`{"_time":1700000000000000000,"a":"b"}`+"\n")
		resp.Body.Close()
	}
	if got := atomic.LoadInt64(&srv.nIngestReq); got != before+5 {
		t.Fatalf("insert requests counted %d -> %d for five forwarded writes", before, got)
	}
}

// MaxQueryDuration and MaxQueryBytes were checked in exactly three places,
// all in the group PRE-FILTER (metadata only, no column decode), and all with
// a literal zero for bytes -- so the byte budget could never fire at all, and
// the deadline could only fire if it had already passed before any work
// started. A 300,000-row scan with a 20 ms budget ran to completion.
func TestQueryBudgetsBoundTheScanNotOnlyThePrefilter(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxQueryDuration = 0 // isolate the byte budget
	c.Limits.MaxQueryBytes = 1
	c.Limits.MaxQueryRows = 0
	// TestLimits caps bodies at 64 KiB, which would refuse the corpus; the
	// decompressed bound has to move with it or Normalize refuses the pair.
	c.Limits.MaxBodyBytes = 64 << 20
	c.Limits.MaxDecompressed = 64 << 20
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Enough rows in enough groups that the scan has somewhere to stop.
	// Posted in batches: one 4 MB body would be refused by MaxBodyBytes, and
	// a rejected body makes the query return nothing, which passes a budget
	// test for the wrong reason.
	const rows = 20000
	for base := 0; base < rows; base += 5000 {
		var sb strings.Builder
		for i := base; i < base+5000; i++ {
			fmt.Fprintf(&sb, `{"_time":%d,"level":"info","payload":"%s"}`+"\n",
				1700000000000000000+int64(i), strings.Repeat("x", 200))
		}
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, sb.String())
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("batch at %d -> %d", base, resp.StatusCode)
		}
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}
	// The scan must actually have something to scan.
	if n := len(query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})); n != rows {
		t.Fatalf("%d rows stored, want %d -- the budget test would pass vacuously", n, rows)
	}

	got, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d with a 1-byte query budget over %d rows, want 504",
			got.StatusCode, rows)
	}
}

// The 504 named -search.maxDuration and -search.maxQueryBytes. Neither
// spelling exists: the limits are max-query-duration and max-query-bytes.
func TestBudgetErrorNamesRealFlags(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxQueryDuration = 1
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
		`{"_time":1700000000000000000,"a":"b"}`+"\n")
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	buf := make([]byte, 4096)
	n, _ := got.Body.Read(buf)
	msg := string(buf[:n])

	// Every flag-looking token in the message must be a flag the binary
	// actually registers. Asserting on two hardcoded spellings was what let
	// the previous round replace one invented pair with another: the message
	// said "max-query-duration / max-query-bytes", the test asserted those
	// two strings, and neither was a flag.
	named := regexp.MustCompile(`-[a-zA-Z][a-zA-Z0-9.\-]*`).FindAllString(msg, -1)
	if len(named) == 0 {
		t.Fatalf("the 504 names no flag at all: %q", msg)
	}
	for _, tok := range named {
		name := strings.TrimPrefix(tok, "-")
		if flagNames[name] {
			continue
		}
		t.Errorf("the 504 names %q, which cmd/simdlogs does not register (message: %q)", tok, msg)
	}
}

// flagNames is every flag cmd/simdlogs defines, read from its source. A test
// in package api cannot import package main, and a hand-copied list is
// exactly the thing that goes stale.
var flagNames = func() map[string]bool {
	src, err := os.ReadFile("../../cmd/simdlogs/main.go")
	if err != nil {
		panic(err)
	}
	out := map[string]bool{}
	for _, m := range regexp.MustCompile(`flag\.[A-Za-z0-9]+\("([^"]+)"`).FindAllSubmatch(src, -1) {
		out[string(m[1])] = true
	}
	return out
}()

// Round 10. The budget was wired into three read routes; twelve others ran
// with no bound at all, so a 1-byte budget still returned 15 MB and a 10 ms
// budget still took 122 ms. Every route that scans must be bounded.
func TestEveryReadRouteIsBounded(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxBodyBytes = 64 << 20
	c.Limits.MaxDecompressed = 64 << 20
	c.Limits.MaxQueryDuration = 1 // 1ns: spent before any group is read
	c.Limits.MaxQueryBytes = 1
	c.Limits.MaxQueryRows = 0
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var sb strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"level":"info","svc":"api","msg":"%s"}`+"\n",
			1700000000000000000+int64(i), strings.Repeat("x", 64))
	}
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, sb.String())
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed -> %d", resp.StatusCode)
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		"/select/logsql/query?query=*",
		"/select/logsql/hits?query=*",
		"/select/logsql/facets?query=*",
		"/select/logsql/field_names?query=*",
		"/select/logsql/field_values?query=*&field=svc",
		"/select/logsql/streams?query=*",
		"/select/logsql/stream_ids?query=*",
		"/select/logsql/stream_field_names?query=*",
		"/select/logsql/stream_field_values?query=*&field=svc",
		"/select/logsql/stats_query?query=" + url.QueryEscape("* | stats count() c"),
		"/select/logsql/stats_query_range?query=" + url.QueryEscape("* | stats count() c"),
		"/select/logsql/query?query=" + url.QueryEscape("* | uniq by (svc)"),
		"/select/logsql/query?query=" + url.QueryEscape("* | top 3 by (svc)"),
	} {
		got, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		code := got.StatusCode
		got.Body.Close()
		if code != http.StatusGatewayTimeout {
			t.Errorf("%s -> %d with a 1ns budget, want 504", path, code)
		}
	}
}

// A liveness probe is not a write. It sits under /insert only because that is
// where the reference put it, and forwarding it made a router answer 401 to
// an unauthenticated Kubernetes probe -- forever -- while the same probe on a
// non-router node answered 200.
func TestReadyProbeAnswersTheSameInRouterMode(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	for _, router := range []bool{false, true} {
		c := config.Default()
		c.Dir = t.TempDir()
		c.Limits = config.TestLimits()
		srv, err := NewServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.SetAuth(&config.AuthConfig{Tokens: []config.TokenSpec{
			{SHA256: config.HashToken("t"), Subject: "s", Roles: []string{"ingest"}, Tenants: []string{"*"}},
		}}); err != nil {
			t.Fatal(err)
		}
		if router {
			srv.SetBackends([]string{backend.URL})
		}
		ts := httptest.NewServer(srv.Handler())
		resp := do(t, ts, http.MethodGet, "/insert/ready", "", nil, "")
		code := resp.StatusCode
		resp.Body.Close()
		ts.Close()
		srv.Close()
		if code != http.StatusOK {
			t.Errorf("router=%v: GET /insert/ready with no credential -> %d, want 200", router, code)
		}
	}
}

// A subquery ran with no budget at all: only From/To/Now were copied, so
// appending `| union (*)` to any query ran a full unbounded scan while the
// outer deadline was already spent. The route still answered 504, which is
// why this was invisible from outside.
func TestSubqueriesInheritTheBudget(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxBodyBytes = 64 << 20
	c.Limits.MaxDecompressed = 64 << 20
	c.Limits.MaxQueryDuration = 1
	c.Limits.MaxQueryRows = 0
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var sb strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"svc":"api"}`+"\n", 1700000000000000000+int64(i))
	}
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, sb.String())
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	// The subquery must stop too, not just the outer scan. Measured at the
	// engine, where the HTTP 504 cannot hide it.
	stopped := new(atomic.Bool)
	q, perr := query.ParseLogsQL(`* | union (*)`)
	if perr != nil {
		t.Fatal(perr)
	}
	q.From, q.To = 0, int64(1)<<62
	q.MatAll = true
	q.Deadline = time.Now().Add(-time.Second) // already spent
	q.Stopped = stopped
	rows := query.RunPipeline(srv.def.store, q)
	if len(rows) != 0 {
		t.Fatalf("a spent deadline returned %d rows through a union subquery", len(rows))
	}
}

// The `by=` fallback re-parses, so the budget applied to the Query that
// failed does not cover it. It answered a COMPLETE 200 with the deadline
// already spent.
func TestStatsQueryByFallbackIsBounded(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxBodyBytes = 64 << 20
	c.Limits.MaxDecompressed = 64 << 20
	c.Limits.MaxQueryDuration = 1
	c.Limits.MaxQueryRows = 0
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var sb strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"svc":"api"}`+"\n", 1700000000000000000+int64(i))
	}
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, sb.String())
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := http.Get(ts.URL + "/select/logsql/stats_query?by=svc&query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("stats_query?by= -> %d with a 1ns budget, want 504", got.StatusCode)
	}
}

// "7", "07" and "007" were three separate stores with three directories and
// mutually invisible data, all reachable by varying one request header:
// ParseUint accepts leading zeros and the raw string keyed the map.
func TestTenantIDsAreCanonical(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for i, acc := range []string{"7", "07", "007"} {
		resp := do(t, ts, http.MethodPost, "/insert/jsonline", "",
			map[string]string{"AccountID": acc},
			fmt.Sprintf(`{"_time":%d,"n":"%d"}`+"\n", 1700000000000000000+int64(i), i))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("AccountID=%q -> %d", acc, resp.StatusCode)
		}
	}
	srv.mu.Lock()
	open := len(srv.tenants)
	_, canonical := srv.tenants["7:0"]
	srv.mu.Unlock()
	// The default tenant plus exactly one for account 7.
	if open != 2 || !canonical {
		keys := []string{}
		srv.mu.Lock()
		for k := range srv.tenants {
			keys = append(keys, k)
		}
		srv.mu.Unlock()
		t.Fatalf("three spellings of account 7 opened %d tenants: %v", open, keys)
	}
}

// The tail's end-of-stream marker is only distinguishable from a log line if
// a log line can never carry its key. Nothing reserved a leading dot, so the
// marker was forgeable -- the comment asserted a property the store did not
// enforce.
func TestControlFieldNamesAreNotStorable(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil,
		`{"_time":1700000000000000000,".error":"forged","ok":"kept"}`+"\n")
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}
	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	for _, f := range rows[0].Fields {
		if f.Key == ".error" {
			t.Fatalf("a control field name was stored: %q = %q", f.Key, f.Value)
		}
	}
}

// Round 12. ParallelConfig carried no RecordLimits, so every shard writer
// ran with a zero limit set and add() skipped truncateForLimits entirely: a
// body over MinParallelBytes on /insert/jsonline or either bulk route got
// no field cap, no name or value cap, and no control-name drop -- which
// made the tail's {".error": ...} sentinel forgeable again for any body
// over 1 MiB.
func TestParallelIngestAppliesRecordLimits(t *testing.T) {
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	c.Limits.MaxBodyBytes = 64 << 20
	c.Limits.MaxDecompressed = 64 << 20
	c.Limits.MaxLineBytes = 0
	c.Limits.MaxFieldsPerRecord = 4
	c.Limits.MaxFieldNameBytes = 8
	c.Limits.MaxFieldValueBytes = 16
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Over MinParallelBytes so the parallel path is taken.
	var sb strings.Builder
	longName := strings.Repeat("n", 64)
	bigVal := strings.Repeat("v", 512)
	for i := 0; sb.Len() < 2<<20; i++ {
		fmt.Fprintf(&sb, `{"_time":%d,".error":"forged",%q:"x","big":%q,"a":"1","b":"2","c":"3","d":"4"}`+"\n",
			1700000000000000000+int64(i), longName, bigVal)
	}
	resp := do(t, ts, http.MethodPost, "/insert/jsonline", "", nil, sb.String())
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) == 0 {
		t.Fatal("nothing stored")
	}
	for _, r := range rows {
		payload := 0
		for _, f := range r.Fields {
			switch {
			case f.Key == ".error":
				t.Fatal("the parallel path stored a control field name")
			case len(f.Key) > 8 && f.Key != "_time" && f.Key != "_msg" && f.Key != "_stream" && f.Key != "_stream_id":
				t.Fatalf("stored a %d-byte field name; the cap is 8", len(f.Key))
			case f.Key == "big" && len(f.Value) > 16:
				t.Fatalf("stored a %d-byte value; the cap is 16", len(f.Value))
			}
			if f.Key != "_time" && f.Key != "_stream" && f.Key != "_stream_id" {
				payload++
			}
		}
		if payload > 4 {
			t.Fatalf("%d payload fields stored; the cap is 4", payload)
		}
	}
}

// routeWrites checked method and media type BEFORE the role check, which is
// the ordering Handler() documents avoiding: a wrong Content-Type answered
// 415 first, telling an anonymous caller which media types a route accepts.
// And it applied the media-type check unconditionally, so a spec with
// deliberately nil types -- datadogValidateSpec, the route specForPath
// exists to select -- rejected every Content-Type.
func TestRouterMatchesSingleNodeOnShapeAndOrdering(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	run := func(router bool, method, path, ctype, tok string) int {
		c := config.Default()
		c.Dir = t.TempDir()
		c.Limits = config.TestLimits()
		srv, err := NewServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		if err := srv.SetAuth(&config.AuthConfig{Tokens: []config.TokenSpec{
			{SHA256: config.HashToken("tok"), Subject: "s", Roles: []string{"ingest"}, Tenants: []string{"*"}},
		}}); err != nil {
			t.Fatal(err)
		}
		if router {
			srv.SetBackends([]string{backend.URL})
		}
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		hdr := map[string]string{}
		if ctype != "" {
			hdr["Content-Type"] = ctype
		}
		resp := do(t, ts, method, path, tok, hdr, "x")
		resp.Body.Close()
		return resp.StatusCode
	}

	for _, c := range []struct {
		name, method, path, ctype, tok string
	}{
		{"datadog validate typed", http.MethodGet, "/insert/datadog/api/v1/validate", "application/json", "tok"},
		{"datadog validate posted", http.MethodPost, "/insert/datadog/api/v1/validate", "application/json", "tok"},
		{"wrong media type anonymous", http.MethodPost, "/insert/jsonline", "application/xml", ""},
		{"wrong method anonymous", http.MethodDelete, "/insert/jsonline", "", ""},
	} {
		single := run(false, c.method, c.path, c.ctype, c.tok)
		routed := run(true, c.method, c.path, c.ctype, c.tok)
		if single != routed {
			t.Errorf("%s: single-node=%d router=%d", c.name, single, routed)
		}
	}
}
