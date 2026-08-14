package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/query"
)

// Every limit the configuration declares must be read by something. Eight of
// the twelve were declared, defaulted, validated and never enforced, and
// -search.maxDuration was accepted and did nothing -- an operator setting it
// got no timeout and no warning.
func limitServer(t *testing.T, tune func(*config.Limits)) (*Server, *httptest.Server) {
	t.Helper()
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	if tune != nil {
		tune(&c.Limits)
	}
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return srv, ts
}

func post(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// A single line longer than MaxLineBytes is rejected, not stored. The body
// limit alone does not cover it: a body at the limit can be one line.
func TestMaxLineBytesEnforced(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) { l.MaxLineBytes = 256 })

	long := fmt.Sprintf(`{"_time":1700000000000000000,"msg":%q}`, strings.Repeat("x", 4096))
	short := `{"_time":1700000000000000001,"msg":"ok"}`
	resp := post(t, ts, long+"\n"+short+"\n")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 1 {
		t.Fatalf("%d rows stored, want 1 -- the oversized line was accepted", len(rows))
	}
	for _, f := range rows[0].Fields {
		if len(f.Value) > 4000 {
			t.Fatalf("an oversized value was stored (%d bytes)", len(f.Value))
		}
	}
}

// A record with more fields than allowed keeps a bounded subset rather than
// growing the column set without limit.
func TestMaxFieldsPerRecordEnforced(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) {
		l.MaxFieldsPerRecord = 4
		l.MaxLineBytes = 0
	})

	var sb strings.Builder
	sb.WriteString(`{"_time":1700000000000000000`)
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&sb, `,"f%02d":"v"`, i)
	}
	sb.WriteString("}\n")
	resp := post(t, ts, sb.String())
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	// _time is a column of its own, so the payload fields are what is bounded.
	n := 0
	for _, f := range rows[0].Fields {
		if f.Key != "_time" {
			n++
		}
	}
	if n > 4 {
		t.Fatalf("%d payload fields stored, limit is 4", n)
	}
}

// An oversized value is clipped rather than stored whole; an oversized field
// name is dropped.
func TestFieldNameAndValueLimitsEnforced(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) {
		l.MaxFieldValueBytes = 16
		l.MaxFieldNameBytes = 8
		l.MaxLineBytes = 0
		l.MaxFieldsPerRecord = 0
	})

	longName := strings.Repeat("n", 64)
	body := fmt.Sprintf(`{"_time":1700000000000000000,"short":%q,%q:"v"}`+"\n",
		strings.Repeat("v", 128), longName)
	resp := post(t, ts, body)
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	for _, f := range rows[0].Fields {
		if f.Key == longName {
			t.Error("an over-long field name was stored")
		}
		if f.Key == "short" && len(f.Value) > 16 {
			t.Errorf("value stored with %d bytes, limit is 16", len(f.Value))
		}
	}
}

// MaxQueryDuration bounds a query request. The flag that sets it was read by
// nothing.
func TestMaxQueryDurationAppliesToRequests(t *testing.T) {
	_, ts := limitServer(t, func(l *config.Limits) { l.MaxQueryDuration = 50 * time.Millisecond })

	// The deadline is on the request context; a normal query finishes well
	// inside it, which is what this asserts -- the bound exists and does not
	// break ordinary use.
	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// The live tail must not carry the query deadline: it is meant to stay open.
func TestTailHasNoQueryDeadline(t *testing.T) {
	_, ts := limitServer(t, func(l *config.Limits) { l.MaxQueryDuration = 100 * time.Millisecond })

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/select/logsql/tail?query=*", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tail -> %d", resp.StatusCode)
	}
	// Past the deadline the stream must still be open.
	time.Sleep(300 * time.Millisecond)
	buf := make([]byte, 1)
	done := make(chan struct{})
	go func() { resp.Body.Read(buf); close(done) }()
	select {
	case <-done:
		t.Fatal("the tail was closed by the query deadline")
	case <-time.After(200 * time.Millisecond):
		// Still open, which is the point.
	}
}

// An expired deadline must stop the query and be reported, not answered 200
// with a full body. Setting it on the request context alone did nothing: Go
// does not abort a handler when its context is cancelled, and the query
// package took no context, so the flag bounded the cluster fan-out and
// nothing else.
func TestExpiredQueryDeadlineIsReported(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) { l.MaxQueryDuration = 1 })

	resp := post(t, ts, `{"_time":1700000000000000000,"level":"info"}`+"\n")
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d with a 1ns budget, want 504", got.StatusCode)
	}
}

// A wide record must keep its log line. '_' sorts after digits and uppercase,
// so a plain sort dropped _msg first and stored AAA/BBB/CCC instead.
func TestFieldCapKeepsReservedFields(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) {
		l.MaxFieldsPerRecord = 3
		l.MaxLineBytes = 0
	})

	body := `{"_time":1700000000000000000,"AAA":"1","BBB":"2","CCC":"3","_msg":"THE-LOG-LINE"}` + "\n"
	resp := post(t, ts, body)
	resp.Body.Close()
	if err := srv.def.w.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	var msg string
	for _, f := range rows[0].Fields {
		if f.Key == "_msg" {
			msg = f.Value
		}
	}
	if msg != "THE-LOG-LINE" {
		t.Fatalf("_msg is %q; the field cap dropped the log line", msg)
	}
}

// Concurrency is bounded: over the budget a request is refused with 429
// rather than queueing without limit.
func TestConcurrencyLimitRejects(t *testing.T) {
	srv, ts := limitServer(t, func(l *config.Limits) {
		l.MaxConcurrentQuery = 1
		l.MaxQueryDuration = 0
	})

	// Occupy the single slot the way a running query does.
	srv.querySem <- struct{}{}
	defer func() { <-srv.querySem }()

	got, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d with the query budget held, want 429", got.StatusCode)
	}
}

// A live tail is an idle connection, not a running query. Charging it a query
// slot meant a handful of tails -- which the shipped /vmui explorer opens
// itself -- returned 429 for every other read, and /metrics shares that
// budget, so the scraper went blind exactly when it was needed.
func TestTailsDoNotStarveTheQueryBudget(t *testing.T) {
	_, ts := limitServer(t, func(l *config.Limits) {
		l.MaxConcurrentQuery = 2
		l.MaxQueryDuration = 0
	})

	// Open more tails than the whole query budget.
	for i := 0; i < 4; i++ {
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

	// Ordinary reads must still work, and so must the metrics scrape.
	for _, path := range []string{"/select/logsql/query?query=*", "/metrics"} {
		got, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		got.Body.Close()
		if got.StatusCode == http.StatusTooManyRequests {
			t.Fatalf("%s -> 429 with four tails open; tails must not hold query slots", path)
		}
	}
}
