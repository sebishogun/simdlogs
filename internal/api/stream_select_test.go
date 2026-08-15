package api

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// streamServer returns a server whose bare selects stream: -search.maxRows=-1,
// which is the configuration where the old path materialized every matching
// row before writing any of them.
func streamServer(t *testing.T, rows int, maxRows int) *httptest.Server {
	t.Helper()
	_, ts := streamServerWith(t, rows, maxRows)
	return ts
}

func streamServerWith(t *testing.T, rows int, maxRows int) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetMaxRows(maxRows)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	var body strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&body, `{"_msg":"line %d","app":"x","seq":"%d"}`+"\n", i, i)
	}
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return srv, ts
}

func bodyOf(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The streamed body and the materialized body are the same bytes.
//
// Same corpus, same query, only -search.maxRows differs: -1 takes the
// streaming path, a cap above the row count takes the materialized one. Any
// difference in field order, timestamp format or row order would show here,
// which is the assertion that lets the streaming path be the default for
// uncapped selects.
func TestStreamedAndMaterializedBodiesMatch(t *testing.T) {
	// ONE server queried twice with the cap toggled between. Two servers would
	// have ingested at two wall-clock instants, so every _time would differ
	// and the comparison would be of the clock rather than of the two paths.
	const rows = 500
	srv, ts := streamServerWith(t, rows, -1)

	code, sBody := bodyOf(t, ts.URL+"/select/logsql/query?query=*")
	if code != 200 {
		t.Fatalf("streamed: %d\n%s", code, sBody)
	}
	srv.SetMaxRows(rows * 10)
	code, mBody := bodyOf(t, ts.URL+"/select/logsql/query?query=*")
	if code != 200 {
		t.Fatalf("materialized: %d\n%s", code, mBody)
	}
	if n := strings.Count(sBody, "\n"); n != rows {
		t.Fatalf("streamed returned %d lines, want %d", n, rows)
	}
	if sBody != mBody {
		t.Fatalf("the streamed body differs from the materialized one\nstreamed head:\n%.400s\nmaterialized head:\n%.400s",
			sBody, mBody)
	}
}

// An uncapped select of more rows than any materialization budget would allow
// is answered in full. Before this it was the configuration that had no bound
// at all: every matching row in memory before the first one went out.
func TestAnUncappedSelectStreamsMoreRowsThanTheDefaultCap(t *testing.T) {
	// Above config.DefaultLimits' 1000-row test cap and above the 500 the
	// other test uses, so the answer is one no capped configuration would
	// return at all.
	const rows = 5000
	ts := streamServer(t, rows, -1)

	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-ndjson") {
		t.Errorf("Content-Type = %q", ct)
	}
	// Consumed line by line, which is the point: the client never holds the
	// whole answer either.
	seen := 0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		seen++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("reading the stream: %v", err)
	}
	if seen != rows {
		t.Fatalf("%d rows streamed, want %d", seen, rows)
	}
}

// The row cap still refuses. Streaming does not quietly lift -search.maxRows:
// the cap has to be decided before the first byte, so a capped select stays on
// the materialized path and still answers 400 rather than a truncated 200.
func TestTheRowCapStillRefusesWhenStreamingIsAvailable(t *testing.T) {
	ts := streamServer(t, 20, 5)
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*")
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("an over-cap bare select returned %d (%s), want 413", code, body)
	}
	if !strings.Contains(body, "maxRows") {
		t.Errorf("the refusal does not name the flag: %q", body)
	}
}

// A pipe query keeps the materialized path, because a stats or sort pipe is
// defined over the whole result and cannot emit its first row until it has
// seen its last.
func TestPipedQueriesStillAnswerFromTheMaterializedPath(t *testing.T) {
	ts := streamServer(t, 100, -1)
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query="+
		"%2A%20%7C%20stats%20count%28%29%20n") // * | stats count() n
	if code != 200 {
		t.Fatalf("%d: %s", code, body)
	}
	if !strings.Contains(body, `"n"`) {
		t.Fatalf("the stats answer is missing its column: %q", body)
	}
	if lines := strings.Count(strings.TrimSpace(body), "\n") + 1; lines != 1 {
		t.Fatalf("a stats query returned %d lines: %q", lines, body)
	}
}

// `limit=` is the newest-n tail, which walks groups backwards and so is not a
// forward stream. It keeps working and keeps its order.
func TestTheNewestNTailIsUnchanged(t *testing.T) {
	ts := streamServer(t, 200, -1)
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*&limit=3")
	if code != 200 {
		t.Fatalf("%d: %s", code, body)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 {
		t.Fatalf("%d lines, want 3:\n%s", len(lines), body)
	}
	// Newest first: seq 199, 198, 197.
	for i, want := range []int{199, 198, 197} {
		if !strings.Contains(lines[i], `"seq":"`+strconv.Itoa(want)+`"`) {
			t.Fatalf("line %d is not seq %d: %s", i, want, lines[i])
		}
	}
}

// The streaming path is the one that answered.
//
// Without this the other tests here are satisfied by the materialized path
// returning the same rows -- which it does, by construction, since the two
// bodies are asserted identical. The counter is the only thing that separates
// them, which is why it is a metric and not a test hook.
func TestTheStreamingPathIsActuallyTaken(t *testing.T) {
	srv, ts := streamServerWith(t, 50, -1)

	if code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*"); code != 200 {
		t.Fatalf("%d: %s", code, body)
	}
	if got := atomic.LoadInt64(&srv.nStreamedSelects); got != 1 {
		t.Fatalf("streamed selects = %d after one uncapped bare select, want 1", got)
	}

	// A capped select, a piped select and a newest-n tail must NOT take it.
	srv.SetMaxRows(1000)
	bodyOf(t, ts.URL+"/select/logsql/query?query=*")
	srv.SetMaxRows(-1)
	bodyOf(t, ts.URL+"/select/logsql/query?query=%2A%20%7C%20stats%20count%28%29%20n")
	bodyOf(t, ts.URL+"/select/logsql/query?query=*&limit=3")
	if got := atomic.LoadInt64(&srv.nStreamedSelects); got != 1 {
		t.Fatalf("streamed selects = %d; a capped, piped or newest-n query streamed", got)
	}

	code, body := bodyOf(t, ts.URL+"/metrics")
	if code != 200 || !strings.Contains(body, "simdlogs_query_streamed_total 1") {
		t.Errorf("/metrics does not report the streamed count:\n%s", body)
	}
}

// A piped query whose input exceeds -search.maxRows errors instead of
// answering from a truncated input.
//
// This is the defect task 6.4 fixed at the HTTP layer: the cap was SET for
// every non-projecting pipe chain and REPORTED for exactly one of them, so a
// `| sort` over an oversized result returned 200 and a sort of the first N
// rows -- which is not the first N of the sort.
func TestAPipedQueryOverTheRowCapErrorsRatherThanTruncating(t *testing.T) {
	ts := streamServer(t, 200, 20)
	for _, q := range []string{
		"*%20%7C%20sort%20by%20(seq)", // * | sort by (seq)
		"*%20%7C%20offset%205",        // * | offset 5
		"*%20%7C%20delete%20app",      // * | delete app
	} {
		code, body := bodyOf(t, ts.URL+"/select/logsql/query?query="+q)
		if code != http.StatusRequestEntityTooLarge {
			t.Errorf("query %q returned %d, want 413\n%.300s", q, code, body)
		}
	}
	// And one that IS bounded by its own LogsQL limit still answers.
	code, body := bodyOf(t, ts.URL+"/select/logsql/query?query=*%20%7C%20limit%205")
	if code != 200 {
		t.Fatalf("`| limit 5` returned %d: %s", code, body)
	}
	if n := strings.Count(strings.TrimSpace(body), "\n") + 1; n != 5 {
		t.Fatalf("`| limit 5` returned %d rows", n)
	}
}
