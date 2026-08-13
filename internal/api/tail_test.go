package api

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestTailLive proves the live tail streams only groups that arrive after the
// client subscribes, filtered by the query: a pre-existing record is never
// seen, a later matching one is, a later non-matching one is not.
func TestTailLive(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	postBody(t, ts, `{"_time":1,"service":"old","_msg":"before"}`+"\n") // pre-subscribe: must not tail

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/select/logsql/tail?query=service:=auth", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// After the tail has subscribed, ingest a non-matching then a matching row.
	go func() {
		time.Sleep(300 * time.Millisecond)
		postBody(t, ts, `{"_time":2,"service":"nope","_msg":"skip"}`+"\n")
		postBody(t, ts, `{"_time":3,"service":"auth","_msg":"hit"}`+"\n")
	}()

	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read tail line: %v", err)
	}
	if !strings.Contains(line, `"service":"auth"`) || strings.Contains(line, `"service":"old"`) {
		t.Fatalf("tail line = %s (want the auth record, not old)", line)
	}
}

func postBody(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	r, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
}

// TestTailReplaysRecentWindow covers what the reference does when a client
// opens a live tail: the last few seconds are replayed immediately, so the pane
// is not blank until the next record happens to arrive. start_offset names the
// window; records older than it are not replayed.
func TestTailReplaysRecentWindow(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	now := time.Now()
	postBody(t, ts, `{"_time":`+itoa64(now.Add(-time.Second).UnixNano())+`,"service":"auth","_msg":"recent"}`+"\n")
	postBody(t, ts, `{"_time":`+itoa64(now.Add(-time.Hour).UnixNano())+`,"service":"auth","_msg":"ancient"}`+"\n")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/select/logsql/tail?query=service:=auth", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	seen := map[string]bool{}
	sc := bufio.NewScanner(resp.Body)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sc.Scan() {
			line := sc.Text()
			if strings.Contains(line, "recent") {
				seen["recent"] = true
			}
			if strings.Contains(line, "ancient") {
				seen["ancient"] = true
			}
			if seen["recent"] {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	cancel()

	if !seen["recent"] {
		t.Error("tail did not replay the recent window: a live tail opened on a busy stream showed nothing")
	}
	if seen["ancient"] {
		t.Error("tail replayed a record from an hour ago; the window is start_offset, not everything")
	}
}

func itoa64(n int64) string {
	return strconv.FormatInt(n, 10)
}
