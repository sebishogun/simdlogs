package api

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
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
