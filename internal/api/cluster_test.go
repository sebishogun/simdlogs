package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestClusterFederation stands up two storage nodes with different data and a
// front router pointed at both, then queries the front and checks the rows are
// merged newest-first across both backends.
func TestClusterFederation(t *testing.T) {
	mk := func(body string) *httptest.Server {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(srv.Handler())
		postBody(t, ts, body)
		return ts
	}
	b1 := mk(`{"_time":1000,"service":"a","_msg":"one"}` + "\n")
	defer b1.Close()
	b2 := mk(`{"_time":2000,"service":"b","_msg":"two"}` + "\n")
	defer b2.Close()

	front, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	front.SetBackends([]string{b1.URL, b2.URL})
	fs := httptest.NewServer(front.Handler())
	defer fs.Close()

	r, err := http.Get(fs.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 {
		t.Fatalf("federated select got %d rows, want 2 (one per backend):\n%s", len(lines), b)
	}
	// _time 2000 (two) is newer than 1000 (one), so it merges first.
	if !strings.Contains(lines[0], "two") || !strings.Contains(lines[1], "one") {
		t.Fatalf("merge order wrong:\n%s", b)
	}
}
