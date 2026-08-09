package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestClusterWriteRouting checks that ingest through a router spreads across
// storage nodes round-robin and a federated read then returns every record.
func TestClusterWriteRouting(t *testing.T) {
	mk := func() (*Server, *httptest.Server) {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return srv, httptest.NewServer(srv.Handler())
	}
	s1, b1 := mk()
	defer b1.Close()
	s2, b2 := mk()
	defer b2.Close()

	front, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	front.SetBackends([]string{b1.URL, b2.URL})
	fs := httptest.NewServer(front.Handler())
	defer fs.Close()

	for i := 0; i < 4; i++ {
		postBody(t, fs, fmt.Sprintf(`{"_time":%d,"service":"s","_msg":"m%d"}`+"\n", 1000+i, i))
	}
	if a, b := s1.def.store.TotalRows(), s2.def.store.TotalRows(); a != 2 || b != 2 {
		t.Fatalf("write spread = %d/%d, want 2/2 across backends", a, b)
	}
	r, err := http.Get(fs.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if n := len(strings.Split(strings.TrimSpace(string(body)), "\n")); n != 4 {
		t.Fatalf("federated read got %d rows, want 4:\n%s", n, body)
	}
}

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
