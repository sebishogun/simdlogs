package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestClusterWriteRouting checks that distinct write ids spread across storage
// shards and a federated read then returns every record.
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

	ids := writeIDsForShards(2, 2)
	for i := 0; i < 4; i++ {
		shard := i % 2
		postBodyWithID(t, fs, ids[shard][i/2],
			fmt.Sprintf(`{"_time":%d,"service":"s","_msg":"m%d"}`+"\n", 1000+i, i))
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
	// Federated group-by count sums across both backends.
	if m := statsBy(t, fs, "service"); m["s"] != 4 {
		t.Fatalf("federated stats by service = %v, want s:4", m)
	}
}

// TestClusterReplication checks replicas=2: each record lands on both replicas
// of its shard, a federated read returns each record once (not per replica),
// and a read survives a downed primary.
func TestClusterReplication(t *testing.T) {
	mk := func() (*Server, *httptest.Server) {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return srv, httptest.NewServer(srv.Handler())
	}
	s1, b1 := mk()
	s2, b2 := mk()
	s3, b3 := mk()
	s4, b4 := mk()
	defer b2.Close()
	defer b3.Close()
	defer b4.Close()

	front, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	front.SetBackends([]string{b1.URL, b2.URL, b3.URL, b4.URL})
	front.SetReplicas(2) // shards: [b1,b2], [b3,b4]
	fs := httptest.NewServer(front.Handler())
	defer fs.Close()

	ids := writeIDsForShards(2, 1)
	postBodyWithID(t, fs, ids[0][0], `{"_time":1,"service":"a"}`+"\n") // -> shard0 (b1,b2)
	postBodyWithID(t, fs, ids[1][0], `{"_time":2,"service":"b"}`+"\n") // -> shard1 (b3,b4)

	if s1.def.store.TotalRows() != 1 || s2.def.store.TotalRows() != 1 {
		t.Fatalf("shard0 replicas = %d/%d, want 1/1 (replicated)", s1.def.store.TotalRows(), s2.def.store.TotalRows())
	}
	if s3.def.store.TotalRows() != 1 || s4.def.store.TotalRows() != 1 {
		t.Fatalf("shard1 replicas = %d/%d, want 1/1", s3.def.store.TotalRows(), s4.def.store.TotalRows())
	}

	count := func() int {
		r, err := http.Get(fs.URL + "/select/logsql/query?query=*")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		return len(strings.Split(strings.TrimSpace(string(b)), "\n"))
	}
	if n := count(); n != 2 { // one per shard, not 4
		t.Fatalf("federated read = %d rows, want 2 (dedup across replicas)", n)
	}
	b1.Close() // down the shard0 primary
	if n := count(); n != 2 {
		t.Fatalf("after primary loss = %d rows, want 2 (failover to replica)", n)
	}
}

func writeIDsForShards(shards, perShard int) [][]string {
	ids := make([][]string, shards)
	left := shards * perShard
	for n := uint64(0); left > 0; n++ {
		id := fmt.Sprintf("%016x", n)
		shard := writeShardIndex(id, shards)
		if len(ids[shard]) == perShard {
			continue
		}
		ids[shard] = append(ids[shard], id)
		left--
	}
	return ids
}

func postBodyWithID(t *testing.T, ts *httptest.Server, id, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set(HdrWriteID, id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("write id %s returned %d", id, resp.StatusCode)
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
	// OLDEST first, because this select carries no `limit`.
	//
	// This asserted newest-first, which is what the merge used to do
	// unconditionally -- and it is what a single node does only when `limit` is
	// set. `*` on one node answers oldest-first (scan order); `*&limit=N`
	// answers the newest N, newest-first. An unlimited cluster select was
	// coming back reversed relative to the server it is a cluster of, and this
	// test was pinning the reversal.
	if !strings.Contains(lines[0], "one") || !strings.Contains(lines[1], "two") {
		t.Fatalf("merge order wrong -- an unlimited select is oldest-first, as a "+
			"single node returns it:\n%s", b)
	}
}
