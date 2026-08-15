package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// A parse that fails part way must report what it stored, and store it now.
//
// Journald keeps the entries that came before a truncation rather than
// discarding data a client really sent -- a deliberate decision with its own
// tests. Two things follow that used to happen nowhere:
//
//   - Those rows sit in the tenant's SHARED writer buffer. The failure path
//     returned without flushing, which did not stop them being stored: it moved
//     the moment to whenever the next unrelated request flushed. A client saw
//     400 and its rows appeared later, under someone else's write.
//   - The client was told only "400". Re-sending the upload then duplicates
//     every record that landed, and a log store cannot tell a duplicate from a
//     line that happened twice.
func TestAPartiallyAcceptedIngestReportsAndStoresWhatItKept(t *testing.T) {
	node := realShard(t, nil)

	// Two complete entries, then a binary field whose length prefix is cut.
	body := "MESSAGE=one\n\nMESSAGE=two\n\nMESSAGE\n\xff\xff"
	resp, err := http.Post(node.URL+"/insert/journald",
		"application/vnd.fdo.journal", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %.200s", resp.StatusCode, raw)
	}
	var got struct {
		Error      string  `json:"error"`
		Accepted   int     `json:"accepted"`
		Rejected   int     `json:"rejected"`
		RejectedAt []int32 `json:"rejectedAt"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("the error body does not parse: %v: %.200s", err, raw)
	}
	if got.Accepted != 2 {
		t.Fatalf("the response reports %d accepted; a client that re-sends the "+
			"upload duplicates whatever landed: %s", got.Accepted, raw)
	}
	if got.Rejected != 1 || len(got.RejectedAt) != 1 {
		t.Errorf("rejected %d at %v, want 1 rejection with its position: %s",
			got.Rejected, got.RejectedAt, raw)
	}
	if !strings.Contains(got.Error, "truncated") {
		t.Errorf("the error does not say what went wrong: %s", raw)
	}

	// Durable NOW, under this request -- not on whatever writes next.
	code, rows, body2 := queryRows(t, node, "*")
	if code != 200 {
		t.Fatalf("read back: %d %.200s", code, body2)
	}
	if len(rows) != got.Accepted {
		t.Fatalf("%d rows in the store, %d reported accepted: the rows this "+
			"request kept were left for a later request to flush",
			len(rows), got.Accepted)
	}
}

// An ingest that stored nothing keeps the plain error shape.
func TestAWhollyRejectedIngestKeepsThePlainErrorShape(t *testing.T) {
	node := realShard(t, nil)
	resp, err := http.Post(node.URL+"/insert/journald",
		"application/vnd.fdo.journal", strings.NewReader("MESSAGE\n\xff\xff"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %.200s", resp.StatusCode, raw)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("the error body does not parse: %v", err)
	}
	if _, ok := got["accepted"]; ok {
		t.Errorf("an ingest that stored nothing reports an accepted count: %s", raw)
	}
	if _, code := got["error"], got["status"]; got["error"] == nil || code == nil {
		t.Errorf("the ordinary error shape changed: %s", raw)
	}
	if _, rows, _ := queryRows(t, node, "*"); len(rows) != 0 {
		t.Fatalf("%d rows stored by an upload that was wholly rejected", len(rows))
	}
}
