package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/ingest"
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

// THE RENDERED POSITION LIST IS BOUNDED, AND THAT BOUND IS THE WHOLE REASON
// RAISING THE ATTRIBUTION BOUND 16x DID NOT GROW AN ERROR BODY.
//
// `ingest.MaxRejectedAt` went from 1<<16 to the /_bulk action cap (1<<20), and
// the justification given three times in prose -- in result.go, in esbulk.go
// and in docs/wrong.md entry 133 -- is "what one route RENDERS is bounded
// separately (maxRenderedRejectedAt, still 64Ki), so no error body grew".
// Nothing ran it. Three mutations, each applied to the tree, GREEN at 32 CPUs
// AND under `taskset -c 0-3`, all three observable on the wire:
//
//	at, trunc = at[:maxRenderedRejectedAt], true  ->  at = at[:...]
//	    65,536 of 131,073 positions rendered and `rejectedTruncated` ABSENT:
//	    a client is handed a SHORT list that says it is complete, and the
//	    field whose entire meaning is "the list you have is shorter than the
//	    count" is the one that vanishes.
//	the clamp deleted
//	    an 806,529-byte error body at 131,073 rejects, and 7,277,653 bytes at
//	    the 1<<20 attribution cap -- which is the amplification the old bound
//	    existed to prevent, moved rather than removed.
//	maxRenderedRejectedAt 1<<16 -> 1<<20
//	    the same body.
//
// Dropping the `rejectedAt` field entirely IS red (the test above reads it),
// so the field was gated and its bound was not.
//
// The counting boundary is the decoded JSON of one /insert/journald error
// body: `rejected` is the count, `len(rejectedAt)` the rendered positions.
func TestTheRenderedRejectedPositionsAreBounded(t *testing.T) {
	// journaldRejects builds a body of `n` entries the parser counts and
	// refuses -- an entry carrying only __REALTIME_TIMESTAMP has no storable
	// field -- preceded by one entry that IS stored, so res.Accepted > 0 and
	// writeIngestErr renders the counts at all, and closed by a field cut
	// mid-name so the request is a 400 rather than a 202.
	journaldRejects := func(n int) string {
		var sb strings.Builder
		sb.Grow(n*24 + 64)
		sb.WriteString("MESSAGE=stored\n\n")
		for i := 0; i < n; i++ {
			sb.WriteString("__REALTIME_TIMESTAMP=1\n\n")
		}
		sb.WriteString("TRUNCATED")
		return sb.String()
	}
	post := func(t *testing.T, body string) (int, int, []byte) {
		t.Helper()
		node := realShard(t, nil)
		resp, err := http.Post(node.URL+"/insert/journald",
			"application/vnd.fdo.journal", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, len(raw), raw
	}
	type errBody struct {
		Accepted   int     `json:"accepted"`
		Rejected   int     `json:"rejected"`
		RejectedAt []int32 `json:"rejectedAt"`
		Truncated  bool    `json:"rejectedTruncated"`
	}
	decode := func(t *testing.T, raw []byte) errBody {
		t.Helper()
		var got errBody
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("the error body does not parse: %v: %.200s", err, raw)
		}
		return got
	}

	// The bound is pinned as a LITERAL, not read out of the code, because it
	// is wire-visible: it decides how many positions a client is handed and
	// how big the body it has to read is.
	//
	// A LITERAL THAT RESTATES A CONSTANT IS USUALLY A GATE THAT MEASURES
	// NOTHING, and a raise is already RED twice over below -- the rendered
	// length and `rejectedTruncated` are both asserted on the wire. What only
	// this line can refuse is a TIGHTENING, which is correct code and is still
	// a wire change: at 1<<15 a client that receives 65,536 positions today
	// receives 32,768 and a `rejectedTruncated` it did not get before. So the
	// message has to speak to both directions, or a reader who tightened is
	// answered with an argument about raising. Measured with the clamp deleted
	// at 32 CPUs and under `taskset -c 0-3`: the /insert/journald error body
	// at the attribution bound is 7,277,653 bytes.
	if maxRenderedRejectedAt != 1<<16 {
		t.Fatalf("maxRenderedRejectedAt is %d, not 1<<16, and this number is on the "+
			"wire. RAISED, it grows the error body: unclamped at the attribution bound "+
			"(%d) the /insert/journald body is 7,277,653 bytes, the amplification "+
			"raising that bound was said not to cause. LOWERED, it shortens a list "+
			"clients already receive whole and adds rejectedTruncated to responses "+
			"that did not carry it. Either way it is a compatibility change and "+
			"docs/compatibility.md has to move with it.",
			maxRenderedRejectedAt, ingest.MaxRejectedAt)
	}

	t.Run("past the bound the list is cut and says so", func(t *testing.T) {
		const entries = 1 << 16 // 65,536 refused entries...
		code, size, raw := post(t, journaldRejects(entries))
		if code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400: %.200s", code, raw)
		}
		got := decode(t, raw)
		if got.Accepted != 1 {
			t.Fatalf("accepted %d, want 1: with nothing stored writeIngestErr "+
				"returns the plain error shape and this gate measures nothing", got.Accepted)
		}
		// ...plus the truncated field itself, so the recorded list is one
		// longer than the render bound. That is the smallest input that
		// separates the bound from the count.
		if got.Rejected != entries+1 {
			t.Fatalf("rejected %d, want %d", got.Rejected, entries+1)
		}
		if len(got.RejectedAt) != maxRenderedRejectedAt {
			t.Fatalf("%d positions rendered, want exactly maxRenderedRejectedAt = %d. "+
				"ingest.MaxRejectedAt is %d and RECORDED %d of them: an unbounded render "+
				"puts every one of them on the wire, which is the amplification raising "+
				"the attribution bound was said not to cause.",
				len(got.RejectedAt), maxRenderedRejectedAt, ingest.MaxRejectedAt, got.Rejected)
		}
		if !got.Truncated {
			t.Fatalf("rejectedTruncated is absent with %d of %d positions rendered. "+
				"The field means exactly \"the list you have is shorter than the count\"; "+
				"without it the client reads %d complete rejections and treats the other "+
				"%d records as stored.",
				len(got.RejectedAt), got.Rejected, len(got.RejectedAt), got.Rejected-len(got.RejectedAt))
		}
		// The positions are ascending and start at 1: entry 0 is the stored
		// one. A rendered list that is right in length and wrong in content
		// would otherwise pass every line above.
		if got.RejectedAt[0] != 1 || got.RejectedAt[len(got.RejectedAt)-1] != entries {
			t.Fatalf("rendered positions run %d..%d, want 1..%d",
				got.RejectedAt[0], got.RejectedAt[len(got.RejectedAt)-1], entries)
		}
		// And the body is the size that bound buys. Unclamped this is
		// 806,529 bytes at twice this reject count and 7,277,653 at the
		// attribution cap.
		if size > 1<<20 {
			t.Errorf("the error body is %d bytes for %d rejections", size, got.Rejected)
		}
	})

	// THE CONTROL, so a build that always reported truncation and always cut
	// the list would not pass the subtest above. Three rejections is under the
	// bound: all three are rendered and rejectedTruncated must be ABSENT.
	t.Run("under the bound the list is whole and says nothing", func(t *testing.T) {
		code, _, raw := post(t, journaldRejects(3))
		if code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400: %.200s", code, raw)
		}
		got := decode(t, raw)
		if got.Rejected != 4 || len(got.RejectedAt) != 4 {
			t.Fatalf("rejected %d at %v, want 4 positions rendered whole", got.Rejected, got.RejectedAt)
		}
		if got.Truncated {
			t.Errorf("rejectedTruncated on a list that is complete: %s", raw)
		}
		var m map[string]any
		json.Unmarshal(raw, &m)
		if _, ok := m["rejectedTruncated"]; ok {
			t.Errorf("rejectedTruncated is present at all on a complete list: %s", raw)
		}
	})

	// AND THE BOUND HOLDS AT THE ATTRIBUTION CAP, priced directly rather than
	// by sending a 25 MB upload. ingest.MaxRejectedAt is the /_bulk action cap
	// now; a Result carrying that many positions is what the raise made
	// possible, and this is the body it produces.
	t.Run("at the attribution cap", func(t *testing.T) {
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		res := ingest.Result{Accepted: 1, Rejected: ingest.MaxRejectedAt}
		res.RejectedAt = make([]int32, ingest.MaxRejectedAt)
		for i := range res.RejectedAt {
			res.RejectedAt[i] = int32(i)
		}
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/insert/journald", nil)
		srv.writeIngestErr(w, r, ndjsonSpec(), http.StatusBadRequest, "truncated", res)

		got := decode(t, w.Body.Bytes())
		if len(got.RejectedAt) != maxRenderedRejectedAt || !got.Truncated {
			t.Fatalf("%d positions rendered, truncated=%v, want %d and true",
				len(got.RejectedAt), got.Truncated, maxRenderedRejectedAt)
		}
		// Measured with the clamp deleted, on THIS call: 7,277,580 bytes.
		// The bound is what keeps a 4 MB int32 array off the wire.
		//
		// 7,277,653 is the figure for the ROUTE (the two comments above), and
		// it was attached here as well -- 73 bytes off, because this fixture
		// is not that request. The 73 decompose exactly, and the first version
		// of this list accounted for 67 of them:
		//
		//	+42  the message: the route's 51-character "ingest: envelope
		//	     error: journal export is truncated" against this one's
		//	     "truncated"
		//	+25  `,"rejectedTruncated":true`, which the route's body carries
		//	     and this one's does not
		//	 +6  THE POSITIONS ARE 1-BASED ON THE ROUTE. Entry 0 is the stored
		//	     record, so the route renders 1..1,048,576 where this fixture
		//	     renders 0..1,048,575: the two sets differ by dropping "0" and
		//	     adding "1048576", one digit for seven.
		//	 +0  `rejected` is 1,048,577 against 1,048,576 -- same digit count,
		//	     which is why it was listed and contributes nothing.
		//
		// Both re-measured with the clamp deleted at 32 CPUs and under
		// `taskset -c 0-3`.
		if n := w.Body.Len(); n > 1<<20 {
			t.Errorf("the error body is %d bytes for %d recorded positions; the "+
				"render bound is not holding", n, ingest.MaxRejectedAt)
		}
	})

	// PRE-EXISTING, RECORDED HERE BECAUSE IT IS THE POSITION BESIDE THIS FIX:
	// writeIngestErr returns the PLAIN error shape when res.Accepted == 0, so
	// a wholly-rejected upload reports neither `rejected` nor a single
	// position -- the client is told "400" and nothing else about a batch in
	// which every record was refused for a nameable reason.
	// TestAWhollyRejectedIngestKeepsThePlainErrorShape pins that shape; this
	// says what it costs. Not changed here: the shape is documented in
	// docs/compatibility.md and changing it is a wire change.
	t.Run("nothing stored still says nothing about what was refused", func(t *testing.T) {
		code, _, raw := post(t, "__REALTIME_TIMESTAMP=1\n\nTRUNCATED")
		if code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400: %.200s", code, raw)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("not JSON: %v: %.200s", err, raw)
		}
		if _, ok := m["rejected"]; ok {
			t.Fatalf("the wholly-rejected shape now carries counts; "+
				"docs/compatibility.md and the entry that recorded this are stale: %s", raw)
		}
	})
}
