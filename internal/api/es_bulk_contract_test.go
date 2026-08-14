package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// The Elasticsearch _bulk action contract.
//
// A _bulk response is one item per ACTION, in order, and clients match items to
// their requests BY POSITION. Everything here follows from that: an action that
// produces no item shifts every later status onto the wrong document, which is
// worse than an error, because it is a wrong answer that looks like a right
// one.
//
// This store is APPEND-ONLY. index and create are supported; update and delete
// are rejected per item, explicitly.

type bulkItem struct {
	Op     string
	Status int
	ErrTy  string
	Index  string
	ID     string
}

// postBulk sends a body and flattens the response items for comparison.
func postBulk(t *testing.T, ts string, body string) (items []bulkItem, errors bool, status int) {
	t.Helper()
	resp, err := http.Post(ts+"/_bulk", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	status = resp.StatusCode
	if status != http.StatusOK {
		return nil, false, status
	}

	var out struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Index  string `json:"_index"`
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response is not a bulk envelope: %v\n%s", err, raw)
	}
	for _, m := range out.Items {
		if len(m) != 1 {
			t.Errorf("an item has %d operation keys, want exactly 1: %v", len(m), m)
		}
		for op, v := range m {
			it := bulkItem{Op: op, Status: v.Status, Index: v.Index, ID: v.ID}
			if v.Error != nil {
				it.ErrTy = v.Error.Type
				if v.Error.Reason == "" {
					t.Errorf("item %q has an error with no reason", op)
				}
			}
			items = append(items, it)
		}
	}
	return items, out.Errors, status
}

func bulkServer(t *testing.T) string {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return ts.URL
}

// testingAllocs is testing.AllocsPerRun with the warmup the allocator needs to
// reach steady state, so a first-call size-class allocation is not counted as
// a per-call one.
func testingAllocs(f func()) float64 {
	f()
	return testing.AllocsPerRun(100, f)
}

func TestBulkActionContract(t *testing.T) {
	const doc = `{"@timestamp":"2023-11-14T22:13:20Z","level":"error"}`

	for _, tc := range []struct {
		name   string
		body   string
		want   []bulkItem
		errors bool
	}{
		{
			name: "index and create are supported",
			body: `{"index":{"_index":"logs","_id":"1"}}` + "\n" + doc + "\n" +
				`{"create":{"_index":"logs"}}` + "\n" + doc + "\n",
			want: []bulkItem{
				{Op: "index", Status: 201, Index: "logs", ID: "1"},
				{Op: "create", Status: 201, Index: "logs"},
			},
		},
		{
			// The source of an update is {"doc":...} or {"script":...} -- an
			// instruction, not a document. It used to be stored as an EMPTY
			// row: the ingester drops object-valued fields, so the wrapper's
			// only field vanished and the row carried nothing but a
			// synthesized timestamp.
			name: "update is rejected, not stored",
			body: `{"update":{"_index":"logs","_id":"7"}}` + "\n" +
				`{"doc":{"level":"info"}}` + "\n",
			want:   []bulkItem{{Op: "update", Status: 400, ErrTy: "illegal_argument_exception", Index: "logs", ID: "7"}},
			errors: true,
		},
		{
			// A delete used to produce NO item at all, so a client got a
			// success for a deletion that never happened -- and every later
			// item was one position out.
			name:   "delete is rejected and still produces an item",
			body:   `{"delete":{"_index":"logs","_id":"9"}}` + "\n",
			want:   []bulkItem{{Op: "delete", Status: 400, ErrTy: "illegal_argument_exception", Index: "logs", ID: "9"}},
			errors: true,
		},
		{
			// The whole point of one-item-per-action: the rejected middle
			// action must not shift the last one's status.
			name: "a rejected action does not shift the others",
			body: `{"index":{"_index":"logs"}}` + "\n" + doc + "\n" +
				`{"delete":{"_index":"logs","_id":"9"}}` + "\n" +
				`{"create":{"_index":"logs"}}` + "\n" + doc + "\n",
			want: []bulkItem{
				{Op: "index", Status: 201, Index: "logs"},
				{Op: "delete", Status: 400, ErrTy: "illegal_argument_exception", Index: "logs", ID: "9"},
				{Op: "create", Status: 201, Index: "logs"},
			},
			errors: true,
		},
		{
			// The action was detected with bytes.Contains(line, `"delete"`),
			// so an index NAMED delete was read as a delete action and the
			// document that followed was then read as an action line --
			// desynchronizing the entire rest of the body.
			name: "an index named delete is an index action",
			body: `{"index":{"_index":"delete"}}` + "\n" + doc + "\n" +
				`{"index":{"_index":"logs"}}` + "\n" + doc + "\n",
			want: []bulkItem{
				{Op: "index", Status: 201, Index: "delete"},
				{Op: "index", Status: 201, Index: "logs"},
			},
		},
		{
			name:   "an action with no source line is rejected",
			body:   `{"index":{"_index":"logs"}}` + "\n",
			want:   []bulkItem{{Op: "index", Status: 400, ErrTy: "illegal_argument_exception", Index: "logs"}},
			errors: true,
		},
		{
			name:   "a source that is not an object is rejected",
			body:   `{"index":{"_index":"logs"}}` + "\n" + `"just a string"` + "\n",
			want:   []bulkItem{{Op: "index", Status: 400, ErrTy: "mapper_parsing_exception", Index: "logs"}},
			errors: true,
		},
		{
			// Metadata is optional in Elasticsearch; neither absent nor null
			// is an error.
			name: "absent and null metadata are both fine",
			body: `{"index":{}}` + "\n" + doc + "\n" +
				`{"create":null}` + "\n" + doc + "\n",
			want: []bulkItem{
				{Op: "index", Status: 201},
				{Op: "create", Status: 201},
			},
		},
		{
			name: "blank lines between actions are skipped",
			body: "\n" + `{"index":{"_index":"logs"}}` + "\n\n" + doc + "\n\n",
			want: []bulkItem{{Op: "index", Status: 201, Index: "logs"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := bulkServer(t)
			items, errors, status := postBulk(t, ts, tc.body)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200: a bulk with per-item failures is still a 200", status)
			}
			if len(items) != len(tc.want) {
				t.Fatalf("%d items, want %d: %+v", len(items), len(tc.want), items)
			}
			for i, w := range tc.want {
				g := items[i]
				if g.Op != w.Op || g.Status != w.Status || g.ErrTy != w.ErrTy {
					t.Errorf("item %d = %+v, want %+v", i, g, w)
				}
				if w.Index != "" && g.Index != w.Index {
					t.Errorf("item %d _index = %q, want %q", i, g.Index, w.Index)
				}
				if w.ID != "" && g.ID != w.ID {
					t.Errorf("item %d _id = %q, want %q", i, g.ID, w.ID)
				}
			}
			if errors != tc.errors {
				t.Errorf("errors = %v, want %v", errors, tc.errors)
			}
		})
	}
}

// The rejected actions must not reach the store. An update wrapper stored as a
// row is the defect; a delete stored as a row would be worse.
func TestBulkRejectedActionsAreNotStored(t *testing.T) {
	ts := bulkServer(t)
	body := `{"index":{"_index":"logs"}}` + "\n" +
		`{"@timestamp":"2023-11-14T22:13:20Z","level":"kept"}` + "\n" +
		`{"update":{"_index":"logs","_id":"1"}}` + "\n" +
		`{"doc":{"level":"from-update"}}` + "\n" +
		`{"delete":{"_index":"logs","_id":"2"}}` + "\n"
	if _, _, status := postBulk(t, ts, body); status != http.StatusOK {
		t.Fatalf("status %d", status)
	}

	resp, err := http.Post(ts+"/_count", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var cnt struct{ Count int }
	json.NewDecoder(resp.Body).Decode(&cnt)
	resp.Body.Close()
	if cnt.Count != 1 {
		t.Errorf("stored %d rows, want 1: only the index action carries a document", cnt.Count)
	}

	// And specifically: no row carries the update wrapper's field.
	resp, err = http.Post(ts+"/_search", "application/json",
		strings.NewReader(`{"query":{"bool":{"filter":[{"term":{"level":"from-update"}}]}}}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(raw), "from-update") {
		t.Errorf("the update wrapper reached the store: %s", raw)
	}
}

// parseBulkAction is the allocation-sensitive half: it runs once per action, so
// a bulk of 200k documents runs it 200k times. The obvious spelling decodes
// into a map[string]json.RawMessage, which allocates one map per call.
func TestParseBulkActionDoesNotAllocate(t *testing.T) {
	line := []byte(`{"index":{"_index":"logs","_id":"abc123"}}`)
	// The two metadata strings are the only allocations the contract permits:
	// _index and _id have to reach the response.
	const want = 2
	got := testingAllocs(func() {
		op, meta, errMsg := parseBulkAction(line)
		if op != "index" || meta.Index != "logs" || meta.ID != "abc123" || errMsg != "" {
			t.Fatalf("parseBulkAction = %q %+v %q", op, meta, errMsg)
		}
	})
	if got > want {
		t.Errorf("parseBulkAction allocates %.0f times per action, want at most %d; "+
			"at 200k documents that is %.0f allocations", got, want, got*200000)
	}
}

// An action line with no metadata fields must allocate NOTHING at all.
func TestParseBulkActionBareIsAllocationFree(t *testing.T) {
	line := []byte(`{"create":{}}`)
	if got := testingAllocs(func() {
		if op, _, e := parseBulkAction(line); op != "create" || e != "" {
			t.Fatalf("parseBulkAction = %q %q", op, e)
		}
	}); got != 0 {
		t.Errorf("a bare action line allocates %.0f times, want 0", got)
	}
}

// An action line that cannot be identified fails the WHOLE request.
//
// Not a per-item error: the body's meaning is the alternation of action and
// source lines, so once a line cannot be identified the parser does not know
// whether the next one is a source or an action, and every item after it would
// be a guess. Elasticsearch answers 400 for the request in exactly these
// cases.
func TestBulkUnparseableActionFailsTheRequest(t *testing.T) {
	const doc = `{"@timestamp":"2023-11-14T22:13:20Z","level":"error"}`
	for _, tc := range []struct{ name, body string }{
		{"not JSON", "not json at all\n" + doc + "\n"},
		{"unknown action", `{"upsert":{"_index":"logs"}}` + "\n" + doc + "\n"},
		{"two keys", `{"index":{},"create":{}}` + "\n" + doc + "\n"},
		{"a scalar where an action belongs", `"index"` + "\n" + doc + "\n"},
		{"metadata is not an object", `{"index":"logs"}` + "\n" + doc + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := bulkServer(t)
			_, _, status := postBulk(t, ts, tc.body)
			if status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: the action stream is not parseable "+
					"past this line, so no item after it can be attributed", status)
			}
		})
	}
}

// And a valid body's documents still reach the store after all of this.
func TestBulkStoresWhatItAccepts(t *testing.T) {
	ts := bulkServer(t)
	body := `{"index":{"_index":"logs"}}` + "\n" +
		`{"@timestamp":"2023-11-14T22:13:20Z","level":"a"}` + "\n" +
		`{"create":{"_index":"logs"}}` + "\n" +
		`{"@timestamp":"2023-11-14T22:13:21Z","level":"b"}` + "\n"
	items, errors, status := postBulk(t, ts, body)
	if status != http.StatusOK || errors || len(items) != 2 {
		t.Fatalf("status %d errors %v items %d", status, errors, len(items))
	}
	resp, err := http.Post(ts+"/_count", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var cnt struct{ Count int }
	json.NewDecoder(resp.Body).Decode(&cnt)
	resp.Body.Close()
	if cnt.Count != 2 {
		t.Errorf("stored %d rows, want 2", cnt.Count)
	}
}

// The reject attribution must name the RIGHT documents.
//
// The first implementation marked the FIRST n doc-carrying items 500, and the
// ingester rejects in body order -- so when the rejected document was not the
// first, the item that SUCCEEDED was reported failed and the item that was
// DROPPED was reported created. Elastic's own client matches items positionally
// and retries anything over 201, so it re-sent the document that had landed (a
// duplicate, in an append-only store) and recorded the one that vanished as
// delivered. Both statuses exactly wrong, under a 200.
func TestBulkRejectAttributionNamesTheRightDocument(t *testing.T) {
	ts := bulkServer(t)
	// The second document passes isJSONObject -- it opens and closes as an
	// object -- and fails the real parse, so the INGESTER rejects it. That is
	// the only way to exercise the attribution path.
	body := `{"index":{"_index":"logs","_id":"GOOD"}}` + "\n" +
		`{"@timestamp":"2023-11-14T22:13:20Z","level":"kept"}` + "\n" +
		`{"index":{"_index":"logs","_id":"BAD"}}` + "\n" +
		`{"@timestamp":"2023-11-14T22:13:21Z","level":}` + "\n"

	items, errors, status := postBulk(t, ts, body)
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if len(items) != 2 {
		t.Fatalf("%d items, want 2: %+v", len(items), items)
	}
	if !errors {
		t.Error("errors = false though a document was rejected")
	}
	// GOOD is stored, so it must NOT be reported as a failure.
	if items[0].Status >= 300 {
		t.Errorf("the STORED document is reported %d %q -- a client will re-send it",
			items[0].Status, items[0].ErrTy)
	}
	// BAD is not stored, so it must NOT be reported as created.
	if items[1].Status == 201 {
		t.Error("the DROPPED document is reported 201 -- a client will record it as delivered")
	}

	// And the store agrees: exactly one row.
	resp, err := http.Post(ts+"/_count", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var cnt struct{ Count int }
	json.NewDecoder(resp.Body).Decode(&cnt)
	resp.Body.Close()
	if cnt.Count != 1 {
		t.Errorf("stored %d rows, want 1", cnt.Count)
	}
}

// A body the client fills with newlines must not steer the server's
// allocation. The presize was bytes.Count(body,'\n')/2 ops of 112 bytes each --
// 56 bytes reserved per body byte, so 64 MiB of newlines asked for 3.5 GiB,
// and the action cap did not bound it because the reservation ran first.
func TestBulkNewlineBodyDoesNotAmplifyAllocation(t *testing.T) {
	const n = 1 << 20 // 1 MiB of newlines
	body := make([]byte, n)
	for i := range body {
		body[i] = '\n'
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ops, perr := parseBulk(body)
	runtime.ReadMemStats(&after)

	if perr != "" {
		t.Fatalf("a body of newlines should parse to zero actions, got %q", perr)
	}
	if len(ops) != 0 {
		t.Fatalf("%d actions from a body of newlines", len(ops))
	}
	grew := after.TotalAlloc - before.TotalAlloc
	// A FIXED reservation, not one the client chooses. bulkPresize is ~458 KB
	// and is paid once per request regardless of body size; 4 MiB is generous
	// against it.
	if grew > 4<<20 {
		t.Errorf("parsing %d bytes of newlines allocated %d bytes (%.1fx the body); "+
			"the reservation is steered by the client", n, grew, float64(grew)/float64(n))
	}
	t.Logf("%d-byte newline body -> %d actions, %d bytes allocated (%.2fx)",
		n, len(ops), grew, float64(grew)/float64(n))
}

// Actions past the cap must FAIL the request, not vanish under a 200.
func TestBulkOverActionCapFails(t *testing.T) {
	var sb strings.Builder
	// One more action than the cap allows. Each is minimal so the body stays
	// inside the size limits.
	for i := 0; i <= esBulkMaxActions; i++ {
		sb.WriteString("{\"delete\":{}}\n") // no source line: one line per action
	}
	ops, perr := parseBulk([]byte(sb.String()))
	if perr == "" {
		t.Errorf("%d actions parsed with no error; the ones past the cap vanished silently", len(ops))
	}
	if !strings.Contains(perr, "actions") {
		t.Errorf("error %q does not say what was exceeded", perr)
	}
}
