package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/ingest"
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

func bulkServer(t *testing.T) string { return bulkServerCfg(t, nil) }

// bulkServerCfg is bulkServer with a hook that configures the server before it
// serves. It exists for the shard-count override: without one the concurrent
// ingest branch needs runtime.NumCPU()/3 >= 2 and is unreachable on any host
// with fewer than six cores, so a test that means to exercise it runs the
// serial fallback and passes either way.
func bulkServerCfg(t *testing.T, cfg func(*Server)) string {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		cfg(srv)
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

// A CLIENT-SIDE REJECTION IS A 4xx ITEM, NOT A 5xx.
//
// Every rejection that reaches markBulkRejects is the ingester's, and every
// storage failure has already returned before it is called -- so a 500 named a
// server fault for a document no server could have stored. Beats, Logstash and
// Fluentd all retry a 5xx bulk item indefinitely and give up permanently on a
// 4xx, so a document whose `_time` says 9999 became a pipeline that never
// drains: the retry can never succeed and the batch behind it never moves.
//
// Both shapes are here because they are the same class through two different
// paths -- one the ingester cannot read at all, one it reads and cannot file.
func TestABulkRejectionIsAClientErrorNotAServerError(t *testing.T) {
	for _, tc := range []struct {
		name, bad string
	}{
		{"a document that does not parse", `{"@timestamp":"2023-11-14T22:13:21Z","level":}`},
		{"a `_time` outside the storable range", `{"_time":"9999-01-01T00:00:00Z","level":"far"}`},
		{"a `_time` past int64 as all digits", `{"_time":"253402300800000000000","level":"far"}`},
		{"a `_time` past int64 as a JSON number", `{"_time":253402300800000000000,"level":"far"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := bulkServer(t)
			body := `{"index":{"_index":"logs","_id":"GOOD"}}` + "\n" +
				`{"@timestamp":"2023-11-14T22:13:20Z","level":"kept"}` + "\n" +
				`{"index":{"_index":"logs","_id":"BAD"}}` + "\n" +
				tc.bad + "\n"

			items, errs, status := postBulk(t, ts, body)
			if status != http.StatusOK {
				t.Fatalf("the bulk request answered %d, want 200 with per-item statuses", status)
			}
			if !errs {
				t.Fatalf("errors = false though a document was rejected: %+v", items)
			}
			if len(items) != 2 {
				t.Fatalf("%d items, want 2: %+v", len(items), items)
			}
			if items[0].Status >= 300 {
				t.Errorf("the STORED document is reported %d %q", items[0].Status, items[0].ErrTy)
			}
			if items[1].Status/100 != 4 {
				t.Errorf("the rejected document is reported %d %q, want a 4xx.\n"+
					"A 5xx tells Beats/Logstash/Fluentd to retry forever, and this "+
					"document can never be stored however many times it is sent.",
					items[1].Status, items[1].ErrTy)
			}
			if items[1].ErrTy == "server_error" {
				t.Errorf("the rejected document's error type is %q: the fault is "+
					"the document's, not the server's", items[1].ErrTy)
			}
		})
	}
}

// A BULK BIG ENOUGH TO SHARD MUST NOT REPORT THE DOCUMENTS IT STORED AS FAILED.
//
// `esBulk` takes the parallel ingest path once the document lines reach
// `ingest.MinParallelBytes` (1 MiB), and that path returned
// `(ingested, skipped, error)` with the per-record positions thrown away. The
// handler then declared the positions UNKNOWN, and `markBulkRejects` marks
// EVERY candidate item when it cannot place the rejections -- which after
// round 18 meant every item in the batch at 400 `document_parsing_exception`.
//
// Measured on this tree before the fix, a 6 MiB body of 20,871 `index` actions
// with ONE unstorable `_time` in it:
//
//	items at 2xx                  0
//	items at 400                  20871
//	rows in the store afterwards  20870
//
// 400 is PERMANENT to every shipper -- Beats, Logstash and Fluentd all give up
// on a 4xx and never re-send it -- so 20,870 documents that are on disk are
// recorded by the client as lost, and nothing in the response says otherwise.
// Round 18 changed the status from 500 to 400 for the right reason and left
// this branch, whose justification four lines above it still read
// "over-reporting causes duplicates, which a caller can reconcile". That was
// true at 500. At 400 over-reporting produces no duplicates at all: it
// produces exactly the loss the same sentence says a caller cannot recover.
//
// The shard ordinals ARE recoverable. `splitLines` cuts on line boundaries and
// the chunks are contiguous and in body order, so shard k's first record is at
// the sum of the earlier shards' record counts -- a number each shard already
// returns (Accepted+Rejected is every non-blank line it saw).
func TestALargeBulkReportsOnlyTheDocumentItRejected(t *testing.T) {
	const (
		n   = 12000 // enough doc bytes to cross MinParallelBytes
		bad = 7777  // not the first and not the last: an off-by-one is visible
	)
	pad := strings.Repeat("x", 64)
	var sb strings.Builder
	sb.Grow(n * 160)
	for i := 0; i < n; i++ {
		sb.WriteString(`{"index":{"_index":"logs"}}` + "\n")
		if i == bad {
			// PARSES and cannot be STORED: outside the int64-nanosecond
			// domain, which is the ingester's own refusal and not a syntax
			// error, so it exercises the attribution path rather than the
			// action parser.
			sb.WriteString(`{"_time":"9999-01-01T00:00:00Z","level":"far","pad":"` + pad + `"}` + "\n")
			continue
		}
		sb.WriteString(`{"_time":"2026-06-01T12:00:00Z","level":"info","pad":"` + pad + `"}` + "\n")
	}
	body := sb.String()

	// BOTH SHARD COUNTS, because the derived one is not a choice this test
	// gets to make. `runtime.NumCPU()/3` is below the 2-shard minimum on any
	// host with fewer than six cores, so on a stock CI runner -- or under
	// `taskset -c 0-3` -- the first row runs the SERIAL path and asserts
	// nothing about the rebase. Measured: `base += 0` in mergeShardResults is
	// RED at 32 CPUs and GREEN at 4 without the second row.
	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the derived shard count", 0},
		{"shards forced to 4", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The forced row must actually shard, and the byte count alone
			// cannot say so: ParallelConfig needs Shards >= 2 as well, and the
			// bulk branch is keyed on the DOCUMENT bytes, not the body's.
			cfg := ingest.ParallelConfig{Shards: tc.shards}
			if tc.shards != 0 && cfg.ShardsFor(len(body)-n*28) < 2 {
				t.Fatalf("shards forced to %d resolves to %d: this row runs the "+
					"serial fallback", tc.shards, cfg.ShardsFor(len(body)-n*28))
			}
			ts := bulkServerCfg(t, func(s *Server) { s.setIngestShardsForTest(tc.shards) })
			items, errs, status := postBulk(t, ts, body)
			if status != http.StatusOK {
				t.Fatalf("status %d", status)
			}
			if len(items) != n {
				t.Fatalf("%d items, want %d", len(items), n)
			}
			if !errs {
				t.Fatal("errors = false though a document was rejected")
			}
			failed := make([]int, 0, 8)
			for i, it := range items {
				if it.Status >= 300 {
					failed = append(failed, i)
				}
			}
			if len(failed) != 1 || failed[0] != bad {
				shown := failed
				if len(shown) > 8 {
					shown = shown[:8]
				}
				t.Fatalf("%d of %d items report a failure (first few at %v), want exactly one at %d.\n"+
					"Every item but one names a document that is ON DISK. A 4xx is permanent "+
					"to Beats, Logstash and Fluentd: they will not re-send, so the client's "+
					"record of a stored document becomes `failed` forever.",
					len(failed), len(items), shown, bad)
			}
			if items[bad].Status/100 != 4 {
				t.Errorf("the rejected document is reported %d %q, want a 4xx",
					items[bad].Status, items[bad].ErrTy)
			}

			// THE STORE IS THE OTHER HALF OF THE ASSERTION. Without it a build
			// that stored nothing and reported one failure would pass every
			// line above.
			resp, err := http.Post(ts+"/_count", "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			var cnt struct{ Count int }
			json.NewDecoder(resp.Body).Decode(&cnt)
			resp.Body.Close()
			if cnt.Count != n-1 {
				t.Errorf("the store holds %d rows, want %d", cnt.Count, n-1)
			}
		})
	}
}

// THE ATTRIBUTION BOUND MUST COVER THE ACTION CAP, or a body a client can
// send outruns the positions and the per-item statuses stop being computable.
//
// `ingest.MaxRejectedAt` was 65,536 against an action cap of 1<<20, and the
// gap was not theoretical: a 5,254,000-byte body -- inside the 64 MiB request
// limit, an ordinary shipper batch -- with 70,000 `index` actions of which
// 66,000 carried an unstorable `_time` crossed it and every one of the 70,000
// items came back 429 `es_rejected_execution_exception` over 4,000 stored rows.
// The two constants live in two packages, so nothing but this holds them
// together.
//
// TWO-SIDED, BECAUSE A RAISE COSTS MEMORY AND NOTHING ELSE MEASURES IT.
// This read `MaxRejectedAt < esBulkMaxActions` while entry 134 described the
// invariant as "an exact fit: MaxRejectedAt == esBulkMaxActions == 1<<20".
// `ingest.MaxRejectedAt = 1<<24` compiles and was GREEN at 32 CPUs and under
// `taskset -c 0-3` -- the prose said fit and the gate said floor.
//
// `>=` is the safety-relevant direction: below the action cap a `_bulk` batch
// stops being attributable per item (see the doc on MaxRejectedAt). But
// `/insert/journald` has no action cap at all -- its positions are bounded
// only by the body limit -- so the bound is what stops one upload from
// recording a position per entry. Measured, a 67,108,864-byte journald body
// (the default MaxBodyBytes) of 24-byte rejecting entries:
//
//	MaxRejectedAt   entries    recorded    int32     rejectedTruncated
//	1<<20           2,796,202  1,048,576   4.00 MiB  true
//	1<<24           2,796,202  2,796,202  10.67 MiB  false
//
// 2.67x the live list on a route the action cap does not reach, which is the
// half of the trade the one-sided form could not see.
func TestTheAttributionBoundCoversTheActionCap(t *testing.T) {
	if ingest.MaxRejectedAt < esBulkMaxActions {
		t.Fatalf("ingest.MaxRejectedAt is %d and a _bulk may carry %d actions: a batch "+
			"with more than %d rejected documents cannot be attributed, and the items "+
			"array is then a status per document that no server can stand behind",
			ingest.MaxRejectedAt, esBulkMaxActions, ingest.MaxRejectedAt)
	}
	if ingest.MaxRejectedAt > esBulkMaxActions {
		// BOTH WAYS ROUND, because this arm fires on two different edits and
		// the message used to answer only one of them. It trips when
		// MaxRejectedAt is RAISED and equally when esBulkMaxActions is
		// LOWERED, and a reader who just lowered the action cap was told to
		// "raise the action cap first" -- an argument about the change they
		// did not make. Lowering the cap is the correct fix for a different
		// problem (a _bulk body that is too large to answer per item), and the
		// answer to it is to lower MaxRejectedAt with it, not to put the cap
		// back.
		t.Fatalf("ingest.MaxRejectedAt is %d against an action cap of %d. Nothing "+
			"/_bulk accepts needs the extra %d positions, and /insert/journald -- which "+
			"has no action cap -- then records one position per rejecting entry up to "+
			"the body limit: at MaxBodyBytes a 24-byte-entry body records %d of them "+
			"(%.2f MiB of int32) instead of %d (%.2f MiB). If MaxRejectedAt was RAISED, "+
			"raise the action cap with it or price the extra list against MaxBodyBytes; "+
			"if esBulkMaxActions was LOWERED, lower MaxRejectedAt to match -- the two "+
			"are one number and this gate is what keeps them one.",
			ingest.MaxRejectedAt, esBulkMaxActions, ingest.MaxRejectedAt-esBulkMaxActions,
			min(ingest.MaxRejectedAt, (64<<20)/24), float64(min(ingest.MaxRejectedAt, (64<<20)/24))*4/(1<<20),
			esBulkMaxActions, float64(esBulkMaxActions)*4/(1<<20))
	}
}

// A 429 ON A DOCUMENT THAT CAN NEVER BE STORED IS A PIPELINE THAT NEVER DRAINS.
//
// This is the measurement that reopened round 18's own finding. 70,000 `index`
// actions, 66,000 of them carrying `"_time":"9999-01-01T00:00:00Z"`, one
// 5,254,000-byte body:
//
//	HTTP 200  errors=true
//	items                 70000
//	byStatus              map[429:70000]
//	byType                map[es_rejected_execution_exception:70000]
//	rows on disk           4000
//
// 429 is the one 4xx that is NOT permanent: Beats, Logstash and Fluentd all
// back off and re-send it. 66,000 of those documents are refused identically
// forever by ErrTimeOutOfRange, so the batch never drains -- the exact
// sentence that made 500 wrong -- and the 4,000 that landed are re-sent into
// an append-only store on every pass.
//
// The trigger was `ingest.MaxRejectedAt` being smaller than the action cap.
// With that closed the positions survive, and the answer is per item.
func TestABulkPastTheOldAttributionBoundIsStillAnsweredPerItem(t *testing.T) {
	const (
		n   = 70000
		bad = 66000 // over the old 65,536 bound, under the 1<<20 action cap
	)
	var sb strings.Builder
	sb.Grow(n * 72)
	docBytes := 0
	for i := 0; i < n; i++ {
		sb.WriteString(`{"index":{"_index":"logs"}}` + "\n")
		doc := `{"_time":"2026-06-01T12:00:00Z","level":"info"}` + "\n"
		if i < bad {
			doc = `{"_time":"9999-01-01T00:00:00Z","level":"far"}` + "\n"
		}
		sb.WriteString(doc)
		docBytes += len(doc)
	}
	body := sb.String()

	// THE SIZE IS A DOCUMENTED NUMBER, SO IT IS CHECKED HERE. Five other
	// places quote this body's length as measured on this tree -- result.go,
	// esbulk.go, this file twice and docs/lld/ingest.md, all from
	// docs/wrong.md entry 133 -- and every one of them said 4,966,000 while
	// the fixture below produces 70000*28 + 66000*47 + 4000*48. Nobody counts
	// a number in prose; this is where it can be counted.
	const wantBody = 70000*28 + 66000*47 + 4000*48 // 5,254,000
	if len(body) != wantBody {
		t.Fatalf("the fixture is %d bytes and every document that describes it says %d. "+
			"Fix the number in result.go, esbulk.go, this file's two comments and "+
			"docs/lld/ingest.md, or fix the fixture.", len(body), wantBody)
	}

	// BOTH SHARD COUNTS, AND THE GUARD ASKS THE FUNCTION.
	//
	// This ran `bulkServer(t)` -- no shard override -- behind a guard that
	// read `len(body) < ingest.MinParallelBytes` and, when it passed, said
	// "the parallel path is not exercised" as though it had established that.
	// It had not, three times over:
	//
	//   - `ParallelConfig.ShardsFor` needs `Shards >= 2` as well, and the
	//     derived value is runtime.NumCPU()/3 -- 1 on a four-core host, and 0
	//     shards means the SERIAL fallback.
	//   - The parallel branch is keyed on len(DOCS), the concatenated document
	//     lines, not len(body): the action lines are 1,960,000 of this body's
	//     bytes and never reach the ingester.
	//   - The message asserted the conclusion the guard did not check, so the
	//     failure it would print was a claim rather than a measurement.
	//
	// Measured with `base += pr.Accepted + pr.Rejected` mutated to `base += 0`
	// in mergeShardResults: this gate RED at 32 CPUs and GREEN under
	// `taskset -c 0-3`. The rebase of 66,000 positions is what a four-core CI
	// stopped covering.
	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the derived shard count", 0},
		{"shards forced to 4", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ingest.ParallelConfig{Shards: tc.shards}
			if tc.shards != 0 && cfg.ShardsFor(docBytes) < 2 {
				t.Fatalf("shards forced to %d resolves to %d over %d document bytes: "+
					"this row runs the serial fallback and asserts nothing about the "+
					"ordinal rebase", tc.shards, cfg.ShardsFor(docBytes), docBytes)
			}
			ts := bulkServerCfg(t, func(s *Server) { s.setIngestShardsForTest(tc.shards) })
			items, errs, status := postBulk(t, ts, body)
			if status != http.StatusOK {
				t.Fatalf("status %d, want 200 with per-item statuses", status)
			}
			if !errs || len(items) != n {
				t.Fatalf("errors=%v with %d items, want true and %d", errs, len(items), n)
			}
			byStatus := map[int]int{}
			firstWrong := -1
			for i, it := range items {
				byStatus[it.Status]++
				want4xx := i < bad
				if (it.Status >= 300) != want4xx && firstWrong < 0 {
					firstWrong = i
				}
				if it.Status == http.StatusTooManyRequests && firstWrong < 0 {
					firstWrong = i
				}
			}
			if byStatus[http.StatusTooManyRequests] > 0 {
				t.Errorf("%d items are 429 es_rejected_execution_exception. 429 is retryable to "+
					"every shipper and these documents can never be stored: the pipeline never "+
					"drains, and each pass re-sends the %d documents that DID land into an "+
					"append-only store.", byStatus[http.StatusTooManyRequests], n-bad)
			}
			if firstWrong >= 0 {
				t.Fatalf("item %d is %d %q; items 0..%d must be 4xx and %d..%d must be 2xx. byStatus=%v",
					firstWrong, items[firstWrong].Status, items[firstWrong].ErrTy, bad-1, bad, n-1, byStatus)
			}

			resp, err := http.Post(ts+"/_count", "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			var cnt struct{ Count int }
			json.NewDecoder(resp.Body).Decode(&cnt)
			resp.Body.Close()
			if cnt.Count != n-bad {
				t.Errorf("the store holds %d rows, want %d", cnt.Count, n-bad)
			}
		})
	}
}

// AND THE BRANCH THAT CANNOT PLACE A REJECTION IS COVERED, at the one level
// where it can be reached deliberately.
//
// `markBulkRejects` cannot place a rejection when the ingester's positions do
// not account for the rejections -- a truncated position list, or an ordinal
// that indexes nothing. Nothing in the repository reached those lines through
// a request: every _bulk test in the tree sends positions that are exact, so
// reverting the whole branch compiled and left `go test ./...` green.
//
// THE ANSWER IS NO LONGER A PER-ITEM STATUS. It was 429
// `es_rejected_execution_exception`, on the reasoning that a retryable 4xx
// over-reports without losing anything -- and 429 is the one 4xx that is not
// permanent, so it hands a permanently-unstorable document a transience it
// does not have and re-sends every stored document in the batch on every pass.
// See TestABulkPastTheOldAttributionBoundIsStillAnsweredPerItem for that
// measured at 70,000 items.
//
// What is left: with EVERY candidate rejected the positions are not needed and
// all of them are a permanent 400; with a mix, no per-item status is true, so
// none is written and the caller answers a request-level error.
func TestAnUnattributableBulkRejectionIsNotWrittenAsAnItemStatus(t *testing.T) {
	mk := func() []bulkOp {
		ops := make([]bulkOp, 5)
		for i := range ops {
			ops[i] = bulkOp{op: "index", doc: []byte(`{"a":1}`), status: 201}
		}
		return ops
	}
	untouched := func(t *testing.T, ops []bulkOp) {
		t.Helper()
		for i, o := range ops {
			if o.status != 201 || o.errType != "" {
				t.Fatalf("item %d was stamped %d %q. Four of these five documents are "+
					"stored and one can never be: no per-item status is true about "+
					"both, so none may be written.", i, o.status, o.errType)
			}
		}
	}

	t.Run("positions truncated, a mix", func(t *testing.T) {
		ops := mk()
		if markBulkRejects(ops, 1, nil, true) {
			t.Fatal("reported the items usable with 1 of 5 rejected and no positions")
		}
		untouched(t, ops)
	})

	t.Run("an ordinal that indexes nothing", func(t *testing.T) {
		ops := mk()
		// 99 is past the end of the candidate list: the positions are wrong,
		// so none of them may be trusted.
		if markBulkRejects(ops, 1, []int32{99}, false) {
			t.Fatal("reported the items usable with an ordinal that indexes nothing")
		}
		untouched(t, ops)
	})

	// EVERY CANDIDATE REJECTED NEEDS NO POSITIONS: there is no stored document
	// among them to mislabel, so the permanent 400 is exact for all of them.
	t.Run("every candidate rejected, positions unknown", func(t *testing.T) {
		ops := mk()
		if !markBulkRejects(ops, 5, nil, true) {
			t.Fatal("refused to answer a batch in which every document was rejected")
		}
		for i, o := range ops {
			if o.status != 400 || o.errType != "document_parsing_exception" {
				t.Fatalf("item %d is %d %q, want a permanent 400", i, o.status, o.errType)
			}
		}
	})

	// THE CONTROL: with the positions known, exactly the named item fails, and
	// it fails PERMANENTLY -- that document can never be stored however many
	// times it is sent.
	t.Run("positions known (control)", func(t *testing.T) {
		ops := mk()
		if !markBulkRejects(ops, 1, []int32{3}, false) {
			t.Fatal("refused a batch whose positions are exact")
		}
		for i, o := range ops {
			want := 201
			if i == 3 {
				want = 400
			}
			if o.status != want {
				t.Fatalf("item %d is %d %q, want %d", i, o.status, o.errType, want)
			}
		}
		if ops[3].errType != "document_parsing_exception" {
			t.Errorf("the placed rejection is %q", ops[3].errType)
		}
	})
}
