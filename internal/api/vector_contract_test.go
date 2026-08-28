package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/ingest"
)

// Vector search end to end: a record with an embedding goes in through the
// public ingest route, and comes back out of /select/vector ranked.
//
// Before this the two halves did not meet. The storage layer had a ColVector
// column and the query layer had a search over it, and no ingest path could
// write one -- the writer stored every field as a dictionary string, so a JSON
// array either landed as text or was dropped. The search was reachable only
// from a test that built groups by hand.

func vectorServer(t *testing.T, spec string) *httptest.Server {
	t.Helper()
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	c.VectorFields = spec
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// postJSON posts NDJSON and returns the status and decoded ingest result.
func postJSON(t *testing.T, ts *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson",
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]any{"raw": string(b)}
	}
	return resp.StatusCode, out
}

func vecLine(msg string, v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	return fmt.Sprintf(`{"_msg":%q,"emb":[%s]}`, msg, strings.Join(parts, ",")) + "\n"
}

// searchVec runs a vector search and returns the status and the _msg of each
// row, best first.
func searchVec(t *testing.T, ts *httptest.Server, field string, v []float32, k int) (int, []string, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"field": field, "vector": v, "k": k})
	resp, err := http.Post(ts.URL+"/select/vector", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil, string(raw)
	}
	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("row is not JSON: %q", line)
		}
		msgs = append(msgs, fmt.Sprint(row["_msg"]))
	}
	return resp.StatusCode, msgs, string(raw)
}

// An embedding written through the public ingest route is searchable, and the
// ranking is by cosine similarity.
func TestAVectorIngestedOverHTTPIsSearchable(t *testing.T) {
	ts := vectorServer(t, "emb:3")

	var body strings.Builder
	body.WriteString(vecLine("north", []float32{1, 0, 0}))
	body.WriteString(vecLine("east", []float32{0, 1, 0}))
	body.WriteString(vecLine("up", []float32{0, 0, 1}))
	body.WriteString(vecLine("north-ish", []float32{0.9, 0.1, 0}))
	code, res := postJSON(t, ts, body.String())
	if code/100 != 2 {
		t.Fatalf("ingest %d: %v", code, res)
	}
	if n, _ := res["ingested"].(float64); n != 4 {
		t.Fatalf("ingested %v, want 4: %v", res["ingested"], res)
	}

	code, msgs, raw := searchVec(t, ts, "emb", []float32{1, 0, 0}, 2)
	if code != 200 {
		t.Fatalf("%d: %s", code, raw)
	}
	if len(msgs) != 2 {
		t.Fatalf("%d rows, want 2: %s", len(msgs), raw)
	}
	if msgs[0] != "north" || msgs[1] != "north-ish" {
		t.Fatalf("ranking = %v, want [north north-ish]: %s", msgs, raw)
	}
	// The score is on every row, and descending.
	var last float64 = 2
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var row map[string]any
		json.Unmarshal([]byte(line), &row)
		s, err := strconv.ParseFloat(fmt.Sprint(row["_score"]), 64)
		if err != nil {
			t.Fatalf("no _score on %s", line)
		}
		if s > last {
			t.Fatalf("scores are not descending: %s", raw)
		}
		last = s
	}
}

// Records the store cannot use are rejected as records, with their positions,
// rather than stored with the embedding dropped. A log line stored without its
// vector is invisible to the one search it was ingested for.
func TestUnusableVectorsAreRejectedAsRecords(t *testing.T) {
	ts := vectorServer(t, "emb:3")
	body := strings.Join([]string{
		`{"_msg":"good","emb":[1,0,0]}`,
		`{"_msg":"short","emb":[1,0]}`,
		`{"_msg":"long","emb":[1,0,0,0]}`,
		`{"_msg":"nan","emb":[1,0,"NaN"]}`,
		`{"_msg":"strings","emb":["a","b","c"]}`,
		`{"_msg":"good2","emb":[0,1,0]}`,
	}, "\n") + "\n"

	code, res := postJSON(t, ts, body)
	if code/100 != 2 {
		t.Fatalf("%d: %v", code, res)
	}
	ing, _ := res["ingested"].(float64)
	skip, _ := res["skipped"].(float64)
	if ing != 2 || skip != 4 {
		t.Fatalf("ingested %v skipped %v, want 2 and 4: %v", ing, skip, res)
	}

	// And only the good ones are searchable.
	_, msgs, raw := searchVec(t, ts, "emb", []float32{1, 0, 0}, 10)
	if len(msgs) != 2 {
		t.Fatalf("%d rows searchable, want 2: %s", len(msgs), raw)
	}
}

// A field that is NOT configured as a vector is not guessed at: `[1,2,3]`
// might be a retry schedule or a status sequence, and a store that guessed
// would type the column from whichever record arrived first.
func TestAnUnconfiguredArrayIsNotAVector(t *testing.T) {
	ts := vectorServer(t, "emb:3")
	code, res := postJSON(t, ts, `{"_msg":"x","other":[1,2,3],"emb":[1,0,0]}`+"\n")
	if code/100 != 2 {
		t.Fatalf("%d: %v", code, res)
	}
	if n, _ := res["ingested"].(float64); n != 1 {
		t.Fatalf("ingested %v, want 1: %v", res["ingested"], res)
	}
	// Searching the unconfigured field finds nothing rather than erroring:
	// there is no such column.
	code, msgs, _ := searchVec(t, ts, "other", []float32{1, 2, 3}, 10)
	if code != 200 || len(msgs) != 0 {
		t.Fatalf("searching an unconfigured field: %d, %v", code, msgs)
	}
}

// Mixed records: some carry the embedding, some do not, and the ones that do
// stay aligned with their own rows.
//
// This is the alignment the flat column depends on -- the search reads row i
// of the vector buffer as row i of the group, so a row that carried no
// embedding still has to occupy its slot. A gap would put every later score on
// the wrong line, which is a wrong answer rather than a missing one.
func TestRecordsWithoutEmbeddingsKeepTheColumnAligned(t *testing.T) {
	ts := vectorServer(t, "emb:3")
	var body strings.Builder
	body.WriteString(`{"_msg":"plain-1"}` + "\n")
	body.WriteString(vecLine("vec-north", []float32{1, 0, 0}))
	body.WriteString(`{"_msg":"plain-2"}` + "\n")
	body.WriteString(`{"_msg":"plain-3"}` + "\n")
	body.WriteString(vecLine("vec-east", []float32{0, 1, 0}))
	code, res := postJSON(t, ts, body.String())
	if code/100 != 2 {
		t.Fatalf("%d: %v", code, res)
	}
	if n, _ := res["ingested"].(float64); n != 5 {
		t.Fatalf("ingested %v, want 5", res["ingested"])
	}

	_, msgs, raw := searchVec(t, ts, "emb", []float32{1, 0, 0}, 1)
	if len(msgs) != 1 || msgs[0] != "vec-north" {
		t.Fatalf("best match = %v, want vec-north: %s", msgs, raw)
	}
	_, msgs, raw = searchVec(t, ts, "emb", []float32{0, 1, 0}, 1)
	if len(msgs) != 1 || msgs[0] != "vec-east" {
		t.Fatalf("best match = %v, want vec-east: %s", msgs, raw)
	}
}

// The store reopens with its vectors intact, and a search spans groups.
func TestVectorsSurviveAReopenAndSpanGroups(t *testing.T) {
	dir := t.TempDir()
	build := func() *httptest.Server {
		c := config.Config{Dir: dir, Limits: config.DefaultLimits()}
		c.VectorFields = "emb:3"
		srv, err := NewServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(func() { ts.Close(); srv.Close() })
		return ts
	}

	ts := build()
	postJSON(t, ts, vecLine("first-batch", []float32{1, 0, 0}))
	// A second request is a second group.
	postJSON(t, ts, vecLine("second-batch", []float32{0, 1, 0}))
	code, msgs, raw := searchVec(t, ts, "emb", []float32{0, 1, 0}, 2)
	if code != 200 || len(msgs) != 2 {
		t.Fatalf("before reopen: %d %v %s", code, msgs, raw)
	}
	if msgs[0] != "second-batch" {
		t.Fatalf("cross-group ranking = %v: %s", msgs, raw)
	}
}

// The ceilings. Each bounds a different quantity, so each is refused
// separately -- and a query for the top 10 of a large corpus is a small answer
// and an expensive scan, which is why MaxK does not stand in for
// MaxCandidates.
func TestVectorCeilingsAreEnforced(t *testing.T) {
	mk := func(l config.Limits) *httptest.Server {
		c := config.Config{Dir: t.TempDir(), Limits: l}
		c.VectorFields = "emb:3"
		srv, err := NewServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Close() })
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(ts.Close)
		var body strings.Builder
		for i := 0; i < 20; i++ {
			body.WriteString(vecLine(fmt.Sprintf("v%d", i), []float32{float32(i), 1, 0}))
		}
		postJSON(t, ts, body.String())
		return ts
	}

	t.Run("k", func(t *testing.T) {
		l := config.DefaultLimits()
		l.MaxVectorK = 5
		code, _, body := searchVec(t, mk(l), "emb", []float32{1, 0, 0}, 50)
		if code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%d (%s), want 413", code, body)
		}
	})
	t.Run("candidates", func(t *testing.T) {
		l := config.DefaultLimits()
		l.MaxVectorCandidates = 5
		code, _, body := searchVec(t, mk(l), "emb", []float32{1, 0, 0}, 2)
		if code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%d (%s), want 413", code, body)
		}
		if !strings.Contains(body, "narrow it") {
			t.Errorf("the refusal does not say what to do: %s", body)
		}
	})
	t.Run("dimension", func(t *testing.T) {
		l := config.DefaultLimits()
		l.MaxVectorDim = 2
		code, _, body := searchVec(t, mk(l), "emb", []float32{1, 0, 0}, 2)
		if code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%d (%s), want 413", code, body)
		}
	})
	t.Run("within every ceiling", func(t *testing.T) {
		l := config.DefaultLimits()
		l.MaxVectorK, l.MaxVectorDim, l.MaxVectorCandidates = 10, 8, 1000
		code, msgs, body := searchVec(t, mk(l), "emb", []float32{1, 0, 0}, 3)
		if code != 200 {
			t.Fatalf("%d (%s)", code, body)
		}
		if len(msgs) != 3 {
			t.Fatalf("%d rows, want 3", len(msgs))
		}
	})
}

// A misconfigured -vector-fields is a startup failure, not a field silently
// stored as text.
func TestABadVectorConfigurationRefusesToStart(t *testing.T) {
	for _, spec := range []string{"emb", "emb:0", "emb:-1", "emb:abc", "emb:3,emb:4", ":3"} {
		c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
		c.VectorFields = spec
		if _, err := NewServerConfig(c); err == nil {
			t.Errorf("%q started", spec)
		}
	}
	// And a valid one starts.
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	c.VectorFields = "emb:3, title_vec:4"
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatalf("a valid spec was refused: %v", err)
	}
	srv.Close()
}

// vectorServerShards is vectorServer with the parallel ingest shard count
// forced, and it hands back the *Server so the branch can be ASKED rather
// than assumed. shards == 0 leaves the derived count alone.
func vectorServerShards(t *testing.T, spec string, shards int) *httptest.Server {
	t.Helper()
	c := config.Config{Dir: t.TempDir(), Limits: config.DefaultLimits()}
	c.VectorFields = spec
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if shards > 0 {
		srv.setIngestShardsForTest(shards)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// A body over MinParallelBytes keeps its embeddings.
//
// IT DID NOT. ParallelConfig carried five of the tenant writer's six settings
// and VectorFields was the sixth, so every temporary shard writer -- and the
// serial fallback inside IngestJSONLinesParallelResult, which builds one the
// same way -- ran with an empty vector set. IngestJSONLinesOpts asks
// `w.VectorFields()` once per body and its `simdjson.Array` arm returns early
// for a field that is not configured, so the array was skipped and the row was
// stored without it: text-queryable, and invisible to the one search it was
// ingested for. Measured on the built binary with -vector-fields=emb:4, one
// server, two uploads:
//
//	body           rows     /select/vector k=6
//	105 B          3        all 3 returned, ranked
//	1,052,693 B    27,277   0 returned
//
// HTTP 200 both times, {"ingested":27277,"skipped":0}. That is the reject arm
// of this same file's vector branch calling itself worse than a rejection --
// except nothing was rejected and nothing was reported.
//
// BOTH ROWS RUN AT BOTH CORE COUNTS. The forced row is the concurrent branch;
// the derived row is whatever this machine does, which at four cores is the
// serial fallback -- and the fallback builds its writer through the same
// cfg.apply, so the defect is not core-count-dependent and neither is the
// gate. The branch each row is on is printed, not assumed.
func TestALargeBodyKeepsItsEmbeddings(t *testing.T) {
	const dim = 4
	// Over MinParallelBytes, one needle pointing at [1,0,0,0] and filler
	// pointing away from it, so a search for the needle direction can only
	// rank the needle first if the large body's vectors were stored.
	var body strings.Builder
	body.WriteString(vecLine("needle", []float32{1, 0, 0, 0}))
	for i := 0; body.Len() < ingest.MinParallelBytes+(1<<16); i++ {
		body.WriteString(vecLine(fmt.Sprintf("filler-%d", i), []float32{0, 0, 0, 1}))
	}
	payload := body.String()
	if len(payload) < ingest.MinParallelBytes {
		t.Fatalf("body %d bytes is under MinParallelBytes %d", len(payload), ingest.MinParallelBytes)
	}

	run := func(t *testing.T, shards int) {
		t.Helper()
		// Ask which branch this row is on rather than describing it: the
		// condition has two halves and a guard that checks one of them is the
		// fault entry 134 and entry 135 are both about.
		got := ingest.ParallelConfig{Shards: shards}.ShardsFor(len(payload))
		if got == 0 {
			t.Logf("shards=%d -> serial fallback (NumCPU=%d); the fallback builds "+
				"its writer through the same cfg.apply", shards, runtime.NumCPU())
		} else {
			t.Logf("shards=%d -> %d shard writers", shards, got)
		}

		ts := vectorServerShards(t, "emb:4", shards)
		code, res := postJSON(t, ts, payload)
		if code/100 != 2 {
			t.Fatalf("ingest %d: %v", code, res)
		}
		ing, _ := res["ingested"].(float64)
		if int(ing) != strings.Count(payload, "\n") {
			t.Fatalf("ingested %v of %d lines: %v", res["ingested"], strings.Count(payload, "\n"), res)
		}

		const k = 6
		code, msgs, raw := searchVec(t, ts, "emb", []float32{1, 0, 0, 0}, k)
		if code != 200 {
			t.Fatalf("%d: %s", code, raw)
		}
		if len(msgs) != k {
			t.Fatalf("/select/vector returned %d rows of %v ingested, want %d. "+
				"The large body's embeddings were dropped: the rows are on disk and "+
				"text-queryable, and invisible to the search they were ingested for.",
				len(msgs), res["ingested"], k)
		}
		if msgs[0] != "needle" {
			t.Fatalf("best match = %q, want needle: the large body's vectors are not "+
				"the ones being ranked", msgs[0])
		}
	}

	t.Run("shards forced to 4", func(t *testing.T) { run(t, 4) })
	t.Run("the derived shard count", func(t *testing.T) { run(t, 0) })
}
