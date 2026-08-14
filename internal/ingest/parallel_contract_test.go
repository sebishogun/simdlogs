package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// testShards forces the concurrent branch on regardless of the machine.
//
// The derived shard count is runtime.NumCPU()/3, which is below the 2-shard
// minimum on anything with fewer than six cores -- including a stock CI
// runner. Without this the tests below silently exercise the serial fallback
// and pass even when the concurrent code is broken.
const testShards = 4

// monotonic is a race-free fallback clock. The parallel path calls the
// fallback from every shard goroutine, so a closure over a plain int is a
// data race; it stays invisible only while every test line happens to carry
// a parseable _time.
func monotonic() func() int64 {
	var n int64
	return func() int64 { return atomic.AddInt64(&n, 1) }
}

// bigBody builds an NDJSON body over MinParallelBytes so the parallel path
// runs, with a distinguishable host per line.
func bigBody() string {
	var sb strings.Builder
	base := int64(1_700_000_000_000_000_000)
	for i := 0; sb.Len() < MinParallelBytes+(1<<16); i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"level":"info","service":"api","host":"h%d","_msg":"filler filler filler filler %d"}`+"\n",
			base+int64(i)*1000, i%4, i)
	}
	return sb.String()
}

// blockGroupIDs makes group creation fail for the named IDs by putting a
// directory where the temp file wants to be: writeFileAtomic opens
// group-<id>.bin.tmp, which returns EISDIR.
//
// This replaces chmod-based injection, which does nothing when the tests run
// as root -- the chmod succeeds and so does the write, so the test went red
// against correct code in any root container.
func blockGroupIDs(t *testing.T, dir string, ids ...int) {
	t.Helper()
	for _, id := range ids {
		p := filepath.Join(dir, fmt.Sprintf("group-%d.bin.tmp", id))
		if err := os.MkdirAll(p, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

// A write that cannot reach disk must not be reported as ingested. The
// parallel path returned the parsed line count and discarded every shard
// writer's Close error, so a store that could not append a single group still
// answered 200 with a row count -- data loss that looks like success.
func TestParallelIngestSurfacesWriteError(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Block every id any shard could claim.
	ids := make([]int, 64)
	for i := range ids {
		ids[i] = i
	}
	blockGroupIDs(t, dir, ids...)

	body := bigBody()
	ing, _, err := IngestJSONLinesParallelCfg(s, []byte(body), monotonic(),
		ParallelConfig{Shards: testShards}, nil)
	if err == nil {
		t.Fatal("parallel ingest reported success with an unwritable store")
	}
	if ing != 0 {
		t.Fatalf("reported %d rows ingested when every shard failed", ing)
	}
}

// The serial fallback inside the same function has to report it too, or the
// error surfaces only above a size threshold.
func TestSerialFallbackSurfacesWriteError(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	blockGroupIDs(t, dir, 0, 1, 2, 3)

	body := `{"_time":1700000000000000000,"level":"info","service":"api"}` + "\n"
	if _, _, err := IngestJSONLinesParallelCfg(s, []byte(body), monotonic(),
		ParallelConfig{}, nil); err == nil {
		t.Fatal("serial fallback reported success with an unwritable store")
	}
}

// A partial failure is the interesting case: some shards land, one does not.
// The error must name how many failed out of how many, and the accepted count
// must cover only the durable rows -- reporting the parsed total would tell a
// caller its whole batch is safe when part of it is gone.
func TestParallelIngestReportsPartialFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Block one id in the middle of the range the shards will use.
	blockGroupIDs(t, dir, 2)

	body := bigBody()
	ing, _, err := IngestJSONLinesParallelCfg(s, []byte(body), monotonic(),
		ParallelConfig{Shards: testShards}, nil)
	if err == nil {
		t.Skip("no shard claimed the blocked id; nothing to assert")
	}

	var pe *ParallelWriteError
	if !asParallelWriteError(err, &pe) {
		t.Fatalf("err is %T, want *ParallelWriteError", err)
	}
	if pe.Shards < 2 {
		t.Fatalf("reported %d shards started, want the concurrent branch", pe.Shards)
	}
	if pe.Failed < 1 || pe.Failed >= pe.Shards {
		t.Fatalf("reported %d of %d shards failed, want a partial failure", pe.Failed, pe.Shards)
	}
	// The rows the surviving shards wrote are durable and are counted; the
	// failed shard's are not.
	stored := 0
	for _, r := range query.Run(s, &query.Query{From: 0, To: int64(1) << 62}) {
		_ = r
		stored++
	}
	if ing != stored {
		t.Fatalf("reported %d rows ingested, store holds %d -- the count must match what is durable", ing, stored)
	}
	if ing == 0 {
		t.Fatal("a partial failure reported nothing durable")
	}
}

func asParallelWriteError(err error, out **ParallelWriteError) bool {
	pe, ok := err.(*ParallelWriteError)
	if ok {
		*out = pe
	}
	return ok
}

// schemaOf returns the sorted column names and the per-row _stream values, so
// two ingest paths can be compared on what they actually stored rather than
// on a count that two different schemas could both satisfy.
func schemaOf(t *testing.T, s *storage.Store) (cols []string, streams []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, r := range query.Run(s, &query.Query{From: 0, To: int64(1) << 62, MatAll: true}) {
		for _, f := range r.Fields {
			seen[f.Key] = true
			if f.Key == "_stream" {
				streams = append(streams, f.Value)
			}
		}
	}
	for k := range seen {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	sort.Strings(streams)
	return cols, streams
}

// The temporary shard writers copied Compact but not the configured stream
// fields, so the same records produced a _stream column under the small-body
// path and none under the parallel one. Compare the stored values and the
// column set, not just how many _stream values came back.
func TestParallelIngestKeepsConfiguredStreamFields(t *testing.T) {
	body := bigBody()
	cfg := ParallelConfig{StreamFields: []string{"service"}, Shards: testShards}

	run := func(t *testing.T, useParallel bool) ([]string, []string) {
		t.Helper()
		s, err := storage.OpenStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if useParallel {
			if _, _, err := IngestJSONLinesParallelCfg(s, []byte(body), monotonic(), cfg, nil); err != nil {
				t.Fatal(err)
			}
		} else {
			w := NewWriter(s)
			w.SetStreamFields(cfg.StreamFields)
			IngestJSONLinesOpts(w, []byte(body), monotonic(), nil)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		}
		return schemaOf(t, s)
	}

	serialCols, serialStreams := run(t, false)
	parCols, parStreams := run(t, true)

	if len(serialStreams) == 0 {
		t.Fatal("the serial path produced no _stream values; the test proves nothing")
	}
	if strings.Join(serialCols, ",") != strings.Join(parCols, ",") {
		t.Fatalf("column sets differ:\n serial   %v\n parallel %v", serialCols, parCols)
	}
	if len(serialStreams) != len(parStreams) {
		t.Fatalf("_stream value counts differ: serial %d, parallel %d", len(serialStreams), len(parStreams))
	}
	for i := range serialStreams {
		if serialStreams[i] != parStreams[i] {
			t.Fatalf("_stream values differ at %d: serial %q, parallel %q", i, serialStreams[i], parStreams[i])
		}
	}
}

// A request naming its own _stream_fields, against a writer that also has
// deployment-wide stream fields, wrote two values into the _stream column for
// one row. Every later row in that column then read one row late.
func TestRequestStreamFieldsDoNotDoubleTheColumn(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := NewWriter(s)
	w.SetStreamFields([]string{"service"}) // deployment-wide default
	opts := &Options{StreamFields: []string{"host"}}

	var sb strings.Builder
	base := int64(1_700_000_000_000_000_000)
	const rows = 200
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"service":"api","host":"h%d","seq":"s%d"}`+"\n",
			base+int64(i)*1000, i, i)
	}
	ingRes, _ := IngestJSONLinesOpts(w, []byte(sb.String()), monotonic(), opts)
	if ingRes.Accepted != rows || ingRes.Rejected != 0 {
		t.Fatalf("ingested %d skipped %d", ingRes.Accepted, ingRes.Rejected)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	got := query.Run(s, &query.Query{From: 0, To: int64(1) << 62, MatAll: true})
	if len(got) != rows {
		t.Fatalf("query returned %d rows, want %d", len(got), rows)
	}
	for _, r := range got {
		var stream, host, seq string
		n := 0
		for _, f := range r.Fields {
			switch f.Key {
			case "_stream":
				n++
				stream = f.Value
			case "host":
				host = f.Value
			case "seq":
				seq = f.Value
			}
		}
		if n > 1 {
			t.Fatalf("seq %s: %d _stream fields on one row", seq, n)
		}
		if want := `{host="` + host + `"}`; stream != want {
			t.Fatalf("seq %s: _stream %q, want %q (host %q)", seq, stream, want, host)
		}
	}
}

// The override is a property of the request, not of the row. A row whose
// override label comes out empty -- because it carries none of the named
// fields -- must not fall back to the deployment default: that mixed two
// labelling schemes inside one column, and since _stream drives stream-scoped
// retention, it let one request land rows in a retention bucket it did not
// ask for.
func TestStreamOverrideIsPerRequestNotPerRow(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := NewWriter(s)
	w.SetStreamFields([]string{"service"})
	opts := &Options{StreamFields: []string{"host"}}

	// Row 0 has host; row 1 does not, so its override label is empty.
	body := `{"_time":1700000000000000001,"service":"api","host":"h1"}` + "\n" +
		`{"_time":1700000000000000002,"service":"api"}` + "\n"
	if ingRes, _ := IngestJSONLinesOpts(w, []byte(body), monotonic(), opts); ingRes.Accepted != 2 {
		t.Fatalf("ingested %d rows", ingRes.Accepted)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, r := range query.Run(s, &query.Query{From: 0, To: int64(1) << 62, MatAll: true}) {
		for _, f := range r.Fields {
			if f.Key == "_stream" && strings.Contains(f.Value, "service=") {
				t.Fatalf("a row of an overriding request fell back to the deployment label: %q", f.Value)
			}
		}
	}
}

// A payload field literally named _stream must not decide labelling when the
// deployment owns it: _stream is what stream-scoped retention groups on, so
// honouring a client-supplied value lets the client choose its own retention
// bucket.
func TestPayloadStreamFieldDoesNotOverrideDeployment(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := NewWriter(s)
	w.SetStreamFields([]string{"service"})

	body := `{"_time":1700000000000000001,"service":"api","_stream":"{tenant=\"attacker\"}"}` + "\n"
	if ingRes, _ := IngestJSONLines(w, []byte(body), monotonic()); ingRes.Accepted != 1 {
		t.Fatal("row not ingested")
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, r := range query.Run(s, &query.Query{From: 0, To: int64(1) << 62, MatAll: true}) {
		for _, f := range r.Fields {
			if f.Key != "_stream" {
				continue
			}
			if strings.Contains(f.Value, "attacker") {
				t.Fatalf("a client-supplied _stream survived deployment labelling: %q", f.Value)
			}
			if want := `{service="api"}`; f.Value != want {
				t.Fatalf("_stream %q, want %q", f.Value, want)
			}
		}
	}
}
