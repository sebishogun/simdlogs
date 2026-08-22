package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

// mergeShardResults IS GATED DIRECTLY, because nothing else can gate it on
// every machine.
//
// The concurrent branch needs `runtime.NumCPU()/3 >= 2`, so on any host with
// fewer than six cores -- a stock CI runner, `taskset -c 0-3` -- every test
// that reaches this function through an HTTP handler runs the serial fallback
// instead and passes. Measured: `base += pr.Accepted + pr.Rejected` mutated to
// `base += 0` is RED at 32 CPUs and GREEN under `taskset -c 0-3`. This test
// calls the function, so the machine cannot answer for it.
//
// The three things the rebase carries are asserted separately, because they
// are three different fields and only one of them had a gate at all:
// RejectedAt, RejectedTruncated, and the ordinal on a Warning.
func TestMergeShardResultsRebasesEveryPosition(t *testing.T) {
	// Four shards of 10 records each. Shard 0 rejects its record 3, shard 1
	// its record 0, shard 2 nothing, shard 3 its records 2 and 9 -- so the
	// batch positions are 3, 10, 32 and 39, and every one of them is
	// different from the shard-local ordinal that produced it.
	per := []Result{
		{Accepted: 9, Rejected: 1, RejectedAt: []int32{3},
			Warnings: []Warning{{Offset: UnknownPos, Ordinal: 3, Msg: "s0"}}},
		{Accepted: 9, Rejected: 1, RejectedAt: []int32{0},
			Warnings: []Warning{{Offset: UnknownPos, Ordinal: 0, Msg: "s1"}}},
		{Accepted: 10, Rejected: 0,
			Warnings: []Warning{{Offset: 7, Ordinal: UnknownPos, Msg: "s2 byte offset"}}},
		{Accepted: 8, Rejected: 2, RejectedAt: []int32{2, 9},
			Warnings: []Warning{{Offset: UnknownPos, Ordinal: 9, Msg: "s3"}}},
	}
	out := mergeShardResults(per, make([]bool, len(per)))

	if out.Accepted != 36 || out.Rejected != 4 {
		t.Fatalf("accepted %d rejected %d, want 36 and 4", out.Accepted, out.Rejected)
	}
	want := []int32{3, 10, 32, 39}
	if len(out.RejectedAt) != len(want) {
		t.Fatalf("RejectedAt %v, want %v", out.RejectedAt, want)
	}
	for i, w := range want {
		if out.RejectedAt[i] != w {
			t.Fatalf("RejectedAt %v, want %v: a shard's ordinal is relative to its "+
				"own chunk, and a client matches it against the whole batch",
				out.RejectedAt, want)
		}
	}
	if out.RejectedTruncated {
		t.Error("RejectedTruncated with every position recorded")
	}
	// The warning ordinals rebase by the same base. Warning.Ordinal has no
	// reader on any wire today -- see docs/lld/ingest.md -- so nothing but
	// this asserts it.
	wantWarn := []int64{3, 10, UnknownPos, 39}
	if len(out.Warnings) != len(wantWarn) {
		t.Fatalf("%d warnings, want %d", len(out.Warnings), len(wantWarn))
	}
	for i, w := range wantWarn {
		if out.Warnings[i].Ordinal != w {
			t.Errorf("warning %d ordinal %d, want %d (%q)",
				i, out.Warnings[i].Ordinal, w, out.Warnings[i].Msg)
		}
	}
	// AND THE BYTE OFFSET IS PASSED THROUGH BECAUSE NOTHING HERE CAN REBASE
	// IT, which is a different statement from "not rebasing it is right".
	// mergeShardResults is handed per-shard RECORD COUNTS; the chunk byte
	// starts never leave IngestJSONLinesParallelResult, and splitLines gives
	// each shard `data[start:end]`, so a shard's Offset is CHUNK-relative and
	// passing it through publishes it as body-relative. No producer exists on
	// this path today -- Result.Warn's callers are lokipb.go with UnknownPos
	// and three sites in journald.go, and journald does not shard -- so this
	// fixture's Offset: 7 is invented. It pins the pass-through so a future
	// reader sees the value is untouched, not that it is meaningful.
	if out.Warnings[2].Offset != 7 {
		t.Errorf("a BYTE offset was rebased as an ordinal: %d. It cannot be "+
			"rebased correctly either: this function has record counts and no "+
			"chunk byte starts, so the first shard-path caller of Result.Warn "+
			"must pass UnknownPos or hand mergeShardResults the chunk starts.",
			out.Warnings[2].Offset)
	}

	t.Run("a lost shard still moves the base", func(t *testing.T) {
		// Shard 1 never reached the store: its Accepted is not counted and
		// its record count still is, because shard 2 and 3's positions are
		// measured from it.
		lost := []bool{false, true, false, false}
		out := mergeShardResults(per, lost)
		if out.Accepted != 27 {
			t.Errorf("accepted %d, want 27 with shard 1 lost", out.Accepted)
		}
		for i, w := range want {
			if out.RejectedAt[i] != w {
				t.Fatalf("RejectedAt %v, want %v", out.RejectedAt, want)
			}
		}
	})

	t.Run("a truncated shard truncates the merge", func(t *testing.T) {
		per := []Result{
			{Accepted: 1, Rejected: 2, RejectedAt: []int32{0}, RejectedTruncated: true},
			{Accepted: 2, Rejected: 1, RejectedAt: []int32{1}},
		}
		out := mergeShardResults(per, make([]bool, 2))
		if !out.RejectedTruncated {
			t.Fatal("a shard reported truncation and the merge did not")
		}
		// The positions that ARE known still rebase: shard 1's record 1 is
		// batch record 4.
		if len(out.RejectedAt) != 2 || out.RejectedAt[1] != 4 {
			t.Fatalf("RejectedAt %v, want the second at 4", out.RejectedAt)
		}
	})
}

// THE SERIAL FALLBACK AND THE SHARDED PATH MUST AGREE ON WHAT IS DURABLE.
//
// They did not. The sharded branch zeroes a lost shard's Accepted through
// mergeShardResults; the serial branch returned the parse-time Accepted
// alongside the write error, so the SAME request against the SAME unwritable
// store answered `"durable":0` on a machine that sharded and `"durable":9698`
// on one that did not. The split is `runtime.NumCPU()/3 < 2`, i.e. every host
// with fewer than six cores, so the wrong answer was the one a small host
// gave. Measured before the fix: `taskset -c 0-2/0-3/0-4` FAIL, `0-5/0-7` pass,
// on an unmutated tree.
func TestSerialAndShardedAgreeOnWhatIsDurable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the serial fallback", 1}, // below the 2-shard minimum: serial
		{"the sharded path", testShards},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := storage.OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			// Every id a shard could claim.
			ids := make([]int, 64)
			for i := range ids {
				ids[i] = i
			}
			blockGroupIDs(t, dir, ids...)

			res, werr := IngestJSONLinesParallelResult(s, []byte(bigBody()), monotonic(),
				ParallelConfig{Shards: tc.shards}, nil)
			if werr == nil {
				t.Fatal("an unwritable store answered success")
			}
			if res.Accepted != 0 {
				t.Fatalf("reported %d accepted with every write refused: an operator "+
					"is told rows landed when none did", res.Accepted)
			}
			stored := 0
			for range query.Run(s, &query.Query{From: 0, To: int64(1) << 62}) {
				stored++
			}
			if stored != 0 {
				t.Fatalf("the store holds %d rows; the fixture did not block every write", stored)
			}
		})
	}
}

// THE BOUNDED WARNING LIST MUST NOT FORMAT WHAT IT DROPS.
//
// maxWarnings is 32 and a `/_bulk` at the action cap can reject 1,048,575
// records, so all but 32 of the messages are built and thrown away. The bound
// was checked inside warn(), which is AFTER fmt.Sprintf has run at the call
// site.
//
// RE-MEASURED IN ROUND 20, because the pair this comment carried did not
// reproduce and one of the two numbers is not a number this allocator can
// produce. `281` is not a multiple of 8. Interleaved A/B in one session --
// BEFORE is warnFull() moved back inside warn() and parseTime's out-of-range
// arm back to `fmt.Errorf("%w: %s ns since the epoch", ...)` -- exact
// runtime.MemStats deltas over 200,000 calls, three A/B pairs, every figure
// identical across them:
//
//	measure                                    before        after
//	parseTime, out of range                  5 / 264.5 B   3 / 104.0 B
//	WarnAt past the bound, `("%v", tsErr)`   1 / 144.2 B   0 / 0 B
//	  ...the shape jsonline.go:607 uses
//	WarnAt past the bound, `("filler %d")`   1 /  16.0 B   0 / 0 B
//	WarnAt past the bound, no arguments      1 /  48.1 B   0 / 0 B
//
// The old "48 B" was the NO-ARGUMENT shape; the reject path boxes an error and
// formats it, so the real saving is 144.2 B -- 3x what was disclosed. On the
// full per-record path (IngestJSONLinesOpts over 2,000 documents whose `_time`
// is out of range, which is the shape a rejected _bulk document takes) the two
// fixes together are 19.012 -> 16.042 allocations and -300.1 B per rejected
// record; the absolute byte figure depends on the document, the delta does not.
//
// This is the tenets' zero-allocations-per-record rule on the path in this
// tree that sees the most records per request.
func TestABoundedWarningCostsNothingToDiscard(t *testing.T) {
	var r Result
	for i := 0; i < maxWarnings; i++ {
		r.WarnAt(i, "filler %d", i)
	}
	if len(r.Warnings) != maxWarnings {
		t.Fatalf("%d warnings recorded, want the bound %d", len(r.Warnings), maxWarnings)
	}
	// The shape the reject path calls: jsonline.go:607 is
	// `res.WarnAt(ordinal, "%v", tsErr)`, which boxes an error and calls its
	// Error() inside Sprintf. It is 3x the no-argument shape and it is the one
	// a cap-sized _bulk pays.
	_, _, tsErr := parseTime("253402300800000000000")
	if tsErr == nil {
		t.Fatal("the fixture no longer produces an out-of-range error")
	}
	for _, tc := range []struct {
		name string
		f    func()
	}{
		{"WarnAt", func() { r.WarnAt(7, "entry carries a timestamp and no storable field") }},
		{"WarnAt with arguments", func() { r.WarnAt(7, "%s: %d", "reason", 12345) }},
		{"WarnAt with a boxed error", func() { r.WarnAt(7, "%v", tsErr) }},
		{"Warn", func() { r.Warn(3, "stream labels %q: %v", "x", "y") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.f() // warm up: the first call must not be counted
			// AND THE WARM-UP IS WHERE THE BOUND IS ASKED. The list is filled
			// to exactly maxWarnings above and was then asserted to be
			// maxWarnings, so `warnFull() { return len(r.Warnings) >
			// maxWarnings }` -- off by one, 33 warnings kept -- compiled and
			// was GREEN at 32 CPUs and under `taskset -c 0-3`: the warm-up
			// call absorbed the extra slot and every AllocsPerRun below then
			// ran against a list the mutant agreed was full. Asserting AFTER
			// the warm-up is what makes the 33rd slot visible.
			if len(r.Warnings) != maxWarnings {
				t.Fatalf("%d warnings after a call past the bound, want %d: the bound "+
					"admitted one more. Every measurement below runs against a list "+
					"the guard already calls full, so an off-by-one in warnFull is "+
					"invisible to them.", len(r.Warnings), maxWarnings)
			}
			if got := testing.AllocsPerRun(200, tc.f); got != 0 {
				t.Errorf("%v allocations per discarded warning. The message is built "+
					"and dropped: past maxWarnings the bound has to be checked BEFORE "+
					"fmt.Sprintf, and a _bulk at the action cap pays this up to "+
					"1,048,543 times.", got)
			}
		})
	}
	// THE CONTROL: under the bound a warning is still recorded, with its
	// message. A guard that returned early always would pass every line above.
	var u Result
	u.WarnAt(4, "ordinal %d", 4)
	u.Warn(9, "offset %d", 9)
	if len(u.Warnings) != 2 || u.Warnings[0].Msg != "ordinal 4" || u.Warnings[1].Msg != "offset 9" {
		t.Fatalf("under the bound the warnings are %+v", u.Warnings)
	}
	// And the counts are untouched by the bound either way.
	if r.Rejected != 0 {
		t.Errorf("Warn/WarnAt changed the reject count: %d", r.Rejected)
	}
}

// Every ParallelConfig field except Shards survives the round trip a shard
// writer is built through: Writer.ShardSettings reads it off the configured
// writer, apply stamps it onto a fresh one, and the two writers report the
// same thing.
//
// THIS IS THE GATE THAT REPLACES A HAND-KEPT LIST. The same omission happened
// three times -- Compact copied and StreamFields forgotten, then Limits, then
// VectorFields -- because the settings were enumerated twice, once where the
// tenant writer is built and once where the shard config is assembled, and
// nothing compared the two. It walks the struct by REFLECTION rather than
// naming fields, so a field added to ParallelConfig and forgotten in either
// half fails here: forgotten in ShardSettings it comes back zero, forgotten in
// apply it comes back unequal.
//
// A new field also has to be configured on src below. That is the point: the
// author of the field is made to say what a non-default value of it looks
// like, which is the thing the two-list version never asked anyone.
func TestShardSettingsRoundTripEveryField(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Every setting a tenant writer can carry, at a value that is not the
	// zero one, so a field that is not copied is visibly absent.
	src := NewWriter(st)
	defer src.Close()
	src.SetCompact(true)
	src.SetStreamFields([]string{"service", "host"})
	src.SetMaxLineBytes(4096)
	src.SetRecordLimits(RecordLimits{
		MaxFields: 8, MaxNameBytes: 16, MaxValueBytes: 32,
	})
	src.SetVectorFields(VectorFields{"emb": 4})

	cfg := src.ShardSettings()

	// Half one: ShardSettings reads every field. Shards is the only field
	// that is not a writer setting -- the caller supplies it.
	rv := reflect.ValueOf(cfg)
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		name := rt.Field(i).Name
		if name == "Shards" {
			if !rv.Field(i).IsZero() {
				t.Errorf("ShardSettings set Shards; the caller supplies it")
			}
			continue
		}
		if rv.Field(i).IsZero() {
			t.Errorf("ParallelConfig.%s came back zero from ShardSettings on a "+
				"fully configured writer. Either ShardSettings does not read it, "+
				"or src above does not set it -- both are the omission this gate "+
				"exists for: a large body would be stored under different rules "+
				"from the same body one byte smaller.", name)
		}
	}

	// Half two: apply sets every field it read.
	dst := NewWriterWorkers(st, 2)
	defer dst.Close()
	cfg.apply(dst)
	got := dst.ShardSettings()
	if !reflect.DeepEqual(got, cfg) {
		for i := 0; i < rt.NumField(); i++ {
			name := rt.Field(i).Name
			a, b := rv.Field(i).Interface(), reflect.ValueOf(got).Field(i).Interface()
			if !reflect.DeepEqual(a, b) {
				t.Errorf("ParallelConfig.%s does not survive apply: shard writer has %v, "+
					"the tenant writer has %v", name, b, a)
			}
		}
	}
}
