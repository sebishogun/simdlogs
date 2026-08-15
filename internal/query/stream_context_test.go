package query

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// stream_context returns the rows around a match IN ITS OWN STREAM.
//
// Scoped to the query window instead -- which is what it did -- the neighbours
// of an error on one host are whatever other hosts happened to write at the
// same moment. On a busy server `before 5 after 5` returns ten lines from ten
// unrelated processes and none of the ten from the process that failed. Not a
// smaller answer: a different one, indistinguishable from a correct one.

// ctxRow is one row of a stream_context fixture.
type ctxRow struct {
	time   int64
	stream string
	msg    string
}

// ctxStore writes the rows in the order given, one group per `groups` split,
// with `_stream` as a real column -- which is what it is: the query layer
// synthesizes nothing here, StatsByField reads it off the group.
func ctxStore(t *testing.T, rows []ctxRow, groupSize int) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if groupSize <= 0 {
		groupSize = len(rows)
	}
	for lo := 0; lo < len(rows); lo += groupSize {
		hi := lo + groupSize
		if hi > len(rows) {
			hi = len(rows)
		}
		chunk := rows[lo:hi]
		times := make([]int64, len(chunk))
		msgs := make([]string, len(chunk))
		streams := make([]string, len(chunk))
		for i, r := range chunk {
			times[i], msgs[i], streams[i] = r.time, r.msg, r.stream
		}
		md := storage.BuildDict(msgs)
		sd := storage.BuildDict(streams)
		if _, err := s.AppendGroup(&storage.Group{Rows: len(chunk), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "_msg", Type: storage.ColDict, Dict: &md},
			{Name: "_stream", Type: storage.ColDict, Dict: &sd},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func ctxQueryOf(t *testing.T, src string) *Query {
	t.Helper()
	q, err := ParseLogsQL(src)
	if err != nil {
		t.Fatalf("%s: %v", src, err)
	}
	q.From, q.To = 0, math.MaxInt64
	q.MatAll = true
	return q
}

// interleaved builds two streams whose rows alternate in time, so every
// neighbour in WINDOW order belongs to the other stream. A window-scoped
// context returns none of the right rows; a stream-scoped one returns only
// the right rows.
func interleaved() []ctxRow {
	var rows []ctxRow
	for i := 0; i < 10; i++ {
		rows = append(rows,
			ctxRow{time: int64(i*2 + 1), stream: `{host="a"}`, msg: fmt.Sprintf("a%d", i)},
			ctxRow{time: int64(i*2 + 2), stream: `{host="b"}`, msg: fmt.Sprintf("b%d", i)},
		)
	}
	return rows
}

func msgSet(rows []Row) map[string]bool {
	m := make(map[string]bool, len(rows))
	for _, r := range rows {
		m[rowField(r, "_msg")] = true
	}
	return m
}

// The neighbours of a match come from its own stream.
func TestStreamContextStaysInTheMatchedStream(t *testing.T) {
	s := ctxStore(t, interleaved(), 4)
	q := ctxQueryOf(t, "a5 | stream_context before 2 after 2")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	got := msgSet(out)
	for _, want := range []string{"a3", "a4", "a5", "a6", "a7"} {
		if !got[want] {
			t.Errorf("missing %s from the matched stream's context: %v", want, keysOf(got))
		}
	}
	for m := range got {
		if strings.HasPrefix(m, "b") {
			t.Errorf("context leaked into another stream: %s (%v)", m, keysOf(got))
		}
	}
	if len(got) != 5 {
		t.Fatalf("%d rows, want 5: %v", len(got), keysOf(got))
	}
}

// A match at a stream's edge is clamped to that stream, not padded from the
// window. Clamping to the window would silently substitute another stream's
// rows for the ones that do not exist.
func TestContextIsClampedToTheStreamNotTheWindow(t *testing.T) {
	s := ctxStore(t, interleaved(), 3)
	q := ctxQueryOf(t, "a0 | stream_context before 3 after 1")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	got := msgSet(out)
	// a0 is the stream's first row: nothing before it, one row after.
	if len(got) != 2 || !got["a0"] || !got["a1"] {
		t.Fatalf("%v, want exactly a0 and a1", keysOf(got))
	}
}

// Duplicate timestamps do not make an unrelated row a match.
//
// The pipe used to locate matches by TIMESTAMP, so any row written in the same
// millisecond as a match got its own context -- and on the same timestamp in a
// different stream, that context was a second stream's worth of rows nobody
// asked for.
func TestADuplicateTimestampIsNotAMatch(t *testing.T) {
	rows := []ctxRow{
		{time: 10, stream: `{host="a"}`, msg: "a-before"},
		{time: 20, stream: `{host="a"}`, msg: "needle"},
		{time: 20, stream: `{host="b"}`, msg: "b-same-instant"},
		{time: 30, stream: `{host="a"}`, msg: "a-after"},
		{time: 30, stream: `{host="b"}`, msg: "b-after"},
		{time: 40, stream: `{host="b"}`, msg: "b-later"},
	}
	s := ctxStore(t, rows, 2)
	q := ctxQueryOf(t, "needle | stream_context before 1 after 1")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	got := msgSet(out)
	want := map[string]bool{"a-before": true, "needle": true, "a-after": true}
	if len(got) != len(want) {
		t.Fatalf("%v, want %v", keysOf(got), keysOf(want))
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing %s: %v", k, keysOf(got))
		}
	}
}

// Overlapping context ranges collapse: two matches close together share their
// neighbours and each shared row appears once.
func TestOverlappingContextRangesAreDeduplicated(t *testing.T) {
	var rows []ctxRow
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("line%d", i)
		if i == 3 || i == 5 {
			msg = fmt.Sprintf("needle%d", i)
		}
		rows = append(rows, ctxRow{time: int64(i + 1), stream: `{host="a"}`, msg: msg})
	}
	s := ctxStore(t, rows, 4)
	q := ctxQueryOf(t, "needle | stream_context before 2 after 2")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, r := range out {
		seen[rowField(r, "_msg")]++
	}
	for m, n := range seen {
		if n != 1 {
			t.Errorf("%s appeared %d times", m, n)
		}
	}
	// indices 1..7 -- 3's window is 1..5, 5's is 3..7, union is 1..7.
	if len(out) != 7 {
		t.Fatalf("%d rows, want 7: %v", len(out), keysOf(msgSet(out)))
	}
	// And they come back in time order, not grouped by which match pulled them.
	for i := 1; i < len(out); i++ {
		if out[i-1].Time > out[i].Time {
			t.Fatalf("out of time order at %d: %d then %d", i, out[i-1].Time, out[i].Time)
		}
	}
}

// With no _stream column every row is in the empty stream, so an unconfigured
// deployment gets the window-scoped behaviour it had -- because there really
// is only one stream to scope to.
func TestWithNoStreamsContextIsWindowScoped(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	times := make([]int64, 8)
	msgs := make([]string, 8)
	for i := range times {
		times[i] = int64(i + 1)
		msgs[i] = fmt.Sprintf("line%d", i)
	}
	msgs[4] = "needle"
	d := storage.BuildDict(msgs)
	if _, err := s.AppendGroup(&storage.Group{Rows: 8, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: times},
		{Name: "_msg", Type: storage.ColDict, Dict: &d},
	}}); err != nil {
		t.Fatal(err)
	}
	q := ctxQueryOf(t, "needle | stream_context before 2 after 2")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 {
		t.Fatalf("%d rows, want 5: %v", len(out), keysOf(msgSet(out)))
	}
}

// A window the pipe cannot hold is refused, not answered from a prefix.
func TestStreamContextRefusesAnOversizedWindow(t *testing.T) {
	s := ctxStore(t, interleaved(), 4)
	q := ctxQueryOf(t, "a5 | stream_context before 2 after 2")
	q.Bind(nil, Limits{MaxPipeRows: 5})
	out := RunPipeline(s, q)
	err := q.StopErr()
	if !errors.Is(err, ErrPipeRowLimit) {
		t.Fatalf("StopErr = %v (%d rows), want ErrPipeRowLimit", err, len(out))
	}
	if !strings.Contains(err.Error(), "narrow the window") {
		t.Errorf("the refusal does not say what to do: %v", err)
	}
}

// Two streams each containing a match get their own context, and the two do
// not bleed into each other.
func TestEachStreamGetsItsOwnContext(t *testing.T) {
	rows := interleaved()
	s := ctxStore(t, rows, 5)
	q := ctxQueryOf(t, "a5 OR b5 | stream_context before 1 after 1")
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatal(err)
	}
	got := msgSet(out)
	for _, want := range []string{"a4", "a5", "a6", "b4", "b5", "b6"} {
		if !got[want] {
			t.Errorf("missing %s: %v", want, keysOf(got))
		}
	}
	if len(got) != 6 {
		t.Fatalf("%d rows, want 6: %v", len(got), keysOf(got))
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
