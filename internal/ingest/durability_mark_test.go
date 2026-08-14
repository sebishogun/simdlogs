package ingest

import (
	"os"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A caller must not be told its rows landed when they did not.
//
// The row buffer is shared by every request and every syslog connection on
// a tenant, so another goroutine's Flush routinely carries this caller's
// rows away. Three earlier attempts each reported on the wrong thing: a
// writer-wide error slot (one caller took it, the rest got success), a
// failure generation (a flush that added nothing missed failures already
// reported), and retiring failed batches on the next fresh buffer (a caller
// whose rows were in the retired batch found nothing outstanding).
// Mark/FlushMark asks about exactly the batches the caller's own rows could
// have joined.
//
// Deterministic rather than concurrent: two rows share one buffer, X's
// flush carries both and fails, the store heals, and Y -- whose row was in
// that same failed batch -- must still be told so. An INTERMITTENT store is
// what makes the hole visible; against a permanently broken one the
// replacement batch fails too and hides it.
func TestFlushMarkReportsAFailureToEveryContributor(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	add := func(ts int64) {
		t.Helper()
		w.Add(ts, map[string]string{"a": "b"})
	}

	// Both rows go into the same buffer, so into the same batch.
	markX := w.Mark()
	add(1700000000000000001)
	markY := w.Mark()
	add(1700000000000000002)
	if markX != markY {
		t.Fatalf("marks %d and %d differ; the rows are not in one batch", markX, markY)
	}

	// Break the store, then let X carry both rows into a batch that fails.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushMark(markX); err == nil {
		t.Fatal("X was told its rows landed while the store could not write")
	}

	// The store recovers before Y asks.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushMark(markY); err == nil {
		t.Fatal("Y was told its row landed; it was in the batch that failed")
	}

	// A caller that marks AFTER the failure gets a clean answer: the report
	// is scoped to contributors, not sticky.
	markZ := w.Mark()
	add(1700000000000000003)
	if err := w.FlushMark(markZ); err != nil {
		t.Fatalf("a caller after the recovery was told about the old failure: %v", err)
	}
}

// A flush that adds nothing -- an empty body, a body whose lines were all
// rejected, Close at shutdown -- must not inherit an earlier failure. The
// previous attempt retained errored batches for any Flush, which made every
// later empty flush return a failure that had already been reported.
func TestAnEmptyFlushDoesNotInheritAnOldFailure(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	mark := w.Mark()
	w.Add(1700000000000000001, map[string]string{"a": "b"})
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushMark(mark); err == nil {
		t.Fatal("the contributor was told its row landed")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w.Flush(); err != nil {
			t.Fatalf("empty flush %d inherited the old failure: %v", i+1, err)
		}
	}
}
