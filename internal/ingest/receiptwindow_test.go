package ingest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The write id and the rows it covers commit in ONE manifest record.
//
// Store.AppendGroupIdempotent commits the id in the same record as the group,
// and its doc says why: "Written separately there would be a window in which
// the rows are visible and the receipt is not -- and a retry landing in that
// window duplicates every row, which is the exact failure this exists to
// prevent, made rarer and therefore harder to find."
//
// It had no production caller. Production took Writer.FlushWithReceipt, which
// was Flush() followed by CommitReceipt(id) -- the two separate operations the
// doc warns about -- and the window was measured: ingest ten rows under a write
// id, flush, stop before the receipt, reopen, and the store holds 10 rows and
// does not remember the id, so a client retry re-ingests all ten.
//
// ASSERTED ON THE RECORD COUNT, not on a reopen. Reopening after a crash shows
// the end state, and both designs have the same end state when no crash
// happens; the number of manifest records is what "one transaction" means, and
// it is one number apart between the two designs. The manifest sequence is that
// count, and SnapshotAllWithSeq reports it under the same lock acquisition as
// the groups.
func TestTheWriteIDCommitsInTheSameRecordAsItsRows(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	const id = storage.WriteID("01234567-89ab-cdef-0123-456789abcdef")
	var body strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&body, `{"_msg":"row %d"}`+"\n", i)
	}
	if _, err := IngestJSONLines(w, []byte(body.String()), ts1); err != nil {
		t.Fatal(err)
	}

	before := manifestSeq(t, st)
	if err := w.FlushWithReceipt(id); err != nil {
		t.Fatal(err)
	}
	after := manifestSeq(t, st)

	if !st.CommittedWrite(id) {
		t.Fatal("the write id is not remembered after FlushWithReceipt")
	}
	if n := after - before; n != 1 {
		t.Errorf("FlushWithReceipt wrote %d manifest records for one group and one "+
			"write id (seq %d -> %d); one record is one transaction, and two of them "+
			"is a window in which the rows are queryable and the id is not -- a retry "+
			"landing in it duplicates every row", n, before, after)
	}
}

// The rows and the receipt survive a reopen together.
//
// Weaker than the record count and kept because it is the end state a client
// actually experiences: the retry that arrives after a restart is refused, and
// the rows it would have duplicated are there exactly once.
func TestAWriteIDSurvivesARestartWithItsRows(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(st)

	const id = storage.WriteID("01234567-89ab-cdef-0123-456789abcdef")
	var body strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&body, `{"_msg":"row %d"}`+"\n", i)
	}
	if _, err := IngestJSONLines(w, []byte(body.String()), ts1); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushWithReceipt(id); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	rows := 0
	sn, err := st2.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range sn.Groups {
		rows += g.Rows
	}
	sn.Close()

	if rows != 10 {
		t.Errorf("%d rows survived the restart, want 10", rows)
	}
	if !st2.CommittedWrite(id) {
		t.Errorf("%d rows are durable and the id is NOT remembered, so a client "+
			"retry duplicates all of them", rows)
	}
}

// A write id whose rows could NOT ride one group still commits, separately.
//
// flushCarrying only hands the id to the group when nothing else is in flight
// and there are rows to flush. Neither condition can be relaxed -- stamping the
// id on every group of a partial flush commits the receipt for rows that were
// never written, which refuses the retry that would have saved them. So the
// two-step path stays, and it has to keep working: a write id that reaches it
// must still be remembered, or every replicated write in a busy tenant becomes
// re-ingestable.
//
// The case forced here is "no rows buffered": the request's rows were already
// carried away by another caller's flush, which is the ordinary outcome under
// concurrency.
func TestAWriteIDWithNothingLeftToFlushIsStillCommitted(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	const id = storage.WriteID("fedcba98-7654-3210-fedc-ba9876543210")
	if _, err := IngestJSONLines(w, []byte(`{"_msg":"one"}`+"\n"), ts1); err != nil {
		t.Fatal(err)
	}
	// Somebody else's flush takes the rows first.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushWithReceipt(id); err != nil {
		t.Fatal(err)
	}
	if !st.CommittedWrite(id) {
		t.Error("a write id that arrived with nothing left to flush was not " +
			"committed at all: the retry that follows re-ingests the request")
	}
}

// Committing the same id twice is not an error, and does not double-write.
//
// The middleware checks CommittedWrite before the handler, so reaching the
// writer with a committed id means two retries raced. The group must still land
// -- it holds every row buffered on the tenant, not only the racing request's.
func TestARacingDuplicateStillStoresTheGroup(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	const id = storage.WriteID("11111111-2222-3333-4444-555555555555")
	// The id is already committed, as a racing retry would have left it.
	if err := st.CommitReceipt(id); err != nil {
		t.Fatal(err)
	}
	if _, err := IngestJSONLines(w, []byte(`{"_msg":"a"}`+"\n"+`{"_msg":"b"}`+"\n"), ts1); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushWithReceipt(id); err != nil {
		t.Fatalf("a duplicate id refused the flush: %v", err)
	}

	rows := 0
	sn, err := st.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range sn.Groups {
		rows += g.Rows
	}
	sn.Close()
	if rows != 2 {
		t.Errorf("%d rows landed, want 2: a group dropped for ErrDuplicateWrite "+
			"takes every other caller's buffered rows with it", rows)
	}
}

func manifestSeq(t *testing.T, st *storage.Store) uint64 {
	t.Helper()
	sn, seq, err := st.SnapshotAllWithSeq()
	if err != nil {
		t.Fatal(err)
	}
	sn.Close()
	return seq
}
