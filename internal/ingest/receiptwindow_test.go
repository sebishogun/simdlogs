package ingest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The window between a flush and its receipt.
//
// Store.AppendGroupIdempotent commits the write id in the SAME manifest record
// as the group, and its doc says why: "Written separately there would be a
// window in which the rows are visible and the receipt is not -- and a retry
// landing in that window duplicates every row, which is the exact failure this
// exists to prevent, made rarer and therefore harder to find."
//
// It has no production caller. The path production takes is
// Writer.FlushWithReceipt, which is Flush() followed by CommitReceipt(id) --
// exactly the two separate operations the doc warns about.
//
// This measures the window rather than arguing about it: ingest a request's
// rows, flush them, and stop before the receipt, which is what a crash there
// leaves behind. If the rows are durable and the id is not remembered, a client
// retry re-ingests every one of them.
func TestTheReceiptWindowAfterAFlushIsReal(t *testing.T) {
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

	// FlushWithReceipt's first half, and nothing after it: the process dies here.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart.
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

	committed := st2.CommittedWrite(id)
	t.Logf("after a crash between Flush and CommitReceipt: %d rows durable, "+
		"receipt remembered=%v", rows, committed)

	if rows == 0 {
		t.Fatal("the rows did not survive the flush, so this is not the window under test")
	}
	if committed {
		// If this ever becomes true the window is closed and this test should
		// be deleted, not left asserting the opposite of what is true.
		t.Fatal("the receipt IS remembered after a crash between the two halves: " +
			"the window is closed and this test is stale")
	}
	// The window is open: the rows are durable and a retry of the same id would
	// be treated as a first attempt.
	t.Logf("CONFIRMED: %d rows are durable and the id is not remembered, so a "+
		"client retry duplicates all of them", rows)
}
