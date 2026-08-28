package storage

import (
	"errors"
	"fmt"
	"testing"
)

// Receipts, at the storage layer.

func receiptStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func oneRow(t *testing.T, ts int64, msg string) *Group {
	t.Helper()
	d := BuildDict([]string{msg})
	return &Group{Rows: 1, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{ts}},
		{Name: "_msg", Type: ColDict, Dict: &d},
	}}
}

// A second append under the same write id writes nothing.
func TestASecondAppendUnderOneWriteIDIsRefused(t *testing.T) {
	s, _ := receiptStore(t)
	const id WriteID = "aaaabbbbccccdddd"

	if _, err := s.AppendGroupIdempotent(oneRow(t, 1, "first"), id); err != nil {
		t.Fatal(err)
	}
	before := len(s.groups)

	_, err := s.AppendGroupIdempotent(oneRow(t, 2, "again"), id)
	if !errors.Is(err, ErrDuplicateWrite) {
		t.Fatalf("%v, want ErrDuplicateWrite", err)
	}
	if len(s.groups) != before {
		t.Fatalf("groups went %d -> %d on a duplicate write id", before, len(s.groups))
	}
	if !s.CommittedWrite(id) {
		t.Error("the id is not recorded as committed")
	}
}

// Receipts survive a reopen: a retry after a restart must still be recognised,
// or every restart is a window in which every in-flight retry duplicates.
func TestReceiptsSurviveAReopen(t *testing.T) {
	dir := t.TempDir()
	const id WriteID = "0123456789abcdef"

	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendGroupIdempotent(oneRow(t, 1, "x"), id); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.CommittedWrite(id) {
		t.Fatal("a committed write id was forgotten across a restart")
	}
	if _, err := s2.AppendGroupIdempotent(oneRow(t, 2, "y"), id); !errors.Is(err, ErrDuplicateWrite) {
		t.Fatalf("%v, want the retry refused after a reopen", err)
	}
}

// CommitReceipt records an id without a group -- the path a batching writer
// takes, where the rows are already durable in some earlier group.
func TestCommitReceiptRecordsWithoutAGroup(t *testing.T) {
	s, dir := receiptStore(t)
	const id WriteID = "feedfacefeedface"
	if _, err := s.AppendGroup(oneRow(t, 1, "x")); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitReceipt(id); err != nil {
		t.Fatal(err)
	}
	if !s.CommittedWrite(id) {
		t.Fatal("not recorded")
	}
	// Idempotent itself: committing twice is not an error.
	if err := s.CommitReceipt(id); err != nil {
		t.Fatalf("committing the same receipt twice: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.CommittedWrite(id) {
		t.Fatal("a receipt committed without a group did not survive a reopen")
	}
}

// The set is bounded, and the bound is the honest limit of the guarantee: a
// retry that arrives after maxReceipts further writes is no longer recognised
// and WILL duplicate.
func TestTheReceiptSetIsBounded(t *testing.T) {
	rs := newReceiptSet()
	first := WriteID("first")
	rs.add(first)
	for i := 0; i < maxReceipts; i++ {
		rs.add(WriteID(fmt.Sprintf("id-%d", i)))
	}
	if rs.count() > maxReceipts {
		t.Fatalf("%d receipts remembered against a bound of %d", rs.count(), maxReceipts)
	}
	if rs.has(first) {
		t.Error("the oldest id was not evicted; the set is unbounded")
	}
	if !rs.has(WriteID(fmt.Sprintf("id-%d", maxReceipts-1))) {
		t.Error("the newest id was evicted")
	}
}

// An id must be usable before it reaches the manifest: it is
// attacker-controlled, and it is written into a file the store replays at
// startup.
func TestWriteIDValidation(t *testing.T) {
	for _, ok := range []string{"0123456789abcdef", "AABBCCDD", "aaaaaaaa"} {
		if !ValidWriteID(ok) {
			t.Errorf("%q was refused", ok)
		}
	}
	for _, bad := range []string{
		"", "short", "zzzzzzzz", "0123-4567-89ab",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0", // 65
	} {
		if ValidWriteID(bad) {
			t.Errorf("%q was accepted", bad)
		}
	}
	// Minted ids pass their own validation.
	id, err := NewWriteID()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidWriteID(string(id)) {
		t.Fatalf("a minted id %q does not validate", id)
	}
	// And two are different: a counter would collide across routers, which is
	// the multi-writer case receipts exist for.
	id2, _ := NewWriteID()
	if id == id2 {
		t.Fatal("two minted write ids are equal")
	}
}
