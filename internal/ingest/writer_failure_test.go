package ingest

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// What a caller is told when the disk fails under it.
//
// The fault points are the real steps of the durable write, injected through
// storage.SetFaultHookForTest, so these exercise the same code an actual
// ENOSPC would. What is being asserted is not that the store fails -- the
// storage suite covers that -- but that the failure survives the two layers
// above it with its meaning intact: the caller gets an error rather than a
// silent success, and that error says whether retrying is worth anything and
// whether it duplicates.

// newFailWriter opens a store in a temp dir and returns a writer over it.
func newFailWriter(t *testing.T) (*Writer, *storage.Store) {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	w := NewWriterWorkers(s, 1)
	t.Cleanup(func() {
		w.Close()
		s.Close()
	})
	return w, s
}

// TestEveryWriteFaultReachesTheCaller sweeps every fault point in the durable
// write path and asserts none of them produces a successful flush.
//
// Enumerated rather than listed: a fault point added later is covered the day
// it exists. A hand-written list is a list that the next step of the write
// path quietly falls out of, and a write step whose failure is not surfaced
// is a write step that loses data with a 200.
func TestEveryWriteFaultReachesTheCaller(t *testing.T) {
	injected := errors.New("injected")

	nonWrite := map[string]bool{}
	for _, name := range storage.NonWriteFaultPointNames() {
		nonWrite[name] = true
	}
	for _, name := range storage.FaultPointNames() {
		if nonWrite[name] {
			// Points that are not steps of the write at all -- the crash
			// matrix's buffering and post-acknowledgement stops, and the
			// staged restore's post-rename hook. The list lives in the
			// storage package, next to the points themselves, so a new WRITE
			// step is covered here the day it exists rather than falling out
			// of a list kept in this file.
			continue
		}
		switch name {
		case "manifest-sync":
			// Handled by its own test below rather than skipped in silence.
			// It is the one point where a reported failure does NOT mean the
			// group is absent, and a bare skip is where that fact would go
			// unstated.
			continue
		}
		t.Run(name, func(t *testing.T) {
			w, _ := newFailWriter(t)
			hook, err := storage.FailAt(name, injected)
			if err != nil {
				t.Fatalf("arm %s: %v", name, err)
			}
			mark := w.Mark()
			w.Add(1, map[string]string{"_msg": "one"})

			restore := storage.SetFaultHookForTest(hook)
			ferr := w.FlushMark(mark)
			restore()

			if ferr == nil {
				t.Fatalf("%s: flush reported success with the write failed", name)
			}
			var we *WriteError
			if !errors.As(ferr, &we) {
				t.Fatalf("%s: flush returned %T (%v), want *WriteError", name, ferr, ferr)
			}
			if we.TotalGroups != 1 || we.FailedGroups != 1 {
				t.Fatalf("%s: %d of %d groups failed, want 1 of 1", name, we.FailedGroups, we.TotalGroups)
			}
			// One group, and it failed: nothing landed, so a retry is clean.
			if we.DuplicatesOnRetry() {
				t.Fatalf("%s: reported duplicate-on-retry with every group failed", name)
			}
			if !errors.Is(ferr, injected) {
				t.Fatalf("%s: the injected cause did not survive: %v", name, ferr)
			}
		})
	}
}

// A failure that only some of a caller's groups hit is the case a retry
// cannot be blind about: part of the payload is durable, and there is no
// idempotency key, so resending stores that part twice.
func TestPartialBatchFailureIsReportedAsDuplicating(t *testing.T) {
	w, _ := newFailWriter(t)

	// Fail the second group's temp-file creation and nothing else. The
	// counter is the discriminator rather than the file name, because the
	// name depends on ids this test should not have to predict.
	var mu sync.Mutex
	seen := 0
	hook := func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen == 2 {
			return syscall.ENOSPC
		}
		return nil
	}

	mark := w.Mark()
	restore := storage.SetFaultHookForTest(hook)
	// Two groups in one window: add rows, force a flush job, add more, then
	// FlushMark. Both jobs join the batch the mark named.
	w.Add(1, map[string]string{"_msg": "first"})
	w.mu.Lock()
	w.flushLocked()
	w.mu.Unlock()
	w.Add(2, map[string]string{"_msg": "second"})
	err := w.FlushMark(mark)
	restore()

	if err == nil {
		t.Fatal("a failed group reported success")
	}
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("got %T, want *WriteError", err)
	}
	if we.TotalGroups != 2 || we.FailedGroups != 1 {
		t.Fatalf("%d of %d groups failed, want 1 of 2", we.FailedGroups, we.TotalGroups)
	}
	if !we.Partial || !we.DuplicatesOnRetry() {
		t.Fatalf("one of two groups failed and it was not reported as partial: %+v", we)
	}
	if !strings.Contains(we.Error(), "duplicate") {
		t.Fatalf("the message does not mention duplication: %s", we.Error())
	}
	// ENOSPC is not fixed by trying again in a second.
	if we.Class != RetryAfterRepair {
		t.Fatalf("ENOSPC classified %v, want %v", we.Class, RetryAfterRepair)
	}
	if we.RetryAfter() != retryAfterRepair {
		t.Fatalf("Retry-After %v, want %v", we.RetryAfter(), retryAfterRepair)
	}
}

// The classification is the contract the status code is derived from, so it
// is asserted directly rather than only through a response.
func TestRetryClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryClass
	}{
		{"disk full", syscall.ENOSPC, RetryAfterRepair},
		{"disk full, wrapped", fmt.Errorf("append: %w", syscall.ENOSPC), RetryAfterRepair},
		{"read-only mount", syscall.EROFS, RetryAfterRepair},
		{"unwritable directory", syscall.EACCES, RetryAfterRepair},
		{"os permission", os.ErrPermission, RetryAfterRepair},
		{"io error", syscall.EIO, RetryAfterRepair},
		{"descriptor limit", syscall.EMFILE, RetrySoon},
		{"out of memory", syscall.ENOMEM, RetrySoon},
		{"writer closing", ErrWriterClosed, RetrySoon},
		{"corrupt group", fmt.Errorf("x: %w", storage.ErrCorruptGroup), RetryAfterRepair},
		{"unrecognised", errors.New("something new"), RetrySoon},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classify(c.err); got != c.want {
				t.Fatalf("classify(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// Every write failure carries a retry interval, and the interval separates
// "wait a second" from "someone has to fix the disk".
//
// There was a never-retry class, and a group that failed its own checksum the
// instant after being written was put in it -- answered 500, retryable=false,
// no Retry-After. That told a shipper to DROP data whose only symptom is that
// the bytes read back differ from the bytes written, which is a media error at
// least as often as it is a deterministic one. It is now a repair-class 503.
func TestRetryIntervalsSeparateTheTwoCases(t *testing.T) {
	badGroup := newWriteError(fmt.Errorf("x: %w", storage.ErrCorruptGroup), 1, 1)
	if !badGroup.Retryable() {
		t.Fatal("a group that would not read back was reported as never-retry")
	}
	if badGroup.HTTPStatus() != 503 {
		t.Fatalf("answered %d, want 503", badGroup.HTTPStatus())
	}
	if badGroup.RetryAfter() != retryAfterRepair {
		t.Fatalf("Retry-After %v, want %v", badGroup.RetryAfter(), retryAfterRepair)
	}

	full := newWriteError(syscall.ENOSPC, 1, 1)
	if full.HTTPStatus() != 503 || full.RetryAfter() != retryAfterRepair {
		t.Fatalf("ENOSPC answered %d after %v", full.HTTPStatus(), full.RetryAfter())
	}

	closing := newWriteError(ErrWriterClosed, 0, 0)
	if closing.RetryAfter() != retryAfterSoon {
		t.Fatalf("a closing writer says wait %v, want %v", closing.RetryAfter(), retryAfterSoon)
	}
}

// A failure at the LAST step of the durable write must leave nothing behind.
//
// dir-sync runs after os.Rename has already landed, so the group file is at
// its final name when the error is returned. That is the case the first
// version of the orphan cleanup missed: it described its own window as "every
// step after writeFileAtomic returns", and two steps inside writeFileAtomic
// are already past the point of no return.
//
// This test used to inject at partial-write, which fires BEFORE the rename --
// so writeFileAtomic's own deferred temp-file removal made it green and it
// guarded nothing this change added.
func TestPostRenameFailureLeavesNothingReadable(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w := NewWriterWorkers(s, 1)

	hook, herr := storage.FailAt("dir-sync", syscall.EIO)
	if herr != nil {
		t.Fatalf("arm: %v", herr)
	}
	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "truncated"})
	restore := storage.SetFaultHookForTest(hook)
	ferr := w.FlushMark(mark)
	restore()
	if ferr == nil {
		t.Fatal("a short write reported success")
	}
	w.Close()
	s.Close()

	// Nothing on disk from the failed write: no group file, and no temp file
	// that a later glob could adopt.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "group-") {
			t.Fatalf("a failed write left %s behind", e.Name())
		}
	}

	// And the store reopens clean and empty.
	s2, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	snap, err := s2.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	if n := len(snap.Groups); n != 0 {
		t.Fatalf("reopened store holds %d groups after a failed write", n)
	}
}

// SetFaultHookForTest is a test seam and must stay one. The guard is what
// keeps it from becoming a production switch, so it is asserted rather than
// trusted -- and asserting it needs the negative case, which only a
// subprocess without the testing flags can produce. What is checked here is
// the positive half: inside a test binary it works and restores cleanly.
func TestFaultHookRestores(t *testing.T) {
	hook, err := storage.FailAt("temp-create", errors.New("x"))
	if err != nil {
		t.Fatalf("arm: %v", err)
	}
	restore := storage.SetFaultHookForTest(hook)
	restore()

	w, _ := newFailWriter(t)
	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "after restore"})
	if err := w.FlushMark(mark); err != nil {
		t.Fatalf("the hook outlived its restore: %v", err)
	}
}

// A name that does not resolve must be an error, not a hook that injects
// nothing. A typo there produces a test that passes for the wrong reason.
func TestUnknownFaultNameIsRejected(t *testing.T) {
	if _, err := storage.FailAt("no-such-point", errors.New("x")); err == nil {
		t.Fatal("an unknown fault point was accepted")
	}
}

// The one point where a reported failure does not mean the group is absent.
//
// faultManifestSync fires after the record is synced and after m.apply has
// run, so the group IS committed and durable while AppendGroup returns an
// error. The caller is told "1 of 1 failed", which reads as "nothing landed,
// a retry is clean" -- and a retry would store the rows twice.
//
// This is crash-only in production: nothing installs a hook, and there is no
// real error between Sync succeeding and the return. It is asserted rather
// than skipped so that the semantics are written down, and so that a future
// change making this point reachable fails here instead of shipping.
func TestManifestSyncFaultCommitsDespiteTheError(t *testing.T) {
	w, s := newFailWriter(t)

	hook, err := storage.FailAt("manifest-sync", errors.New("injected"))
	if err != nil {
		t.Fatalf("arm: %v", err)
	}
	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "committed anyway"})
	restore := storage.SetFaultHookForTest(hook)
	ferr := w.FlushMark(mark)
	restore()

	if ferr == nil {
		t.Fatal("the injected fault produced no error")
	}
	snap, serr := s.Snapshot(0, 1<<62)
	if serr != nil {
		t.Fatalf("snapshot: %v", serr)
	}
	defer snap.Close()
	// The group is NOT in s.groups: AppendGroup unmapped and returned before
	// appending it. The manifest holds the id, so the disagreement is between
	// the store's two in-memory structures and is resolved by a reopen.
	if n := len(snap.Groups); n != 0 {
		t.Fatalf("the store shows %d groups; AppendGroup returned before indexing", n)
	}
	// Note what this does NOT assert: that the group file survived. A commit
	// that reached its sync is not an orphan whatever the error says, and the
	// isVisible guard in discardUncommitted is what keeps it -- but nothing
	// here checks the file, and a comment claiming otherwise is why that guard
	// read as covered when it is not.
	var we *WriteError
	if !errors.As(ferr, &we) {
		t.Fatalf("got %T, want *WriteError", ferr)
	}
	if we.DuplicatesOnRetry() {
		t.Fatal("reported duplicate-on-retry; the accounting cannot know this case")
	}
}

// A caller's batch must not age out of the writer's memory while the caller is
// still going to ask about it.
//
// FlushMark answered from a 64-entry ring of batches, and EVERY flush installed
// a new one whether or not the old one carried anything. So 64 flushes from any
// other caller on the tenant -- other requests, a syslog connection flushing per
// line, the FlushEvery timer, Close at shutdown -- evicted the batch a marked
// caller's rows were in. FlushMark then never waited on it, never saw its
// error, and returned nil: a 200 for rows that are not in the store.
func TestEvictedBatchIsNotReportedAsSuccess(t *testing.T) {
	w, s := newFailWriter(t)

	// ONLY the marked caller's group fails. Failing every temp-create made
	// every later batch carry the same ENOSPC, so FlushMark found an error in
	// a live hist entry and never consulted the outcome log at all -- the test
	// passed with the fold-in deleted, which is the mechanism it is named for.
	var mu sync.Mutex
	seen := 0
	hook := func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen == 1 {
			return syscall.ENOSPC
		}
		return nil
	}
	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "these rows must not vanish"})

	restore := storage.SetFaultHookForTest(hook)
	// The marked row becomes its own job before anyone else's flush runs.
	// The buffer is shared, so without this the first "someone else" flush
	// carries the marked row too and takes the ENOSPC with it -- which is a
	// real behaviour, and not the one this test is about.
	w.mu.Lock()
	w.flushLocked()
	w.batch = w.newBatchLocked()
	w.mu.Unlock()
	// Someone else's traffic, far past the ring's capacity, and all of it
	// succeeding. Each one takes its OWN mark and calls FlushMark, which is
	// what a request does: a plain Flush waits on every live batch and would
	// see the marked caller's ENOSPC, which is correct behaviour and not what
	// this test is about. Each flush has to CARRY a row, because an empty
	// batch is dropped rather than retired.
	for i := 0; i < batchHistory+16; i++ {
		m := w.Mark()
		w.Add(int64(1000+i), map[string]string{"_msg": "someone else"})
		if ferr := w.FlushMark(m); ferr != nil {
			t.Fatalf("someone else's request %d failed: %v", i, ferr)
		}
	}
	// Checked BEFORE the FlushMark that consumes it: the marked batch must be
	// out of the ring and in the outcome log, or the answer below comes from a
	// live entry and the fold-in is untested.
	w.mu.Lock()
	retired := len(w.outcomes)
	ringLen := len(w.hist)
	markInRing := false
	for _, b := range w.hist {
		if b.seq == mark {
			markInRing = true
		}
	}
	w.mu.Unlock()
	if retired == 0 {
		t.Fatalf("nothing was retired (hist=%d); this test is not testing eviction", ringLen)
	}
	if markInRing {
		t.Fatal("the marked batch is still in the ring; the outcome log is not exercised")
	}
	ferr := w.FlushMark(mark)
	restore()

	snap, serr := s.Snapshot(0, 1<<62)
	if serr != nil {
		t.Fatalf("snapshot: %v", serr)
	}
	stored := len(snap.Groups)
	snap.Close()

	if ferr == nil {
		t.Fatalf("FlushMark reported success; the store holds %d groups", stored)
	}
	if !errors.Is(ferr, syscall.ENOSPC) && !errors.Is(ferr, ErrDurabilityUnknown) {
		t.Fatalf("FlushMark returned %v; it says neither what failed nor that it does not know", ferr)
	}
}

// The same window, but with one group landed and one failed: the caller must
// still be told a retry can duplicate.
//
// This is the claim the change ADDED, so an eviction that hid it would be a
// new defect rather than an old one. With the failed batch aged out,
// failed == total over what remained and the caller was told nothing landed.
func TestEvictedBatchStillReportsPartial(t *testing.T) {
	w, _ := newFailWriter(t)

	var mu sync.Mutex
	seen := 0
	hook := func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen == 2 {
			return syscall.ENOSPC
		}
		return nil
	}

	mark := w.Mark()
	restore := storage.SetFaultHookForTest(hook)
	w.Add(1, map[string]string{"_msg": "first"})
	w.mu.Lock()
	w.flushLocked()
	w.mu.Unlock()
	w.Add(2, map[string]string{"_msg": "second"})
	w.mu.Lock()
	w.flushLocked()
	w.mu.Unlock()
	// Age both batches out of the ring. Each flush carries a row, or the
	// empty-drop retires nothing and the marked batch never leaves.
	for i := 0; i < batchHistory+16; i++ {
		w.Add(int64(1000+i), map[string]string{"_msg": "someone else"})
		_ = w.Flush()
	}
	w.mu.Lock()
	retired := len(w.outcomes)
	w.mu.Unlock()
	if retired == 0 {
		t.Fatal("nothing was retired; this test is not testing eviction")
	}
	err := w.FlushMark(mark)
	restore()

	if err == nil {
		t.Fatal("a failed group reported success after its batch aged out")
	}
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("got %T, want *WriteError", err)
	}
	if !we.DuplicatesOnRetry() {
		t.Fatalf("one group landed and one failed, reported %d of %d, partial=%v",
			we.FailedGroups, we.TotalGroups, we.Partial)
	}
}

// Past even the outcome log, the writer must say it does not know rather than
// say nothing failed.
func TestUnanswerableMarkIsAnError(t *testing.T) {
	w, _ := newFailWriter(t)
	mark := w.Mark()

	// Every flush here carries a job, so every batch leaves an outcome behind
	// and the log itself overflows.
	for i := 0; i < outcomeHistory+batchHistory+8; i++ {
		w.Add(int64(i+1), map[string]string{"_msg": "row"})
		if err := w.Flush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	err := w.FlushMark(mark)
	if err == nil {
		t.Fatal("a mark older than anything remembered was answered with success")
	}
	if !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("got %v, want ErrDurabilityUnknown", err)
	}
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("got %T, want *WriteError", err)
	}
	if !we.DuplicatesOnRetry() {
		t.Fatal("an unknown outcome must warn that a retry can duplicate")
	}
}

// A batch may not be retired while one of its jobs is still running.
//
// The outcome log froze a retired batch's counters. FlushMark waits only on
// batches at or after its own mark, so a later caller never blocks on an older
// one, and enough later flushes retired a batch whose job was still in flight
// -- snapshotting "one job, none failed" for a job that went on to fail with
// ENOSPC. FlushMark then folded in that snapshot and returned nil for rows
// that are not in the store: the same 200-for-lost-rows failure the outcome
// log was added to prevent, moved from the ring into the log.
func TestRunningJobIsNotRetiredWithFrozenCounters(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	// Several workers, so the other callers' flushes proceed while one job is
	// stalled. With a single worker they would queue behind it and the
	// interleaving could not happen at all.
	w := NewWriterWorkers(s, 4)
	defer func() {
		w.Close()
		s.Close()
	}()

	release := make(chan struct{})
	var releaseOnce sync.Once
	freeWorker := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered AFTER the writer cleanup, so it runs BEFORE it: defers are
	// LIFO, and w.Close flushes, which waits on the stalled batch. Without
	// this the test deadlocks in its own cleanup on any early failure, which
	// turns a clear assertion failure into a timeout.
	defer freeWorker()

	var once sync.Once
	stalled := make(chan struct{})
	hook := func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		first := false
		once.Do(func() { first = true })
		if !first {
			return nil
		}
		close(stalled)
		<-release
		return syscall.ENOSPC
	}
	restore := storage.SetFaultHookForTest(hook)
	defer restore()

	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "must not be reported stored"})
	// Hand it to the pool without waiting for it, and swap in a fresh batch
	// the way Flush would. Without the swap the later callers' marks are the
	// stalled batch's own seq, so their FlushMark waits on it and the
	// interleaving this test is about cannot happen.
	w.mu.Lock()
	w.flushLocked()
	w.batch = w.newBatchLocked()
	w.mu.Unlock()
	<-stalled

	// Everyone else's traffic, far past the ring. Each one takes its OWN mark
	// and calls FlushMark, which is what a request does -- and which, unlike
	// Flush, does not wait on a batch older than its mark. That is the whole
	// exposure: a plain Flush waits on every live batch and would block here.
	for i := 0; i < batchHistory+16; i++ {
		m := w.Mark()
		w.Add(int64(1000+i), map[string]string{"_msg": "someone else"})
		_ = w.FlushMark(m)
	}

	// The stalled batch must still be answerable: either in the ring, or with
	// FINAL counters in the log. It may not be in the log with a frozen zero.
	w.mu.Lock()
	var frozen *batchOutcome
	for i := range w.outcomes {
		if w.outcomes[i].seq == mark {
			frozen = &w.outcomes[i]
		}
	}
	inRing := false
	for _, b := range w.hist {
		if b.seq == mark {
			inRing = true
		}
	}
	w.mu.Unlock()
	if frozen != nil && frozen.failed == 0 {
		t.Fatalf("the stalled batch was retired with failed=%d while its job was still running", frozen.failed)
	}
	if !inRing && frozen == nil {
		t.Fatal("the stalled batch is neither in the ring nor in the outcome log")
	}

	freeWorker()
	if ferr := w.FlushMark(mark); ferr == nil {
		t.Fatal("FlushMark reported success for a row whose only group failed with ENOSPC")
	} else if !errors.Is(ferr, syscall.ENOSPC) {
		t.Fatalf("FlushMark returned %v, want the ENOSPC that failed the write", ferr)
	}
}

// dir-open is the other post-rename fault point, and it needs its own
// permanent cleanliness test: the sweep runs it but asserts only that the
// caller gets an error, so it passes with the orphan cleanup reverted.
func TestDirOpenFailureLeavesNothingReadable(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w := NewWriterWorkers(s, 1)

	hook, herr := storage.FailAt("dir-open", syscall.EMFILE)
	if herr != nil {
		t.Fatalf("arm: %v", herr)
	}
	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "post-rename"})
	restore := storage.SetFaultHookForTest(hook)
	ferr := w.FlushMark(mark)
	restore()
	if ferr == nil {
		t.Fatal("a dir-open failure reported success")
	}
	w.Close()
	s.Close()

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "group-") {
			t.Fatalf("a failed write left %s behind", e.Name())
		}
	}
}

// The unanswerable-drop watermark only ever rises.
//
// `retireLocked` assigned oldestAnswerable unconditionally, and the ceiling
// path in newBatchLocked also moves it -- so a normal retire following an
// unanswerable drop lowered it again and un-hid a batch that is in neither the
// ring nor the outcome log. FlushMark then answered nil for a group that had
// failed. Reproduced end to end with two stalled workers; this asserts the
// invariant directly, which is cheaper and covers every route to it.
func TestOldestAnswerableNeverMovesBackwards(t *testing.T) {
	w, _ := newFailWriter(t)

	w.mu.Lock()
	w.oldestAnswerable = 5000
	w.mu.Unlock()

	// Enough job-carrying flushes to overflow the OUTCOME LOG, which is the
	// path that used to lower the watermark. The first batchHistory flushes
	// retire nothing -- they only fill the ring -- so the count has to clear
	// both.
	for i := 0; i < outcomeHistory+batchHistory+16; i++ {
		m := w.Mark()
		w.Add(int64(i+1), map[string]string{"_msg": "row"})
		// The return is not checked: the watermark was raised past every mark
		// this loop takes, so ErrDurabilityUnknown is the CORRECT answer here
		// and says nothing about the invariant under test.
		_ = w.FlushMark(m)
		w.mu.Lock()
		got := w.oldestAnswerable
		w.mu.Unlock()
		if got < 5000 {
			t.Fatalf("oldestAnswerable fell to %d from 5000 at flush %d", got, i)
		}
	}
}

// The ring has a hard ceiling, and a stall must not let client traffic grow it
// without bound.
func TestHistIsBoundedUnderAStalledJob(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	w := NewWriterWorkers(s, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	freeWorker := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		w.Close()
		s.Close()
	}()
	defer freeWorker()

	var once sync.Once
	stalled := make(chan struct{})
	restore := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		first := false
		once.Do(func() { first = true })
		if !first {
			return nil
		}
		close(stalled)
		<-release
		return nil
	})
	defer restore()

	w.Add(1, map[string]string{"_msg": "pinned"})
	w.mu.Lock()
	w.flushLocked()
	w.batch = w.newBatchLocked()
	w.mu.Unlock()
	<-stalled

	for i := 0; i < maxHistory+256; i++ {
		m := w.Mark()
		w.Add(int64(1000+i), map[string]string{"_msg": "traffic"})
		_ = w.FlushMark(m)
	}
	w.mu.Lock()
	n := len(w.hist)
	w.mu.Unlock()
	if n > maxHistory {
		t.Fatalf("hist grew to %d with a stalled job; the ceiling is %d", n, maxHistory)
	}
}

// A closed writer must not claim a retry is clean: Close FLUSHES before it
// sets closed, so an in-flight handler's rows are routinely durable.
func TestClosedWriterDoesNotClaimACleanRetry(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	w := NewWriterWorkers(s, 1)

	mark := w.Mark()
	w.Add(1, map[string]string{"_msg": "durable after Close"})
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	snap, _, err := s.SnapshotAllWithSeq()
	if err != nil {
		t.Fatal(err)
	}
	stored := len(snap.Groups)
	snap.Close()
	if stored == 0 {
		t.Fatal("Close stored nothing; this test's premise is wrong")
	}

	err = w.FlushMark(mark)
	if err == nil {
		t.Fatal("FlushMark on a closed writer reported success")
	}
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("got %T, want *WriteError", err)
	}
	if !we.DuplicatesOnRetry() {
		t.Fatalf("the store holds %d groups and the caller was told a retry is clean", stored)
	}
	if !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("the cause was lost: %v", err)
	}
}

// The counts have a unit, and the parallel path's is not "groups".
func TestWriteErrorUnits(t *testing.T) {
	ordinary := newWriteError(syscall.ENOSPC, 1, 2)
	if ordinary.Units() != "groups" {
		t.Fatalf("ordinary units %q, want groups", ordinary.Units())
	}
	shards := &ParallelWriteError{Shards: 4, Failed: 2, Err: syscall.ENOSPC}
	var we *WriteError
	if !errors.As(shards, &we) {
		t.Fatal("errors.As did not reach a WriteError")
	}
	if we.Units() != "shard writers" {
		t.Fatalf("parallel units %q, want shard writers", we.Units())
	}
	if !strings.Contains(we.Error(), "shard writers") {
		t.Fatalf("the message does not name the unit: %s", we.Error())
	}
}

// errors.As on a sharded failure must see the WHOLE write, not one shard.
func TestParallelAggregationSeesEveryShard(t *testing.T) {
	inner := &WriteError{Err: syscall.ENOSPC, Class: RetryAfterRepair,
		Partial: false, FailedGroups: 1, TotalGroups: 1}
	for _, c := range []struct {
		name        string
		pe          *ParallelWriteError
		wantPartial bool
	}{
		{"all failed, none partial", &ParallelWriteError{Shards: 4, Failed: 4, Err: inner}, false},
		{"all failed, one partial", &ParallelWriteError{Shards: 4, Failed: 4, Err: inner, Partial: true}, true},
		{"some survived", &ParallelWriteError{Shards: 4, Failed: 2, Err: inner}, true},
		{"single shard, not partial", &ParallelWriteError{Shards: 1, Failed: 1, Err: inner}, false},
		{"single shard, partial", &ParallelWriteError{Shards: 1, Failed: 1, Err: inner, Partial: true}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var we *WriteError
			if !errors.As(c.pe, &we) {
				t.Fatal("errors.As did not reach a WriteError")
			}
			if we.DuplicatesOnRetry() != c.wantPartial {
				t.Fatalf("duplicateOnRetry=%v, want %v (%d of %d failed, shardPartial=%v)",
					we.DuplicatesOnRetry(), c.wantPartial, c.pe.Failed, c.pe.Shards, c.pe.Partial)
			}
			// And the underlying cause still reaches errors.Is.
			if !errors.Is(c.pe, syscall.ENOSPC) {
				t.Fatal("the errno did not survive the aggregation")
			}
		})
	}
}

// Partial-ness is collected from the shard's OWN error, driven end to end.
//
// A previous test built a ParallelWriteError by hand and asserted
// writeError()'s `|| e.Partial` arm. That covers the aggregation and not the
// collection: deleting the code that SETS Partial left the suite green, while
// docs/wrong.md said the fix "was verified by reverting it".
//
// This drives the serial branch, which is the one round 4 fixed and the one
// every host with fewer than six cores takes: cfg.ShardsFor() returns 0 below two
// shards, and the wrapper then synthesizes a ParallelWriteError around one
// writer's error. Dropping Partial there made As replace an accurate inner
// answer with a worse one.
//
// The body has to exceed FlushRows so the single writer produces two groups
// and can be partial at all. That is 128Ki rows; a shard cannot be partial
// with fewer, which is why the four-shard version of this test could not
// reach the arm it was written for.
func TestSerialFallbackCollectsPartial(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a multi-megabyte body")
	}
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	// Fail the SECOND group only, so the first lands and the writer's own
	// error is partial.
	var mu sync.Mutex
	seen := 0
	restore := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen == 2 {
			return syscall.ENOSPC
		}
		return nil
	})
	defer restore()

	line := []byte(`{"_msg":"line","_time":"1700000000000000000"}` + "\n")
	body := make([]byte, 0, len(line)*(FlushRows+64))
	for i := 0; i < FlushRows+64; i++ {
		body = append(body, line...)
	}
	if len(body) < MinParallelBytes {
		t.Fatalf("body is %d bytes, under MinParallelBytes %d", len(body), MinParallelBytes)
	}

	_, _, werr := IngestJSONLinesParallelCfg(s, body, func() int64 { return 1 },
		ParallelConfig{Shards: 1}, nil)
	if werr == nil {
		mu.Lock()
		saw := seen
		mu.Unlock()
		t.Fatalf("no failure; %d temp-creates were seen", saw)
	}

	var pe *ParallelWriteError
	if !errors.As(werr, &pe) {
		t.Fatalf("got %T, want *ParallelWriteError", werr)
	}
	if pe.Shards != 1 || pe.Failed != 1 {
		t.Fatalf("%d of %d shards failed; this test needs the serial branch", pe.Failed, pe.Shards)
	}
	if !pe.Partial {
		t.Fatal("the writer's error was partial and the wrapper did not collect it")
	}
	var we *WriteError
	if !errors.As(werr, &we) {
		t.Fatal("errors.As did not reach a WriteError")
	}
	if !we.DuplicatesOnRetry() {
		t.Fatal("a group landed and the client was told a retry is clean")
	}
}

// Partial: true on a closed writer holds at the Flush site as well as the
// FlushMark one. Only one of the two was covered.
func TestClosedWriterFlushAlsoReportsPartial(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	w := NewWriterWorkers(s, 1)

	w.Add(1, map[string]string{"_msg": "durable after Close"})
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	err = w.Flush()
	if err == nil {
		t.Fatal("Flush on a closed writer reported success")
	}
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("got %T, want *WriteError", err)
	}
	if !we.DuplicatesOnRetry() {
		t.Fatal("Flush on a closed writer said a retry is clean; Close flushed those rows")
	}
}

// A batch abandoned at the ceiling leaves `w.live` with the ring.
//
// Leaving it there kept both problems the ceiling was added to solve: its
// counters -- final by then -- were folded into the next unrelated plain
// Flush, and because Flush waits on all of `live`, every Flush and every
// Writer.Close still blocked on the stalled job.
func TestCeilingDropReleasesFlushAndDoesNotLeakItsAnswer(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	w := NewWriterWorkers(s, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	freeWorker := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		w.Close()
		s.Close()
	}()
	defer freeWorker()

	var once sync.Once
	stalled := make(chan struct{})
	restore := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		first := false
		once.Do(func() { first = true })
		if !first {
			return nil
		}
		close(stalled)
		<-release
		return syscall.ENOSPC
	})
	defer restore()

	w.Add(1, map[string]string{"_msg": "pinned"})
	w.mu.Lock()
	w.flushLocked()
	w.batch = w.newBatchLocked()
	w.mu.Unlock()
	<-stalled

	// Past the ceiling, all of it succeeding.
	for i := 0; i < maxHistory+256; i++ {
		m := w.Mark()
		w.Add(int64(1000+i), map[string]string{"_msg": "traffic"})
		_ = w.FlushMark(m)
	}
	w.mu.Lock()
	inLive := false
	for _, b := range w.live {
		if b.outstanding.Load() != 0 {
			inLive = true
		}
	}
	w.mu.Unlock()
	if inLive {
		t.Fatal("the stalled batch is still in live; Flush and Close still block on it")
	}

	// And a plain Flush now returns promptly, with nobody else's answer.
	done := make(chan error, 1)
	go func() { done <- w.Flush() }()
	select {
	case ferr := <-done:
		if ferr != nil {
			t.Fatalf("an unrelated Flush was handed the stalled batch's answer: %v", ferr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an unrelated Flush is still blocked on the abandoned batch")
	}
}

// A mark for a batch the ceiling abandoned must answer "unknown", never nil.
// The ceiling drop must raise the watermark past the batch it abandons.
//
// Driven directly rather than through traffic, because traffic cannot isolate
// it: the flushes that push the ring over the ceiling also overflow the
// outcome log, and the log's own advance lands past the stalled batch's seq
// for its own reasons. A test built that way passes with this line deleted --
// which it did, on the first attempt.
//
// So the ring is constructed: one batch with an outstanding job at the front,
// maxHistory job-less batches behind it, and newBatchLocked called once. The
// front batch is over the ceiling and still running, which is the only state
// that reaches the branch.
func TestCeilingDropRaisesTheWatermarkPastWhatItAbandons(t *testing.T) {
	w, _ := newFailWriter(t)

	w.mu.Lock()
	defer w.mu.Unlock()

	// A batch that will look stalled: one job handed out, none returned.
	stalled := &flushBatch{seq: 1}
	stalled.jobs.Add(1)
	stalled.outstanding.Add(1)

	w.hist = w.hist[:0]
	w.hist = append(w.hist, stalled)
	// JOB-LESS behind it, deliberately. A batch with jobs retires into the
	// outcome log, and the log's own overflow raises the watermark for its own
	// reasons -- past the stalled batch's seq, which makes the assertion below
	// pass with the line under test deleted. Two earlier versions of this test
	// failed exactly that way. retireLocked returns early at jobs==0, so these
	// leave the watermark alone and only the drop can move it.
	for i := 0; i < maxHistory; i++ {
		w.hist = append(w.hist, &flushBatch{seq: uint64(2 + i)})
	}
	w.nextSeq = uint64(maxHistory + 2)
	w.oldestAnswerable = 0

	w.batch = w.newBatchLocked()

	for _, b := range w.hist {
		if b == stalled {
			t.Fatal("the stalled batch is still in the ring past the ceiling")
		}
	}
	for _, o := range w.outcomes {
		if o.seq == stalled.seq {
			t.Fatal("the stalled batch was retired into the log with counters that can still change")
		}
	}
	// In neither, so only the watermark can answer for it.
	if w.oldestAnswerable <= stalled.seq {
		t.Fatalf("batch %d was abandoned and the watermark is %d; its mark answers nil",
			stalled.seq, w.oldestAnswerable)
	}
}

// The SHARDED loop collects Partial from every shard, not from the first
// error recorded.
//
// The serial branch has its own test above; this is the other half, and the
// one round 4's record claimed was covered when it was not. It needs two
// shards each carrying more than FlushRows rows, because a shard cannot be
// partial with fewer -- one group per shard means every shard is wholly
// failed or wholly durable.
func TestShardedLoopCollectsPartial(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a body of a few hundred thousand lines")
	}
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	// Let the FIRST group land and fail every group after it. Whichever shard
	// wins that first write lands one group and fails its second, so it is
	// partial; the other fails outright. Both shards fail, which is the point:
	// with Failed == Shards the `Failed < Shards` arm cannot answer, and
	// duplicateOnRetry rests entirely on what the loop collected. Failing only
	// a later group leaves a shard surviving, and the test then passes with
	// the collection deleted -- which it did.
	var mu sync.Mutex
	seen := 0
	restore := storage.SetFaultHookForTest(func(p storage.FaultPoint) error {
		if storage.FaultPointString(p) != "temp-create" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen >= 2 {
			return syscall.ENOSPC
		}
		return nil
	})
	defer restore()

	const shards = 2
	line := []byte(`{"_msg":"line","_time":"1700000000000000000"}` + "\n")
	perShard := FlushRows + 64
	body := make([]byte, 0, len(line)*perShard*shards)
	for i := 0; i < perShard*shards; i++ {
		body = append(body, line...)
	}

	_, _, werr := IngestJSONLinesParallelCfg(s, body, func() int64 { return 1 },
		ParallelConfig{Shards: shards}, nil)
	if werr == nil {
		mu.Lock()
		saw := seen
		mu.Unlock()
		t.Fatalf("no shard failed; %d temp-creates were seen", saw)
	}

	var pe *ParallelWriteError
	if !errors.As(werr, &pe) {
		t.Fatalf("got %T, want *ParallelWriteError", werr)
	}
	if pe.Shards < 2 {
		t.Fatalf("%d shards; this test needs the sharded branch", pe.Shards)
	}
	var we *WriteError
	if !errors.As(werr, &we) {
		t.Fatal("errors.As did not reach a WriteError")
	}
	if pe.Failed != pe.Shards {
		t.Fatalf("%d of %d shards failed; this test needs every shard to fail, "+
			"or the Failed<Shards arm answers instead of the collection",
			pe.Failed, pe.Shards)
	}
	if !pe.Partial {
		t.Fatalf("every shard failed, one of them after landing a group, and the "+
			"loop collected Partial=false (%d temp-creates seen)", seen)
	}
	if !we.DuplicatesOnRetry() {
		t.Fatalf("%d of %d shards failed, collected Partial=%v, and the client "+
			"was told a retry is clean", pe.Failed, pe.Shards, pe.Partial)
	}
}
