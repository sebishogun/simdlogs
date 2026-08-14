package storage

import (
	"archive/tar"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The crash-recovery matrix: SIGKILL at every persistence phase, then reopen
// and assert what survived.
//
// The contract, and every clause of it is a separate way to lose data:
//
//   - **No partial group.** A group file that was renamed into place but never
//     committed, or a temp file the crash left behind, must not be adopted at
//     the next open. Adopting one resurrects a batch nobody acknowledged, and
//     in the temp-file case a truncated one.
//   - **Every acknowledged batch present, exactly once.** Acknowledged means
//     AppendGroup returned: the group file is fsynced, renamed, its directory
//     synced, and the manifest record synced. If a crash after that loses it,
//     the acknowledgement was a lie. If a crash makes it appear twice, a retry
//     duplicates rows.
//   - **No uncommitted batch visible.** The manifest sync is the commit point.
//     A record written but not synced is not a commit, and the group it names
//     must be invisible after recovery.
//   - **No duplicate rows.** The strongest of the four: it fails for any
//     double-adoption the other three miss.
//
// The phases come from the durable write's own steps rather than from a guess
// about where a crash is likely, because the interesting ones are exactly the
// boundaries: on either side of the file sync, on either side of the rename,
// and on either side of the manifest sync.

// crashPhases is every point the helper can stop at, in the order they occur.
var crashPhases = []string{
	"buffering",     // rows in hand, nothing on disk
	"temp-create",   // the temp file does not exist yet
	"partial-write", // the temp file exists and is EMPTY: the fault fires
	//                    before the write, so no phase produces a torn file
	"file-sync",       // all bytes written, none guaranteed durable
	"file-close",      // synced, descriptor not yet closed
	"rename",          // durable under the temp name, not yet under the real one
	"dir-open",        // renamed, the directory entry not yet durable
	"dir-sync",        // renamed, mid-sync of the directory
	"manifest-append", // the commit record is written and NOT synced
	"manifest-sync",   // the commit record IS synced: the commit point
	"post-ack",        // everything done and returned to the caller
}

// phasesCommittingTheLastBatch names the phases after which the last batch is
// expected to be readable.
//
// WHAT A PROCESS KILL CAN AND CANNOT ESTABLISH -- the reason manifest-append
// is in this list even though its record was never fsynced:
//
// SIGKILL destroys the PROCESS. It does not destroy the PAGE CACHE. A write()
// that returned but was never fsynced is still held by the kernel, so the next
// open reads it back and the batch is visible. That is correct behaviour, not
// a defect: only a power loss or a kernel crash discards those pages.
//
// So this matrix proves the phases that a process kill can distinguish -- no
// partial group adopted, no temp file adopted, no acknowledged batch lost, no
// duplicate rows -- and it does NOT prove the fsync boundary. Testing that
// needs the unsynced writes actually dropped: dm-flakey with drop_writes, a
// filesystem image discarded at the block layer, or an LD_PRELOAD that turns
// fsync into a no-op and the process into a crash. Recorded in docs/wrong.md
// rather than left as a passing test that appears to cover it.
//
// The phase is kept in the matrix because the other three clauses still apply
// to it, and because a change that made this batch INVISIBLE after a process
// kill would be a real regression -- it would mean a record the kernel still
// holds was discarded on replay.
var phasesCommittingTheLastBatch = map[string]bool{
	// The commit record is written and readable from the page cache, which is
	// what the next open sees after a process kill.
	"manifest-append": true,
	"manifest-sync":   true,
	"post-ack":        true,
}

func TestCrashRecoveryMatrix(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the matrix SIGKILLs a child process; not portable to %s", runtime.GOOS)
	}
	for _, phase := range crashPhases {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			acked, crashed := runCrashChild(t, dir, phase)
			if !crashed {
				t.Fatalf("the child did not crash at %q; the phase is unreachable "+
					"and this subtest proves nothing", phase)
			}

			// What the child acknowledged is what must survive -- and WHICH
			// batches it acknowledged is asserted, not just taken.
			//
			// Deriving the expectation purely from the child's output gives it
			// no lower bound: if the fault fires earlier than intended, acked
			// is empty, the presence loop below runs zero times, and the only
			// surviving clauses are satisfied by an EMPTY DIRECTORY. Measured
			// by moving the crash to batch 0: eight of the eleven subtests
			// still passed against a store holding a 0-byte MANIFEST and
			// nothing else.
			wantAcked := crashBatches - 1
			if phase == "post-ack" {
				// post-ack fires after the last batch was acknowledged.
				wantAcked = crashBatches
			}
			if len(acked) != wantAcked {
				t.Fatalf("the child acknowledged %v at %q, want %d batches: the crash "+
					"fired somewhere other than where this phase claims", acked, phase, wantAcked)
			}
			for i, b := range acked {
				if b != i {
					t.Fatalf("the child acknowledged %v at %q, want 0..%d in order",
						acked, phase, wantAcked-1)
				}
			}
			logLeftoverTempFiles(t, dir)
			st := reopenStore(t, dir)
			defer st.Close()

			got := storedBatches(t, st)
			for _, b := range acked {
				// Three-way, not two. countOf returns -1 for a PARTIAL batch,
				// which is neither 0 nor > 1 -- so an if/else-if let a torn
				// group adopted for an acknowledged batch pass silently, and
				// that is clause one of the contract at the top of this file.
				switch n := count(got, b); {
				case n == 0:
					t.Errorf("batch %d was ACKNOWLEDGED and is gone after a crash at %q", b, phase)
				case n < 0:
					t.Errorf("batch %d is PARTIAL after a crash at %q: a torn group was adopted", b, phase)
				case n > 1:
					t.Errorf("batch %d appears %d times after a crash at %q: a retry would duplicate rows",
						b, n, phase)
				}
			}

			// The batch the crash interrupted. Visible only if the crash was
			// after the commit point.
			last := crashBatches - 1
			// count returns -1 for a partial batch. `> 0` read that as
			// ABSENT, which passes at every phase where the batch is not
			// expected -- so a torn adoption of the interrupted batch was
			// invisible to this check as well.
			n := count(got, last)
			if n < 0 {
				t.Errorf("the batch interrupted at %q is PARTIAL: a torn group was adopted", phase)
			}
			has := n > 0
			want := phasesCommittingTheLastBatch[phase]
			if has && !want {
				t.Errorf("the batch interrupted at %q is VISIBLE, but its commit record "+
					"was never synced: an unacknowledged batch became readable", phase)
			}
			if !has && want {
				t.Errorf("the batch interrupted at %q is MISSING, but its commit record "+
					"was synced before the crash: a durable commit was lost", phase)
			}

			assertNoDuplicateRows(t, st)
		})
	}
}

// logLeftoverTempFiles records the temp files a crash left behind. It does not
// fail: a leftover temp file is EXPECTED, since no defer ran, and what must
// not happen is the store adopting one. That property is established by the
// reopen below and by TestCrashLeftoverTempFileIsIgnored, not here.
//
// It was called assertNoTempFiles, which named a failure the body never
// produces.
func logLeftoverTempFiles(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Logf("crash left %s behind, as it must: no defer ran", e.Name())
		}
	}
}

func reopenStore(t *testing.T, dir string) *Store {
	t.Helper()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("the store does not reopen after a crash: %v", err)
	}
	return st
}

// storedBatches returns the batch number of every row in the store, so both
// presence and multiplicity can be asserted.
func storedBatches(t *testing.T, st *Store) []int {
	t.Helper()
	var out []int
	for _, v := range storedValues(t, st) {
		// "b<batch>r<row>"
		i := strings.IndexByte(v, 'r')
		if len(v) < 2 || v[0] != 'b' || i < 0 {
			t.Fatalf("unexpected value %q in the batch column", v)
		}
		n, err := strconv.Atoi(v[1:i])
		if err != nil {
			t.Fatalf("unexpected value %q in the batch column: %v", v, err)
		}
		out = append(out, n)
	}
	return out
}

// storedValues reads every row's batch value across the whole store, over the
// full time range so nothing is excluded by a window.
func storedValues(t *testing.T, st *Store) []string {
	t.Helper()
	var out []string
	for _, r := range st.Groups(0, int64(1)<<62) {
		idx, dict := r.DictIndices("batch")
		for _, ix := range idx {
			if int(ix) >= len(dict) {
				t.Fatalf("dictionary index %d out of range (%d entries): a partial group was adopted",
					ix, len(dict))
			}
			out = append(out, dict[ix])
		}
	}
	return out
}

// count returns how many COMPLETE batches of n are present. A batch is
// crashBatchRows rows, so a partial one is itself a defect.
func count(all []int, n int) int { return countOf(all, n, crashBatchRows) }

func countOf(all []int, n, perBatch int) int {
	rows := 0
	for _, v := range all {
		if v == n {
			rows++
		}
	}
	if rows == 0 {
		return 0
	}
	if rows%perBatch != 0 {
		return -1 // a partial batch: neither absent nor present once
	}
	return rows / perBatch
}

// assertNoDuplicateRows is the strongest clause: every (batch, row) pair
// appears at most once across the whole store.
func assertNoDuplicateRows(t *testing.T, st *Store) {
	t.Helper()
	seen := map[string]int{}
	for _, v := range storedValues(t, st) {
		seen[v]++
	}
	var dupes []string
	for v, n := range seen {
		if n > 1 {
			dupes = append(dupes, v)
		}
	}
	if len(dupes) > 0 {
		sort.Strings(dupes)
		t.Errorf("%d rows appear more than once after recovery: %v", len(dupes), dupes)
	}
}

// A crash leaves a temp file behind, and the next open must IGNORE it -- not
// adopt it as a group, and not fail to open because of it. This is the
// specific failure the group-*.bin glob would have: a temp file named to match
// would be read as a truncated group.
func TestCrashLeftoverTempFileIsIgnored(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(crashGroup(0)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Exactly what a crash between the write and the rename leaves: a temp
	// file holding a HALF-written group.
	blob := crashGroup(1).Marshal()
	tmp := filepath.Join(dir, "group-99.bin.tmp")
	if err := os.WriteFile(tmp, blob[:len(blob)/2], DataFileMode); err != nil {
		t.Fatal(err)
	}

	st2 := reopenStore(t, dir)
	defer st2.Close()
	got := storedBatches(t, st2)
	if n := count(got, 0); n != 1 {
		t.Errorf("the committed batch is present %d times, want 1", n)
	}
	if n := count(got, 1); n != 0 {
		t.Errorf("the half-written temp file was adopted: batch 1 present %d times", n)
	}
}

// A group file renamed into place with NO manifest record must stay invisible.
// That is the state a crash between the rename and the manifest sync leaves,
// and adopting the file would make an unacknowledged batch readable.
func TestCrashUncommittedGroupFileIsInvisible(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(crashGroup(0)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// A complete, valid group file that no commit record names.
	orphan := filepath.Join(dir, "group-77.bin")
	if err := os.WriteFile(orphan, crashGroup(1).Marshal(), DataFileMode); err != nil {
		t.Fatal(err)
	}

	st2 := reopenStore(t, dir)
	defer st2.Close()
	got := storedBatches(t, st2)
	if n := count(got, 1); n != 0 {
		t.Errorf("a group file with no commit record was adopted: batch 1 present %d times. "+
			"An unacknowledged batch became readable.", n)
	}
	if n := count(got, 0); n != 1 {
		t.Errorf("the committed batch is present %d times, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// The same matrix against the two REWRITE paths: manifest compaction and
// recompaction.
//
// Both are second callers of writeFileAtomic, and both have a contract the
// append path does not: they must be VISIBILITY-NEUTRAL. Neither adds or
// removes a row -- manifest compaction folds the record log down to one record
// naming the same groups, and recompaction re-encodes a group file under the
// same path and the same ID. Recompaction in particular writes NO manifest
// record at all, so the rename is the entire commit; there is no second step
// that could make a half-finished rewrite visible or invisible.
//
// So there is no per-phase expectation table here, and that is the point: at
// EVERY phase the answer is the same. All crashBatches batches present, each
// exactly once, no partial group, no duplicate rows. A crash that changes what
// is readable is a defect wherever it lands.
//
// Only the seven writeFileAtomic phases are reachable. buffering, post-ack and
// the two manifest-record phases belong to AppendGroup, which these ops do not
// call -- asserted below rather than assumed.
var rewritePhases = []string{
	"temp-create", "partial-write", "file-sync", "file-close",
	"rename", "dir-open", "dir-sync",
}

func TestCrashDuringRewriteIsVisibilityNeutral(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the matrix SIGKILLs a child process; not portable to %s", runtime.GOOS)
	}
	for _, op := range []string{crashOpManifestCompact, crashOpRecompact} {
		t.Run(op, func(t *testing.T) {
			perBatch := crashOpRows(op)
			for _, phase := range rewritePhases {
				t.Run(phase, func(t *testing.T) {
					dir := t.TempDir()
					acked, crashed := runCrashChildOp(t, dir, phase, op)
					if !crashed {
						t.Fatalf("the child did not crash at %q during %s; the phase is "+
							"unreachable and this subtest proves nothing", phase, op)
					}
					if len(acked) != crashBatches {
						t.Fatalf("the child acknowledged %d batches before %s, want %d: "+
							"the crash landed in the SETUP, not in the operation",
							len(acked), op, crashBatches)
					}

					logLeftoverTempFiles(t, dir)
					st := reopenStore(t, dir)
					defer st.Close()

					got := storedBatches(t, st)
					for b := 0; b < crashBatches; b++ {
						switch n := countOf(got, b, perBatch); {
						case n == 0:
							t.Errorf("batch %d is GONE after a crash at %q during %s. "+
								"A rewrite lost an acknowledged batch.", b, phase, op)
						case n < 0:
							t.Errorf("batch %d is PARTIAL after a crash at %q during %s: "+
								"a torn rewrite was adopted", b, phase, op)
						case n > 1:
							t.Errorf("batch %d appears %d times after a crash at %q during %s: "+
								"both the old and the rewritten copy are live", b, n, phase, op)
						}
					}
					assertNoDuplicateRows(t, st)
				})
			}
		})
	}
}

// The phases AppendGroup owns are not reachable from the rewrite paths. If one
// ever becomes reachable the matrix above is incomplete, so this fails rather
// than letting the gap go unnoticed.
func TestRewritePhaseCoverageIsComplete(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the matrix SIGKILLs a child process; not portable to %s", runtime.GOOS)
	}
	appendOnly := map[string]bool{}
	for _, p := range crashPhases {
		appendOnly[p] = true
	}
	for _, p := range rewritePhases {
		delete(appendOnly, p)
	}
	// manifest-append and manifest-sync are the two that carry signal: they
	// have call sites, in the manifest commit, so a rewrite that grew a commit
	// step would reach them. buffering and post-ack have no fault() call site
	// anywhere -- they are markers the CHILD checks, for "before AppendGroup"
	// and "after it returned" -- so no store change makes the rewrite path
	// reach them. They are run anyway because running them costs one process
	// and asserts the clean-run contract below.
	for _, op := range []string{crashOpManifestCompact, crashOpRecompact} {
		for _, phase := range []string{"buffering", "manifest-append", "manifest-sync", "post-ack"} {
			if !appendOnly[phase] {
				t.Fatalf("%q is listed in rewritePhases; update this test", phase)
			}
			t.Run(op+"/"+phase, func(t *testing.T) {
				dir := t.TempDir()
				acked, crashed := runCrashChildOp(t, dir, phase, op)
				if crashed {
					t.Errorf("%s now reaches %q. It is an AppendGroup phase, so the "+
						"rewrite path gained a commit step and rewritePhases must cover it.", op, phase)
				}
				// It ran to completion, so the full contract still applies --
				// and the child must have done the work, not exited early.
				// runCrashChildOp fails the test on any non-zero exit, which is
				// what makes "did not crash" mean "ran to completion".
				if len(acked) != crashBatches {
					t.Fatalf("the child acknowledged %d batches, want %d", len(acked), crashBatches)
				}
				st := reopenStore(t, dir)
				defer st.Close()
				got := storedBatches(t, st)
				for b := 0; b < crashBatches; b++ {
					if n := countOf(got, b, crashOpRows(op)); n != 1 {
						t.Errorf("batch %d present %d times after a clean %s, want 1", b, n, op)
					}
				}
				assertNoDuplicateRows(t, st)
			})
		}
	}
}

// The recompact fixture must actually BE recompacted. Recompact skips any
// group whose flate rewrite is not smaller, and it skips any group with no LZ4
// block at all -- so a fixture that stops qualifying makes every recompact
// subtest above pass without the code under test ever writing a byte. This is
// the vacuously-green trap, pinned.
func TestCrashRecompactFixtureIsActuallyRecompacted(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows := crashOpRows(crashOpRecompact)
	for b := 0; b < crashBatches; b++ {
		if _, err := st.AppendGroup(crashGroupN(b, rows)); err != nil {
			t.Fatal(err)
		}
	}
	groups, before, after, err := st.Recompact(int64(1)<<62, true)
	if err != nil {
		t.Fatalf("recompact: %v", err)
	}
	if groups == 0 {
		t.Fatalf("recompact rewrote 0 groups: the crash fixture no longer qualifies, so "+
			"every %s subtest is vacuous. Make crashGroupN's wide form more compressible.",
			crashOpRecompact)
	}
	t.Logf("recompacted %d groups, %d -> %d bytes", groups, before, after)
}

// ---------------------------------------------------------------------------
// The empty-visible-set trap.
//
// OpenStore adopts every group file on disk when a directory predates the
// manifest. The gate for that used to be "the visible set is empty", which is
// true of a legacy directory AND of two states that are its opposite: a store
// whose only batch was written and never committed, and a store whose last
// live group was committed-removed but not yet unlinked. Both were adopted.
//
// The matrix could not reach either, because crashBatches is 3 and the crash
// always lands on the last batch -- two batches commit first and the visible
// set is never empty at recovery. That is why these are here as well as the
// crashBatches=1 dimension below: the state is reachable without any crash at
// all, from a failed unlink.

// A group file with no commit record, in a store that has committed nothing.
func TestUncommittedGroupIsInvisibleWhenNothingElseIsCommitted(t *testing.T) {
	dir := t.TempDir()
	// Create the store and close it: the MANIFEST now exists and is empty.
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Exactly what a crash between AppendGroup's rename and its commit leaves
	// when it is the FIRST batch.
	orphan := filepath.Join(dir, "group-0.bin")
	if err := os.WriteFile(orphan, crashGroup(0).Marshal(), DataFileMode); err != nil {
		t.Fatal(err)
	}

	st2 := reopenStore(t, dir)
	defer st2.Close()
	if n := count(storedBatches(t, st2), 0); n != 0 {
		t.Errorf("a group file with no commit record was adopted (present %d times) because "+
			"the store had committed nothing else. An unacknowledged batch became readable.", n)
	}
}

// A committed removal whose unlink did not happen must stay removed, even when
// it emptied the store.
func TestRemovedGroupStaysRemovedWhenItWasTheLastOne(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.AppendGroup(crashGroup(0))
	if err != nil {
		t.Fatal(err)
	}
	// Commit the removal and leave the file behind: a crash between the commit
	// and the unlink, or an unlink that failed and left a tombstone.
	if err := st.CommitRemoval(id); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err := os.Stat(filepath.Join(dir, "group-0.bin")); err != nil {
		t.Fatalf("the fixture needs the file left on disk: %v", err)
	}

	st2 := reopenStore(t, dir)
	defer st2.Close()
	if n := count(storedBatches(t, st2), 0); n != 0 {
		t.Errorf("a committed-removed group came back (present %d times) because it was the "+
			"last one. This is the failure the manifest was introduced to prevent.", n)
	}
}

// The legacy path still works: groups on disk with NO manifest file are
// adopted. This is the case the gate exists for, and a fix that made the
// uncommitted cases invisible by breaking this one would be a worse trade.
func TestLegacyDirectoryWithNoManifestIsAdopted(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(crashGroup(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendGroup(crashGroup(1)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Remove the manifest: the directory now looks exactly like one written
	// before the manifest existed.
	if err := os.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
		t.Fatal(err)
	}

	st2 := reopenStore(t, dir)
	defer st2.Close()
	got := storedBatches(t, st2)
	for b := 0; b < 2; b++ {
		if n := count(got, b); n != 1 {
			t.Errorf("legacy batch %d present %d times, want 1: a pre-manifest directory "+
				"was not adopted", b, n)
		}
	}
}

// The append matrix again with ONE batch, so the crash lands on the first one
// and the store's visible set is empty at recovery. Nothing else reaches that
// state, and it is where the adoption defect lived.
//
// With one batch there is no acknowledged batch to find at any phase before
// post-ack, so the assertion is the other half of the contract: the
// interrupted batch must be invisible everywhere the commit record was not
// synced, and visible where it was.
func TestCrashRecoveryMatrixFirstBatch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the matrix SIGKILLs a child process; not portable to %s", runtime.GOOS)
	}
	for _, phase := range crashPhases {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			acked, crashed := runCrashChildN(t, dir, phase, crashOpAppend, 1)
			if !crashed {
				t.Fatalf("the child did not crash at %q", phase)
			}
			wantAcked := 0
			if phase == "post-ack" {
				wantAcked = 1
			}
			if len(acked) != wantAcked {
				t.Fatalf("the child acknowledged %v at %q, want %d", acked, phase, wantAcked)
			}

			st := reopenStore(t, dir)
			defer st.Close()
			got := storedBatches(t, st)
			n := count(got, 0)
			if n < 0 {
				t.Errorf("the only batch is PARTIAL after a crash at %q", phase)
			}
			has := n > 0
			want := phasesCommittingTheLastBatch[phase]
			if has && !want {
				t.Errorf("the only batch is VISIBLE after a crash at %q, but its commit record "+
					"was never written: an unacknowledged batch became readable with an empty "+
					"visible set", phase)
			}
			if !has && want {
				t.Errorf("the only batch is MISSING after a crash at %q, but its commit record "+
					"was synced: a durable commit was lost", phase)
			}
			if n > 1 {
				t.Errorf("the only batch appears %d times after a crash at %q", n, phase)
			}
			assertNoDuplicateRows(t, st)
		})
	}
}

// An INJECTED ERROR at the two manifest fault points, which is a different
// test from a crash at them and was covered by nothing.
//
// A crash at either point kills the process, so the in-memory state does not
// matter and the matrix cannot tell where the fault sits relative to the
// state update. An injected error returns, and then it matters a great deal:
//
//   - faultManifestWrite fires BEFORE the sync. The record is not a commit,
//     truncateTo removes it, and memory must not advance.
//   - faultManifestSync fires AFTER it. The record IS durable and cannot be
//     truncated away, so memory must advance -- otherwise the store reports
//     the batch absent, the NEXT commit reuses its sequence number, and a
//     reopen makes the rejected batch appear under a duplicated Seq.
//
// The second was the live shape until the fault call was moved below the
// state update, and nothing failed when it was moved back. This is what
// holds it in place.
func TestInjectedManifestFaultKeepsMemoryAndDiskAgreeing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		point       faultPoint
		wantVisible bool // is the failed batch visible in memory afterwards?
	}{
		{"manifest-append", faultManifestWrite, false},
		{"manifest-sync", faultManifestSync, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			m, err := openManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.commit([]uint64{10}, nil, nil); err != nil {
				t.Fatal(err)
			}

			injected := errors.New("injected")
			restore := setFaultHook(func(p faultPoint) error {
				if p == tc.point {
					return injected
				}
				return nil
			})
			err = m.commit([]uint64{20}, nil, nil)
			restore()
			if !errors.Is(err, injected) {
				t.Fatalf("commit returned %v, want the injected error", err)
			}

			if got := m.visible[20]; got != tc.wantVisible {
				t.Errorf("id 20 visible = %v after a failure at %s, want %v",
					got, tc.name, tc.wantVisible)
			}

			// A later commit must not reuse the failed one's sequence number.
			if err := m.commit([]uint64{30}, nil, nil); err != nil {
				t.Fatal(err)
			}
			inMemory := append([]uint64(nil), m.visibleIDs()...)
			m.close()

			// Sequence numbers are asserted on the RECORD STREAM, not on
			// m.seq. `m.seq > seqAtFailure` was the first spelling and it
			// cannot see the defect it names: every commit increments m.seq,
			// so it is satisfied whether or not the number was reused. With
			// the fault above the state update, the failed record carries
			// Seq 2, m.seq stays 1, the next commit computes 1+1 = 2 -- two
			// records with one Seq, and m.seq is still greater than it was.
			if dup, seq := duplicateSeq(t, dir); dup {
				t.Errorf("two manifest records carry Seq %d after a failure at %s", seq, tc.name)
			}

			// The decisive check: what a fresh replay sees must be what the
			// live manifest reported. Divergence here is a store that answers
			// one way before a restart and another way after.
			m2, err := openManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer m2.close()
			onDisk := m2.visibleIDs()
			if len(onDisk) != len(inMemory) {
				t.Fatalf("memory holds %v, a replay holds %v", inMemory, onDisk)
			}
			for i := range onDisk {
				if onDisk[i] != inMemory[i] {
					t.Fatalf("memory holds %v, a replay holds %v", inMemory, onDisk)
				}
			}
		})
	}
}

// duplicateSeq replays the manifest file's records and reports whether two of
// them carry the same sequence number.
func duplicateSeq(t *testing.T, dir string) (bool, uint64) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[uint64]bool{}
	for off := 0; off < len(b); {
		rec, n, ok := decodeManifestRecord(b[off:])
		if !ok {
			break
		}
		if seen[rec.Seq] {
			return true, rec.Seq
		}
		seen[rec.Seq] = true
		off += n
	}
	return false, 0
}

// A bootstrap that FAILS must leave the directory looking exactly as it did,
// so the next open re-decides and adopts the legacy groups.
//
// It did not. bootstrap's error path called reopen(), which opens with
// O_CREATE and therefore created the 0-byte MANIFEST that OpenStore's gate
// reads to recognise a legacy directory. One transient ENOSPC during the
// one-time migration made every later open return a store with zero groups
// and no error, on a directory holding the only copy.
func TestFailedBootstrapLeavesTheLegacyDirectoryRecoverable(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 2; b++ {
		if _, err := st.AppendGroup(crashGroup(b)); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	// Make it a legacy directory: groups on disk, no manifest.
	if err := os.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected")
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultWrite {
			return injected
		}
		return nil
	})
	_, err = OpenStore(dir)
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("OpenStore returned %v, want the injected error", err)
	}

	// The decisive check: the failed bootstrap must not have created the file
	// its own gate reads.
	if _, serr := os.Stat(filepath.Join(dir, ManifestFileName)); serr == nil {
		t.Error("a FAILED bootstrap created the MANIFEST. Every later open will " +
			"read it as 'not a legacy directory' and adopt nothing.")
	}

	// The next open must recover everything.
	st2 := reopenStore(t, dir)
	defer st2.Close()
	got := storedBatches(t, st2)
	for b := 0; b < 2; b++ {
		if n := count(got, b); n != 1 {
			t.Errorf("legacy batch %d present %d times after a failed-then-retried "+
				"bootstrap, want 1: a transient disk error became permanent data loss", b, n)
		}
	}
}

// A restore archive must be able to place group files and nothing else.
//
// RestoreTar wrote any regular entry under its flattened base name, which was
// harmless until OpenStore started deciding "is this a legacy directory" on
// whether MANIFEST exists. After that, an archive carrying an empty MANIFEST
// restores a directory full of groups that the next open reports as EMPTY,
// with no error -- the same silent-invisibility class as the gate defect
// itself, arriving through a different door.
func TestRestoreIgnoresNonGroupEntries(t *testing.T) {
	src := t.TempDir()
	st, err := OpenStore(src)
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 2; b++ {
		if _, err := st.AppendGroup(crashGroup(b)); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := st.BackupTar(&buf); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Append the entries a hand-assembled archive might carry: an empty
	// MANIFEST, and a file with a name nothing should place.
	var withExtras bytes.Buffer
	withExtras.Write(buf.Bytes()[:buf.Len()-1024]) // drop the tar end-of-archive
	tw := tar.NewWriter(&withExtras)
	for _, name := range []string{ManifestFileName, "LOCK", "notes.txt"} {
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: 0, Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := RestoreTar(&withExtras, dst); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{ManifestFileName, "LOCK", "notes.txt"} {
		if _, err := os.Stat(filepath.Join(dst, name)); err == nil {
			t.Errorf("restore placed %q; an archive may place group files only", name)
		}
	}

	st2 := reopenStore(t, dst)
	defer st2.Close()
	got := storedBatches(t, st2)
	for b := 0; b < 2; b++ {
		if n := count(got, b); n != 1 {
			t.Errorf("restored batch %d present %d times, want 1: the archive's MANIFEST "+
				"entry made the restored groups invisible", b, n)
		}
	}
}
