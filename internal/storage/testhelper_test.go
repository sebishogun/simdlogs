package storage

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The crash-recovery helper: a subprocess that writes batches into a store and
// SIGKILLs ITSELF at a named persistence phase.
//
// A subprocess, and SIGKILL rather than an injected error, because those are
// different tests. An injected error unwinds through every defer: the temp
// file is removed, the manifest is truncated back to a record boundary, the
// store is closed. That exercises the ERROR HANDLING. A crash runs none of it,
// which is the only way to see what is actually on disk when a machine loses
// power mid-write -- a temp file nobody removed, a group file renamed into
// place that no commit record mentions.
//
// What it does NOT produce is a torn write. fault(faultWrite) fires before
// f.Write, so the temp file is zero bytes rather than partial, and the
// manifest is always at an exact record boundary. The replay path for a torn
// manifest tail exists and is not exercised here.
//
// The child reports each acknowledged batch on stderr before continuing, with
// an unbuffered write, so the record survives the kill that follows it.

// The operations the matrix runs the phases against. Each is a different
// CALLER of the same durable write, which is what step 4 of the plan asks for:
// a crash in compaction must not lose a commit the append path already made.
const (
	crashOpAppend          = "append"
	crashOpManifestCompact = "manifest-compact"
	crashOpRecompact       = "recompact"
	crashOpGroupCompact    = "group-compact"
)

const (
	crashEnvPhase   = "SIMDLOGS_CRASH_PHASE"
	crashEnvDir     = "SIMDLOGS_CRASH_DIR"
	crashEnvOp      = "SIMDLOGS_CRASH_OP"
	crashEnvBatches = "SIMDLOGS_CRASH_BATCHES"
)

// crashBatchRows is how many rows each batch carries. Small: the matrix is
// about WHERE the process dies, not about volume, and every phase pays the
// cost of a fresh process.
const crashBatchRows = 4

// crashRecompactRows is the batch size for the recompact op.
//
// The 4-row group used everywhere else DOES qualify for recompaction, because
// the child passes dropPostings=true and a group carrying postings is a
// candidate on that alone. Measured, three batches, Recompact(1<<62, drop):
//
//	rows  dropPostings  groups rewritten  bytes
//	   4  false                        0  --
//	   4  true                         3  1110 -> 942
//	 256  false                        3  13725 -> 11948
//	 256  true                         3  13725 -> 10868
//
// So the wide fixture is not what makes the phases reachable. It is here for
// the row that has nothing to do with postings: at 256 rows the flate rewrite
// is itself smaller, so the size test Recompact applies to every candidate is
// exercised rather than short-circuited by the postings check.
// TestCrashRecompactFixtureIsActuallyRecompacted pins that it still qualifies.
const crashRecompactRows = 256

// crashBatches is how many batches the child writes before the phase fires.
// More than one so "every acknowledged batch is present exactly once" has
// something to say -- with a single batch, present-once and present-at-all are
// the same assertion.
//
// It is the DEFAULT, not the only value. The matrix also runs at 1, because
// three batches hid a live defect: the crash always landed on the last batch,
// so two batches were always committed first and the store's visible set was
// never empty at recovery. The empty-visible-set path was the one that adopted
// uncommitted groups, and no phase could reach it.
const crashBatches = 3

// crashOpRows is how many rows a batch carries for an op.
func crashOpRows(op string) int {
	if op == crashOpRecompact {
		return crashRecompactRows
	}
	return crashBatchRows
}

// TestMain lets the test binary re-exec itself as the crash helper. Without
// it the child would run the whole suite.
func TestMain(m *testing.M) {
	if phase := os.Getenv(crashEnvPhase); phase != "" {
		batches := crashBatches
		if v := os.Getenv(crashEnvBatches); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				fmt.Fprintf(os.Stderr, "CHILD_BAD_BATCHES %s\n", v)
				os.Exit(8)
			}
			batches = n
		}
		crashChild(phase, os.Getenv(crashEnvDir), os.Getenv(crashEnvOp), batches)
		os.Exit(0) // unreachable when the phase fires
	}
	os.Exit(m.Run())
}

// crashChild is the subprocess body. It never returns normally when the phase
// is one that fires.
func crashChild(phase, dir, op string, batches int) {
	target, ok := faultPointByName(phase)
	if !ok {
		fmt.Fprintf(os.Stderr, "CHILD_BAD_PHASE %s\n", phase)
		os.Exit(2)
	}

	st, err := OpenStore(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "CHILD_OPEN_FAILED %v\n", err)
		os.Exit(3)
	}

	// The kill happens INSIDE the hook, so the process dies with the syscall
	// half-done rather than after a clean return. os.Exit would run no defers
	// either, but it also would not leave a file descriptor open mid-write the
	// way a signal does.
	suicide := func() {
		_, _ = os.Stderr.WriteString("CHILD_CRASHING " + phase + "\n")
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		// SIGKILL is not deliverable to a stopped process instantly in every
		// scheduler state; block so the function cannot return and let the
		// write finish.
		select {}
	}

	// The last batch is the one that crashes. Everything before it completes
	// and is acknowledged, which is what the parent asserts must survive.
	crashOnBatch := batches - 1
	batch := 0
	if op == crashOpManifestCompact || op == crashOpRecompact || op == crashOpGroupCompact {
		// The hook closure reads THIS variable, and the setup loop leaves
		// batch past it. Without -1 ("fire on any batch") the hook could never
		// match during the operation under test and every subtest would pass
		// vacuously.
		crashOnBatch = -1
	}
	// armed lets the compaction ops write and acknowledge every batch through
	// the same durable path before the hook starts firing. Without it the
	// crash would land in the setup rather than in the operation under test.
	armed := op == "" || op == crashOpAppend
	restore := setFaultHook(func(p faultPoint) error {
		if armed && p == target && (batch == crashOnBatch || crashOnBatch < 0) {
			suicide()
		}
		return nil
	})
	defer restore()

	rows := crashOpRows(op)
	switch op {
	case "", crashOpAppend:
		crashChildAppend(st, target, crashOnBatch, &batch, rows, batches, suicide)
	case crashOpManifestCompact:
		// Every batch is written and acknowledged FIRST, then the manifest is
		// folded down with the hook armed. manifest.compact() rewrites the log
		// through writeFileAtomic, so it reaches the same phases -- which is
		// the point: the compaction path is a second caller of the same
		// durable write, and a crash in it must not lose a commit that the
		// append path already made durable.
		crashChildAppend(st, faultPoint(-1), -1, &batch, rows, batches, suicide)
		armed = true
		if err := st.man.compact(); err != nil {
			fmt.Fprintf(os.Stderr, "CHILD_COMPACT_FAILED %v\n", err)
			os.Exit(5)
		}
	case crashOpRecompact:
		crashChildAppend(st, faultPoint(-1), -1, &batch, rows, batches, suicide)
		armed = true
		// A cutoff past every row, so every group is a candidate.
		if _, _, _, err := st.Recompact(int64(1)<<62, true); err != nil {
			fmt.Fprintf(os.Stderr, "CHILD_RECOMPACT_FAILED %v\n", err)
			os.Exit(6)
		}
	case crashOpGroupCompact:
		// The fourth caller of the durable write, and the one whose commit is
		// a TRANSACTION rather than an append: its record adds the output and
		// removes the inputs together. A kill anywhere in it must leave either
		// the inputs or the output visible and never both.
		//
		// Injecting an error cannot test that. An error unwinds every defer,
		// so the in-memory swap is undone and the store looks consistent
		// whatever the manifest holds; only a kill leaves the two to be
		// reconciled by the next open. Measured: committing the add and the
		// removes as two records leaves the whole shipped suite green and
		// duplicates every row under this lane.
		crashChildAppend(st, faultPoint(-1), -1, &batch, rows, batches, suicide)
		armed = true
		if _, err := st.CompactGroups(CompactOptions{MinGroups: 2}); err != nil {
			fmt.Fprintf(os.Stderr, "CHILD_GROUP_COMPACT_FAILED %v\n", err)
			os.Exit(8)
		}
	default:
		fmt.Fprintf(os.Stderr, "CHILD_BAD_OP %s\n", op)
		os.Exit(7)
	}
	st.Close()
	_, _ = os.Stderr.WriteString("CHILD_DONE\n")
}

// crashChildAppend writes and acknowledges every batch. When target is a real
// fault point it also crashes at it; passing -1 writes them all cleanly, which
// is what the compaction ops need before they arm the hook.
func crashChildAppend(st *Store, target faultPoint, crashOnBatch int, batch *int, rows, batches int, suicide func()) {
	for ; *batch < batches; *batch++ {
		if target == faultBuffered && *batch == crashOnBatch {
			suicide()
		}
		g := crashGroupN(*batch, rows)
		if _, err := st.AppendGroup(g); err != nil {
			fmt.Fprintf(os.Stderr, "CHILD_APPEND_FAILED %d %v\n", *batch, err)
			os.Exit(4)
		}
		// Acknowledged: the group is durable and visible. Reported with an
		// unbuffered write so a kill immediately after still leaves it in the
		// parent's pipe.
		fmt.Fprintf(os.Stderr, "ACK %d\n", *batch)
		if target == faultPostAck && *batch == crashOnBatch {
			suicide()
		}
	}
}

// crashGroup builds batch n's group. Deterministic, so the parent knows
// exactly which rows to look for.
func crashGroup(n int) *Group { return crashGroupN(n, crashBatchRows) }

// crashGroupN builds batch n with the given row count. Past crashBatchRows the
// values carry a repetitive suffix, because the point of the wider group is to
// be one flate can shrink; the "b<batch>r<row>" prefix the parent parses is
// unchanged, so both shapes read back the same way.
func crashGroupN(n, rows int) *Group {
	ts := make([]int64, rows)
	vals := make([]string, rows)
	pad := ""
	if rows > crashBatchRows {
		pad = "|" + strings.Repeat("recompactable-filler-", 8)
	}
	for i := range ts {
		ts[i] = int64(1_700_000_000_000_000_000) + int64(n)*1e9 + int64(i)
		vals[i] = crashRowValue(n, i) + pad
	}
	d := BuildDict(vals)
	return &Group{
		Rows: rows,
		Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: ts},
			{Name: "batch", Type: ColDict, Dict: &d},
		},
	}
}

func crashRowValue(batch, row int) string {
	return "b" + strconv.Itoa(batch) + "r" + strconv.Itoa(row)
}

func faultPointByName(s string) (faultPoint, bool) {
	for p, n := range faultPointName {
		if n == s {
			return p, true
		}
	}
	return 0, false
}

// runCrashChild starts the helper, waits for it to die, and returns the
// batches it acknowledged.
//
// Bounded: a child that neither crashes nor finishes must fail the test rather
// than hold a process. On 2026-08-14 a run of unbounded test binaries filled
// this machine's memory and the OOM killer took its desktop down.
func runCrashChild(t *testing.T, dir, phase string) (acked []int, crashed bool) {
	return runCrashChildN(t, dir, phase, crashOpAppend, crashBatches)
}

func runCrashChildOp(t *testing.T, dir, phase, op string) (acked []int, crashed bool) {
	return runCrashChildN(t, dir, phase, op, crashBatches)
}

func runCrashChildN(t *testing.T, dir, phase, op string, batches int) (acked []int, crashed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestMain")
	cmd.Env = append(os.Environ(), crashEnvPhase+"="+phase, crashEnvDir+"="+dir, crashEnvOp+"="+op,
		crashEnvBatches+"="+strconv.Itoa(batches))
	cmd.WaitDelay = 5 * time.Second
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Failures are recorded and reported AFTER Wait. A t.Fatalf between Start
	// and Wait returns without reaping the child, leaving a zombie for as long
	// as the test binary runs.
	var badAck, lastChild string
	sc := bufio.NewScanner(stderr)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "ACK "):
			n, perr := strconv.Atoi(strings.TrimPrefix(line, "ACK "))
			if perr != nil {
				badAck = line
				continue
			}
			acked = append(acked, n)
		case strings.HasPrefix(line, "CHILD_CRASHING"):
			crashed = true
			lastChild = line
		case strings.HasPrefix(line, "CHILD_"):
			lastChild = line
			t.Logf("child: %s", line)
		}
	}
	// A scanner error truncates acked silently, and acked is what the
	// expectation is derived from -- so an unreported one WEAKENS the
	// assertions instead of failing them.
	scanErr := sc.Err()
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		t.Fatalf("the crash helper for phase %q neither crashed nor finished inside 60s", phase)
	}
	if scanErr != nil {
		t.Fatalf("reading the child's output for phase %q: %v", phase, scanErr)
	}
	if badAck != "" {
		t.Fatalf("child wrote a malformed ack: %q", badAck)
	}

	// A killed child exits with a signal; a clean one exits 0. Anything else
	// is a child that FAILED -- OpenStore returned an error, the operation
	// under test returned an error, the phase name was not recognized -- and
	// treating that as "did not crash" inverts every test that asserts the
	// child ran to completion into a pass. TestRewritePhaseCoverageIsComplete
	// stayed green with Recompact stubbed to return an error immediately.
	if ee, ok := waitErr.(*exec.ExitError); ok {
		ws, isWS := ee.Sys().(syscall.WaitStatus)
		switch {
		case isWS && ws.Signaled():
			if ws.Signal() != syscall.SIGKILL {
				t.Fatalf("child died from %v, want SIGKILL", ws.Signal())
			}
			crashed = true
		default:
			t.Fatalf("the crash helper for phase %q op %q exited %d without crashing; "+
				"last line: %q", phase, op, ee.ExitCode(), lastChild)
		}
	} else if waitErr != nil {
		t.Fatalf("the crash helper for phase %q op %q: %v", phase, op, waitErr)
	}
	return acked, crashed
}
