package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The oracle for every test here: the rows a store answers with, in the order
// it answers them.
//
// Compaction rewrites the physical layout and must not change this. Comparing
// group counts or byte totals would pass for a compactor that dropped a column
// or reordered rows; comparing the answer cannot.
type row struct {
	ts   int64
	vals map[string]string
}

func readAllRows(t *testing.T, s *Store) []row {
	t.Helper()
	var out []row
	for _, r := range s.Groups(0, 1<<62) {
		ts := r.TimestampsRange("_time", 0, r.Rows)
		if len(ts) != r.Rows {
			t.Fatalf("a group returned %d timestamps for %d rows", len(ts), r.Rows)
		}
		names := make([]string, 0, len(r.cols))
		for i := range r.cols {
			if r.cols[i].Type == ColDict {
				names = append(names, r.cols[i].Name)
			}
		}
		cols := map[string][]string{}
		for _, n := range names {
			idx, dict := r.DictIndices(n)
			vals := make([]string, r.Rows)
			for i := 0; i < r.Rows && i < len(idx); i++ {
				if int(idx[i]) < len(dict) {
					vals[i] = dict[idx[i]]
				}
			}
			cols[n] = vals
		}
		for i := 0; i < r.Rows; i++ {
			v := map[string]string{}
			for _, n := range names {
				if s := cols[n][i]; s != "" {
					v[n] = s
				}
			}
			out = append(out, row{ts: ts[i], vals: v})
		}
	}
	return out
}

func sameRows(a, b []row) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d rows against %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ts != b[i].ts {
			return fmt.Errorf("row %d: time %d against %d", i, a[i].ts, b[i].ts)
		}
		if len(a[i].vals) != len(b[i].vals) {
			return fmt.Errorf("row %d: %v against %v", i, a[i].vals, b[i].vals)
		}
		for k, v := range a[i].vals {
			if b[i].vals[k] != v {
				return fmt.Errorf("row %d field %q: %q against %q", i, k, v, b[i].vals[k])
			}
		}
	}
	return nil
}

// oneRowGroup is what a client sending one row per request produces.
func oneRowGroup(ts int64, fields map[string]string) *Group {
	g := &Group{Rows: 1}
	g.Columns = append(g.Columns, Column{Name: "_time", Type: ColTimestamp, Ts: []int64{ts}})
	for k, v := range fields {
		d := BuildDict([]string{v})
		g.Columns = append(g.Columns, Column{Name: k, Type: ColDict, Dict: &d})
	}
	return g
}

// fillOneRowGroups appends n one-row groups and returns the store.
func fillOneRowGroups(t *testing.T, dir string, n int) *Store {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		g := oneRowGroup(int64(i+1)*1000, map[string]string{
			"service": fmt.Sprintf("svc-%d", i%7),
			"_msg":    fmt.Sprintf("message %d", i),
		})
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return s
}

// Thousands of one-row groups compact to a handful, and every row comes back
// with the same timestamp, the same fields and in the same order.
func TestCompactionPreservesEveryRow(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 3000)
	defer s.Close()

	want := readAllRows(t, s)
	if len(want) != 3000 {
		t.Fatalf("%d rows before compaction, want 3000", len(want))
	}
	beforeGroups := len(s.Groups(0, 1<<62))

	st, err := s.CompactGroups(CompactOptions{MinGroups: 4, MaxRowsPerOutput: 512})
	if err != nil {
		t.Fatal(err)
	}
	if st.Inputs == 0 || st.Outputs == 0 {
		t.Fatalf("the pass did nothing: %+v", st)
	}
	afterGroups := len(s.Groups(0, 1<<62))
	if afterGroups >= beforeGroups {
		t.Fatalf("%d groups after against %d before", afterGroups, beforeGroups)
	}
	if err := sameRows(want, readAllRows(t, s)); err != nil {
		t.Fatalf("after compaction: %v", err)
	}
	t.Logf("%d groups -> %d; %+v", beforeGroups, afterGroups, st)

	// And the answer survives a reopen, which is what says the manifest and
	// the files agree rather than the in-memory group list being right alone.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if err := sameRows(want, readAllRows(t, s2)); err != nil {
		t.Fatalf("after reopen: %v", err)
	}
}

// Groups carrying different field sets merge to the UNION, and a row from a
// group without a field keeps not having it rather than inheriting a
// neighbour's.
func TestCompactionUnionsColumnsWithoutBleeding(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 12; i++ {
		fields := map[string]string{"_msg": fmt.Sprintf("m%d", i)}
		switch i % 3 {
		case 0:
			fields["only_a"] = fmt.Sprintf("a%d", i)
		case 1:
			fields["only_b"] = fmt.Sprintf("b%d", i)
		}
		if _, err := s.AppendGroup(oneRowGroup(int64(i+1)*1000, fields)); err != nil {
			t.Fatal(err)
		}
	}
	want := readAllRows(t, s)
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2}); err != nil {
		t.Fatal(err)
	}
	got := readAllRows(t, s)
	if err := sameRows(want, got); err != nil {
		t.Fatalf("%v", err)
	}
	// Explicitly: a row that never had only_a must not have it now. sameRows
	// checks that, and this says so out loud because it is the failure the
	// union exists to avoid in the other direction.
	for i, r := range got {
		if i%3 != 0 {
			if _, ok := r.vals["only_a"]; ok {
				t.Errorf("row %d gained only_a=%q from a neighbour", i, r.vals["only_a"])
			}
		}
	}
}

// A pass is bounded by every threshold it offers.
func TestCompactionThresholdsRefuse(t *testing.T) {
	build := func(t *testing.T, n int) (*Store, string) {
		t.Helper()
		dir := t.TempDir()
		return fillOneRowGroups(t, dir, n), dir
	}

	t.Run("min group count", func(t *testing.T) {
		s, _ := build(t, 8)
		defer s.Close()
		st, err := s.CompactGroups(CompactOptions{MinGroups: 100})
		if err != nil {
			t.Fatal(err)
		}
		if st.Outputs != 0 {
			t.Fatalf("%+v, want nothing done", st)
		}
	})

	t.Run("age", func(t *testing.T) {
		s, _ := build(t, 20)
		defer s.Close()
		// Every row is at or after 1000ns; a cutoff below that leaves nothing
		// old enough.
		st, err := s.CompactGroups(CompactOptions{MinGroups: 2, OlderThan: 1})
		if err != nil {
			t.Fatal(err)
		}
		if st.Outputs != 0 {
			t.Fatalf("%+v, want nothing old enough", st)
		}
		// And a cutoff past every row compacts.
		st, err = s.CompactGroups(CompactOptions{MinGroups: 2, OlderThan: 1 << 40})
		if err != nil {
			t.Fatal(err)
		}
		if st.Outputs == 0 {
			t.Fatal("nothing compacted with every group past the cutoff")
		}
	})

	t.Run("max group bytes", func(t *testing.T) {
		s, _ := build(t, 20)
		defer s.Close()
		st, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxGroupBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		if st.Outputs != 0 {
			t.Fatalf("%+v, want every group refused as too big", st)
		}
	})

	t.Run("max outputs", func(t *testing.T) {
		s, _ := build(t, 40)
		defer s.Close()
		st, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 4, MaxOutputs: 2})
		if err != nil {
			t.Fatal(err)
		}
		if st.Outputs > 2 {
			t.Fatalf("%d outputs against a ceiling of 2", st.Outputs)
		}
		if st.Outputs == 0 {
			t.Fatal("the ceiling stopped everything")
		}
	})

	t.Run("max input bytes", func(t *testing.T) {
		s, _ := build(t, 40)
		defer s.Close()
		st, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 4, MaxInputBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		// One batch runs before the running total can exceed the bound, which
		// is what a byte ceiling on a pass means; what it must not do is run
		// the whole store.
		if st.Outputs > 1 {
			t.Fatalf("%d outputs against a 1-byte input ceiling", st.Outputs)
		}
	})

	t.Run("the zero value compacts nothing beyond the floor", func(t *testing.T) {
		s, _ := build(t, 3)
		defer s.Close()
		want := readAllRows(t, s)
		if _, err := s.CompactGroups(CompactOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := sameRows(want, readAllRows(t, s)); err != nil {
			t.Fatal(err)
		}
	})
}

// Killed at each of the four points, the store reopens with exactly the rows
// it had: none lost, none duplicated.
//
// The point of the matrix is that the four residues are different. Before the
// write there is nothing; after it there is an uncommitted file the next open
// must ignore; at the commit the inputs must still be visible; after it the
// inputs are files nothing names and the rows must come from the OUTPUT only.
// A test that only reopened and counted rows would pass for a build that
// committed nothing at all, so it also asserts the pass eventually completes.
func TestCompactionCrashMatrix(t *testing.T) {
	// No per-point expectation of what is left on disk: an injected error
	// unwinds every defer, so discardUncommitted removes the output at three
	// of the four points and the fourth is covered by its own test below. An
	// earlier version carried a `wrote bool` per row that was never read, and
	// the LLD's residue table was written from it and got two rows wrong.
	for _, point := range []struct {
		name string
		p    faultPoint
	}{
		{"before the output is written", faultCompactWrite},
		{"after the output is durable", faultCompactWritten},
		{"at the manifest commit", faultCompactCommit},
		{"after the commit, before the unlink", faultCompactUnlink},
	} {
		t.Run(point.name, func(t *testing.T) {
			dir := t.TempDir()
			s := fillOneRowGroups(t, dir, 60)
			want := readAllRows(t, s)

			injected := errors.New("a fault the test injected")
			restore := setFaultHook(func(p faultPoint) error {
				if p == point.p {
					return injected
				}
				return nil
			})
			_, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8})
			restore()
			if !errors.Is(err, injected) {
				t.Fatalf("the pass returned %v, want the injected fault", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			s2, err := OpenStore(dir)
			if err != nil {
				t.Fatalf("reopen after a kill at %s: %v", point.name, err)
			}
			if err := sameRows(want, readAllRows(t, s2)); err != nil {
				t.Fatalf("after a kill at %s: %v", point.name, err)
			}
			// The store still works: a second pass with no fault completes and
			// the answer is still the same.
			if _, err := s2.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8}); err != nil {
				t.Fatalf("the retry pass: %v", err)
			}
			if err := sameRows(want, readAllRows(t, s2)); err != nil {
				t.Fatalf("after the retry: %v", err)
			}
			if err := s2.Close(); err != nil {
				t.Fatal(err)
			}
			// And nothing the manifest does not name is left on disk after the
			// retry, which is what says the residue was reclaimed rather than
			// merely ignored.
			s3, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s3.Close()
			if err := sameRows(want, readAllRows(t, s3)); err != nil {
				t.Fatalf("after a third open: %v", err)
			}
		})
	}
}

// A kill after the commit leaves the inputs on disk, and the reopened store
// must answer from the OUTPUT rather than from them.
//
// This is the duplication case and it needs its own assertion: a build that
// left the inputs visible would return every row twice, and a build that lost
// the output would return them once from the inputs. Only counting rows tells
// the two apart from correct behaviour.
func TestCompactionDoesNotDuplicateAfterACommitCrash(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 40)
	want := readAllRows(t, s)

	injected := errors.New("a fault the test injected")
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultCompactUnlink {
			return injected
		}
		return nil
	})
	_, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8})
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("%v, want the injected fault", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The input files are still there.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, e := range ents {
		if _, ok := groupIDFromName(e.Name()); ok {
			files++
		}
	}
	if files <= 40 {
		t.Fatalf("%d group files after a kill before the unlink; the inputs were already gone", files)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got := readAllRows(t, s2)
	if len(got) != len(want) {
		t.Fatalf("%d rows after the reopen against %d before: the merged inputs are visible too",
			len(got), len(want))
	}
	if err := sameRows(want, got); err != nil {
		t.Fatal(err)
	}
}

// Compaction and the group files agree: every visible group has a file, and no
// file the manifest does not name survives a completed pass.
func TestCompactionLeavesNoOrphanFiles(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 200)
	defer s.Close()
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 32}); err != nil {
		t.Fatal(err)
	}
	visible := map[string]bool{}
	s.mu.RLock()
	for _, g := range s.groups {
		visible[filepath.Base(g.path)] = true
	}
	s.mu.RUnlock()

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if _, ok := groupIDFromName(e.Name()); !ok {
			continue
		}
		if !visible[e.Name()] {
			t.Errorf("%s is on disk and not in the group set", e.Name())
		}
	}
	if len(visible) == 0 {
		t.Fatal("no group is visible after compaction")
	}
}

// Compacting twice does nothing the second time: the outputs are already at or
// past the sizes the thresholds accept, so a pass on a timer does not rewrite
// the same rows forever.
func TestCompactionIsIdempotentAtItsThresholds(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 400)
	defer s.Close()
	want := readAllRows(t, s)

	first, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 64})
	if err != nil {
		t.Fatal(err)
	}
	if first.Outputs == 0 {
		t.Fatal("the first pass did nothing")
	}
	second, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 64})
	if err != nil {
		t.Fatal(err)
	}
	if second.Outputs != 0 {
		t.Errorf("the second pass rewrote %d groups; compaction on a timer would never settle", second.Outputs)
	}
	if err := sameRows(want, readAllRows(t, s)); err != nil {
		t.Fatal(err)
	}
}

// compactPhases is every point a KILLED compaction can be interrupted at.
//
// The seven write phases it shares with every other rewrite, the two manifest
// phases -- which it reaches and the other rewrites do not, because its commit
// writes a record rather than only replacing a file -- and its own four.
var compactPhases = []string{
	"temp-create", "partial-write", "file-sync", "file-close",
	"rename", "dir-open", "dir-sync",
	"manifest-append", "manifest-sync",
	"compact-write", "compact-written", "compact-commit", "compact-unlink",
}

// A killed compaction leaves every batch visible exactly once.
//
// This is the test the transaction claim needs, and injecting an error cannot
// be it: an error unwinds every defer, so the in-memory group list is put back
// and the store looks consistent whatever the manifest holds. Only a kill
// leaves the manifest and the files to be reconciled by the next open.
//
// Measured against a mutant that commits the add and the removes as two
// records -- exactly what the design says it must not do: the whole shipped
// suite stays green, and every batch appears twice under this lane.
func TestCrashDuringGroupCompactionIsVisibilityNeutral(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the matrix SIGKILLs a child process; not portable to %s", runtime.GOOS)
	}
	perBatch := crashOpRows(crashOpGroupCompact)
	for _, phase := range compactPhases {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			acked, crashed := runCrashChildOp(t, dir, phase, crashOpGroupCompact)
			if !crashed {
				t.Fatalf("the child did not crash at %q; the phase is unreachable from "+
					"compaction and this subtest proves nothing", phase)
			}
			if len(acked) != crashBatches {
				t.Fatalf("the child acknowledged %d batches before compacting, want %d: "+
					"the crash landed in the SETUP", len(acked), crashBatches)
			}

			st := reopenStore(t, dir)
			defer st.Close()
			got := storedBatches(t, st)
			for b := 0; b < crashBatches; b++ {
				switch n := countOf(got, b, perBatch); {
				case n == 0:
					t.Errorf("batch %d is GONE after a kill at %q: compaction lost an "+
						"acknowledged batch", b, phase)
				case n < 0:
					t.Errorf("batch %d is PARTIAL after a kill at %q", b, phase)
				case n > 1:
					t.Errorf("batch %d appears %d times after a kill at %q: the inputs and "+
						"the output are both visible, so the commit was not one record",
						b, n, phase)
				}
			}
			assertNoDuplicateRows(t, st)
		})
	}
}

// The zero value compacts nothing. Not "nothing beyond the floor" -- nothing.
//
// An earlier version floored MinGroups at 2 and documented the zero value as a
// refusal, and a review measured CompactOptions{} merging a 500-group store
// into one. A field whose zero value rewrites the caller's store is not a
// threshold, and three documents said otherwise because the floor read as one.
func TestTheZeroValueCompactsNothing(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 500)
	defer s.Close()
	before := len(s.Groups(0, 1<<62))
	st, err := s.CompactGroups(CompactOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Inputs != 0 || st.Outputs != 0 {
		t.Fatalf("CompactOptions{} did %+v", st)
	}
	if after := len(s.Groups(0, 1<<62)); after != before {
		t.Fatalf("%d groups after against %d before", after, before)
	}
	// And a negative value is off too, not "floored to 2".
	if st, err := s.CompactGroups(CompactOptions{MinGroups: -1}); err != nil || st.Outputs != 0 {
		t.Fatalf("MinGroups -1: %+v %v", st, err)
	}
}

// A MinGroups above what the row cap can fit is not a permanent silent no-op.
//
// The floor used to be applied to every BATCH, so when the cap split a run
// below it nothing ever merged: MinGroups 10 with a cap of 8 over one-row
// groups compacted forever without doing anything, no error and no stat. The
// floor belongs to the run.
func TestARowCapBelowTheFloorStillCompacts(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 200)
	defer s.Close()
	want := readAllRows(t, s)
	st, err := s.CompactGroups(CompactOptions{MinGroups: 10, MaxRowsPerOutput: 8})
	if err != nil {
		t.Fatal(err)
	}
	if st.Outputs == 0 {
		t.Fatal("a run of 200 with a cap of 8 and a floor of 10 compacted nothing")
	}
	if err := sameRows(want, readAllRows(t, s)); err != nil {
		t.Fatal(err)
	}
}

// One unmergeable group breaks the run; it does not refuse everything around
// it. The version that discovered unmergeability inside mergeGroups threw away
// the whole batch, so one vector column among sixty left all sixty unmerged.
func TestAnUnmergeableGroupBreaksTheRunOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 30; i++ {
		g := oneRowGroup(int64(i+1)*1000, map[string]string{"_msg": fmt.Sprintf("m%d", i)})
		if i == 15 {
			// A vector column cannot be decoded back, so this group is not a
			// candidate at all.
			g.Columns = append(g.Columns, Column{
				Name: "vec", Type: ColVector, Vec: []float32{1, 2}, Dim: 2,
			})
		}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}
	want := readAllRows(t, s)
	st, err := s.CompactGroups(CompactOptions{MinGroups: 2})
	if err != nil {
		t.Fatal(err)
	}
	if st.Inputs < 20 {
		t.Errorf("only %d of 29 mergeable groups merged; one unmergeable group poisoned its neighbours", st.Inputs)
	}
	if err := sameRows(want, readAllRows(t, s)); err != nil {
		t.Fatal(err)
	}
}

// A group with no timestamp column is not a candidate: it has no span, so
// merging it would make its rows visible in a window they were invisible in.
func TestATimestamplessGroupIsNotACandidate(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 6; i++ {
		if _, err := s.AppendGroup(oneRowGroup(int64(i+1)*1000, map[string]string{"_msg": "x"})); err != nil {
			t.Fatal(err)
		}
	}
	// One group with dict columns and no timestamp at all.
	d := BuildDict([]string{"orphan"})
	if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{{Name: "_msg", Type: ColDict, Dict: &d}}}); err != nil {
		t.Fatal(err)
	}
	before := len(s.Groups(0, 1<<62))
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2}); err != nil {
		t.Fatal(err)
	}
	// The timestamp-less group is still its own group: it was never merged.
	found := false
	s.mu.RLock()
	for _, g := range s.groups {
		if g.reader != nil && !hasTimestampColumn(g.reader) {
			found = true
		}
	}
	s.mu.RUnlock()
	if !found {
		t.Fatalf("the timestamp-less group was merged away (%d groups before)", before)
	}
}

// A merge that would grow the store is refused, as Recompact refuses one.
func TestAMergeThatGrowsIsRefused(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Two groups of high-cardinality values: merging them cannot share a
	// dictionary, so the output is at best the same size.
	for g := 0; g < 2; g++ {
		rows := 20000
		ts := make([]int64, rows)
		vals := make([]string, rows)
		for i := range ts {
			ts[i] = int64(g*rows+i+1) * 1000
			vals[i] = fmt.Sprintf("%d-%d-unique-value-with-entropy", g, i)
		}
		d := BuildDict(vals)
		if _, err := s.AppendGroup(&Group{Rows: rows, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: ts},
			{Name: "_msg", Type: ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	var before int64
	s.mu.RLock()
	for _, g := range s.groups {
		before += int64(len(g.reader.blob))
	}
	s.mu.RUnlock()

	st, err := s.CompactGroups(CompactOptions{MinGroups: 2})
	if err != nil {
		t.Fatal(err)
	}
	var after int64
	s.mu.RLock()
	for _, g := range s.groups {
		after += int64(len(g.reader.blob))
	}
	s.mu.RUnlock()
	if after > before {
		t.Errorf("the store grew from %d to %d bytes; %+v", before, after, st)
	}
}

// The union gives rows a field they did not have, with an empty value, and
// ValueCounts reports it.
//
// Pinned rather than hidden: the shipped row oracle filters "" out, so no test
// could see it. A columnar group has one value per row per column, so a row
// from a group that lacked the column gets the zero value -- the same thing
// that already happens to a row inside one group whose sibling rows carry a
// field it does not. Compaction spreads it across requests, and `field_values`
// on a compacted store answers with an extra empty string.
func TestTheUnionAddsEmptyValuesThatFieldValuesReports(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		fields := map[string]string{"_msg": fmt.Sprintf("m%d", i)}
		if i == 0 {
			fields["rare"] = "yes"
		}
		if _, err := s.AppendGroup(oneRowGroup(int64(i+1)*1000, fields)); err != nil {
			t.Fatal(err)
		}
	}
	countsOf := func() map[string]int64 {
		out := map[string]int64{}
		for _, r := range s.Groups(0, 1<<62) {
			for _, vc := range r.ValueCounts("rare") {
				out[vc.Value] += int64(vc.Count)
			}
		}
		return out
	}
	before := countsOf()
	if before["yes"] != 1 || len(before) != 1 {
		t.Fatalf("before: %v, want exactly {yes:1}", before)
	}
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2}); err != nil {
		t.Fatal(err)
	}
	after := countsOf()
	if after["yes"] != 1 {
		t.Fatalf("after: %v lost the real value", after)
	}
	if after[""] != 9 {
		t.Fatalf("after: %v; the nine rows that never carried `rare` should now carry it "+
			"empty. If this changed, the union's contract changed with it", after)
	}
}

// A batch has to agree on which column is the timestamp, not merely each have
// one. Two names for it put the output's span outside its inputs' span.
func TestMixedTimestampColumnNamesAreRefused(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 6; i++ {
		name := "_time"
		if i%2 == 1 {
			name = "ts"
		}
		d := BuildDict([]string{fmt.Sprintf("m%d", i)})
		if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{
			{Name: name, Type: ColTimestamp, Ts: []int64{int64(i+1) * 1_000_000_000}},
			{Name: "_msg", Type: ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	beforeGroups := len(s.Groups(0, 1<<62))
	// A window before every row matches nothing, and must go on matching
	// nothing: a merged output that spans from zero would return all six.
	if got := len(s.Groups(0, 1000)); got != 0 {
		t.Fatalf("%d groups overlap [0,1000) before the pass", got)
	}
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2}); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Groups(0, 1000)); got != 0 {
		t.Errorf("%d groups overlap [0,1000) after the pass: a merged output spans from "+
			"zero because one input's timestamp column was zero-filled", got)
	}
	if after := len(s.Groups(0, 1<<62)); after != beforeGroups {
		t.Errorf("%d groups after against %d: a mixed-timestamp batch was merged", after, beforeGroups)
	}
}

// MaxRowsPerOutput bounds an output's rows, and had no test.
func TestMaxRowsPerOutputBoundsTheOutput(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 100)
	defer s.Close()
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 7}); err != nil {
		t.Fatal(err)
	}
	for _, r := range s.Groups(0, 1<<62) {
		if r.Rows > 7 {
			t.Errorf("an output holds %d rows against a cap of 7", r.Rows)
		}
	}
}

// The byte budget bounds the BATCH, not only the totals between batches. A
// batch runs to MaxRows rows, so without this a store that fits in one batch
// is read in full whatever the ceiling says.
func TestMaxInputBytesBoundsASingleBatch(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 200)
	defer s.Close()
	var total int64
	s.mu.RLock()
	for _, g := range s.groups {
		total += int64(len(g.reader.blob))
	}
	s.mu.RUnlock()

	// No row cap: every group fits in one batch, so only the byte budget can
	// stop it. The earlier version passed its own test purely because that
	// test set a row cap of 4.
	st, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxInputBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if st.BytesBefore > total/4 {
		t.Errorf("read %d of %d bytes against a 1-byte ceiling", st.BytesBefore, total)
	}
}

// Merging must not fill the query index cache with groups it is unlinking.
func TestCompactionDoesNotFillTheIndexCache(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 400)
	defer s.Close()
	before := indexCacheUsed.Load()
	if _, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8192}); err != nil {
		t.Fatal(err)
	}
	grew := indexCacheUsed.Load() - before
	// The budget has no eviction and is decremented nowhere, so anything a
	// pass adds is permanent for the life of the process.
	if grew != 0 {
		t.Errorf("a pass added %d bytes to the index cache; those are the indices of groups "+
			"it then unlinked, and the budget is never given back", grew)
	}
}
