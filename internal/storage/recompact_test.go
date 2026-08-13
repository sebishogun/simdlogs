package storage

import (
	"fmt"
	"testing"
)

// buildTestGroup makes a group with a few dict columns of realistic-ish text
// (LZ4-compressible, not hex) plus timestamps.
func buildTestGroup(rows int, base int64) *Group {
	ts := make([]int64, rows)
	level := make([]string, rows)
	msg := make([]string, rows)
	svc := make([]string, rows)
	levels := []string{"info", "warn", "error", "debug"}
	for i := 0; i < rows; i++ {
		ts[i] = base + int64(i)*1_000_000
		level[i] = levels[i%len(levels)]
		msg[i] = fmt.Sprintf("request completed for user %d in handler /api/v1/resource/%d", i%997, i%53)
		svc[i] = fmt.Sprintf("service-%d", i%17)
	}
	ld, md, sd := BuildDict(level), BuildDict(msg), BuildDict(svc)
	return &Group{Rows: rows, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "level", Type: ColDict, Dict: &ld},
		{Name: "_msg", Type: ColDict, Dict: &md},
		{Name: "service", Type: ColDict, Dict: &sd},
	}}
}

// readAll returns every row's (time, level, msg, service) so two encodings of
// the same group can be compared exactly.
func readAll(t *testing.T, r *Reader) [][4]string {
	t.Helper()
	out := make([][4]string, 0, r.Rows)
	ts := r.TimestampsRange("_time", 0, r.Rows)
	lIdx, lDict := r.DictIndices("level")
	mIdx, mDict := r.DictIndices("_msg")
	sIdx, sDict := r.DictIndices("service")
	for i := 0; i < r.Rows; i++ {
		out = append(out, [4]string{
			fmt.Sprint(ts[i]), lDict[lIdx[i]], mDict[mIdx[i]], sDict[sIdx[i]],
		})
	}
	return out
}

func TestRecompactPreservesDataAndShrinks(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const rows = 20000
	base := int64(1_700_000_000_000_000_000)
	if _, err := s.AppendGroup(buildTestGroup(rows, base)); err != nil {
		t.Fatal(err)
	}
	groups := s.Groups(0, 1<<62)
	if len(groups) != 1 {
		t.Fatalf("got %d groups", len(groups))
	}
	want := readAll(t, groups[0])
	sizeBefore := int64(len(groups[0].blob))

	// Cutoff after the group's newest row: it is eligible.
	n, before, after, err := s.Recompact(base+int64(rows)*1_000_000+1, false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recompacted %d groups, want 1", n)
	}
	if after >= before {
		t.Errorf("recompaction did not shrink: %d -> %d", before, after)
	}
	t.Logf("group %d -> %d bytes (%.1f%% smaller)", before, after, 100*(1-float64(after)/float64(before)))

	// Data must be bit-identical through the new encoding.
	got := readAll(t, s.Groups(0, 1<<62)[0])
	if len(got) != len(want) {
		t.Fatalf("rows changed: %d -> %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d changed:\n old %v\n new %v", i, want[i], got[i])
		}
	}
	if sizeBefore <= int64(len(s.Groups(0, 1<<62)[0].blob)) {
		t.Errorf("blob did not shrink in place")
	}

	// Idempotent: a second pass finds nothing left to do.
	n2, _, _, err := s.Recompact(base+int64(rows)*1_000_000+1, false)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Errorf("second Recompact rewrote %d groups, want 0 (not idempotent)", n2)
	}

	// Reopening the store must see the recompacted data.
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got2 := readAll(t, s2.Groups(0, 1<<62)[0])
	for i := range want {
		if got2[i] != want[i] {
			t.Fatalf("row %d changed after reopen", i)
		}
	}
}

func TestRecompactSkipsRecentGroups(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000_000_000)
	if _, err := s.AppendGroup(buildTestGroup(2000, base)); err != nil {
		t.Fatal(err)
	}
	// Cutoff before the group: too recent to recompact.
	n, _, _, err := s.Recompact(base-1, false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("recompacted %d recent groups, want 0", n)
	}
}

func TestRecompactDropPostingsPreservesData(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const rows = 20000
	base := int64(1_700_000_000_000_000_000)
	if _, err := s.AppendGroup(buildTestGroup(rows, base)); err != nil {
		t.Fatal(err)
	}
	r0 := s.Groups(0, 1<<62)[0]
	want := readAll(t, r0)
	if !r0.hasPostings() {
		t.Fatal("fresh group should have postings")
	}
	n, before, after, err := s.Recompact(base+int64(rows)*1_000_000+1, true)
	if err != nil || n != 1 {
		t.Fatalf("recompact: n=%d err=%v", n, err)
	}
	t.Logf("drop-postings: %d -> %d bytes (%.1f%% smaller)", before, after, 100*(1-float64(after)/float64(before)))
	r1 := s.Groups(0, 1<<62)[0]
	if r1.hasPostings() {
		t.Error("postings still present after drop")
	}
	got := readAll(t, r1)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d changed after drop-postings", i)
		}
	}
	// The size win must be real and large (postings are ~27% of a group).
	if float64(after) > 0.85*float64(before) {
		t.Errorf("drop-postings saved only %.1f%%, expected >15%%", 100*(1-float64(after)/float64(before)))
	}
}
