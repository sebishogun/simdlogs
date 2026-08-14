package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// Replay must stop at the first record that is not complete and valid, and
// everything before it must survive. A crash mid-append leaves exactly this
// shape: whole records followed by a torn one.
func TestManifestReplayStopsAtTornTail(t *testing.T) {
	dir := t.TempDir()
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.commit([]uint64{1, 2}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.commit([]uint64{3}, []uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		t.Fatal(err)
	}

	// Truncate at every byte offset. The result must always be a prefix of
	// the committed history: never a panic, never a partially applied record.
	for n := 0; n <= len(full); n++ {
		func() {
			d := t.TempDir()
			if err := os.WriteFile(filepath.Join(d, ManifestFileName), full[:n], 0o600); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("truncated to %d bytes panicked: %v", n, r)
				}
			}()
			m2, err := openManifest(d)
			if err != nil {
				t.Fatalf("truncated to %d bytes: %v", n, err)
			}
			defer m2.close()
			ids := m2.visibleIDs()
			switch {
			case n < 8: // not even one header
				if len(ids) != 0 {
					t.Fatalf("truncated to %d bytes: visible %v, want none", n, ids)
				}
			default:
				// Whatever survived must be one of the two committed states.
				got := idsKey(ids)
				if got != "" && got != "1,2" && got != "2,3" {
					t.Fatalf("truncated to %d bytes: visible %v, not a committed state", n, ids)
				}
			}
		}()
	}
}

func idsKey(ids []uint64) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += string(rune('0' + id))
	}
	return out
}

// A record whose checksum does not match its payload is where replay stops --
// silent corruption in the middle of the log must not be applied.
func TestManifestReplayStopsAtBadCRC(t *testing.T) {
	dir := t.TempDir()
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.commit([]uint64{7}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.commit([]uint64{8}, nil, nil); err != nil {
		t.Fatal(err)
	}
	m.close()

	path := filepath.Join(dir, ManifestFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the second record's payload. Its length prefix stays valid, so
	// only the checksum can catch it.
	first := int(binary.LittleEndian.Uint32(b[0:])) + 8
	b[first+8] ^= 0xFF
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	m2, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.close()
	if !m2.isVisible(7) {
		t.Error("the record before the corruption was lost")
	}
	if m2.isVisible(8) {
		t.Error("a record with a bad checksum was applied")
	}
}

// A torn tail is truncated at open, so the next append starts on a record
// boundary. Without that the new record follows garbage and the replay after
// it stops at the garbage, losing everything written since.
func TestManifestTruncatesTornTailOnOpen(t *testing.T) {
	dir := t.TempDir()
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	m.commit([]uint64{1}, nil, nil)
	m.close()

	path := filepath.Join(dir, ManifestFileName)
	b, _ := os.ReadFile(path)
	// Append a half record.
	if err := os.WriteFile(path, append(b, 0xFF, 0xFF, 0xFF), 0o600); err != nil {
		t.Fatal(err)
	}

	m2, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.commit([]uint64{2}, nil, nil); err != nil {
		t.Fatal(err)
	}
	m2.close()

	m3, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m3.close()
	if !m3.isVisible(1) || !m3.isVisible(2) {
		t.Fatalf("visible %v, want both 1 and 2 after a torn tail was repaired", m3.visibleIDs())
	}
}

// A directory with group files and no manifest is a store written by an
// older binary. It gets one bootstrap record naming what is actually there.
func TestManifestBootstrapsLegacyDirectory(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}}}}
		if _, err := s.AppendGroup(g); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	// Remove the manifest, leaving a legacy-shaped directory.
	if err := os.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 3 {
		t.Fatalf("%d groups after bootstrap, want 3", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFileName)); err != nil {
		t.Fatalf("bootstrap wrote no manifest: %v", err)
	}
}

// A group file that never committed is not visible. This is the crash
// between rename and commit: the bytes are on disk and nothing says they
// belong to the store.
func TestUncommittedGroupFileIsInvisible(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{1}}}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Drop in a valid group file the manifest never named.
	orphan := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{2}}}}
	if err := os.WriteFile(filepath.Join(dir, "group-99.bin"), orphan.Marshal(), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 1 {
		t.Fatalf("%d groups visible, want 1 -- an uncommitted file was indexed", got)
	}
}

// A committed removal survives a restart even when the unlink afterwards
// never happened. Retention dropped its in-memory entry and then unlinked;
// when the unlink failed the group came back at the next start.
func TestCommittedRemovalSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for i := 0; i < 2; i++ {
		g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}}}}
		id, err := s.AppendGroup(g)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// Commit the removal and deliberately do NOT unlink the file.
	if err := s.CommitRemoval(ids[0]); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 1 {
		t.Fatalf("%d groups after restart, want 1 -- a committed removal came back", got)
	}
}

// Compaction rewrites the log as one record naming the live set, and the
// result replays to the same state.
func TestManifestCompactPreservesState(t *testing.T) {
	dir := t.TempDir()
	m, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 20; i++ {
		if err := m.commit([]uint64{i}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.commit(nil, []uint64{3, 4, 5}, nil); err != nil {
		t.Fatal(err)
	}
	before := m.visibleIDs()
	sizeBefore, _ := os.Stat(filepath.Join(dir, ManifestFileName))
	if err := m.compact(); err != nil {
		t.Fatal(err)
	}
	m.close()

	sizeAfter, _ := os.Stat(filepath.Join(dir, ManifestFileName))
	if sizeAfter.Size() >= sizeBefore.Size() {
		t.Errorf("compaction did not shrink the manifest: %d -> %d", sizeBefore.Size(), sizeAfter.Size())
	}

	m2, err := openManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.close()
	after := m2.visibleIDs()
	if len(before) != len(after) {
		t.Fatalf("visible set changed: %v -> %v", before, after)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("visible set changed: %v -> %v", before, after)
		}
	}
}

// Group ids never collide across a restart: the next id continues past the
// highest committed one.
func TestGroupIDsDoNotCollideAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	seen := map[uint64]bool{}
	for round := 0; round < 3; round++ {
		s, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}}}}
			id, err := s.AppendGroup(g)
			if err != nil {
				t.Fatal(err)
			}
			if seen[id] {
				t.Fatalf("group id %d reused after restart", id)
			}
			seen[id] = true
		}
		s.Close()
	}
}

// groupIDFromName must reject anything that is not exactly group-<n>.bin. A
// Sscanf that ignored its error mapped group-.bin to id 0, colliding with a
// real group.
func TestGroupIDFromName(t *testing.T) {
	for _, c := range []struct {
		name string
		id   uint64
		ok   bool
	}{
		{"group-0.bin", 0, true},
		{"group-42.bin", 42, true},
		{"group-.bin", 0, false},
		{"group-x.bin", 0, false},
		{"group-1.bin.tmp", 0, false},
		{"group-01.bin", 0, false}, // not the canonical spelling
		{"notagroup.bin", 0, false},
		{"group-99999999999999999999.bin", 0, false},
	} {
		id, ok := groupIDFromName(c.name)
		if ok != c.ok || (ok && id != c.id) {
			t.Errorf("%s -> (%d, %v), want (%d, %v)", c.name, id, ok, c.id, c.ok)
		}
	}
}
