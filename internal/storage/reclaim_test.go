package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Reclaiming group files the manifest does not name is safe only for ids the
// manifest RECORDED AS REMOVED.
//
// "Not visible" has two meanings and they are not the same fact: an id nothing
// ever committed, and an id whose commit record replay could not reach. Replay
// stops at the first record that fails its checksum -- by design -- so a
// single flipped byte makes every group after it invisible, and a reclaim
// keyed on visibility deletes them. Measured on the version that did: one
// flipped byte in the fifth of ten records left five group files and a store
// reporting `healthy: 5 groups`; a MANIFEST truncated to zero left none of
// twenty. It also destroyed the documented recovery -- remove the MANIFEST and
// let the legacy path adopt the directory -- by removing what there was to
// adopt.
func TestReclaimKeepsFilesAManifestNeverRetired(t *testing.T) {
	for _, tc := range []struct {
		name    string
		groups  int
		corrupt func(t *testing.T, path string)
	}{
		{
			name:   "a flipped byte mid-log",
			groups: 10,
			corrupt: func(t *testing.T, path string) {
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(b) < 40 {
					t.Fatalf("the manifest is %d bytes; the fixture cannot corrupt it", len(b))
				}
				b[len(b)/2] ^= 0xFF
				if err := os.WriteFile(path, b, DataFileMode); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "truncated to nothing",
			groups:  20,
			corrupt: func(t *testing.T, path string) { truncateTo(t, path, 0) },
		},
		{
			name:    "truncated mid-record",
			groups:  12,
			corrupt: func(t *testing.T, path string) { truncateTo(t, path, 9) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := fillOneRowGroups(t, dir, tc.groups)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before := countGroupFiles(t, dir)
			if before != tc.groups {
				t.Fatalf("%d group files before, want %d", before, tc.groups)
			}
			tc.corrupt(t, filepath.Join(dir, ManifestFileName))

			s2, err := OpenStore(dir)
			if err != nil {
				// Refusing to open is a legitimate answer to a corrupt
				// manifest. Deleting the data is not, and the files have to
				// still be there either way.
				t.Logf("open refused: %v", err)
			} else {
				s2.Close()
			}
			if after := countGroupFiles(t, dir); after != before {
				t.Fatalf("%d group files after opening over a corrupt manifest, want %d: "+
					"committed data was deleted because its record could not be replayed",
					after, before)
			}

			// And the documented recovery still works: drop the manifest and
			// let the legacy path adopt what is on disk.
			if err := os.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
				t.Fatal(err)
			}
			s3, err := OpenStore(dir)
			if err != nil {
				t.Fatalf("adopting the directory after removing the manifest: %v", err)
			}
			defer s3.Close()
			if got := len(readAllRows(t, s3)); got != tc.groups {
				t.Fatalf("the legacy adoption recovered %d rows of %d", got, tc.groups)
			}
		})
	}
}

// What reclamation IS for: a compaction killed after its commit leaves its
// inputs on disk, the manifest recorded them removed, and the next open
// reclaims them.
func TestReclaimRemovesGroupsTheManifestRetired(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 40)
	want := readAllRows(t, s)

	restore := setFaultHook(func(p faultPoint) error {
		if p == faultCompactUnlink {
			return errReclaimFault
		}
		return nil
	})
	_, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8})
	restore()
	if err == nil {
		t.Fatal("the pass returned nil")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	leaked := countGroupFiles(t, dir)
	if leaked <= 5 {
		t.Fatalf("%d group files after a kill before the unlink; the inputs were already gone", leaked)
	}

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := sameRows(want, readAllRows(t, s2)); err != nil {
		t.Fatal(err)
	}
	after := countGroupFiles(t, dir)
	if after >= leaked {
		t.Errorf("%d group files after the reopen against %d before: the merged-away inputs "+
			"were not reclaimed, and the tombstone list does not survive a restart", after, leaked)
	}
	// Every file left is one the store can see.
	visible := map[string]bool{}
	s2.mu.RLock()
	for _, g := range s2.groups {
		visible[filepath.Base(g.path)] = true
	}
	s2.mu.RUnlock()
	if len(visible) != after {
		t.Errorf("%d files on disk against %d visible groups", after, len(visible))
	}
}

// An uncommitted append's file is NOT reclaimed: no record ever named it, so
// nothing says it is residue rather than a group whose commit is still coming.
func TestReclaimLeavesAnUncommittedAppendAlone(t *testing.T) {
	dir := t.TempDir()
	s := fillOneRowGroups(t, dir, 3)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A group file with no manifest record, exactly what a kill between the
	// rename and the commit leaves.
	orphan := filepath.Join(dir, "group-99.bin")
	src := filepath.Join(dir, "group-0.bin")
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, b, DataFileMode); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("an uncommitted group file was reclaimed: %v", err)
	}
}

var errReclaimFault = errors.New("a fault the reclaim test injected")

func countGroupFiles(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if _, ok := groupIDFromName(e.Name()); ok {
			n++
		}
	}
	return n
}

func truncateTo(t *testing.T, path string, n int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(n); err != nil {
		t.Fatal(err)
	}
}
