package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Every phase of the atomic replacement is failed in turn, and the directory
// is inspected afterwards. The contract is the same at every phase: either
// the destination holds the complete new bytes, or it holds what it held
// before -- never a partial file, and never a leftover temp file that a
// later glob could pick up as real data.
func TestWriteFileAtomicFailsCleanlyAtEveryPhase(t *testing.T) {
	want := []byte("the new contents, complete")
	prior := []byte("what was there before")

	for _, c := range []struct {
		name  string
		point faultPoint
		// Whether the destination is expected to exist afterwards when it
		// existed before. Only a fault after the rename can leave the new
		// bytes in place.
		newBytesLand bool
	}{
		{"create", faultCreate, false},
		{"write", faultWrite, false},
		{"sync", faultSync, false},
		{"close", faultClose, false},
		{"rename", faultRename, false},
		{"dir open", faultDirOpen, true},
		{"dir sync", faultDirSync, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "group-1.bin")
			if err := os.WriteFile(path, prior, 0o600); err != nil {
				t.Fatal(err)
			}

			injected := errors.New("injected")
			restore := setFaultHook(func(p faultPoint) error {
				if p == c.point {
					return injected
				}
				return nil
			})
			err := writeFileAtomic(path, want, 0o600)
			restore()

			if !errors.Is(err, injected) {
				t.Fatalf("phase %s: err %v, want the injected error", c.name, err)
			}

			got, rerr := os.ReadFile(path)
			if rerr != nil {
				t.Fatalf("phase %s: destination unreadable: %v", c.name, rerr)
			}
			if c.newBytesLand {
				// The rename succeeded before the fault, so the new bytes are
				// there. The error still has to be reported: the caller must
				// not treat an unsynced directory entry as durable.
				if string(got) != string(want) {
					t.Fatalf("phase %s: destination %q, want the new bytes", c.name, got)
				}
			} else if string(got) != string(prior) {
				t.Fatalf("phase %s: destination %q, want the prior bytes untouched", c.name, got)
			}

			// No temp file may survive. OpenStore globs group-*.bin, so a
			// leftover partial named anything matching that would be read as
			// a real group.
			ents, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range ents {
				if e.Name() != "group-1.bin" {
					t.Fatalf("phase %s: leftover file %q", c.name, e.Name())
				}
			}
		})
	}
}

// The success path writes the bytes, and the file carries the requested mode
// rather than whatever the process umask would have produced.
func TestWriteFileAtomicSucceeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "group-7.bin")
	want := []byte("complete contents")
	if err := writeFileAtomic(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("read %q, want %q", got, want)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("mode %o, want 600 -- log data must not be world-readable", perm)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("%d entries in the directory, want just the destination", len(ents))
	}
}

// Overwriting an existing file replaces it whole. A short new value must not
// leave a tail of the old one, which is what an in-place rewrite would do.
func TestWriteFileAtomicReplacesWhole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "group-9.bin")
	if err := writeFileAtomic(path, []byte("a much longer previous value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "short" {
		t.Fatalf("read %q, want %q", got, "short")
	}
}

// The store's own append path must go through the helper, so the
// parent-directory fsync applies to real group files and not only to the
// helper's unit test. A group written through AppendGroup is readable, has
// no temp sibling, and carries the restricted mode.
func TestAppendGroupUsesDurableReplace(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{1}}}}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file %q survived AppendGroup", e.Name())
		}
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if perm := fi.Mode().Perm(); perm != 0o600 {
			t.Fatalf("%s has mode %o, want 600", e.Name(), perm)
		}
	}
}

// A failure inside AppendGroup must not leave the group in the index: a
// caller that gets an error and then finds the rows queryable cannot tell
// what is durable.
func TestAppendGroupFailureLeavesNoGroup(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	injected := errors.New("injected")
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultRename {
			return injected
		}
		return nil
	})
	g := &Group{Rows: 1, Columns: []Column{{Name: "_time", Type: ColTimestamp, Ts: []int64{1}}}}
	_, aerr := s.AppendGroup(g)
	restore()

	if !errors.Is(aerr, injected) {
		t.Fatalf("AppendGroup err %v, want the injected error", aerr)
	}
	if got := len(s.Groups(0, 1<<62)); got != 0 {
		t.Fatalf("%d groups visible after a failed append, want 0", got)
	}
}
