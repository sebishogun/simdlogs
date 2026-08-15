package storage

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// What a staged restore must guarantee that RestoreTar does not.
//
// RestoreTar writes each group into the destination as it reads it, so a
// failure halfway leaves a destination holding half a store -- and a directory
// of valid group files opens as a store and answers queries with a silent
// subset. That is the shape a disaster-recovery tool must not have: the moment
// it is used is the moment there is nothing to compare against.

// backupOf returns a valid archive of n groups.
func backupOf(t *testing.T, n int) []byte {
	t.Helper()
	s, _ := backupStore(t, n, 4)
	defer s.Close()
	var buf bytes.Buffer
	if err := s.BackupTarWith(&buf, BackupOptions{Tenant: "tenant-1"}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	return buf.Bytes()
}

// The whole store lands, or nothing does.
func TestRestoreIsAtomic(t *testing.T) {
	archive := backupOf(t, 4)

	// Truncate mid-stream. The staged restore must leave the destination
	// untouched, where RestoreTar leaves a partial one.
	cut := len(archive) * 3 / 4
	dst := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(archive[:cut]), dst, RestoreOptions{}); err == nil {
		t.Fatal("a truncated archive restored")
	}
	// No GROUPS, rather than no directory. A failed restore leaves at most an
	// empty directory holding a lock nobody holds -- the lock file is never
	// unlinked, because unlinking one hands a lock to two processes -- and a
	// directory of valid group files is what opens as a store.
	if n := countGroups(dirNames(dst)); n != 0 {
		t.Fatalf("the destination holds %v after a failed restore", dirNames(dst))
	}
	// And the staging directory is gone with it.
	if _, err := os.Stat(dst + ".restoring"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the staging directory survived a failed restore")
	}

	// The same archive whole lands completely and opens.
	man, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(man.Groups) != 4 {
		t.Fatalf("the manifest names %d groups, want 4", len(man.Groups))
	}
	s, err := OpenStore(dst)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer s.Close()
	snap, err := s.SnapshotAll()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	rows := 0
	for _, r := range snap.Groups {
		rows += r.Rows
	}
	if rows != 16 {
		t.Fatalf("the restored store holds %d rows, want 16", rows)
	}
}

// A dry run validates and writes nothing, which is what an operator wants
// before committing and what a scheduled backup check wants instead.
func TestRestoreDryRunWritesNothing(t *testing.T) {
	archive := backupOf(t, 3)
	dst := filepath.Join(t.TempDir(), "store")

	man, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(man.Groups) != 3 {
		t.Fatalf("the manifest names %d groups, want 3", len(man.Groups))
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a dry run created the destination")
	}
	// And it still refuses a bad archive.
	if _, err := Restore(bytes.NewReader(archive[:len(archive)/2]), dst,
		RestoreOptions{DryRun: true}); err == nil {
		t.Fatal("a dry run accepted a truncated archive")
	}
}

// A restore never merges, and never overwrites a live store.
func TestRestoreRefusesAnOccupiedDestination(t *testing.T) {
	archive := backupOf(t, 2)

	// Non-empty.
	occupied := t.TempDir()
	if err := os.WriteFile(filepath.Join(occupied, "something"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(bytes.NewReader(archive), occupied, RestoreOptions{}); !errors.Is(err, ErrDestinationNotEmpty) {
		t.Fatalf("restore into an occupied directory returned %v", err)
	}

	// A live store, which holds a MANIFEST as well as its lock.
	live := t.TempDir()
	s, err := OpenStore(live)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = Restore(bytes.NewReader(archive), live, RestoreOptions{}); !errors.Is(err, ErrDestinationNotEmpty) {
		t.Fatalf("restore over a live store returned %v", err)
	}

	// A directory holding NOTHING but a held lock is refused by the lock, not
	// by the listing -- which is the only thing that can tell a running
	// process from a file it left behind.
	held := t.TempDir()
	lk, err := lockDir(held)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(bytes.NewReader(archive), held, RestoreOptions{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("restore into a locked directory returned %v, want ErrLocked", err)
	}
	lk.unlock()

	// And once nothing holds it, the same directory restores. unlock closes
	// the descriptor and never unlinks the file, so a cleanly stopped store
	// and a SIGKILLed restore both leave a LOCK behind; counting it as an
	// occupant made a killed restore unrecoverable by the tool that produced
	// it.
	if _, err := Restore(bytes.NewReader(archive), held, RestoreOptions{}); err != nil {
		t.Fatalf("restore into a directory holding only a stale lock: %v", err)
	}
	if n := countGroups(dirNames(held)); n != 2 {
		t.Fatalf("the restored store holds %v, want 2 groups", dirNames(held))
	}

	// Empty is fine.
	empty := t.TempDir()
	if _, err := Restore(bytes.NewReader(archive), empty, RestoreOptions{}); err != nil {
		t.Fatalf("restore into an empty directory: %v", err)
	}
}

// An archive from another tenant restores into that tenant's directory and
// answers its queries with someone else's logs. The manifest is the only place
// that fact exists.
func TestRestoreRefusesTheWrongTenant(t *testing.T) {
	archive := backupOf(t, 2)
	dst := filepath.Join(t.TempDir(), "store")

	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{RequireTenant: "tenant-9"})
	if !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("restore of another tenant's archive returned %v", err)
	}
	if n := countGroups(dirNames(dst)); n != 0 {
		t.Fatalf("the wrong-tenant archive wrote %v before it was refused", dirNames(dst))
	}
	// The right one is accepted.
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{RequireTenant: "tenant-1"}); err != nil {
		t.Fatalf("restore of the matching tenant: %v", err)
	}
}

// Every limit is a bound on something an untrusted archive chooses.
//
// Each subtest names the limit it means to trip, and the sizes are chosen so
// that limit is the FIRST one reached: `MaxBytes` set below one group's size
// is not a total-bytes test, it is a second per-entry test, and the
// accumulation -- the only thing that distinguishes the two -- goes untested.
func TestRestoreLimits(t *testing.T) {
	archive := backupOf(t, 4)
	one := oneGroupBytes(t, archive)

	for _, c := range []struct {
		name string
		opt  RestoreOptions
		want string
	}{
		// The manifest names the group count before a single entry is read,
		// so this is where a verified archive is refused -- and it is refused
		// before the decode has sized anything from it.
		{"group count from the manifest", RestoreOptions{MaxFiles: 2}, "the manifest names 4 groups, over the 2 limit"},
		// Two groups fit, the third does not: the running total is what
		// refuses it, which a ceiling below one group's size could not show.
		{"total bytes accumulate", RestoreOptions{MaxBytes: 2*one + one/2}, "exceed the"},
		{"per entry", RestoreOptions{MaxFileBytes: one - 1}, "ceiling"},
		{"manifest bytes", RestoreOptions{MaxManifestBytes: 8}, "the backup manifest declares"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "store")
			_, err := Restore(bytes.NewReader(archive), dst, c.opt)
			if err == nil {
				t.Fatal("the limit was not applied")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			if n := countGroups(dirNames(dst)); n != 0 {
				t.Fatalf("the refusal left %v in the destination", dirNames(dst))
			}
			// The same limits apply to a dry run. They used to not: DryRun
			// called VerifyBackup, which is readBackup with no callback, and
			// every limit lived in that callback -- so the one mode an
			// operator points at an untrusted archive checked nothing.
			dryOpt := c.opt
			dryOpt.DryRun = true
			if _, derr := Restore(bytes.NewReader(archive), "", dryOpt); derr == nil {
				t.Fatal("the dry run accepted an archive the restore refused")
			} else if !strings.Contains(derr.Error(), c.want) {
				t.Fatalf("the dry run refused for a different reason: %v", derr)
			}
		})
	}

	// And the whole archive passes when nothing is bound too tightly, so the
	// subtests above are refusing on their limit rather than on the fixture.
	dst := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("the unbounded restore of the same archive failed: %v", err)
	}
}

// oneGroupBytes reports the size of a single group entry in the archive, so a
// limit can be placed between one group and two rather than guessed.
func oneGroupBytes(t *testing.T, archive []byte) int64 {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if _, ok := groupIDFromName(filepath.Base(hdr.Name)); ok && hdr.Size > 0 {
			return hdr.Size
		}
	}
	t.Fatal("the archive holds no group entry")
	return 0
}

// A crafted entry name cannot escape the staging directory, and cannot place a
// MANIFEST that would make the restored groups invisible.
func TestRestoreRefusesEscapingAndNonGroupNames(t *testing.T) {
	archive := backupOf(t, 2)

	var extra bytes.Buffer
	tw := tar.NewWriter(&extra)
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		b := make([]byte, hdr.Size)
		if _, rerr := tr.Read(b); rerr != nil && hdr.Size > 0 {
			// short read is fine for this fixture
			_ = rerr
		}
		if hdr.Name == backupCompleteName {
			for _, name := range []string{"../escape.bin", ManifestFileName, LockFileName, "notes.txt"} {
				if werr := tw.WriteHeader(&tar.Header{
					Name: name, Mode: 0o600, Size: 0, Typeflag: tar.TypeReg,
				}); werr != nil {
					t.Fatal(werr)
				}
			}
		}
		hdr.Size = int64(len(b))
		if werr := tw.WriteHeader(hdr); werr != nil {
			t.Fatal(werr)
		}
		if _, werr := tw.Write(b); werr != nil {
			t.Fatal(werr)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	parent := t.TempDir()
	dst := filepath.Join(parent, "store")
	if _, err := Restore(bytes.NewReader(extra.Bytes()), dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// LOCK is not in this list: a restored store carries one exactly as a
	// store the server created does, because a lock file is never unlinked.
	// What must not be there is anything the ARCHIVE named.
	for _, name := range []string{ManifestFileName, "notes.txt", "escape.bin"} {
		if _, err := os.Stat(filepath.Join(dst, name)); err == nil {
			t.Errorf("the restore placed %q", name)
		}
	}
	if _, err := os.Stat(filepath.Join(parent, "escape.bin")); err == nil {
		t.Error("an entry escaped the destination")
	}
}

// pausingReader runs a hook once, after at least n bytes have been read, so a
// test can look at the filesystem while a restore is in flight.
type pausingReader struct {
	r    io.Reader
	at   int64
	read int64
	hook func()
	done bool
}

func (p *pausingReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.read += int64(n)
	if !p.done && p.read >= p.at {
		p.done = true
		p.hook()
	}
	return n, err
}

// Nothing reaches the destination until the rename. That is the difference
// between this and RestoreTar, and it is the only thing the staging directory
// buys -- so it has to be asserted while a restore is IN FLIGHT, not after one
// failed. A test that truncates an archive and finds the destination absent
// passes just as well against a build with no staging at all, because the
// error-path defer removes what it wrote; the staging property only shows
// itself mid-stream or after a crash, where no defer runs.
func TestRestoreWritesNothingIntoTheDestinationUntilTheRename(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")

	var midDst, midStaging []string
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: int64(len(archive) / 2),
		hook: func() {
			midDst = dirNames(dst)
			midStaging = dirNames(dst + ".restoring")
		},
	}
	if _, err := Restore(r, dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Halfway through, the destination holds this restore's own lock and
	// nothing else, and the groups read so far are in the staging sibling.
	for _, name := range midDst {
		if name != LockFileName {
			t.Errorf("the destination held %q while the archive was still being read", name)
		}
	}
	if len(midStaging) == 0 {
		t.Error("the staging directory was empty halfway through; nothing was being staged")
	}
	// And afterwards the store is in the destination, carrying a LOCK nobody
	// holds -- exactly as a store the server created does, because a lock file
	// is never unlinked -- and no staging directory left over.
	names := dirNames(dst)
	if countGroups(names) != 4 {
		t.Fatalf("the restored store holds %v, want 4 groups", names)
	}
	// The lock file stays -- unlinking one hands a lock to two processes --
	// and nobody holds it, which is what OpenStore needs.
	if !contains(names, LockFileName) {
		t.Error("the restored store has no lock file")
	}
	if s, oerr := OpenStore(dst); oerr != nil {
		t.Errorf("the restored store does not open: %v", oerr)
	} else {
		s.Close()
	}
	if _, err := os.Stat(dst + ".restoring"); !errors.Is(err, os.ErrNotExist) {
		t.Error("the staging directory survived a successful restore")
	}
}

// A server cannot open the destination while a restore is running.
//
// This is the window the emptiness check could not cover and the lock exists
// for. The check ran once, at the start, and the destination was removed at
// the end: a server that opened the directory in between had its LOCK, its
// manifest and every group deleted, the archive renamed over the top, and the
// call returned nil -- while the still-running writer allocated group ids from
// its own counter and overwrote the archive's files. The store that came out
// of that opened clean and answered with a mixture of the two.
//
// So the assertion has to be made MID-RESTORE. Asserting that a restore into
// an already-live store is refused proves nothing about the lock: the
// start-of-call emptiness check refuses that one on its own, and does so with
// the lock deleted.
func TestRestoreLocksTheDestinationUntilTheRemoval(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")

	var midOpen error
	var opened bool
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: int64(len(archive) / 2),
		hook: func() {
			s, err := OpenStore(dst)
			midOpen = err
			if err == nil {
				opened = true
				s.Close()
			}
		},
	}
	if _, err := Restore(r, dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if opened {
		t.Fatalf("a store opened at the destination while the restore was still reading (%v)", midOpen)
	}
	if !errors.Is(midOpen, ErrLocked) {
		t.Fatalf("the mid-restore open failed with %v, want ErrLocked", midOpen)
	}
	if n := countGroups(dirNames(dst)); n != 4 {
		t.Fatalf("the restored store holds %v, want 4 groups", dirNames(dst))
	}
}

// A store that is already open is refused, and is not damaged by the attempt.
// The start-of-call emptiness check is what refuses this one; the lock covers
// the window it cannot see, in the test above.
func TestRestoreRefusesALiveStoreAndLeavesItIntact(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "store")
	live, err := OpenStore(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if _, err := live.AppendGroup(backupGroupFixture(0, 3)); err != nil {
		t.Fatal(err)
	}
	before := dirNames(dst)

	archive := backupOf(t, 4)
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err == nil {
		t.Fatal("a restore over a live store succeeded")
	}
	// The live store still holds exactly what it held, and still works.
	if got := dirNames(dst); !equalNames(got, before) {
		t.Fatalf("the live store's directory changed from %v to %v", before, got)
	}
	if _, err := live.AppendGroup(backupGroupFixture(1, 2)); err != nil {
		t.Fatalf("the live store broke after the refused restore: %v", err)
	}
	if live.TotalRows() != 5 {
		t.Fatalf("the live store holds %d rows, want 5", live.TotalRows())
	}
}

// A second restore cannot START while the lock is held, which is what this
// test pins. It does not pin the gap between the two renames, where one can
// and the outcome is one of three -- see docs/wrong.md's entry on two
// restores parked in each other's gap. They derive the same
// staging path, so without the lock the second one's RemoveAll deleted the
// first one's staged files mid-stream and both wrote into the same directory:
// measured, that put two groups from each of two archives into the
// destination, with one call returning success and no tenant check asked for
// on either.
func TestRestoreIsSerializedAgainstAnotherRestore(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "store")
	archive := backupOf(t, 4)

	started := make(chan struct{})
	release := make(chan struct{})
	var second error
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: int64(len(archive) / 2),
		hook: func() {
			close(started)
			<-release
		},
	}
	go func() {
		<-started
		_, second = Restore(bytes.NewReader(archive), dst, RestoreOptions{})
		close(release)
	}()
	if _, err := Restore(r, dst, RestoreOptions{}); err != nil {
		t.Fatalf("the first restore: %v", err)
	}
	<-release
	if second == nil {
		t.Fatal("a second restore into the same destination succeeded concurrently")
	}
	if n := countGroups(dirNames(dst)); n != 4 {
		t.Fatalf("the destination holds %v, want exactly one archive's 4 groups", dirNames(dst))
	}
}

// Two destination shapes that reported success or a very late failure.
func TestRestoreRefusesUnsafeDestinationPaths(t *testing.T) {
	dir := t.TempDir()
	archive := backupOf(t, 2)

	// A symbolic link: os.ReadDir follows it, so the emptiness check passed
	// against the TARGET, and os.RemoveAll then unlinked the link itself --
	// the store landed in the link's parent and the target kept whatever it
	// had, with success reported.
	target := filepath.Join(dir, "volume")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "store-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := Restore(bytes.NewReader(archive), link, RestoreOptions{}); !errors.Is(err, ErrRestoreDestination) {
		t.Fatalf("restore into a symlink: %v, want ErrRestoreDestination", err)
	}
	if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("the symlink was replaced")
	}

	// "." -- os.RemoveAll refuses any path ending in one, so the restore
	// failed AFTER writing the whole archive to staging.
	if _, err := Restore(bytes.NewReader(archive), ".", RestoreOptions{}); !errors.Is(err, ErrRestoreDestination) {
		t.Fatalf(`restore into ".": %v, want ErrRestoreDestination`, err)
	}
}

// A trailing slash is not a different directory. filepath.Clean is what makes
// that true, and without it the staging path becomes a CHILD of the
// destination -- which the destination's own removal then eats, so every such
// restore fails after doing all the work.
func TestRestoreAcceptsATrailingSlash(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "store")
	archive := backupOf(t, 2)
	if _, err := Restore(bytes.NewReader(archive), dst+string(filepath.Separator), RestoreOptions{}); err != nil {
		t.Fatalf("restore into a path with a trailing slash: %v", err)
	}
	if n := countGroups(dirNames(dst)); n != 2 {
		t.Fatalf("the restored store holds %v, want 2 groups", dirNames(dst))
	}
}

// A dry run touches no destination at all, so a scheduled backup check needs
// no directory and a dry run against the store you intend to overwrite is not
// refused for being occupied.
func TestDryRunNeedsNoDestination(t *testing.T) {
	archive := backupOf(t, 3)
	man, err := Restore(bytes.NewReader(archive), "", RestoreOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry run with no destination: %v", err)
	}
	if len(man.Groups) != 3 {
		t.Fatalf("the dry run validated %d groups, want 3", len(man.Groups))
	}
	// And against an occupied destination, which is the store an operator is
	// about to replace and the one they most want to check the archive
	// against first.
	occupied := t.TempDir()
	if err := os.WriteFile(filepath.Join(occupied, "in-the-way"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(bytes.NewReader(archive), occupied, RestoreOptions{DryRun: true}); err != nil {
		t.Fatalf("dry run against an occupied destination: %v", err)
	}
	if names := dirNames(occupied); len(names) != 1 || names[0] != "in-the-way" {
		t.Fatalf("the dry run changed the destination: %v", names)
	}
}

// dirNames lists a directory, or nil when it does not exist.
func dirNames(dir string) []string {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// unverifiedArchive builds a pre-format-1 archive: group entries and no
// manifest, under the names given.
func unverifiedArchive(t *testing.T, names ...string) []byte {
	t.Helper()
	blob := backupGroupFixture(0, 3).Marshal()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range names {
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(blob)), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// An archive from before the manifest format has nothing to validate against.
// Refusing it outright left an operator holding an old backup with no
// supported path at all, so AllowUnverified keeps what landed -- and still
// returns ErrBackupUnverified, so a caller that requires a verified restore
// fails rather than being told success with a note.
func TestRestoreUnverifiedArchive(t *testing.T) {
	archive := unverifiedArchive(t, "group-0.bin", "group-1.bin")

	// The default: refused, and nothing kept.
	dst := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("restore of a manifest-less archive: %v, want ErrBackupUnverified", err)
	}
	// A refused restore leaves at most an empty directory holding a lock
	// nobody holds -- the lock file is never unlinked, because unlinking one
	// is what hands a lock to two processes. The next restore accepts it.
	if n := countGroups(dirNames(dst)); n != 0 {
		t.Fatalf("the refused archive left %v behind", dirNames(dst))
	}

	// Allowed: kept, still reported as unverified.
	dst2 := filepath.Join(t.TempDir(), "store")
	_, err := Restore(bytes.NewReader(archive), dst2, RestoreOptions{AllowUnverified: true})
	if !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("AllowUnverified restore: %v, want ErrBackupUnverified", err)
	}
	if n := countGroups(dirNames(dst2)); n != 2 {
		t.Fatalf("the restored store holds %v, want 2 groups", dirNames(dst2))
	}
	if s, oerr := OpenStore(dst2); oerr != nil {
		t.Fatalf("the restored store does not open: %v", oerr)
	} else {
		defer s.Close()
		if s.TotalRows() != 6 {
			t.Fatalf("the restored store holds %d rows, want 6", s.TotalRows())
		}
	}

	// A tenant requirement cannot be met by an archive that names no tenant,
	// and silently accepting one would put another tenant's logs in place
	// under a flag whose whole purpose is to stop exactly that.
	dst3 := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(archive), dst3, RestoreOptions{
		AllowUnverified: true, RequireTenant: "tenant-1",
	}); !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("AllowUnverified with RequireTenant: %v, want ErrWrongTenant", err)
	}

	// That one is refused by the groups-before-manifest rule, which fires on
	// the first entry. The refusal at the END of readRestore is reachable only
	// for an archive with no entries to refuse -- and it has to be there, or
	// an EMPTY manifest-less archive would restore under a tenant it does not
	// name. Two rules, one property, and a probe that deletes either must
	// fail something.
	var empty bytes.Buffer
	tw := tar.NewWriter(&empty)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	dst4 := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(empty.Bytes()), dst4, RestoreOptions{
		AllowUnverified: true, RequireTenant: "tenant-1",
	}); !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("an empty manifest-less archive with RequireTenant: %v, want ErrWrongTenant", err)
	}
}

// A crafted entry name cannot escape, and this is the archive that can prove
// it: the flattening in readBackup is only reachable for an entry that gets as
// far as being written, and every name in a VERIFIED archive is rejected first
// for not being in the manifest. Deleting the flattening left the whole suite
// green while `../group-9.bin` landed in the destination's parent.
func TestRestoreRefusesEscapingNamesInAnUnverifiedArchive(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "sub", "store")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	archive := unverifiedArchive(t, "../group-9.bin", "group-0.bin")

	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{AllowUnverified: true})
	if err != nil && !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("restore: %v", err)
	}
	// The escaping name was flattened to group-9.bin and stayed inside.
	for _, outside := range []string{
		filepath.Join(dir, "sub", "group-9.bin"),
		filepath.Join(dir, "group-9.bin"),
	} {
		if _, serr := os.Stat(outside); serr == nil {
			t.Fatalf("an archive entry escaped to %s", outside)
		}
	}
	names := dirNames(dst)
	if countGroups(names) != 2 {
		t.Fatalf("the restored store holds %v, want group-0.bin and the flattened group-9.bin", names)
	}
}

// A directory sync that fails AFTER the rename is not a failed restore. The
// store is in place; what is not guaranteed is that the rename survives a
// power loss. Reporting it as a plain failure sends an operator to retry into
// a destination that is now occupied, and to believe their data is not there.
func TestRestoreReportsALandedStoreWhenTheFinalSyncFails(t *testing.T) {
	archive := backupOf(t, 2)

	// Count the directory syncs a clean restore performs, so the injection
	// below can fail the LAST one -- the post-rename one -- rather than a
	// per-file sync inside writeFileAtomic.
	var total int
	restoreOK := t.TempDir()
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) == "dir-sync" {
			total++
		}
		return nil
	})
	if _, err := Restore(bytes.NewReader(archive), filepath.Join(restoreOK, "store"), RestoreOptions{}); err != nil {
		stop()
		t.Fatalf("the counting restore: %v", err)
	}
	stop()
	if total < 2 {
		t.Fatalf("a restore performed %d directory syncs; too few to identify the last", total)
	}

	dst := filepath.Join(t.TempDir(), "store")
	n := 0
	stop = SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) != "dir-sync" {
			return nil
		}
		n++
		if n == total {
			return errors.New("injected dir-sync failure")
		}
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	stop()
	if !errors.Is(err, ErrRestoredButUnsynced) {
		t.Fatalf("a failed post-rename sync returned %v, want ErrRestoredButUnsynced", err)
	}
	if n := countGroups(dirNames(dst)); n != 2 {
		t.Fatalf("the destination holds %v; the store did not land", dirNames(dst))
	}
	if s, oerr := OpenStore(dst); oerr != nil {
		t.Fatalf("the landed store does not open: %v", oerr)
	} else {
		s.Close()
	}
}

// A dry run writes nothing ANYWHERE, not merely nothing into a destination.
//
// The first version signalled "do not write" with an empty staging path, and
// filepath.Join("", name) is name -- so a dry run that lost its guard wrote
// group files into the process's working directory, and every test passed
// because no test looked there. The signal is a nil function now, which has no
// such failure mode, and this is the test that would have caught the sentinel.
func TestDryRunWritesNothingIntoTheWorkingDirectory(t *testing.T) {
	// A CLEAN working directory, and its own. Comparing the package
	// directory before and after passes as soon as any earlier test in the
	// package has already polluted it -- the strays are in the "before"
	// snapshot -- so the test failed alone and passed in the suite, which is
	// the same before-and-after mistake as round one's, on a different
	// property.
	t.Chdir(t.TempDir())
	archive := backupOf(t, 3)
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	before := dirNames(wd)
	if len(before) != 0 {
		t.Fatalf("the working directory is not empty to begin with: %v", before)
	}
	if _, err := Restore(bytes.NewReader(archive), "", RestoreOptions{DryRun: true}); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if after := dirNames(wd); !equalNames(after, before) {
		t.Fatalf("the dry run changed the working directory:\nbefore %v\nafter  %v", before, after)
	}
}

// The emptiness check is repeated under the lock, and the repeat is not
// ceremony: the lock stops a process from OPENING a store at the destination,
// it does not stop one from dropping a file in. Without the re-check that file
// is deleted by os.RemoveAll(dst) at the end.
func TestRestoreRefusesADestinationThatFillsMidRun(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	intruder := filepath.Join(dst, "someone-elses-file")

	// Written after the first check has passed and before the rename.
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: 1,
		hook: func() {
			if err := os.WriteFile(intruder, []byte("do not delete me"), 0o600); err != nil {
				t.Error(err)
			}
		},
	}
	_, err := Restore(r, dst, RestoreOptions{})
	if !errors.Is(err, ErrDestinationNotEmpty) {
		t.Fatalf("restore into a destination that filled mid-run: %v, want ErrDestinationNotEmpty", err)
	}
	if b, rerr := os.ReadFile(intruder); rerr != nil || string(b) != "do not delete me" {
		t.Fatalf("the file that appeared mid-restore was destroyed: %v / %q", rerr, b)
	}
}

// A restore killed halfway must not make the destination unrestorable.
//
// lockDir creates dst/LOCK and nothing unlinks it when the process dies, so a
// destination check that counted it left the disaster-recovery tool unable to
// recover from its own crash without a manual rm. Simulated here by leaving
// exactly the residue a SIGKILL leaves: an unheld LOCK plus a staging
// directory of half the groups.
func TestRestoreRecoversFromItsOwnCrashResidue(t *testing.T) {
	archive := backupOf(t, 4)
	parent := t.TempDir()
	dst := filepath.Join(parent, "store")
	if err := os.Mkdir(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	lk, err := lockDir(dst) // creates dst/LOCK
	if err != nil {
		t.Fatal(err)
	}
	lk.unlock() // the process died: the flock is gone, the file is not
	staging := dst + ".restoring"
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	// Exactly what a killed restore leaves: the marker it writes first --
	// BESIDE the staging directory, not inside it -- and however many groups
	// it had got through.
	if err := os.WriteFile(dst+restoringMarkerSuffix, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "group-0.bin"), []byte("half a group"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("a retry after a killed restore: %v", err)
	}
	if n := countGroups(dirNames(dst)); n != 4 {
		t.Fatalf("the restored store holds %v, want 4 groups", dirNames(dst))
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Error("the retry left the crashed run's staging directory behind")
	}
	if _, err := os.Stat(dst + restoringMarkerSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Error("the retry left the crashed run's marker behind")
	}
}

// The staging path is derived from the destination, so it can name a directory
// that has nothing to do with a restore: `-dst /srv/data/logs` derives
// `/srv/data/logs.restoring`. Removing it on the rule "does it hold only group
// files" is not enough -- a pre-MANIFEST store is a directory of group files
// and nothing else, which is exactly what crashed staging looks like, and that
// rule destroyed one.
func TestRestoreRefusesAStagingPathHoldingSomethingElse(t *testing.T) {
	archive := backupOf(t, 2)
	for _, c := range []struct {
		name  string
		files map[string]string
	}{
		{"an unrelated file", map[string]string{"important.txt": "not a group"}},
		// The case the marker exists for: indistinguishable from crashed
		// staging by content alone.
		{"a legacy store", map[string]string{"group-0.bin": "x", "group-1.bin": "y"}},
		{"an empty directory", map[string]string{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			parent := t.TempDir()
			dst := filepath.Join(parent, "store")
			staging := dst + ".restoring"
			if err := os.Mkdir(staging, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, body := range c.files {
				if err := os.WriteFile(filepath.Join(staging, name), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); !errors.Is(err, ErrDestinationNotEmpty) {
				t.Fatalf("restore over an occupied staging path: %v, want ErrDestinationNotEmpty", err)
			}
			for name, body := range c.files {
				b, rerr := os.ReadFile(filepath.Join(staging, name))
				if rerr != nil || string(b) != body {
					t.Fatalf("%s was destroyed: %v / %q", name, rerr, b)
				}
			}
		})
	}
}

// The destination named in the README is `./simdlogs-data/tenant-0-0`, whose
// parent the SERVER creates. After the disk loss a restore is recovering from,
// neither exists -- and os.Mkdir fails on the missing parent.
func TestRestoreCreatesTheDestinationsParents(t *testing.T) {
	archive := backupOf(t, 2)
	dst := filepath.Join(t.TempDir(), "simdlogs-data", "tenant-0-0")
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore into a path whose parent does not exist: %v", err)
	}
	if n := countGroups(dirNames(dst)); n != 2 {
		t.Fatalf("the restored store holds %v, want 2 groups", dirNames(dst))
	}
}

// An operator who made the data directory 0700 did so on purpose, and a rename
// substitutes the staging directory's mode for it.
func TestRestorePreservesTheDestinationsMode(t *testing.T) {
	if !umaskSupported {
		t.Skip("no umask on this platform")
	}
	// A mode the process umask would otherwise strip. Creating the staging
	// directory with the destination's mode is most of the fix, but mkdir
	// applies the umask to it -- so a group-shared data directory comes back
	// group-read-only unless the mode is set explicitly after the fact. 0770
	// under the standard 022 is exactly that case.
	old := setUmask(0o022)
	defer setUmask(old)

	archive := backupOf(t, 2)
	dst := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dst, 0o770); err != nil { // past the umask
		t.Fatal(err)
	}
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o770 {
		t.Fatalf("the destination came out %04o, want 0770", got)
	}

	// A destination this call CREATED must come out exactly as OpenStore
	// would have made it -- umask included. Chmodding it to the hard-coded
	// 0755 regardless made the restore produce a wider directory than the
	// server: 0755 where OpenStore under umask 077 gives 0700, which is log
	// data made world-readable by the fix for the opposite defect. Under 022,
	// the umask this test used to run at, the two agree and nothing shows.
	setUmask(0o077)
	for _, c := range []struct{ name, dir string }{
		{"restored", filepath.Join(t.TempDir(), "new")},
		{"opened", filepath.Join(t.TempDir(), "opened")},
	} {
		if c.name == "restored" {
			if _, err := Restore(bytes.NewReader(backupOf(t, 2)), c.dir, RestoreOptions{}); err != nil {
				t.Fatalf("restore into a fresh path: %v", err)
			}
		} else {
			s, err := OpenStore(c.dir)
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
		}
		fi, err := os.Stat(c.dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := fi.Mode().Perm(); got != 0o700 {
			t.Fatalf("a %s destination under umask 077 came out %04o, want 0700", c.name, got)
		}
	}
}

// Two facts, both of which change what an operator does next: the store landed,
// and nothing validated its groups. Reporting only the first sends them away
// believing the archive was checked.
func TestUnsyncedAndUnverifiedAreBothReported(t *testing.T) {
	archive := unverifiedArchive(t, "group-0.bin", "group-1.bin")

	var total int
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) == "dir-sync" {
			total++
		}
		return nil
	})
	if _, err := Restore(bytes.NewReader(archive), filepath.Join(t.TempDir(), "s"),
		RestoreOptions{AllowUnverified: true}); !errors.Is(err, ErrBackupUnverified) {
		stop()
		t.Fatalf("the counting restore: %v", err)
	}
	stop()

	dst := filepath.Join(t.TempDir(), "store")
	n := 0
	stop = SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) != "dir-sync" {
			return nil
		}
		n++
		if n == total {
			return errors.New("injected dir-sync failure")
		}
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{AllowUnverified: true})
	stop()
	if !errors.Is(err, ErrRestoredButUnsynced) {
		t.Fatalf("err = %v, want ErrRestoredButUnsynced", err)
	}
	if !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("err = %v, want ErrBackupUnverified as well", err)
	}
	if n := countGroups(dirNames(dst)); n != 2 {
		t.Fatalf("the destination holds %v; the store did not land", dirNames(dst))
	}
}

// A negative limit is a typo. Reading it as "give me the default" means
// MaxFiles: -1 restores a hundred thousand files, and the command line already
// refuses what the library was accepting.
func TestRestoreRefusesNegativeLimits(t *testing.T) {
	archive := backupOf(t, 2)
	for _, opt := range []RestoreOptions{
		{MaxFiles: -1}, {MaxBytes: -1}, {MaxFileBytes: -1}, {MaxManifestBytes: -1},
	} {
		dst := filepath.Join(t.TempDir(), "store")
		if _, err := Restore(bytes.NewReader(archive), dst, opt); err == nil {
			t.Fatalf("%+v was accepted", opt)
		}
		if n := countGroups(dirNames(dst)); n != 0 {
			t.Fatalf("%+v left %v in the destination", opt, dirNames(dst))
		}
	}
}

// The manifest ceiling can be RAISED, not only lowered. An operator whose own
// backup carries a manifest over the built-in 64 MiB otherwise has no way to
// restore it, from a flag whose help says it sets the limit.
func TestManifestCeilingCanBeRaised(t *testing.T) {
	if got := (backupReadLimits{maxManifestBytes: 1 << 30}).manifestBytes(); got != 1<<30 {
		t.Fatalf("a 1 GiB ceiling resolved to %d", got)
	}
	if got := (backupReadLimits{}).manifestBytes(); got != maxBackupManifestBytes {
		t.Fatalf("the default resolved to %d, want %d", got, maxBackupManifestBytes)
	}
}

// The tenant is checked BEFORE the first group is written, and the assertion
// has to be about the writes -- not about what survives.
//
// `TestRestoreRefusesTheWrongTenant`'s os.Stat(dst) passes whether the check
// runs on the manifest or after the last group, because the deferred cleanup
// removes the destination either way. The whole reason readBackup grew an
// onManifest parameter is that a 1 TiB wrong-tenant archive must not fill the
// volume first, and that is a statement about writes.
func TestTheWrongTenantIsRefusedBeforeAnythingIsWritten(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")

	var creates int
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) == "temp-create" {
			creates++
		}
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{RequireTenant: "tenant-9"})
	stop()
	if !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("restore of another tenant's archive: %v", err)
	}
	if creates != 0 {
		t.Fatalf("%d group files were written before the tenant was checked", creates)
	}

	// And the matching tenant does write, so the counter is measuring
	// something.
	creates = 0
	stop = SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) == "temp-create" {
			creates++
		}
		return nil
	})
	_, err = Restore(bytes.NewReader(archive), dst, RestoreOptions{RequireTenant: "tenant-1"})
	stop()
	if err != nil {
		t.Fatalf("restore of the matching tenant: %v", err)
	}
	if creates != 4 {
		t.Fatalf("the accepted restore wrote %d files, want 4", creates)
	}
}

// manifestLast re-packs an archive with the manifest moved to the end.
func manifestLast(t *testing.T, archive []byte) []byte {
	t.Helper()
	var manifest []byte
	type entry struct {
		hdr  tar.Header
		body []byte
	}
	var rest []entry
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		body := make([]byte, hdr.Size)
		if _, rerr := io.ReadFull(tr, body); rerr != nil && hdr.Size > 0 {
			t.Fatal(rerr)
		}
		if filepath.Base(hdr.Name) == backupManifestName {
			manifest = body
			continue
		}
		rest = append(rest, entry{*hdr, body})
	}
	if manifest == nil {
		t.Fatal("the archive carries no manifest")
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range rest {
		h := e.hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: backupManifestName, Mode: 0o600, Size: int64(len(manifest)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// readBackup enforces manifest-first RETROACTIVELY: a group arriving before
// the manifest is read, parsed and emitted, and the ordering error is raised
// only when the manifest turns up. For a tenant-checked restore that is the
// whole archive on the destination's own volume before anything looks at the
// tenant -- bounded only by MaxBytes, which defaults to a terabyte.
func TestARequiredTenantRefusesGroupsThatPrecedeTheManifest(t *testing.T) {
	archive := manifestLast(t, backupOf(t, 4))
	dst := filepath.Join(t.TempDir(), "store")

	var creates int
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) == "temp-create" {
			creates++
		}
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{RequireTenant: "tenant-1"})
	stop()
	if !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("a manifest-last archive with a required tenant: %v, want ErrWrongTenant", err)
	}
	if creates != 0 {
		t.Fatalf("%d groups were written from a manifest-last archive before the tenant was known", creates)
	}
	// Without a tenant requirement the archive is still refused, by the
	// ordering rule readBackup already enforces.
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err == nil {
		t.Fatal("a manifest-last archive was restored")
	}
}

// The lock this call holds must be the one the rename installs as dst/LOCK,
// or there is a window between os.RemoveAll(dst) and os.Rename in which a
// server can open a store that then shares the destination with the restore.
func TestTheRestoredStoreIsLockedUntilTheRestoreReturns(t *testing.T) {
	archive := backupOf(t, 2)
	dst := filepath.Join(t.TempDir(), "store")

	// The staging directory's lock file is what becomes dst/LOCK, so after a
	// successful restore the destination must hold no lock at all and must
	// open cleanly.
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// The lock file stays, exactly as it does in a store the server created:
	// unlinking it is what hands one lock to two holders. What matters is that
	// nobody holds it once the restore returns.
	if !contains(dirNames(dst), LockFileName) {
		t.Error("the restored store has no lock file; the staged lock did not become it")
	}
	s, err := OpenStore(dst)
	if err != nil {
		t.Fatalf("the restored store does not open: %v", err)
	}
	s.Close()

	// And AFTER the rename, which is the instant that matters: the store is
	// in place, and until this call releases its lock nobody else may write
	// into it. Before the staging directory was locked, the lock this call
	// held was the one os.RemoveAll(dst) had just unlinked, so a server that
	// won the race here became a live writer over the restored groups --
	// reproduced as a restore returning nil with the archive's group-0.bin
	// overwritten.
	dst2 := filepath.Join(t.TempDir(), "store")
	var lateOpen error
	var opened bool
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) != "restore-renamed" {
			return nil
		}
		s, err := OpenStore(dst2)
		lateOpen = err
		if err == nil {
			opened = true
			s.Close()
		}
		return nil
	})
	_, err = Restore(bytes.NewReader(archive), dst2, RestoreOptions{})
	stop()
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if opened {
		t.Fatalf("a store opened at the destination after the rename and before the restore returned (%v)", lateOpen)
	}
	if !errors.Is(lateOpen, ErrLocked) {
		t.Fatalf("the post-rename open failed with %v, want ErrLocked", lateOpen)
	}
	// The restored store opens once the restore has returned.
	s2, err := OpenStore(dst2)
	if err != nil {
		t.Fatalf("the restored store does not open after the restore returned: %v", err)
	}
	s2.Close()
}

// A restore that loses the rename must not take the winner's lock with it.
//
// os.RemoveAll(dst) unlinks this call's lock file, so from then on the file at
// dst/LOCK belongs to whoever created the directory next -- and the only way
// the rename fails is EEXIST, which means somebody did. Releasing it there
// unlinks a lock a live process is holding, and the next OpenStore creates a
// fresh inode and also succeeds: two writers with independent id counters in
// one directory, reached through the cleanup of the mechanism that prevents
// exactly that.
func TestALostRenameDoesNotTakeTheWinnersLock(t *testing.T) {
	archive := backupOf(t, 2)
	parent := t.TempDir()
	dst := filepath.Join(parent, "store")

	// Create the destination between the removal and the rename, which is
	// what an OpenStore winning that gap does.
	var winner *dirLock
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) != "restore-removed" || winner != nil {
			return nil
		}
		if err := os.MkdirAll(dst, 0o755); err != nil {
			t.Error(err)
			return nil
		}
		lk, err := lockDir(dst)
		if err != nil {
			t.Error(err)
			return nil
		}
		winner = lk
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	stop()
	if winner == nil {
		t.Skip("the injection point was not reached")
	}
	defer winner.release()
	if err == nil {
		t.Fatal("the restore renamed over a directory another process had created")
	}
	// The winner still holds its lock, and its file is still there.
	if _, serr := os.Stat(filepath.Join(dst, LockFileName)); serr != nil {
		t.Fatalf("the aborting restore unlinked the winner's lock file: %v", serr)
	}
	if second, lerr := lockDir(dst); lerr == nil {
		second.release()
		t.Fatal("a second process took the lock while the winner still held it")
	}
}

// A kill anywhere between the marker being written and the rename must leave a
// destination a later restore can still use.
//
// The marker used to live INSIDE the staging directory, so it had to be
// removed before the rename or it would land in the restored store -- and
// everything in between (an fsync of the whole staged store, a lock, a
// readdir, and the removal of the old store) was a window where a kill left a
// complete, marker-less staging directory that every later restore refused.
func TestACrashInAnyWindowLeavesARetryablDestination(t *testing.T) {
	archive := backupOf(t, 2)
	for _, at := range []string{"file-sync", "dir-sync", "rename"} {
		t.Run("killed at "+at, func(t *testing.T) {
			parent := t.TempDir()
			dst := filepath.Join(parent, "store")

			hook, herr := FailAt(at, errors.New("injected"))
			if herr != nil {
				t.Fatal(herr)
			}
			stop := SetFaultHookForTest(hook)
			_, _ = Restore(bytes.NewReader(archive), dst, RestoreOptions{})
			stop()

			// Whatever it left, a retry must work.
			if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err != nil {
				t.Fatalf("a retry after a failure at %s: %v", at, err)
			}
			if n := countGroups(dirNames(dst)); n != 2 {
				t.Fatalf("the restored store holds %v, want 2 groups", dirNames(dst))
			}
			if _, err := os.Stat(dst + restoringMarkerSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Error("the successful retry left a marker behind")
			}
			if _, err := os.Stat(dst + ".restoring"); !errors.Is(err, os.ErrNotExist) {
				t.Error("the successful retry left a staging directory behind")
			}
		})
	}
}

// The marker never reaches the restored store, and an archive cannot plant one.
func TestTheMarkerNeverLandsInAStore(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(backupOf(t, 3)), dst, RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range dirNames(dst) {
		if strings.Contains(name, "restoring") {
			t.Errorf("the restored store holds %q", name)
		}
	}
	if _, err := os.Stat(dst + restoringMarkerSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Error("a successful restore left its marker behind")
	}
}

// countGroups counts the group files in a directory listing, ignoring the lock
// file a restored store carries like any other store.
func countGroups(names []string) int {
	n := 0
	for _, name := range names {
		if _, ok := groupIDFromName(name); ok {
			n++
		}
	}
	return n
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// Every residue a killed restore can leave, and whether a retry survives it.
//
// It does NOT test the marker/mkdir ORDER: the two orderings differ only on a
// kill, where no defer runs, and both leave residues this test can construct
// by hand. What it pins is which residues are retryable -- the marker-first
// order is what makes "an empty staging directory with no marker" unreachable
// from a crash, and that is reasoning rather than a test, recorded in
// docs/wrong.md beside the two other orderings in the same position.
func TestEveryCrashResidueIsRetryable(t *testing.T) {
	archive := backupOf(t, 2)
	parent := t.TempDir()
	dst := filepath.Join(parent, "store")

	// Exactly the residue: a staging directory with no marker beside it.
	if err := os.Mkdir(dst+".restoring", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{}); err == nil {
		t.Fatal("a staging directory with no marker was silently removed")
	}

	// And the residue the code actually leaves -- marker first, then the
	// directory -- retries clean, whichever of the two the kill landed
	// between.
	for _, residue := range []struct {
		name    string
		staging bool
	}{
		{"marker only", false},
		{"marker and an empty staging directory", true},
	} {
		t.Run(residue.name, func(t *testing.T) {
			d := filepath.Join(t.TempDir(), "store")
			if err := os.WriteFile(d+restoringMarkerSuffix, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if residue.staging {
				if err := os.Mkdir(d+".restoring", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Restore(bytes.NewReader(archive), d, RestoreOptions{}); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if n := countGroups(dirNames(d)); n != 2 {
				t.Fatalf("the restored store holds %v, want 2 groups", dirNames(d))
			}
		})
	}
}

// The staging directory is created with the destination's mode, not 0755: a
// 0700 destination would otherwise have its group names and sizes listable by
// anyone for the length of the restore.
func TestStagingTakesTheDestinationsMode(t *testing.T) {
	if !umaskSupported {
		t.Skip("no umask on this platform")
	}
	old := setUmask(0o022)
	defer setUmask(old)

	archive := backupOf(t, 3)
	dst := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	var mid os.FileMode
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: int64(len(archive) / 2),
		hook: func() {
			if fi, err := os.Stat(dst + ".restoring"); err == nil {
				mid = fi.Mode().Perm()
			}
		},
	}
	if _, err := Restore(r, dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if mid != 0o700 {
		t.Fatalf("the staging directory was %04o mid-restore, want 0700", mid)
	}
}

// Releasing a lock must not leak its descriptor. The conditional release --
// guarding an unlink that no longer exists -- also skipped the close, one
// descriptor per restore.
func TestRestoreLeaksNoLockDescriptors(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd here")
	}
	count := func() int {
		ents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range ents {
			if target, lerr := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); lerr == nil &&
				strings.Contains(target, LockFileName) {
				n++
			}
		}
		return n
	}
	archive := backupOf(t, 2)
	base := t.TempDir()
	// Warm up, then measure: the first restore may open things that stay.
	for i := 0; i < 3; i++ {
		if _, err := Restore(bytes.NewReader(archive), filepath.Join(base, fmt.Sprint("warm", i)), RestoreOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before := count()
	// FAILING restores. On unix the deferred release is the ONLY release --
	// releaseBeforeRemoval is a no-op there -- so this covers the early exits
	// and TestSuccessfulRestoresLeakNoLockDescriptors covers the success path.
	// An earlier version of this comment said the success path released
	// explicitly, which was true before round seven made that per-platform.
	for i := 0; i < 20; i++ {
		if _, err := Restore(bytes.NewReader(archive), filepath.Join(base, fmt.Sprint("s", i)),
			RestoreOptions{RequireTenant: "nobody"}); err == nil {
			t.Fatal("the wrong-tenant restore succeeded")
		}
	}
	if after := count(); after > before {
		t.Fatalf("20 restores leaked %d lock descriptors (%d -> %d)", after-before, before, after)
	}
}

// The destination's lock must still be HELD when the destination is removed.
//
// Releasing it first is required on Windows and catastrophic on unix. Because
// the lock file is never unlinked, an early release leaves `dst/LOCK` present
// and unheld: a server flocks the file that is already there, opens the store,
// `os.RemoveAll` deletes that live store, and the rename SUCCEEDS -- the
// winner never had to create the directory, so there is no EEXIST to abort on.
// Measured on the build that released early: a restore returning nil with the
// archive's group-0.bin overwritten by the ghost writer, and the reopened
// store answering with 13 rows where the archive held 16.
func TestTheDestinationLockIsHeldThroughTheRemoval(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")

	var ghost *Store
	var ghostErr error
	stop := SetFaultHookForTest(func(p FaultPoint) error {
		if FaultPointString(p) != "restore-releasing" || ghost != nil {
			return nil
		}
		// Exactly where an early release would have left the door open.
		ghost, ghostErr = OpenStore(dst)
		return nil
	})
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{})
	stop()
	if ghost != nil {
		ghost.Close()
		t.Fatalf("a store opened at the destination while it was being replaced (%v)", ghostErr)
	}
	if !errors.Is(ghostErr, ErrLocked) {
		t.Fatalf("the open in the removal window failed with %v, want ErrLocked", ghostErr)
	}
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// And the archive's groups are the ones that landed, byte for byte.
	if n := countGroups(dirNames(dst)); n != 4 {
		t.Fatalf("the restored store holds %v, want 4 groups", dirNames(dst))
	}
	s, oerr := OpenStore(dst)
	if oerr != nil {
		t.Fatalf("the restored store does not open: %v", oerr)
	}
	defer s.Close()
	if rows := s.TotalRows(); rows != 16 {
		t.Fatalf("the restored store holds %d rows, want the archive's 16", rows)
	}
}

// The marker is WRITTEN, not merely written at the right moment. Two rounds
// hardened its timing; nothing asserted it exists at all, and without it
// clearStaging refuses every leftover staging directory -- so a killed restore
// leaves a destination no later restore accepts without a manual rm.
func TestTheMarkerIsWrittenDuringEveryRestore(t *testing.T) {
	archive := backupOf(t, 4)
	dst := filepath.Join(t.TempDir(), "store")

	var sawMarker, sawStaging bool
	r := &pausingReader{
		r:  bytes.NewReader(archive),
		at: 1,
		hook: func() {
			_, merr := os.Stat(dst + restoringMarkerSuffix)
			sawMarker = merr == nil
			_, serr := os.Stat(dst + ".restoring")
			sawStaging = serr == nil
		},
	}
	if _, err := Restore(r, dst, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !sawStaging {
		t.Fatal("no staging directory existed while the archive was being read")
	}
	if !sawMarker {
		t.Fatal("no marker existed while the archive was being read; a kill here would leave a destination no restore accepts")
	}
}

// An archive with no manifest gets the per-entry count bound, which is the
// only entry-count bound it can get: the manifest bound has no manifest to
// read, and the subtest that names "group count" trips that one.
func TestAnUnverifiedArchiveIsBoundedByItsEntryCount(t *testing.T) {
	archive := unverifiedArchive(t, "group-0.bin", "group-1.bin", "group-2.bin")
	dst := filepath.Join(t.TempDir(), "store")
	_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{
		AllowUnverified: true, MaxFiles: 2,
	})
	if err == nil {
		t.Fatal("a manifest-less archive of three entries passed MaxFiles: 2")
	}
	if !strings.Contains(err.Error(), "more than 2 entries") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	if n := countGroups(dirNames(dst)); n != 0 {
		t.Fatalf("the refusal left %v behind", dirNames(dst))
	}
	// Three entries under a limit of three is fine.
	dst2 := filepath.Join(t.TempDir(), "store")
	if _, err := Restore(bytes.NewReader(archive), dst2, RestoreOptions{
		AllowUnverified: true, MaxFiles: 3,
	}); !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("three entries under a limit of three: %v", err)
	}
}

// The per-entry limit must bound the ALLOCATION, not only the write.
//
// readBackup reads a whole entry with io.ReadAll sized from the manifest's
// declared size, and every per-entry limit used to live in the emit callback
// -- which runs after that read. Measured before the ceiling was pushed down:
// an archive declaring one 128 MiB group cost 268.5 MiB of live heap with
// MaxFileBytes set to 1, in a DRY RUN, which is the mode an operator is told
// to point at an untrusted archive.
func TestThePerEntryLimitBoundsTheAllocation(t *testing.T) {
	// A manifest that declares a huge group, and a tar entry to match. The
	// bytes are never produced -- the point is that nothing sizes a buffer
	// from the declaration.
	const huge = 64 << 20
	archive := oversizedArchive(t, huge)

	for _, dry := range []bool{true, false} {
		name := "restore"
		if dry {
			name = "dry run"
		}
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			dst := filepath.Join(t.TempDir(), "store")
			_, err := Restore(bytes.NewReader(archive), dst, RestoreOptions{
				MaxFileBytes: 1024, AllowUnverified: true, DryRun: dry,
			})
			runtime.ReadMemStats(&after)
			if err == nil {
				t.Fatal("an entry declaring 64 MiB passed a 1 KiB per-entry limit")
			}
			// The MESSAGE, because the allocation alone proves nothing here: a
			// truncated tar is bounded by the bytes it actually carries, so a
			// buffer sized from the DECLARATION and a buffer sized from the
			// stream look the same on this fixture. Only the wording says the
			// refusal came from the declared size, before any read.
			if !strings.Contains(err.Error(), "declares") {
				t.Fatalf("refused after reading rather than on the declaration: %v", err)
			}
			if got := after.TotalAlloc - before.TotalAlloc; got > huge/4 {
				t.Fatalf("refusing a %d-byte entry allocated %d bytes", huge, got)
			}
		})
	}
}

// oversizedArchive builds a manifest-less archive whose single entry declares
// `size` bytes without carrying them -- a tar header is a declaration, and the
// reader must not size a buffer from it.
func oversizedArchive(t *testing.T, size int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: "group-0.bin", Mode: 0o600, Size: size, Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	// A few real bytes; the reader must refuse on the DECLARATION, before it
	// discovers the entry is short.
	if _, err := tw.Write(make([]byte, 512)); err != nil {
		t.Fatal(err)
	}
	// Close would complain about the shortfall, so take the buffer as it is:
	// a truncated tar is exactly what a hostile archive looks like.
	return buf.Bytes()
}

// The deferred release is the ONLY release on unix, so the descriptor test has
// to drive successful restores too.
//
// Its comment said "the success path releases the lock explicitly before the
// removal, so only a failure exercises the deferred release" -- true on
// Windows and false on unix since releaseBeforeRemoval became a no-op there.
// Measured with the conditional release restored: 20 successful restores leak
// 20 descriptors and the whole suite stays green.
func TestSuccessfulRestoresLeakNoLockDescriptors(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd here")
	}
	count := func() int {
		ents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range ents {
			if target, lerr := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); lerr == nil &&
				strings.Contains(target, LockFileName) {
				n++
			}
		}
		return n
	}
	archive := backupOf(t, 2)
	base := t.TempDir()
	for i := 0; i < 3; i++ {
		if _, err := Restore(bytes.NewReader(archive), filepath.Join(base, fmt.Sprint("warm", i)), RestoreOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before := count()
	for i := 0; i < 20; i++ {
		if _, err := Restore(bytes.NewReader(archive), filepath.Join(base, fmt.Sprint("s", i)), RestoreOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if after := count(); after > before {
		t.Fatalf("20 successful restores leaked %d lock descriptors (%d -> %d)", after-before, before, after)
	}
}
