package storage

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// What a backup must guarantee.
//
// A tar of group files is well-formed whether it holds every group, some of
// them, or half of one -- so "the archive parsed" proves nothing an operator
// cares about. These assert the three things it must actually prove: the
// archive holds every group the store held at the instant it was taken, each
// group's bytes are the bytes that were written, and a transfer that stopped
// early is detected as such rather than restored as a smaller store.

// backupStore fills a store with n groups of rows rows each.
func backupStore(t *testing.T, n, rows int) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := s.AppendGroup(backupGroupFixture(i, rows)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return s, dir
}

// backupGroupFixture is one group whose values identify the batch it came
// from, so a restore can be checked for content and not only for count.
func backupGroupFixture(batch, rows int) *Group {
	ts := make([]int64, rows)
	vals := make([]string, rows)
	for i := range ts {
		ts[i] = int64(batch*1_000_000 + i + 1)
		vals[i] = fmt.Sprintf("batch-%d-row-%d", batch, i)
	}
	d := BuildDict(vals)
	return &Group{
		Rows: rows,
		Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: ts},
			{Name: "_msg", Type: ColDict, Dict: &d},
		},
	}
}

// The archive describes itself, and the description matches what it carries.
func TestBackupCarriesAValidatedManifest(t *testing.T) {
	s, dir := backupStore(t, 5, 4)
	defer s.Close()

	var buf bytes.Buffer
	when := time.Unix(1_700_000_000, 0)
	if err := s.BackupTarWith(&buf, BackupOptions{Tenant: "tenant-7", Now: when}); err != nil {
		t.Fatalf("backup: %v", err)
	}

	man, err := VerifyBackup(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if man.Format != BackupFormat {
		t.Fatalf("format %d, want %d", man.Format, BackupFormat)
	}
	if man.Tenant != "tenant-7" {
		t.Fatalf("tenant %q, want tenant-7", man.Tenant)
	}
	if man.CreatedUnix != when.Unix() {
		t.Fatalf("createdUnix %d, want %d", man.CreatedUnix, when.Unix())
	}
	if len(man.Groups) != 5 {
		t.Fatalf("manifest names %d groups, want 5", len(man.Groups))
	}
	if man.TotalRows() != 20 {
		t.Fatalf("manifest claims %d rows, want 20", man.TotalRows())
	}
	// The sequence is the store's own high watermark, so a backup is ordered
	// against every other change to that store.
	if man.ManifestSeq == 0 {
		t.Fatal("the manifest carries no store sequence")
	}
	for _, g := range man.Groups {
		if g.Bytes <= 0 || g.CRC32C == 0 {
			t.Fatalf("group %s has size %d and checksum %#x", g.Name, g.Bytes, g.CRC32C)
		}
		if g.Rows != 4 {
			t.Fatalf("group %s claims %d rows, want 4", g.Name, g.Rows)
		}
		// Read from the file rather than compared to a constant: the field
		// exists so an archive of a store an OLDER binary filled records v7,
		// and asserting the constant this build writes tests nothing about
		// that.
		blob, rerr := os.ReadFile(filepath.Join(dir, g.Name))
		if rerr != nil {
			t.Fatalf("read %s: %v", g.Name, rerr)
		}
		if want := int(get32(blob, 4)); g.Version != want {
			t.Fatalf("group %s claims version %d, the file says %d", g.Name, g.Version, want)
		}
	}
}

// A truncated transfer is the failure a bare tar cannot detect. It must not
// restore as a smaller store.
func TestTruncatedBackupIsRejected(t *testing.T) {
	s, _ := backupStore(t, 6, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	full := buf.Bytes()

	// Cut at three quarters: past the manifest, into the group list.
	cut := len(full) * 3 / 4
	_, err := VerifyBackup(bytes.NewReader(full[:cut]))
	if err == nil {
		t.Fatal("a truncated archive verified")
	}

	// The restore FAILS, and that is the whole guarantee at this layer.
	//
	// It does not leave the destination empty: entries already read are
	// already written, so opening that directory anyway produces a store
	// holding a silent subset. Saying otherwise here would be a comment
	// describing a property the code does not have. Making the restore atomic
	// is Task 5.2, and this asserts the shape of what is left so that task has
	// something to change.
	dst := t.TempDir()
	rerr := restoreTarForTest(bytes.NewReader(full[:cut]), dst)
	if rerr == nil {
		t.Fatal("a truncated archive restored without error")
	}
	written := 0
	ents, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if _, ok := groupIDFromName(e.Name()); ok {
			written++
		}
	}
	if written == 0 || written >= 6 {
		t.Fatalf("a truncated restore left %d of 6 groups; expected a partial subset", written)
	}
}

// The terminator is what separates "the stream ended" from "the stream ended
// where it meant to". An archive whose groups are all present but whose
// terminator is missing is still an incomplete transfer.
func TestBackupWithoutTerminatorIsTruncated(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Rebuild the archive without the terminator entry.
	var stripped bytes.Buffer
	tw := tar.NewWriter(&stripped)
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == backupCompleteName {
			continue
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		hdr.Size = int64(len(b))
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := VerifyBackup(bytes.NewReader(stripped.Bytes()))
	if !errors.Is(err, ErrBackupTruncated) {
		t.Fatalf("verify returned %v, want ErrBackupTruncated", err)
	}
}

// A flipped bit in a group must be caught before the store is opened over it,
// not by the first query that reads that column.
func TestCorruptGroupInBackupIsRejected(t *testing.T) {
	s, _ := backupStore(t, 3, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	// Rebuild the archive with one byte of one GROUP flipped.
	//
	// Flipping at a byte offset into the whole archive does not work and
	// quietly tests nothing: tar pads every entry out to a 512-byte block,
	// small groups are mostly padding, and a flipped padding byte changes no
	// entry's contents. The first version of this test did exactly that and
	// reported a passing verify as a defect.
	var damaged bytes.Buffer
	tw := tar.NewWriter(&damaged)
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	hit := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := groupIDFromName(hdr.Name); ok && !hit && len(b) > 40 {
			b[len(b)/2] ^= 0xFF // into the body, past the magic and version
			hit = true
		}
		hdr.Size = int64(len(b))
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("no group entry was found to damage")
	}

	if _, err := VerifyBackup(bytes.NewReader(damaged.Bytes())); err == nil {
		t.Fatal("an archive with a flipped byte in a group verified")
	}
}

// A group the manifest does not name is a group nothing checked.
func TestUnlistedGroupInBackupIsRejected(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Rebuilt with the extra group BEFORE the terminator. Appending it after
	// the end-of-archive blocks puts it past BACKUP-COMPLETE, where the
	// terminator-position check refuses it first -- also a correct refusal,
	// and not the one this test is named for.
	var extra bytes.Buffer
	tw := tar.NewWriter(&extra)
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	payload := []byte("not a group")
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == backupCompleteName {
			if err := tw.WriteHeader(&tar.Header{
				Name: "group-9999.bin", Mode: 0o600,
				Size: int64(len(payload)), Typeflag: tar.TypeReg,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
		hdr.Size = int64(len(b))
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := VerifyBackup(bytes.NewReader(extra.Bytes()))
	if err == nil {
		t.Fatal("an archive carrying an unlisted group verified")
	}
	if !strings.Contains(err.Error(), "manifest does not name") {
		t.Fatalf("error does not identify the cause: %v", err)
	}
}

// A timestamp is a number a client sends, and a group at the top of the range
// must still be in the backup.
//
// The archive was built from Snapshot(MinInt64, MaxInt64), whose overlap test
// is `TimeMin < to && TimeMax >= from` -- half-open at the top. A group whose
// TimeMin is MaxInt64 fails it and is absent from the archive AND from the
// manifest, since both come from the same snapshot. The backup then verifies
// clean while missing data, which is the one thing a self-describing archive
// exists to make impossible.
func TestBackupKeepsGroupsAtTheEdgeOfTheTimeRange(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if _, err := s.AppendGroup(backupGroupFixture(0, 4)); err != nil {
		t.Fatalf("append: %v", err)
	}
	// One group pinned at the top of the int64 range.
	edge := backupGroupFixture(1, 2)
	for i := range edge.Columns[0].Ts {
		edge.Columns[0].Ts[i] = math.MaxInt64
	}
	if _, err := s.AppendGroup(edge); err != nil {
		t.Fatalf("append edge: %v", err)
	}
	// And one with no timestamp column at all, whose TimeMin/TimeMax the
	// marshaller leaves at the sentinel.
	d := BuildDict([]string{"a", "b"})
	if _, err := s.AppendGroup(&Group{
		Rows:    2,
		Columns: []Column{{Name: "_msg", Type: ColDict, Dict: &d}},
	}); err != nil {
		t.Fatalf("append timeless: %v", err)
	}

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	man, err := VerifyBackup(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(man.Groups) != 3 {
		t.Fatalf("the backup names %d of the store's 3 groups; a group was dropped by a time filter",
			len(man.Groups))
	}
}

// Hundreds of groups, so the manifest and the streaming path are exercised at
// a size where an O(n^2) mistake or a per-group full-file read would show.
func TestBackupAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	s, _ := backupStore(t, 400, 2)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	man, err := VerifyBackup(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(man.Groups) != 400 {
		t.Fatalf("manifest names %d groups, want 400", len(man.Groups))
	}
	if man.TotalRows() != 800 {
		t.Fatalf("manifest claims %d rows, want 800", man.TotalRows())
	}

	dst := t.TempDir()
	if err := restoreTarForTest(bytes.NewReader(buf.Bytes()), dst); err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2, err := OpenStore(dst)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer s2.Close()
	snap, err := s2.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	rows := 0
	for _, r := range snap.Groups {
		rows += r.Rows
	}
	if rows != 800 {
		t.Fatalf("restored store holds %d rows, want 800", rows)
	}
}

// An archive from before the manifest existed can still be restored, and the
// caller is told nothing was verified rather than told success.
func TestUnverifiedRestoreIsReportedAsSuch(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	var groups [][]byte
	var names []string
	snap, err := s.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range snap.entries {
		groups = append(groups, append([]byte(nil), snap.Groups[i].blob...))
		names = append(names, filepath.Base(e.path))
	}
	snap.Close()
	s.Close()

	// A bare tar of group files: what BackupTar produced before format 1.
	var legacy bytes.Buffer
	tw := tar.NewWriter(&legacy)
	for i, name := range names {
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(groups[i])), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(groups[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	err = restoreTarForTest(bytes.NewReader(legacy.Bytes()), dst)
	if !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("restore of a manifest-less archive returned %v, want ErrBackupUnverified", err)
	}
	// The files are there: it is a restore that happened without a check,
	// not a restore that did not happen.
	s2, err := OpenStore(dst)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer s2.Close()
	snap2, err := s2.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer snap2.Close()
	if len(snap2.Groups) != 2 {
		t.Fatalf("restored store holds %d groups, want 2", len(snap2.Groups))
	}
}

// blockingWriter stalls the backup after its first Write, so a test can run
// something concurrent while the archive is genuinely mid-stream.
//
// The test this replaced just started two goroutines and hoped. Measured over
// 40 runs it captured 12 groups every single time -- retention always won the
// start, the snapshot was always taken after the drop, and the
// streaming-under-retention path was never entered. A race a test does not
// create is a race a test does not cover.
type blockingWriter struct {
	buf     bytes.Buffer
	first   sync.Once
	stalled chan struct{}
	release chan struct{}
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{stalled: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	b.first.Do(func() {
		close(b.stalled)
		<-b.release
	})
	return b.buf.Write(p)
}

// Retention, an append and a recompaction all run while the archive is
// mid-stream. Every one of them is a structural change to the store, and none
// of them may make the archive disagree with its own manifest.
//
// The plan asked for all three; only retention had a test, and that one did
// not create the race.
func TestBackupIsCompleteUnderConcurrentStoreChanges(t *testing.T) {
	for _, c := range []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{"retention", func(t *testing.T, s *Store) {
			s.DropGroupsBefore(int64(6 * 1_000_000))
		}},
		{"append", func(t *testing.T, s *Store) {
			if _, err := s.AppendGroup(backupGroupFixture(99, 4)); err != nil {
				t.Errorf("append: %v", err)
			}
		}},
		{"recompact", func(t *testing.T, s *Store) {
			// Whatever it does, it must not invalidate a mapping the backup
			// holds. An error here is a store-level failure, not a backup one.
			_, _, _, _ = s.Recompact(int64(20*1_000_000), true)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := backupStore(t, 12, 8)
			defer s.Close()

			bw := newBlockingWriter()
			done := make(chan error, 1)
			go func() { done <- s.BackupTar(bw) }()

			<-bw.stalled // the snapshot is taken and bytes are flowing
			c.run(t, s)
			close(bw.release)

			if err := <-done; err != nil {
				t.Fatalf("backup under %s: %v", c.name, err)
			}
			man, err := VerifyBackup(bytes.NewReader(bw.buf.Bytes()))
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			if len(man.Groups) != 12 {
				t.Fatalf("the archive names %d groups; the snapshot held 12", len(man.Groups))
			}
			// And every byte still reads back, which is the property the lease
			// exists for: the mapping outlives an unlink.
			dst := t.TempDir()
			if err := restoreTarForTest(bytes.NewReader(bw.buf.Bytes()), dst); err != nil {
				t.Fatalf("restore: %v", err)
			}
			s2, err := OpenStore(dst)
			if err != nil {
				t.Fatalf("open restored: %v", err)
			}
			defer s2.Close()
			snap, _, err := s2.SnapshotAllWithSeq()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			rows := 0
			for _, r := range snap.Groups {
				rows += r.Rows
			}
			if rows != 96 {
				t.Fatalf("the restored store holds %d rows, want 96", rows)
			}
		})
	}
}

// An archive whose manifest comes AFTER its groups must be refused.
//
// Validation runs inside the manifest branch, so a group read before the
// manifest is read with no size and no checksum -- and the completeness loop
// afterwards passes, because those groups HAD been seen. Ordering was written
// down and never enforced.
//
// The archive here carries VALID groups and asserts the refusal names the
// ordering. An earlier version corrupted them, which made the unverified
// branch's own parse refuse the first group before the manifest was read: the
// test passed with the ordering rule deleted.
func TestManifestMustComeFirst(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Rebuild with the manifest moved to the end and one group destroyed.
	var manEntry []byte
	type entry struct {
		hdr  tar.Header
		body []byte
	}
	var rest []entry
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == backupManifestName {
			manEntry = b
			continue
		}
		if hdr.Name == backupCompleteName {
			continue // re-emitted last, after the manifest
		}
		// The groups stay VALID. Corrupting them made the unverified
		// branch's ReadGroup parse refuse the first one before the manifest
		// was ever read, so this test passed with the ordering rule deleted --
		// a second test of the parse wearing the ordering rule's name.
		hdr.Size = int64(len(b))
		rest = append(rest, entry{*hdr, b})
	}
	if manEntry == nil {
		t.Fatal("no manifest in the archive")
	}

	var reordered bytes.Buffer
	tw := tar.NewWriter(&reordered)
	for _, e := range rest {
		if err := tw.WriteHeader(&e.hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: backupManifestName, Mode: 0o600, Size: int64(len(manEntry)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(manEntry); err != nil {
		t.Fatal(err)
	}
	// Terminator LAST, so the terminator-position rule cannot be what refuses
	// this archive. Leaving it where BackupTar put it -- before the manifest
	// moved to the end -- made that rule answer first.
	if err := tw.WriteHeader(&tar.Header{
		Name: backupCompleteName, Mode: 0o600, Size: 0, Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	_, verr := VerifyBackup(bytes.NewReader(reordered.Bytes()))
	if verr == nil {
		t.Fatal("a manifest-last archive verified clean")
	}
	if !strings.Contains(verr.Error(), "manifest must come first") {
		t.Fatalf("refused for the wrong reason: %v", verr)
	}
	dst := t.TempDir()
	if err := restoreTarForTest(bytes.NewReader(reordered.Bytes()), dst); err == nil {
		t.Fatal("a manifest-last archive restored")
	}
}

// A pre-format-1 archive has no manifest at ALL, which is a different thing
// from one whose manifest arrives late, and it must still restore.
func TestNoManifestIsStillDistinctFromManifestLast(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	var groups [][]byte
	var names []string
	snap, _, err := s.SnapshotAllWithSeq()
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range snap.entries {
		groups = append(groups, append([]byte(nil), snap.Groups[i].blob...))
		names = append(names, filepath.Base(e.path))
	}
	snap.Close()
	s.Close()

	var legacy bytes.Buffer
	tw := tar.NewWriter(&legacy)
	for i, name := range names {
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(groups[i])), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(groups[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := restoreTarForTest(bytes.NewReader(legacy.Bytes()), dst); !errors.Is(err, ErrBackupUnverified) {
		t.Fatalf("a manifest-less archive returned %v, want ErrBackupUnverified", err)
	}
}

// The terminator's position is documented, so it is enforced.
//
// Only its PRESENCE was checked, so an archive carrying BACKUP-COMPLETE as its
// FIRST entry verified clean and restored -- the same "written down and never
// enforced" the manifest-ordering test above is about, one entry type over.
func TestTerminatorMustComeLast(t *testing.T) {
	s, _ := backupStore(t, 2, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Two placements, because there are two checks and either one alone
	// answers for an archive that violates both. Putting the terminator
	// FIRST puts the manifest after it as well, so that version was green
	// with the group check deleted and green with the manifest check deleted,
	// red only with both -- a test of the pair, not of either line.
	for _, c := range []struct {
		name  string
		after string // the entry the terminator is placed before
	}{
		{"before the manifest", backupManifestName},
		{"before the groups", "group-0.bin"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var reordered bytes.Buffer
			tw := tar.NewWriter(&reordered)
			tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
			placed := false
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(tr)
				if err != nil {
					t.Fatal(err)
				}
				if hdr.Name == backupCompleteName {
					continue // moved
				}
				if hdr.Name == c.after && !placed {
					if err := tw.WriteHeader(&tar.Header{
						Name: backupCompleteName, Mode: 0o600, Size: 0, Typeflag: tar.TypeReg,
					}); err != nil {
						t.Fatal(err)
					}
					placed = true
				}
				hdr.Size = int64(len(b))
				if err := tw.WriteHeader(hdr); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(b); err != nil {
					t.Fatal(err)
				}
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			if !placed {
				t.Fatalf("the terminator was never placed before %s", c.after)
			}

			_, err := VerifyBackup(bytes.NewReader(reordered.Bytes()))
			if err == nil {
				t.Fatal("a terminator-first archive verified clean")
			}
			if !strings.Contains(err.Error(), "terminator must come last") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}
}

// A refused archive must not have written unreadable bytes to the destination.
//
// The manifest-last refusal fires when the manifest finally arrives, by which
// point the groups before it are already on disk -- and nothing had checked
// them, because validation runs in the manifest branch. Every group is parsed
// on the way in now, manifest or not: what lands is at least a readable group.
//
// That is not atomicity. A refused restore still leaves a partial destination;
// making it all-or-nothing is Task 5.2, and this pins the weaker property so
// that task has something to strengthen.
func TestRefusedArchiveWritesNothingUnreadable(t *testing.T) {
	s, _ := backupStore(t, 3, 4)
	defer s.Close()

	var buf bytes.Buffer
	if err := s.BackupTar(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Manifest last, and every group destroyed.
	var manEntry []byte
	var groups []struct {
		hdr  tar.Header
		body []byte
	}
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == backupManifestName {
			manEntry = b
			continue
		}
		if _, ok := groupIDFromName(hdr.Name); ok {
			for i := range b {
				b[i] = 0xFF
			}
		}
		hdr.Size = int64(len(b))
		groups = append(groups, struct {
			hdr  tar.Header
			body []byte
		}{*hdr, b})
	}

	var reordered bytes.Buffer
	tw := tar.NewWriter(&reordered)
	for _, e := range groups {
		if err := tw.WriteHeader(&e.hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: backupManifestName, Mode: 0o600, Size: int64(len(manEntry)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(manEntry); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := restoreTarForTest(bytes.NewReader(reordered.Bytes()), dst); err == nil {
		t.Fatal("a manifest-last archive of corrupt groups restored")
	}
	ents, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if _, ok := groupIDFromName(e.Name()); !ok {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dst, e.Name()))
		if rerr != nil {
			t.Fatal(rerr)
		}
		if _, perr := ReadGroup(b); perr != nil {
			t.Fatalf("the refused restore wrote %s, which does not parse: %v", e.Name(), perr)
		}
	}
}
