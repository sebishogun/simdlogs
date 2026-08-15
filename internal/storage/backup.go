package storage

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// BackupOptions carries what a store does not know about itself.
type BackupOptions struct {
	// Tenant names the store, so a restore can refuse an archive taken from
	// a different one. Empty means the archive does not say.
	Tenant string

	// Now stamps the manifest. Zero means time.Now(); a test passes a fixed
	// instant so two backups of the same bytes compare equal.
	Now time.Time
}

// BackupTar streams a self-describing tar of the store's groups to w.
func (s *Store) BackupTar(w io.Writer) error { return s.BackupTarWith(w, BackupOptions{}) }

// BackupTarWith is BackupTar with the store's tenant and a fixed clock.
//
// # Why a lease and not a path list
//
// The previous version copied the group PATHS out under the read lock and then
// read each file with os.ReadFile, skipping any that had gone. That reads as
// careful and is the opposite: a group retention removed between the two steps
// was dropped from the archive and the backup still succeeded. A backup
// missing groups that reports success is a backup discovered to be incomplete
// at restore time, which is the one moment there is nothing to fall back to.
//
// A snapshot removes the reason for the skip rather than handling it. Every
// group in it is guaranteed mapped until Close whatever retention,
// recompaction or cold demotion do, so a group in the archive's manifest is a
// group whose bytes are readable -- and any failure past that point is a real
// failure and is returned.
//
// It also removes the copy. The blob is the mmap the store already holds, so
// the bytes go from the page cache to the tar writer without a per-group
// os.ReadFile allocating the whole file on the heap first. At the 64 MiB flush
// ceiling that was one 64 MiB allocation per group, live for as long as the
// write took.
//
// # Why the archive is bracketed
//
// The manifest is the first entry and a terminator is the last. A reader that
// sees the manifest knows what the archive should contain before it has
// consumed any of it, and a reader that reaches EOF without the terminator
// knows the transfer did not finish. Neither fact is recoverable from a bare
// tar of group files.
func (s *Store) BackupTarWith(w io.Writer, opt BackupOptions) error {
	// Every group and the manifest sequence, under one lock acquisition.
	//
	// Not Snapshot(MinInt64, MaxInt64): its overlap test is half-open at the
	// top, so a group whose TimeMin is MaxInt64 is excluded -- and excluded
	// from the manifest too, since both are built from this snapshot, so the
	// archive verifies clean while missing data. And not a second RLock for
	// the sequence: an AppendGroup between the two makes the archive declare
	// a watermark it does not contain.
	snap, seq, err := s.SnapshotAllWithSeq()
	if err != nil {
		return err
	}
	defer snap.Close()

	now := opt.Now
	if now.IsZero() {
		now = time.Now()
	}
	man := &BackupManifest{
		Format:      BackupFormat,
		CreatedUnix: now.Unix(),
		Tenant:      opt.Tenant,
		ManifestSeq: seq,
		Groups:      make([]BackupGroup, 0, len(snap.entries)),
	}
	for i, e := range snap.entries {
		man.Groups = append(man.Groups,
			backupGroupOf(filepath.Base(e.path), e.id, snap.Groups[i].Rows, snap.Groups[i].blob))
	}

	manBytes, err := man.encode()
	if err != nil {
		return err
	}

	tw := tar.NewWriter(w)
	if err := writeTarFile(tw, backupManifestName, manBytes, now); err != nil {
		return err
	}
	// In manifest order, so an archive and its manifest agree entry for entry
	// and a restore can validate as it reads rather than buffering.
	byName := make(map[string][]byte, len(snap.entries))
	for i, e := range snap.entries {
		byName[filepath.Base(e.path)] = snap.Groups[i].blob
	}
	for _, g := range man.Groups {
		blob, ok := byName[g.Name]
		if !ok {
			// Unreachable: the manifest is built from the same snapshot.
			// Returned rather than skipped, because the alternative is an
			// archive whose manifest names a group it does not carry.
			return fmt.Errorf("storage: backup manifest names %s, which the snapshot does not hold", g.Name)
		}
		if err := writeTarFile(tw, g.Name, blob, now); err != nil {
			return err
		}
	}
	if err := writeTarFile(tw, backupCompleteName, nil, now); err != nil {
		return err
	}
	return tw.Close()
}

// writeTarFile writes one regular entry.
func writeTarFile(tw *tar.Writer, name string, data []byte, mod time.Time) error {
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o600,
		Size:     int64(len(data)),
		ModTime:  mod,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := tw.Write(data)
	return err
}

// ErrBackupTruncated reports an archive that ended before its terminator. It
// is a distinct error because it has a distinct cause -- a transfer that did
// not finish, not a corrupt one -- and a distinct remedy.
var ErrBackupTruncated = errors.New("storage: the backup archive is truncated")

// ErrBackupUnverified reports an archive with no manifest. Restoring it is
// possible and nothing about it was checked.
var ErrBackupUnverified = errors.New("storage: the backup archive carries no manifest")

// VerifyBackup reads an archive and checks it against its own manifest without
// writing anything, which is what a `-dry-run` restore and a backup gate both
// need.
//
// It returns the manifest on success. ErrBackupUnverified means the archive
// predates the manifest: a caller that requires verification must treat that
// as a failure rather than as a pass with a note, which is why it is an error
// and not a boolean.
func VerifyBackup(r io.Reader) (*BackupManifest, error) {
	man, _, err := readBackup(r, backupReadLimits{}, nil, nil)
	return man, err
}

// readBackup walks an archive, validating every group against the manifest and
// handing each one to emit. emit may be nil, which makes this a pure check.
//
// Validation is streaming rather than collect-then-check: a 200 GiB archive
// must not be buffered to be verified, and an entry that fails is reported
// before anything after it has been written anywhere.
// backupReadLimits bounds what an archive can make readBackup spend BEFORE any
// entry reaches emit. The manifest is decoded first and sizes everything after
// it, so a limit applied only in emit is a limit applied after the expensive
// part: a 24 MiB manifest decodes into roughly 340 MiB of live heap, and the
// callback that would have refused it has not been called yet.
//
// The zero value is the built-in ceiling for the manifest and no group-count
// bound, which is what VerifyBackup and RestoreTar have always had.
type backupReadLimits struct {
	maxManifestBytes int64 // 0 means maxBackupManifestBytes
	maxGroups        int   // 0 means unbounded
	// maxEntryBytes bounds one entry BEFORE io.ReadAll sizes a buffer from
	// the manifest's declared size. A limit applied in emit is applied after
	// the allocation it was meant to prevent: an archive declaring one 128 MiB
	// group cost 268.5 MiB of live heap with MaxFileBytes set to 1, in a dry
	// run, which is the mode an operator is told to point at an untrusted
	// archive. The same argument this file already makes about the manifest,
	// one entry type over. 0 means the built-in ceiling.
	maxEntryBytes int64
}

func (l backupReadLimits) entryBytes() int64 {
	if l.maxEntryBytes > 0 && l.maxEntryBytes < maxUnverifiedGroupBytes {
		return l.maxEntryBytes
	}
	return maxUnverifiedGroupBytes
}

// manifestBytes resolves the ceiling. A caller may RAISE it as well as lower
// it: the built-in is a default for an untrusted archive, not a statement that
// no legitimate manifest is bigger, and an operator whose own backup carries a
// larger one would otherwise have no way to restore it.
func (l backupReadLimits) manifestBytes() int64 {
	if l.maxManifestBytes > 0 {
		return l.maxManifestBytes
	}
	return maxBackupManifestBytes
}

// readBackup walks an archive with limits, calling onManifest once the
// manifest is decoded -- before any group is emitted, which is the only place
// a check that must precede the first write can go -- and emit per group.
// Both may be nil.
func readBackup(r io.Reader, lim backupReadLimits, onManifest func(*BackupManifest) error, emit func(name string, data []byte) error) (*BackupManifest, []string, error) {
	tr := tar.NewReader(r)
	var man *BackupManifest
	var restored []string
	byName := map[string]BackupGroup{}
	seen := map[string]bool{}
	complete := false
	// groupsBeforeManifest separates a pre-format-1 archive, which has no
	// manifest at all, from one whose manifest came after its groups. The
	// second is refused: validation runs inside the manifest branch, so a
	// group read before the manifest was read with no size, checksum or parse
	// check, and the completeness loop afterwards passes because those groups
	// HAD been seen -- just not checked. A manifest-last archive with a wholly
	// corrupt group verified clean and restored. The ordering was written down
	// and never enforced.
	groupsBeforeManifest := false

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return man, restored, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		// Flattened, so a crafted archive cannot escape a destination
		// directory whatever its entry names claim.
		name := filepath.Base(hdr.Name)

		switch name {
		case backupManifestName:
			if complete {
				return man, restored, errAfterTerminator(name)
			}
			if man != nil {
				return man, restored, fmt.Errorf("storage: the archive carries two manifests")
			}
			if hdr.Size > lim.manifestBytes() {
				return nil, restored, fmt.Errorf("storage: the backup manifest declares %d bytes, over the %d ceiling",
					hdr.Size, lim.manifestBytes())
			}
			b, err := io.ReadAll(io.LimitReader(tr, lim.manifestBytes()+1))
			if err != nil {
				return nil, restored, err
			}
			if int64(len(b)) > lim.manifestBytes() {
				return nil, restored, fmt.Errorf("storage: the backup manifest exceeds %d bytes", lim.manifestBytes())
			}
			if groupsBeforeManifest {
				return nil, restored, fmt.Errorf(
					"storage: the archive carries groups before its %s; the manifest must come first",
					backupManifestName)
			}
			man, err = decodeBackupManifest(b)
			if err != nil {
				return nil, restored, err
			}
			if lim.maxGroups > 0 && len(man.Groups) > lim.maxGroups {
				return man, restored, fmt.Errorf("storage: the manifest names %d groups, over the %d limit",
					len(man.Groups), lim.maxGroups)
			}
			for _, g := range man.Groups {
				byName[g.Name] = g
			}
			if onManifest != nil {
				if err := onManifest(man); err != nil {
					return man, restored, err
				}
			}
			continue
		case backupCompleteName:
			if complete {
				return man, restored, fmt.Errorf(
					"storage: the archive carries two %s entries", backupCompleteName)
			}
			complete = true
			continue
		}

		// The terminator is documented as the LAST entry and only its presence
		// was checked, so an archive carrying it FIRST verified clean and
		// restored. "The ordering was written down and never enforced" is the
		// sentence this file already carries about the manifest, one entry
		// type over.
		//
		// Only entries this function ACTS on are refused after it. A hand
		// assembled archive may carry a LOCK or a notes.txt anywhere; those
		// are skipped below either way, and refusing them would break a real
		// archive over junk that changes nothing.
		if complete {
			if _, ok := groupIDFromName(name); ok {
				return man, restored, errAfterTerminator(name)
			}
		}
		if _, ok := groupIDFromName(name); !ok {
			// Only group files. An archive carrying a MANIFEST entry used to
			// be harmless; since OpenStore decides "is this a legacy
			// directory" on whether MANIFEST exists, an empty or truncated
			// one restores a directory full of groups that the next open
			// reports as EMPTY, with no error.
			continue
		}
		if seen[name] {
			return man, restored, fmt.Errorf("storage: the archive carries %s twice", name)
		}
		seen[name] = true

		var data []byte
		if man == nil {
			// No manifest YET. Either this is a pre-format-1 archive, which
			// has none at all and is restored unverified, or the manifest is
			// coming later -- which is refused at the end, because validation
			// runs against the manifest and a group read before it was read
			// against nothing. The two are indistinguishable here and are
			// separated once the stream ends.
			//
			// Bounded, because an entry read with no manifest to size it is an
			// allocation an untrusted archive chooses. This is the same
			// ceiling the manifest entry itself has, one entry type over.
			groupsBeforeManifest = true
			if hdr.Size > lim.entryBytes() {
				return man, restored, fmt.Errorf(
					"storage: unverified entry %s declares %d bytes, over the %d ceiling",
					name, hdr.Size, lim.entryBytes())
			}
			data, err = io.ReadAll(io.LimitReader(tr, lim.entryBytes()+1))
			if err != nil {
				return man, restored, err
			}
			if int64(len(data)) > maxUnverifiedGroupBytes {
				return man, restored, fmt.Errorf(
					"storage: unverified entry %s exceeds the %d-byte ceiling", name, maxUnverifiedGroupBytes)
			}
			// Parsed even with no manifest to check it against, because the
			// alternative is writing bytes nothing looked at. A manifest-LAST
			// archive is refused -- but the refusal happens when the manifest
			// finally arrives, by which point the groups before it are already
			// on disk. Parsing here means what lands is at least a readable
			// group rather than arbitrary bytes; the manifest adds the size
			// and the checksum on top.
			if _, perr := ReadGroup(data); perr != nil {
				return man, restored, fmt.Errorf("storage: %s does not parse: %w", name, perr)
			}
		} else {
			g, ok := byName[name]
			if !ok {
				// A group the manifest does not name is a group nothing
				// checked. Restoring it makes the manifest a description of
				// part of the archive, which is worse than no manifest.
				return man, restored, fmt.Errorf("storage: the archive carries %s, which its manifest does not name", name)
			}
			if hdr.Size != g.Bytes {
				return man, restored, fmt.Errorf("storage: %s is %d bytes in the archive, the manifest says %d",
					name, hdr.Size, g.Bytes)
			}
			// The SAME ceiling the manifest-less branch applies, and for the
			// same reason. g.Bytes comes from the manifest, and the manifest
			// is part of the archive being distrusted -- so sizing a read from
			// it while calling the other branch's identical read "an
			// allocation a hostile archive chooses" was a bound present in one
			// of two places, which this repository has now recorded three
			// times as reading like a bound present in both.
			if g.Bytes > lim.entryBytes() {
				return man, restored, fmt.Errorf(
					"storage: %s declares %d bytes, over the %d ceiling",
					name, g.Bytes, lim.entryBytes())
			}
			data, err = io.ReadAll(io.LimitReader(tr, g.Bytes+1))
			if err != nil {
				return man, restored, err
			}
			if err := g.verifyGroupBytes(data); err != nil {
				return man, restored, err
			}
			// Parsed, not merely checksummed. A checksum proves the bytes
			// survived the transfer; it does not prove they were ever a
			// readable group, which is what a store opened over them needs.
			if _, err := ReadGroup(data); err != nil {
				return man, restored, fmt.Errorf("storage: %s does not parse: %w", name, err)
			}
		}
		if emit != nil {
			if err := emit(name, data); err != nil {
				return man, restored, err
			}
		}
		restored = append(restored, name)
	}

	if man == nil {
		return nil, restored, ErrBackupUnverified
	}
	// Every group the manifest names must be present. The missing-group case
	// is the one a bare tar cannot detect at all, and it is the likely one:
	// a transfer cut short ends in the middle of the group list.
	for _, g := range man.Groups {
		if !seen[g.Name] {
			return man, restored, fmt.Errorf("%w: %s is named by the manifest and absent", ErrBackupTruncated, g.Name)
		}
	}
	if !complete {
		return man, restored, ErrBackupTruncated
	}
	return man, restored, nil
}

// errAfterTerminator reports an entry that arrived after BACKUP-COMPLETE.
func errAfterTerminator(name string) error {
	return fmt.Errorf("storage: the archive carries %s after its %s; the terminator must come last",
		name, backupCompleteName)
}

// RestoreTar unpacks a backup into dir (created if absent), so a fresh store
// can be opened over it.
//
// A format-1 archive is validated entry by entry as it is read: size,
// checksum, and a full ReadGroup parse. A pre-format-1 archive has nothing to
// validate against and is restored with ErrBackupUnverified returned, so a
// caller that requires a verified restore fails rather than being told
// success. The files are already written in that case -- this is the
// unstaged restore, and Task 5.2 replaces it with a staged one.
func RestoreTar(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	_, _, err := readBackup(r, backupReadLimits{}, nil, func(name string, data []byte) error {
		return writeFileAtomic(filepath.Join(dir, name), data, DataFileMode)
	})
	return err
}
