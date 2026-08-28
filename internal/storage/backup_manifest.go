package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
)

// The backup manifest: what an archive claims to contain, so a restore can
// tell a complete backup from a truncated one.
//
// A tar of group files describes itself only in the weakest sense. Every entry
// is well-formed on its own, so a transfer cut in half, an archive missing the
// groups a concurrent retention pass removed, and a complete backup are three
// things that look identical to `tar t`. For a disaster-recovery artifact that
// is the worst property it can have: the difference is discovered at restore
// time, which is the one moment there is nothing to fall back to.
//
// So the archive is bracketed. The manifest goes FIRST, before any group, so a
// reader knows what to expect while it still has the whole stream ahead of it;
// a terminator goes LAST, so a stream that ends early is detectable without
// trusting a byte count. Neither is a checksum of the other -- they answer
// different questions ("is anything missing" and "did the transfer finish"),
// and a backup can fail either one alone.

// Backup archive entry names. Both are outside the group-* namespace that
// groupIDFromName accepts, so neither can be mistaken for data.
const (
	backupManifestName = "BACKUP-MANIFEST"
	backupCompleteName = "BACKUP-COMPLETE"
	// clusterManifestName is the entry a CLUSTER archive carries. This package
	// never writes one and cannot restore one; it is here so a cluster archive
	// can be told apart from a node archive with no manifest, which is what it
	// otherwise looks like. See ErrClusterArchive.
	clusterManifestName = "cluster.json"
)

// BackupFormat is the archive format this build writes.
//
// Version 1 is the first format with a manifest. An archive without one is a
// pre-format-1 backup; it can still be restored, and the restore says plainly
// that nothing was verified.
const BackupFormat = 1

// maxUnverifiedGroupBytes bounds an entry read before any manifest has been
// seen -- a pre-format-1 archive, where nothing sizes it. A group is written
// at or below the ingest flush ceiling, so 1 GiB is far past anything real.
const maxUnverifiedGroupBytes = 1 << 30

// maxBackupManifestBytes bounds the manifest entry a restore will read. The
// manifest is one JSON object holding a fixed-size record per group, and a
// store cannot hold more groups than the filesystem holds files -- but the
// archive is untrusted input, and an unbounded read of an entry whose declared
// size a hostile archive chooses is an allocation a hostile archive chooses.
const maxBackupManifestBytes = 64 << 20

// BackupManifest is the archive's own description of itself.
type BackupManifest struct {
	// Format is BackupFormat. A newer archive is refused rather than
	// partially understood: a field this build ignores is a field whose
	// absence from validation is silent.
	Format int `json:"format"`

	// CreatedUnix is when the backup was taken, in seconds.
	CreatedUnix int64 `json:"createdUnix"`

	// Tenant names the store this came from, so a restore CAN refuse an
	// archive taken from a different one -- which matters because a restore
	// that mixes two tenants' groups into one directory produces a store that
	// answers another tenant's logs, and nothing else in the archive records
	// which tenant a group belongs to.
	//
	// Nothing reads it yet. RestoreTar takes no tenant and cannot refuse; the
	// staged restore of Task 5.2 is where the check lives. Recorded as written
	// and unread rather than described as a property this build has.
	Tenant string `json:"tenant"`

	// ManifestSeq is the store manifest's sequence number, read under the same
	// lock acquisition that took the snapshot: the high watermark this backup
	// represents. Two backups of the same store are ordered by it.
	//
	// The same acquisition matters. Reading it afterwards is a different
	// number -- an AppendGroup in between advances it -- and the archive then
	// declares a watermark covering a group it does not contain.
	ManifestSeq uint64 `json:"manifestSeq"`

	// Groups is every group in the archive, in ascending id order.
	Groups []BackupGroup `json:"groups"`
}

// BackupGroup is one group's entry in the manifest.
type BackupGroup struct {
	Name    string `json:"name"`    // the archive entry name, e.g. "group-12.bin"
	ID      uint64 `json:"id"`      // the store's group id
	Version int    `json:"version"` // the group format version in the blob's header
	Rows    int    `json:"rows"`
	Bytes   int64  `json:"bytes"`
	CRC32C  uint32 `json:"crc32c"` // over the whole group file
}

// TotalRows is how many rows the archive claims to hold.
func (m *BackupManifest) TotalRows() int {
	n := 0
	for _, g := range m.Groups {
		n += g.Rows
	}
	return n
}

// encode renders the manifest, with groups in a stable order so two backups of
// the same store bytes produce the same manifest bytes.
func (m *BackupManifest) encode() ([]byte, error) {
	sort.Slice(m.Groups, func(i, j int) bool { return m.Groups[i].ID < m.Groups[j].ID })
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("storage: encoding the backup manifest: %w", err)
	}
	return b, nil
}

// decodeBackupManifest parses a manifest entry and checks it is one this build
// can act on.
//
// The format check comes first and refuses anything newer. A future format
// might add a field a restore must honour -- a compression codec, a per-group
// tenant -- and a build that ignores unknown fields would restore the archive
// while silently skipping whatever that field required. Refusing is the only
// answer that cannot be wrong.
func decodeBackupManifest(b []byte) (*BackupManifest, error) {
	// The version FIRST, with a lenient decode, and only then the strict one.
	//
	// Running DisallowUnknownFields first made the check's own comment false:
	// a format-2 manifest carrying a field this build does not know was
	// reported as `json: unknown field "codec"` rather than as an unsupported
	// format, which sends an operator looking for a corrupt archive instead of
	// a newer one.
	var probe struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("storage: the backup manifest is not readable: %w", err)
	}
	if probe.Format != BackupFormat {
		return nil, fmt.Errorf("storage: backup format %d, this build reads %d", probe.Format, BackupFormat)
	}

	var m BackupManifest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("storage: the backup manifest is not readable: %w", err)
	}
	seen := make(map[string]bool, len(m.Groups))
	for _, g := range m.Groups {
		if g.Name == "" {
			return nil, fmt.Errorf("storage: the backup manifest names a group with no name")
		}
		if seen[g.Name] {
			// Two entries for one name means one of them will be validated
			// against the other's checksum, and whichever loses is restored
			// without ever being checked.
			return nil, fmt.Errorf("storage: the backup manifest names %s twice", g.Name)
		}
		seen[g.Name] = true
		if _, ok := groupIDFromName(g.Name); !ok {
			return nil, fmt.Errorf("storage: the backup manifest names %q, which is not a group file", g.Name)
		}
		if g.Bytes < 0 {
			return nil, fmt.Errorf("storage: the backup manifest gives %s a size of %d", g.Name, g.Bytes)
		}
	}
	return &m, nil
}

// verifyGroupBytes checks one restored group against its manifest entry.
//
// Size before checksum, so a truncated file is reported as truncated rather
// than as a checksum mismatch -- the two have different causes and the message
// is the only thing an operator has to work from.
func (g BackupGroup) verifyGroupBytes(b []byte) error {
	if int64(len(b)) != g.Bytes {
		return fmt.Errorf("storage: %s is %d bytes, the manifest says %d", g.Name, len(b), g.Bytes)
	}
	if got := crc32.Checksum(b, crc32c); got != g.CRC32C {
		return fmt.Errorf("storage: %s fails its checksum (%#x, want %#x)", g.Name, got, g.CRC32C)
	}
	return nil
}

// backupGroupOf describes one leased group for the manifest.
//
// The version is read from the blob's header rather than assumed to be the one
// this build writes: a store that has not been rewritten since an older binary
// filled it holds v7 groups, and a manifest claiming they are v8 would make a
// restore's own version check the thing that is wrong.
func backupGroupOf(name string, id uint64, rows int, blob []byte) BackupGroup {
	version := 0
	if len(blob) >= 8 {
		version = int(get32(blob, 4))
	}
	return BackupGroup{
		Name:    name,
		ID:      id,
		Version: version,
		Rows:    rows,
		Bytes:   int64(len(blob)),
		CRC32C:  crc32.Checksum(blob, crc32c),
	}
}
