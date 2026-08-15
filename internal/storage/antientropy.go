package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Group-level anti-entropy: naming a group by its CONTENT, not by its id.
//
// # Why not by id
//
// A group's manifest id is assigned by the store that wrote it -- nextID++ --
// so it is meaningful only within one store. Two replicas of a shard normally
// agree, because the router sends both the same writes in the same order. They
// stop agreeing the moment one of them misses a write, which is exactly the
// state repair exists to fix:
//
//	replica A: id 1 = W1, id 2 = W2, id 3 = W3
//	replica B: id 1 = W1, id 2 = W3          (missed W2)
//
// B is not "missing id 2". B has id 2, holding different rows. Repair by id
// would decide B needs A's id 3, copy it in, and leave B holding W3 twice --
// turning a missing write into a duplicated one, which is worse: a gap shows up
// as a short answer, and a duplicate shows up as a plausible larger number.
//
// The digest of a group's bytes is the same on every replica that has it, and
// different for every group that differs. Reconciling on the digest gives the
// right answer in the diverged case above: B is missing W2, and copying it in
// leaves B with all three writes.
//
// # What a repaired group's id becomes
//
// A fresh local id, from the same counter every other write uses. It cannot be
// the source's id -- that id may already mean something else here, as it does
// for B above.
//
// This is a real limitation, not a detail: a pagination cursor names a row by
// (timestamp, group id, row index), so a cursor issued while reading one
// replica does not name the same rows on another. That was already true before
// repair existed, because the ids only ever agreed by coincidence of ordering.

// GroupDigest names one group by its content.
type GroupDigest struct {
	// Digest is the SHA-256 of the group file's bytes, hex encoded. The file is
	// immutable once sealed, so the digest is stable for as long as the group
	// exists.
	Digest string `json:"digest"`
	// ID is the local manifest id. Reported so an operator can find the group
	// on this node; it is NOT how replicas are compared.
	ID uint64 `json:"id"`
	// Rows, TimeMin and TimeMax describe the group, so a repair can report what
	// it is about to copy and bound itself by size or by window.
	Rows    int   `json:"rows"`
	TimeMin int64 `json:"timeMin"`
	TimeMax int64 `json:"timeMax"`
	// Bytes is the group file's size, so a bounded repair can decide before
	// fetching rather than after.
	Bytes int64 `json:"bytes"`
}

// GroupDigests lists every group this store holds, by content.
//
// Taken under a snapshot, so the list describes one consistent moment: without
// it a compaction running mid-walk would produce a list holding both a group
// and the groups it was compacted from, and a repair reading that would copy
// rows that already exist in merged form.
func (s *Store) GroupDigests() ([]GroupDigest, error) {
	snap, _, err := s.SnapshotAllWithSeq()
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	out := make([]GroupDigest, 0, len(snap.Groups))
	for i, g := range snap.Groups {
		id := snap.GroupIDs[i]
		path := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
		fi, err := os.Stat(path)
		if err != nil {
			// A group in the manifest with no file is a corrupt store, not a
			// repairable difference. Reporting it as absent would make a peer
			// copy its own copy in, which does not fix this node.
			return nil, fmt.Errorf("storage: group %d is in the manifest but not on disk: %w", id, err)
		}
		d, err := fileDigest(path)
		if err != nil {
			return nil, err
		}
		out = append(out, GroupDigest{
			Digest: d, ID: id, Rows: g.Rows,
			TimeMin: g.TimeMin, TimeMax: g.TimeMax, Bytes: fi.Size(),
		})
	}
	return out, nil
}

// GroupBytes returns one group's file bytes, addressed by content.
//
// By digest rather than by id, so a caller that asked for a specific group
// cannot be handed a different one: between the inventory and the fetch, a
// compaction can retire the group that held that id and a later write can reuse
// nothing -- ids are never reused -- but the group can be gone, and answering
// "here is id 7" for a different group would silently copy the wrong rows.
func (s *Store) GroupBytes(digest string) ([]byte, error) {
	digests, err := s.GroupDigests()
	if err != nil {
		return nil, err
	}
	for _, d := range digests {
		if d.Digest != digest {
			continue
		}
		path := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", d.ID))
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		// Re-checked after the read: the inventory hashed the file at one
		// moment and this read happened at another. A mismatch means the file
		// changed underneath, which for an immutable group means corruption.
		if got := digestBytes(b); got != digest {
			return nil, fmt.Errorf(
				"storage: group %d changed while being read (%s, wanted %s)", d.ID, got, digest)
		}
		return b, nil
	}
	return nil, fmt.Errorf("storage: no group with digest %s", digest)
}

// AdoptGroup takes a group's bytes from a peer and commits them here.
//
// It is the repair half of anti-entropy, and it is deliberately narrow:
//
//   - The bytes are VALIDATED before anything is committed -- parsed as a
//     group, with the digest checked against what the caller asked for. A peer
//     that is compromised, buggy or on another format version cannot write
//     arbitrary bytes into this store's directory.
//   - It never deletes and never replaces. Repair only ever adds, so no repair
//     can destroy the last good copy of anything. A store that already has this
//     digest is left alone and reports it, which also makes repair idempotent
//     and safe to retry.
//   - The group is committed under a FRESH local id, because the peer's id is
//     meaningful only on the peer.
func (s *Store) AdoptGroup(digest string, blob []byte) (adopted bool, err error) {
	if got := digestBytes(blob); got != digest {
		return false, fmt.Errorf(
			"storage: refusing a group whose bytes hash to %s, not the %s that was asked for",
			got, digest)
	}
	// Parsed before it is written: a blob that is not a group must not reach
	// the directory, where the next open would find it.
	g, err := ReadGroup(blob)
	if err != nil {
		return false, fmt.Errorf("storage: refusing a group that does not parse: %w", err)
	}
	if g.Rows <= 0 {
		return false, fmt.Errorf("storage: refusing a group with %d rows", g.Rows)
	}

	have, err := s.GroupDigests()
	if err != nil {
		return false, err
	}
	for _, d := range have {
		if d.Digest == digest {
			return false, nil // already here; repair is idempotent
		}
	}
	if err := s.appendRawGroup(blob); err != nil {
		return false, err
	}
	return true, nil
}

// fileDigest hashes a file's bytes.
func fileDigest(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return digestBytes(b), nil
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
