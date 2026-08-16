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

// groupDigestCached is the digest of one group, hashed at most once.
//
// A group file is IMMUTABLE once sealed, so its digest is a property of its id
// for as long as the group exists -- which makes caching it exact rather than a
// heuristic that needs invalidating on change.
//
// Without it, every lookup walked and hashed the store: GroupBytes for one
// group cost O(store), and a repair pass paid that per group at both ends.
// Measured at 9.7x for 10x the groups before, 4.0x with the walk stopping at
// the first match, and flat with the cache.
//
// Eviction is by ABSENCE, not by hook: an entry whose id is no longer in the
// store is dead weight of one string, and pruning against the live set on each
// full inventory keeps it bounded without every removal path needing to know
// this cache exists.
func (s *Store) groupDigestCached(id uint64) (string, error) {
	s.digestMu.Lock()
	if d, ok := s.digestByID[id]; ok {
		s.digestMu.Unlock()
		return d, nil
	}
	s.digestMu.Unlock()

	d, err := fileDigest(filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id)))
	if err != nil {
		return "", err
	}
	s.digestMu.Lock()
	if s.digestByID == nil {
		s.digestByID = map[uint64]string{}
	}
	s.digestByID[id] = d
	s.digestMu.Unlock()
	return d, nil
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
	live := make(map[uint64]bool, len(snap.Groups))
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
		d, err := s.groupDigestCached(id)
		if err != nil {
			return nil, err
		}
		live[id] = true
		out = append(out, GroupDigest{
			Digest: d, ID: id, Rows: g.Rows,
			TimeMin: g.TimeMin, TimeMax: g.TimeMax, Bytes: fi.Size(),
		})
	}
	// Prune entries for groups this store no longer holds. A full inventory is
	// the natural moment: it already knows the live set.
	s.digestMu.Lock()
	for id := range s.digestByID {
		if !live[id] {
			delete(s.digestByID, id)
		}
	}
	s.digestMu.Unlock()
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
	// The store is walked, not INVENTORIED. GroupDigests reads and hashes every
	// group file, so serving one group cost O(store) -- measured at 9.7x for
	// 10x the groups -- and a repair pass paid it per group at both ends. Here
	// the walk stops at the first match and hashes only what it reads.
	snap, _, err := s.SnapshotAllWithSeq()
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	for i := range snap.Groups {
		id := snap.GroupIDs[i]
		d, err := s.groupDigestCached(id)
		if err != nil || d != digest {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id)))
		if err != nil {
			return nil, err
		}
		// Re-hashed: the cache says these bytes had this digest, and this read
		// is a different moment. For an immutable group a mismatch means the
		// file changed underneath, which is corruption.
		if got := digestBytes(b); got != digest {
			return nil, fmt.Errorf(
				"storage: group %d changed while being read (%s, wanted %s)",
				id, got, digest)
		}
		// The bytes returned are the bytes that were hashed, in one read --
		// there is no window in which the file could change between the two.
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

	// THE CHECK AND THE APPEND ARE ONE STEP.
	//
	// Held across both, because between them is where the duplicate lands: the
	// two used to take s.mu separately, so concurrent adopts of one group all
	// saw it absent and all appended. The destination is the only participant
	// that can see it already holds the group -- a router deciding what is
	// missing is reading a state another router may already be changing, which
	// is why the router's own latch cannot close this.
	s.adoptMu.Lock()
	defer s.adoptMu.Unlock()

	if s.hasDigest(digest) {
		return false, nil // already here; repair is idempotent
	}
	// The same budget a client write faces.
	//
	// AdoptGroup reached appendBlob directly and never called CheckWrite, and
	// the admin route wrapper does not apply checkStorage the way the ingest
	// one does -- so a store refusing client writes with "disk space below the
	// reserve" accepted repaired groups. The reserve exists so the store can
	// still compact and record a retention removal; repair spent it.
	if err := s.CheckWrite(); err != nil {
		return false, fmt.Errorf("storage: refusing a repaired group: %w", err)
	}
	if err := s.appendRawGroup(blob); err != nil {
		return false, err
	}
	return true, nil
}

// hasDigest reports whether this store already holds a group with these bytes,
// without hashing the whole store.
//
// GroupDigests reads and SHA-256s every group file, so calling it to answer a
// yes/no question about ONE digest made a single adopt cost O(store) -- and a
// repair pass pays that per group, on both ends. Sizes come from the file
// system, so a group whose length differs cannot match and is never read.
func (s *Store) hasDigest(digest string) bool {
	snap, _, err := s.SnapshotAllWithSeq()
	if err != nil {
		return false
	}
	defer snap.Close()
	for i := range snap.Groups {
		if d, err := s.groupDigestCached(snap.GroupIDs[i]); err == nil && d == digest {
			return true
		}
	}
	return false
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

// DigestForTest is digestBytes, exported for tests in other packages that need
// to address a group by content without duplicating the hash.
func DigestForTest(b []byte) string { return digestBytes(b) }
