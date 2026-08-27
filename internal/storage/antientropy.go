package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
// heuristic that needs invalidating on change. The one exception is
// recompaction, which rewrites a group's bytes in place under the same id;
// invalidateDigest drops the entry at the rewrite's install, so the cache's
// premise holds again immediately after.
//
// Without it, serving one group walked and hashed the store: one fetch cost
// O(store), and a repair pass paid that per group at both ends.
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

// invalidateDigest drops the cached digest for one id, so the next inventory
// or fetch re-hashes the file. Called when a group's bytes change under the
// same id (recompaction) -- the one thing the cache's "immutable once sealed"
// premise does not cover.
func (s *Store) invalidateDigest(id uint64) {
	s.digestMu.Lock()
	delete(s.digestByID, id)
	s.digestMu.Unlock()
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

// OpenGroupBytes returns one group's file for streaming to a peer, verified
// against its digest first.
//
// A repaired group may be a gigabyte, and the router spools what it fetches to
// its own disk, so buffering the same bytes again here serves no one: the file
// is hashed in one pass and returned for a second.
//
// The two passes are the limit of what a streaming answer can verify: the
// bytes SERVED are not the bytes HASHED, and a change between the passes
// means the file changed underneath an immutable group -- corruption. The
// receiver hashes everything it is given against the digest it asked for, so
// a change here is caught there rather than trusted.
func (s *Store) OpenGroupBytes(digest string) (*os.File, error) {
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
		path := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
		if got, err := fileDigest(path); err != nil {
			return nil, err
		} else if got != digest {
			return nil, fmt.Errorf(
				"storage: group %d changed while being read (%s, wanted %s)",
				id, got, digest)
		}
		return os.Open(path)
	}
	return nil, fmt.Errorf("storage: no group with digest %s", digest)
}

// AdoptGroupStream validates and commits a group streamed from a peer. A
// repaired group may be a gigabyte, so the bytes land on disk once, hashed on
// the way in, and the digest check and parse happen before anything is
// committed.
//
// It never deletes or replaces, and commits under a fresh local id. A store
// that already has the digest is left alone, making repair idempotent.
//
// refuse, when non-nil, is the caller's chance to veto the group between its
// validation and its commit (the server refuses groups its retention horizon
// has already deleted). Its error is returned unchanged, and the staged file
// is discarded rather than committed.
func (s *Store) AdoptGroupStream(digest string, r io.Reader, refuse func(*Reader) error) (adopted bool, size int64, err error) {
	// THE CHECK AND THE APPEND ARE ONE STEP: the lock spans
	// the "do I have this?" and the commit, so a second adopt of one digest
	// landing while this one streams sees the first and does nothing. It is
	// held across the stream deliberately: a second router copying the same
	// group must wait for the first to commit, not stage its own copy.
	s.adoptMu.Lock()
	defer s.adoptMu.Unlock()

	if s.hasDigest(digest) {
		return false, 0, nil // already here; repair is idempotent
	}
	// The same budget a client write faces.
	//
	// The old buffered adoption reached appendBlob directly and never called CheckWrite, and
	// the admin route wrapper does not apply checkStorage the way the ingest
	// one does -- so a store refusing client writes with "disk space below the
	// reserve" accepted repaired groups. The reserve exists so the store can
	// still compact and record a retention removal; repair spent it.
	if err := s.CheckWrite(); err != nil {
		return false, 0, fmt.Errorf("storage: refusing a repaired group: %w", err)
	}

	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()

	// Staged beside the final name, leaving the same residue a crashed
	// writeFileAtomic leaves: a *.tmp file OpenStore ignores. The final name
	// is NOT used until every validation has passed -- see the commit below.
	final := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
	tmp := final + ".tmp"
	if err := fault(faultCreate); err != nil {
		return false, 0, err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, DataFileMode)
	if err != nil {
		return false, 0, err
	}
	h := sha256.New()
	if err := fault(faultWrite); err != nil {
		f.Close()
		os.Remove(tmp)
		return false, 0, err
	}
	n, err := io.Copy(io.MultiWriter(f, h), r)
	// Fsynced only when the copy succeeded: syncing a partial file that is
	// about to be deleted is wasted IO, and a copy failure is the failure
	// that matters.
	if err == nil {
		if ferr := fault(faultSync); ferr != nil {
			err = ferr
		} else if ferr = f.Sync(); ferr != nil {
			err = ferr
		}
	}
	// Closed either way -- a handle must not leak on the error path.
	if cerr := fault(faultClose); cerr != nil {
		err = cerr
	}
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return false, n, err
	}
	size = n
	// Hashed against the digest that was asked for -- a peer
	// that returns the wrong group is caught here rather than committed.
	if got := hex.EncodeToString(h.Sum(nil)); got != digest {
		os.Remove(tmp)
		return false, size, fmt.Errorf(
			"storage: refusing a group whose bytes hash to %s, not the %s that was asked for",
			got, digest)
	}
	// The parse, the pre-commit checks, the rename and the manifest record
	// are the shared commit, in that order: every refusal (unparseable, zero
	// rows, the caller's) happens while the bytes still carry the .tmp name,
	// so refused input can never leave a group-*.bin file behind -- not in
	// process and not after a crash.
	if _, err := s.commitGroupFile(tmp, final, id, size, "", func(g *Reader) error {
		if g.Rows <= 0 {
			return fmt.Errorf("storage: refusing a group with %d rows", g.Rows)
		}
		if refuse != nil {
			return refuse(g)
		}
		return nil
	}); err != nil {
		return false, size, err
	}
	return true, size, nil
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
//
// Streamed rather than read whole: GroupDigests and OpenGroupBytes both hash
// every group's file, and a group may be a gigabyte, which is not a slice
// worth allocating to sum.
func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// DigestForTest is digestBytes, exported for tests in other packages that need
// to address a group by content without duplicating the hash.
func DigestForTest(b []byte) string { return digestBytes(b) }
