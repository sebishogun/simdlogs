package storage

import (
	"os"
)

// Tiered storage. A group is written LZ4-compressed for fast value reads; once
// it is old enough that queries against it are rare, re-encoding its dictionaries
// with flate trades that decode speed for size. The per-block codec is flagged in
// the dict section itself, so a store can hold both kinds at once and readers need
// no change -- recompaction is purely a background rewrite.
//
// The mmap lifetime is the subtlety: a query that started before a swap still
// holds the old *Reader and reads its mapped blob, so unmapping immediately
// would segfault.
//
// This used to be handled by retiring the replaced mapping and unmapping it
// after a five-minute grace period, on the assumption that no request lives
// that long. Nothing bounded query duration to five minutes, so the
// assumption was unfounded rather than merely tight. Ownership is explicit
// now: a swap installs a NEW group version and retires the old one, whose
// mapping is released when the last snapshot holding it closes (snapshot.go).

// needsRecompact reports whether any of the group's dict blocks is still stored
// with the default LZ4 codec -- i.e. whether flate has anything left to do. A
// group whose blocks are all hex-packed or already flated is skipped, so
// recompaction is idempotent and survives restarts without a marker.
func (r *Reader) needsRecompact() bool {
	for i := range r.cols {
		m := &r.cols[i]
		if m.Type != ColDict || m.DictLen == 0 {
			continue
		}
		d := parseDictSec(r.dictSec(m))
		for k := 0; k < d.numBlocks; k++ {
			rawField := get32(d.idx, k*12+8)
			if rawField&(dictCodecFlate|dictCodecHex) == 0 {
				return true // an LZ4 block: flate can shrink it
			}
		}
	}
	return false
}

// hasPostings reports whether any dict column still carries an inverted index.
func (r *Reader) hasPostings() bool {
	for i := range r.cols {
		if r.cols[i].Type == ColDict && r.cols[i].PostLen > 0 {
			return true
		}
	}
	return false
}

// rebuild decodes the group back into its in-memory form so it can be re-encoded
// under a different codec. Returns nil if the group holds a column type that
// cannot be round-tripped, in which case the caller leaves it alone.
func (r *Reader) rebuild(compact, dropPostings bool) *Group {
	g := &Group{Rows: r.Rows, Compact: compact, NoPostings: dropPostings}
	for i := range r.cols {
		m := &r.cols[i]
		switch m.Type {
		case ColTimestamp:
			ts := r.TimestampsRange(m.Name, 0, r.Rows)
			if len(ts) != r.Rows {
				return nil
			}
			g.Columns = append(g.Columns, Column{Name: m.Name, Type: ColTimestamp, Ts: ts})
		case ColDict:
			idx, dict := r.DictIndices(m.Name)
			if idx == nil {
				return nil
			}
			vals := make([]string, r.Rows)
			for row := 0; row < r.Rows && row < len(idx); row++ {
				if int(idx[row]) < len(dict) {
					vals[row] = dict[idx[row]]
				}
			}
			d := BuildDict(vals)
			g.Columns = append(g.Columns, Column{Name: m.Name, Type: ColDict, Dict: &d})
		default:
			return nil // vector or a future type: not rewritten
		}
	}
	return g
}

// Recompact re-encodes every group whose newest row is older than cutoff (unix
// nanos) with flate dictionaries, in place and crash-safely (temp file, fsync,
// atomic rename over the group). Returns how many groups were rewritten and the
// bytes before and after, so the caller can report the saving. Groups already
// free of LZ4 blocks are skipped, so calling it repeatedly is cheap.
func (s *Store) Recompact(cutoff int64, dropPostings bool) (groups int, before, after int64, err error) {
	// structMu, like Demote and Promote. Recompaction writes a group file in
	// place, so running it alongside a promotion that writes the same path is
	// two writers on one name. store.go documents this mutex as serializing
	// recompaction; without the lock here that was simply untrue.
	s.structMu.Lock()
	defer s.structMu.Unlock()

	s.mu.RLock()
	cands := make([]*groupEntry, 0, len(s.groups))
	for _, g := range s.groups {
		if g.timeMax < cutoff {
			// Take a reference: the candidate list is read outside the
			// lock, and retention or demotion may retire the entry in the
			// meantime. Without this, needsRecompact reads an unmapped
			// region -- a segfault, not a stale answer.
			if g.acquire() {
				cands = append(cands, g)
			}
		}
	}
	s.mu.RUnlock()
	defer func() {
		for _, g := range cands {
			g.release()
		}
	}()

	for _, ge := range cands {
		r := ge.reader
		if r == nil || !(r.needsRecompact() || (dropPostings && r.hasPostings())) {
			continue
		}
		g := r.rebuild(true, dropPostings)
		if g == nil {
			continue // not round-trippable: leave it as it is
		}
		blob := g.Marshal()
		oldSize := int64(len(r.blob))
		if int64(len(blob)) >= oldSize {
			continue // flate did not help this group: keep the faster LZ4 form
		}
		if err = writeGroupFile(ge.path, blob); err != nil {
			return groups, before, after, err
		}
		if s.beforeMmap != nil {
			s.beforeMmap(ge.path)
		}
		mb, unmap, merr := mmapFile(ge.path)
		if merr != nil {
			return groups, before, after, merr
		}
		nr, rerr := ReadGroup(mb)
		if rerr != nil {
			unmap()
			return groups, before, after, rerr
		}
		// Install the rewritten group as a new version and retire the old
		// one. The entry is replaced rather than mutated in place because a
		// snapshot captured the old *Reader: mutating ge.unmap under it would
		// leave the snapshot's reference guarding the new mapping while the
		// old one it is actually reading went unowned.
		s.mu.Lock()
		idx := -1
		for i, cur := range s.groups {
			if cur == ge {
				idx = i
				break
			}
		}
		if idx < 0 || ge.retired.Load() {
			// This entry is no longer the store's. Either retention removed
			// it while the rewrite was in flight, or another structural
			// operation replaced it.
			//
			// Only delete the file if no live entry claims that path. The
			// unconditional Remove here deleted a *new*, committed group that
			// a promote had put at the same name, and OpenStore then failed
			// with "group N is committed but its file is missing" -- the
			// whole tenant unopenable.
			claimed := false
			for _, cur := range s.groups {
				if cur.path == ge.path {
					claimed = true
					break
				}
			}
			s.mu.Unlock()
			unmap()
			if !claimed {
				if rerr := os.Remove(ge.path); rerr != nil && !os.IsNotExist(rerr) {
					// Task 4.3 requires this error be checked rather than
					// dropped: a file left behind is an uncommitted group the
					// next open ignores, but it is still disk.
					retentionFailures.Add(1)
					pendingTombstones.Add(1)
					s.addTombstone(ge.path)
				}
			}
			continue
		}
		ne := &groupEntry{
			id: ge.id, path: ge.path, reader: nr,
			timeMin: nr.TimeMin, timeMax: nr.TimeMax, unmap: unmap,
		}
		s.groups[idx] = ne
		// The bytes under this id changed, so the digest cache is stale: the
		// cached digest is a property of the OLD bytes, and an inventory that
		// reports it sends peers to fetch a group that no longer exists. The
		// cache is dropped at the INSTALL -- the point at which the new bytes
		// become the store's -- so the next inventory or fetch re-hashes the
		// file. An inventory that read the cache between the file write and
		// the install reports the old digest once, which is a transient stale
		// view; permanent staleness is what this prevents.
		s.invalidateDigest(ge.id)
		s.mu.Unlock()
		ge.retire()

		groups++
		before += oldSize
		after += int64(len(blob))
	}
	// A rewrite that could not unlink its stale file leaves a tombstone;
	// retry them here as well, since retention may be disabled.
	s.retryTombstones()
	return groups, before, after, nil
}

// writeGroupFile replaces path durably: temp file, fsync, rename, then an
// fsync of the parent directory. A crash leaves either the old group or the
// new one, never a torn file and never a directory entry that outlived the
// data it names.
func writeGroupFile(path string, blob []byte) error {
	return writeFileAtomic(path, blob, DataFileMode)
}
