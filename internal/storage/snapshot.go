package storage

import (
	"errors"
	"sync/atomic"
)

// ErrStoreClosed is returned by Snapshot once the store is closing. A caller
// that gets it has no groups to read and should answer as it would for an
// empty window rather than treating it as corruption.
var ErrStoreClosed = errors.New("storage: store is closed")

// A Snapshot is a set of group readers the caller owns for as long as it
// holds the snapshot. Every reader in it is guaranteed to stay mapped until
// Close, whatever retention, recompaction, cold demotion or store close do in
// the meantime.
//
// This is the replacement for handing out raw *Reader values. A reader is a
// window onto an mmap, so releasing that mapping while a query is walking it
// is a segfault, not a stale read. The old code had two ways to do exactly
// that: recompaction retired replaced mappings on a five-minute timer that no
// query duration was bound by, and retention and cold demotion dropped the
// index entry while discarding the unmap callback entirely -- which leaked
// the mapping rather than freeing it, so the file's blocks stayed allocated
// after the unlink.
//
// Ownership is a reference count per group version instead. Retirement marks
// a version and unmaps it when the last snapshot releases it; a snapshot
// taken after retirement never sees it at all.
type Snapshot struct {
	// Groups are the readers whose time range overlaps the request, in the
	// store's time order. Valid until Close.
	Groups []*Reader

	// GroupIDs are the manifest ids of Groups, index for index. Pagination
	// needs a row identity that survives a second query, and the position of a
	// group WITHIN a snapshot does not: compaction, retention and cold
	// demotion all change it. The manifest id is assigned once by AppendGroup
	// and never reused, so a (time, group id, row index) tuple names the same
	// row for as long as the row exists.
	GroupIDs []uint64

	entries []*groupEntry
	// closed is atomic: two concurrent Close calls both passed a plain-bool
	// check and released every entry twice, driving refs below zero and
	// unmapping a version another snapshot still held.
	closed atomic.Bool
}

// Close releases every group this snapshot holds. It is idempotent, so a
// deferred Close next to an early return is safe.
func (s *Snapshot) Close() error {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	for _, e := range s.entries {
		if err := e.release(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.entries = nil
	s.Groups = nil
	s.GroupIDs = nil
	return firstErr
}

// EmptySnapshot is a valid snapshot holding nothing. Callers that cannot
// return an error use it when the store is closed, so their loops run zero
// times instead of needing a nil check.
func EmptySnapshot() *Snapshot { return &Snapshot{} }

// acquire takes a reference if the version is still usable. It returns false
// for a version already retired and released, which cannot be resurrected.
func (e *groupEntry) acquire() bool {
	for {
		n := e.refs.Load()
		if n < 0 {
			return false // already fully released
		}
		if e.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// release drops a reference, unmapping when the last holder of a retired
// version lets go.
func (e *groupEntry) release() error {
	if e.refs.Add(-1) == 0 && e.retired.Load() {
		return e.unmapOnce()
	}
	return nil
}

// retire marks the version as replaced or deleted. New snapshots will not see
// it; the mapping goes away when the last existing reader releases it, which
// may be immediately.
func (e *groupEntry) retire() error {
	e.retired.Store(true)
	if e.refs.Load() == 0 {
		return e.unmapOnce()
	}
	return nil
}

// unmapOnce releases the mapping exactly once, however many callers race to
// do it. It parks the reference count below zero so a late acquire cannot
// hand out a reader over freed memory.
func (e *groupEntry) unmapOnce() error {
	if !e.unmapped.CompareAndSwap(false, true) {
		return nil
	}
	e.refs.Store(-1 << 40)
	if e.unmap == nil {
		return nil
	}
	return e.unmap()
}

// Snapshot returns the groups overlapping [from, to), each held against
// unmapping until the returned snapshot is closed.
//
// References are taken under the store lock, so a retirement cannot land
// between the overlap test and the acquire: the retiring side needs the same
// lock to remove the entry from the index.
func (s *Store) Snapshot(from, to int64) (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	snap := &Snapshot{}
	for _, g := range s.groups {
		if !g.reader.TimeRangeMatches(from, to) {
			continue
		}
		if !g.acquire() {
			continue // retired and released under us; it is not in the window any more
		}
		if s.openHook != nil {
			s.openHook(g.id)
		}
		snap.entries = append(snap.entries, g)
		snap.Groups = append(snap.Groups, g.reader)
		snap.GroupIDs = append(snap.GroupIDs, g.id)
	}
	return snap, nil
}

// SnapshotAllWithSeq leases every group and reads the manifest sequence at the
// SAME instant, under one lock acquisition.
//
// There was a SnapshotAll beside it that dropped the sequence, and nothing in
// production called it -- every caller needs the pair, which is the whole
// reason this exists. It was a two-line wrapper that let a caller take the
// snapshot without the number that makes it verifiable, so it is gone rather
// than kept for symmetry.
//
// Reading the sequence in a second acquisition is not the same number: an
// AppendGroup between the two advances it, and the archive then declares a
// high watermark covering a group it does not contain.
func (s *Store) SnapshotAllWithSeq() (*Snapshot, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrStoreClosed
	}
	snap := &Snapshot{}
	for _, g := range s.groups {
		if !g.acquire() {
			continue
		}
		if s.openHook != nil {
			s.openHook(g.id)
		}
		snap.entries = append(snap.entries, g)
		snap.Groups = append(snap.Groups, g.reader)
		snap.GroupIDs = append(snap.GroupIDs, g.id)
	}
	return snap, s.man.seq, nil
}

// SnapshotAfterID is the live-tail form: every group with an ID at or above
// cursor, held until Close, plus the next cursor to ask for.
func (s *Store) SnapshotAfterID(cursor uint64) (*Snapshot, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, cursor, ErrStoreClosed
	}
	snap := &Snapshot{}
	next := cursor
	for _, g := range s.groups {
		if g.id < cursor {
			continue
		}
		if !g.acquire() {
			continue
		}
		snap.entries = append(snap.entries, g)
		snap.Groups = append(snap.Groups, g.reader)
		snap.GroupIDs = append(snap.GroupIDs, g.id)
		if g.id+1 > next {
			next = g.id + 1
		}
	}
	return snap, next, nil
}
