package storage

import (
	"os"
	"sync/atomic"
)

// Retention counters, exported for /metrics. They are process-wide rather
// than per-store because the server holds one store per tenant and the
// operator question is "is retention working", not "which tenant".
var (
	retentionFailures atomic.Int64 // removals whose unlink failed
	pendingTombstones atomic.Int64 // committed removals whose file is still on disk
)

// RetentionFailures is the number of unlinks that have failed.
func RetentionFailures() int64 { return retentionFailures.Load() }

// PendingTombstones is the number of groups committed as removed whose files
// are still on disk awaiting a retry.
func PendingTombstones() int64 { return pendingTombstones.Load() }

// DropGroupsWhere removes groups for which drop(timeMax, streams) reports
// true, where streams is the group's distinct _stream label values. It is the
// basis for per-stream retention: the caller decides per group from its
// streams and newest timestamp. Returns the number dropped.
func (s *Store) DropGroupsWhere(drop func(timeMax int64, streams []string) bool) int {
	return s.dropGroups(func(g *groupEntry) bool {
		var streams []string
		for _, vc := range g.reader.ValueCounts("_stream") {
			streams = append(streams, vc.Value)
		}
		return drop(g.timeMax, streams)
	})
}

// DropGroupsBefore removes every group whose entire time span is older than
// cutoff -- time-based retention, VL's shape. A group is dropped only when its
// whole span is outside the window, so no group a query could still need is
// removed. Returns how many groups were dropped.
func (s *Store) DropGroupsBefore(cutoff int64) int {
	return s.dropGroups(func(g *groupEntry) bool { return g.timeMax < cutoff })
}

// dropGroups is the one retention path, in the order that makes a removal
// survivable:
//
//  1. Commit the removal to the manifest. This is the durable decision, and
//     it comes first because the previous order -- drop from the in-memory
//     index, then unlink -- brought the group back at the next start whenever
//     the unlink failed, and the unlink error was discarded so nothing knew.
//  2. Retire the group version. The mapping is released when the last
//     snapshot holding it closes. The previous code dropped the entry and
//     discarded its unmap callback, which did not release the mapping at all:
//     an unlinked file's blocks stayed allocated until the process exited,
//     which is the opposite of what retention is for.
//  3. Unlink. A failure is retried on the next pass and counted; the group is
//     already invisible and stays that way.
func (s *Store) dropGroups(match func(*groupEntry) bool) int {
	// structMu, like Recompact, Demote and Promote: retention unlinks group
	// paths, and a removal running alongside a rewrite of the same path is
	// two writers deciding what the path holds. Without the lock here, the
	// unlink could land between the rewrite's rename and its mmap -- a
	// spurious Recompact error. Lock order stays structMu then s.mu, as
	// everywhere.
	s.structMu.Lock()
	defer s.structMu.Unlock()

	s.mu.Lock()
	var victims []*groupEntry
	// A fresh slice, not s.groups[:0]. Filtering in place overwrote the
	// backing array before the manifest commit was known to succeed, so a
	// failed commit left the index corrupted -- [0 1 2] became [1 2 2], with
	// group 0 invisible until a restart and group 2 counted twice by every
	// query and by TotalRows.
	kept := make([]*groupEntry, 0, len(s.groups))
	for _, g := range s.groups {
		if match(g) {
			victims = append(victims, g)
			continue
		}
		kept = append(kept, g)
	}
	if len(victims) == 0 {
		s.mu.Unlock()
		s.retryTombstones()
		return 0
	}

	ids := make([]uint64, 0, len(victims))
	for _, g := range victims {
		ids = append(ids, g.id)
	}
	if err := s.man.commit(nil, ids, nil); err != nil {
		// Nothing is removed if the decision cannot be made durable. Dropping
		// them from the index anyway would make the store answer short until
		// a restart brought them back.
		s.mu.Unlock()
		retentionFailures.Add(1)
		return 0
	}
	s.groups = kept
	paths := make([]string, 0, len(victims))
	for _, g := range victims {
		paths = append(paths, g.path)
		g.retire()
	}
	s.mu.Unlock()

	dropped := 0
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			retentionFailures.Add(1)
			pendingTombstones.Add(1)
			s.addTombstone(p)
			continue
		}
		dropped++
	}
	s.retryTombstones()
	return dropped
}

func (s *Store) addTombstone(path string) {
	s.mu.Lock()
	s.tombstones = append(s.tombstones, path)
	s.mu.Unlock()
}

// retryTombstones re-attempts unlinks that failed earlier. The groups are
// already committed as removed, so this only reclaims disk; a file that keeps
// failing stays counted rather than being forgotten.
func (s *Store) retryTombstones() {
	s.mu.Lock()
	pending := s.tombstones
	s.tombstones = nil
	s.mu.Unlock()
	var still []string
	for _, p := range pending {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			still = append(still, p)
			continue
		}
		pendingTombstones.Add(-1)
	}
	if len(still) > 0 {
		s.mu.Lock()
		s.tombstones = append(s.tombstones, still...)
		s.mu.Unlock()
	}
}
