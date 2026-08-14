package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// Store is the immutable group set: one file per group (mmap-append is
// racy, so a group is written whole and never mutated), with an index
// listing them in time order. Readers are safe for concurrent queries.
type Store struct {
	dir      string
	mu       sync.RWMutex
	groups   []*groupEntry
	nextID   uint64
	closed   bool
	openHook func(uint64)
	lock     *dirLock
	man      *manifest
	// tombstones are files whose group is committed as removed but whose
	// unlink failed; retried on every later retention pass.
	tombstones []string
	// structMu serializes structural operations -- recompaction, cold
	// demotion, promotion -- against each other. They each read a candidate
	// set, do IO, then swap; overlapping two of them lets a stale candidate
	// recreate a group another one removed.
	structMu sync.Mutex
}

type groupEntry struct {
	id      uint64
	path    string
	reader  *Reader
	timeMin int64
	timeMax int64
	unmap   func() error // releases the mmap backing reader.blob

	// Ownership. A snapshot holds a reference for as long as a caller can
	// read the mapping; retirement marks a version replaced or deleted, and
	// the mapping is released when the two meet at zero. See snapshot.go.
	refs     atomic.Int64
	retired  atomic.Bool
	unmapped atomic.Bool
}

// OpenStore opens or creates a store rooted at dir, loading any groups
// already present in time order.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Exclusive for the life of the store. Two processes on one directory
	// each allocate ids from their own nextID, so both write group-7.bin and
	// one destroys the other's data with nothing to detect it.
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	man, err := openManifest(dir)
	if err != nil {
		lock.unlock()
		return nil, err
	}
	s := &Store{dir: dir, lock: lock, man: man}

	files, _ := filepath.Glob(filepath.Join(dir, "group-*.bin"))
	onDisk := make(map[uint64]string, len(files))
	for _, f := range files {
		if id, ok := groupIDFromName(f); ok {
			onDisk[id] = f
		}
	}
	// A directory with groups and no manifest predates it. Validate what is
	// there and commit one snapshot naming it, so from here on visibility is
	// a committed fact rather than whatever the glob returned.
	if len(man.visible) == 0 && len(onDisk) > 0 {
		ids := make([]uint64, 0, len(onDisk))
		for id, path := range onDisk {
			b, unmap, err := mmapFile(path)
			if err != nil {
				continue
			}
			_, rerr := ReadGroup(b)
			unmap()
			if rerr == nil {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if err := man.bootstrap(ids); err != nil {
			man.close()
			lock.unlock()
			return nil, err
		}
	}

	for _, id := range man.visibleIDs() {
		path, ok := onDisk[id]
		if !ok {
			// Committed but absent: the file was removed behind the
			// manifest's back. Reported rather than skipped, because a
			// silently short store answers queries with missing data.
			man.close()
			lock.unlock()
			return nil, fmt.Errorf("storage: group %d is committed but its file is missing", id)
		}
		b, unmap, err := mmapFile(path)
		if err != nil {
			man.close()
			lock.unlock()
			return nil, err
		}
		r, err := ReadGroup(b)
		if err != nil {
			unmap()
			man.close()
			lock.unlock()
			return nil, err
		}
		if id >= s.nextID {
			s.nextID = id + 1
		}
		s.groups = append(s.groups, &groupEntry{id: id, path: path, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	}
	// Any group file the manifest does not name never committed -- a crash
	// between the rename and the commit. It stays on disk untouched; nothing
	// reads it, and an operator can inspect it.
	s.sortGroups()
	return s, nil
}

// AppendGroup writes a group crash-safely: the bytes go to a temp file,
// fsync, rename into place (atomic), then the index picks it up. A crash
// between temp and rename leaves the temp file, which OpenStore ignores.
func (s *Store) AppendGroup(g *Group) (uint64, error) {
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()

	blob := g.Marshal()
	final := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
	if err := writeFileAtomic(final, blob, DataFileMode); err != nil {
		return 0, err
	}
	// Map the freshly written file rather than keeping the marshaled blob on
	// the heap, so a large store holds only its working set in RAM (the OS
	// pages the mapping in and out). The Marshal output is now free to GC.
	mb, unmap, err := mmapFile(final)
	if err != nil {
		return 0, err
	}
	r, err := ReadGroup(mb)
	if err != nil {
		unmap()
		return 0, err
	}
	s.mu.Lock()
	// Commit before the group is visible. A crash between the rename and this
	// point leaves an uncommitted file that the next open ignores, which is
	// the difference between "not written" and "half written but queryable".
	if err := s.man.commit([]uint64{id}, nil, nil); err != nil {
		s.mu.Unlock()
		unmap()
		return 0, err
	}
	s.groups = append(s.groups, &groupEntry{id: id, path: final, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	s.sortGroups()
	s.mu.Unlock()
	return id, nil
}

// Close releases every group's mmap. The store must not be used afterward.
// Close stops new snapshots and retires every group. A mapping still held by
// an open snapshot is released when that snapshot closes, not here: unmapping
// under a live reader is a segfault, and shutdown is exactly when in-flight
// queries are most likely to still be running.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var firstErr error
	for _, g := range s.groups {
		if err := g.retire(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.groups = nil
	if err := s.man.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.lock.unlock(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// CommitRemoval records that groups are no longer part of the store. The
// manifest is the commit point, so a removal that is committed stays removed
// across a restart even if the unlink afterwards fails -- which is what let
// retention resurrect a group it had already dropped.
func (s *Store) CommitRemoval(ids ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.man.commit(nil, ids, nil)
}

func (s *Store) sortGroups() {
	sort.Slice(s.groups, func(i, j int) bool { return s.groups[i].timeMin < s.groups[j].timeMin })
}

// Groups returns the readers whose time span overlaps [from, to), in time
// order -- the first skip, before any column is touched. openHook, if set,
// is called per returned group, so a test can prove skipped groups are
// never opened.
func (s *Store) Groups(from, to int64) []*Reader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Reader
	for _, g := range s.groups {
		if g.reader.TimeRangeMatches(from, to) {
			if s.openHook != nil {
				s.openHook(g.id)
			}
			out = append(out, g.reader)
		}
	}
	return out
}

// openHook counts group opens for the skip test; nil in production.
var _ = 0

func (s *Store) SetOpenHook(fn func(uint64)) { s.openHook = fn }

// Len is the group count.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.groups)
}

// TotalRows sums the row counts of every group -- the stored record total,
// for the /metrics gauge.
func (s *Store) TotalRows() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, g := range s.groups {
		n += g.reader.Rows
	}
	return n
}

// TailCursor is the live-tail watermark: the delivery boundary a tailer
// subscribes at, so it streams only groups appended afterward. It is one past
// the highest current id (0 on an empty store), which -- because ids start at
// 0 -- is what makes even the first-ever group tailable.
func (s *Store) TailCursor() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var next uint64 // 0 => empty store; first group (id 0) is >= 0 and thus delivered
	for _, g := range s.groups {
		if g.id+1 > next {
			next = g.id + 1
		}
	}
	return next
}

// GroupsAfterID returns the readers of groups at or beyond the cursor, and the
// advanced watermark -- the live-tail poll: each tick processes only what
// arrived since the last cursor. Pair with TailCursor for the initial value.
func (s *Store) GroupsAfterID(cursor uint64) (readers []*Reader, next uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	next = cursor
	for _, g := range s.groups {
		if g.id >= cursor {
			readers = append(readers, g.reader)
			if g.id+1 > next {
				next = g.id + 1
			}
		}
	}
	return readers, next
}
