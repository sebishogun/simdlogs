package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Store is the immutable group set: one file per group (mmap-append is
// racy, so a group is written whole and never mutated), with an index
// listing them in time order. Readers are safe for concurrent queries.
type Store struct {
	dir      string
	mu       sync.RWMutex
	groups   []*groupEntry
	nextID   uint64
	openHook func(uint64)
}

type groupEntry struct {
	id      uint64
	path    string
	reader  *Reader
	timeMin int64
	timeMax int64
	unmap   func() error // releases the mmap backing reader.blob
}

// OpenStore opens or creates a store rooted at dir, loading any groups
// already present in time order.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	files, _ := filepath.Glob(filepath.Join(dir, "group-*.bin"))
	for _, f := range files {
		b, unmap, err := mmapFile(f)
		if err != nil {
			return nil, err
		}
		r, err := ReadGroup(b)
		if err != nil {
			unmap()
			return nil, err // a truncated partial flush is skipped by the index, see AppendGroup
		}
		var id uint64
		fmt.Sscanf(filepath.Base(f), "group-%d.bin", &id)
		if id >= s.nextID {
			s.nextID = id + 1
		}
		s.groups = append(s.groups, &groupEntry{id: id, path: f, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	}
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
	tmp := final + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(blob); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	f.Close()
	if err := os.Rename(tmp, final); err != nil {
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
	s.groups = append(s.groups, &groupEntry{id: id, path: final, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	s.sortGroups()
	s.mu.Unlock()
	return id, nil
}

// Close releases every group's mmap. The store must not be used afterward.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, g := range s.groups {
		if g.unmap != nil {
			if err := g.unmap(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	s.groups = nil
	return firstErr
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
