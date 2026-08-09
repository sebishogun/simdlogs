package storage

import "os"

// DropGroupsBefore removes every group whose entire time span is older
// than cutoff -- time-based retention, VL's shape. A group is dropped only
// when its whole span is outside the window, so no group a query could
// still see is ever removed; the file is deleted after it leaves the
// index. Returns how many groups were dropped.
func (s *Store) DropGroupsBefore(cutoff int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.groups[:0]
	dropped := 0
	var toRemove []string
	for _, g := range s.groups {
		if g.timeMax < cutoff {
			toRemove = append(toRemove, g.path)
			dropped++
			continue
		}
		kept = append(kept, g)
	}
	s.groups = kept
	// Remove files after they are out of the index, so a concurrent reader
	// holding a reader still sees valid mmap'd bytes until it is done.
	for _, p := range toRemove {
		os.Remove(p)
	}
	return dropped
}
