package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// ColdStore is a tiered-storage backend for aging group files off local disk
// onto cheaper storage (S3/GCS/filesystem). It is deliberately a tiny
// blob interface so an S3 adapter is a thin wrapper with no dependency pulled
// into the core. Demote uploads a group and drops it locally; Promote brings
// it back to be queried (Glacier-style: archived cold, restored to query).
type ColdStore interface {
	Put(name string, data []byte) error
	Get(name string) ([]byte, error)
	List() ([]string, error)
	Delete(name string) error
}

// LocalCold is a filesystem-backed cold tier -- a stand-in for object storage
// in tests and single-host deployments, and the reference an S3 adapter mirrors.
type LocalCold struct{ Dir string }

func (c LocalCold) path(name string) string { return filepath.Join(c.Dir, filepath.Base(name)) }

func (c LocalCold) Put(name string, data []byte) error {
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return err
	}
	// Durable replacement, same as the hot tier: the cold copy is the only
	// copy once demotion unlinks the local group, so a rename whose directory
	// entry never reached disk loses it.
	return writeFileAtomic(c.path(name), data, DataFileMode)
}

func (c LocalCold) Get(name string) ([]byte, error) { return os.ReadFile(c.path(name)) }

func (c LocalCold) List() ([]string, error) {
	entries, err := os.ReadDir(c.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".bin" {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (c LocalCold) Delete(name string) error { return os.Remove(c.path(name)) }

// Demote uploads every group whose whole span is older than cutoff to the cold
// store and removes it from local disk, returning how many moved. Like
// DropGroupsBefore it does not unmap live readers -- the unlinked file's mapping
// stays valid until the reader is done (space is reclaimed then), so a
// concurrent query never sees invalid bytes.
func (s *Store) Demote(cutoff int64, cold ColdStore) (int, error) {
	s.structMu.Lock()
	defer s.structMu.Unlock()

	// Pick candidates, then upload outside the store lock: cold Put is
	// network or disk IO and holding the write lock across it stalls every
	// query.
	s.mu.RLock()
	var cands []*groupEntry
	for _, g := range s.groups {
		if g.timeMax < cutoff {
			// Same reason as recompaction: the upload happens outside the
			// lock, and the entry must stay mapped until this operation is
			// done with it.
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

	moved := 0
	for _, g := range cands {
		data, err := os.ReadFile(g.path)
		if err != nil {
			continue
		}
		if err := cold.Put(filepath.Base(g.path), data); err != nil {
			return moved, err
		}
		// The cold copy is durable, so the local one may go. Same order as
		// retention: commit the removal, retire the version so its mapping
		// outlives any reader still using it, then unlink. The previous code
		// dropped the entry and discarded the unmap callback, which leaked
		// the mapping -- an unlinked file's blocks stay allocated while a
		// mapping of it lives, so demotion freed nothing until process exit.
		s.mu.Lock()
		idx := -1
		for i, cur := range s.groups {
			if cur == g {
				idx = i
				break
			}
		}
		if idx < 0 {
			s.mu.Unlock()
			continue // already removed by retention
		}
		if err := s.man.commit(nil, []uint64{g.id}, nil); err != nil {
			s.mu.Unlock()
			return moved, err
		}
		s.groups = append(s.groups[:idx], s.groups[idx+1:]...)
		s.mu.Unlock()
		g.retire()
		if rerr := os.Remove(g.path); rerr != nil && !os.IsNotExist(rerr) {
			retentionFailures.Add(1)
			pendingTombstones.Add(1)
			s.addTombstone(g.path)
		}
		moved++
	}
	return moved, nil
}

// Promote restores a cold group back to local disk and into the index so it can
// be queried again.
func (s *Store) Promote(name string, cold ColdStore) error {
	s.structMu.Lock()
	defer s.structMu.Unlock()
	data, err := cold.Get(name)
	if err != nil {
		return err
	}
	final := filepath.Join(s.dir, filepath.Base(name))
	if err := writeFileAtomic(final, data, DataFileMode); err != nil {
		return err
	}
	b, unmap, err := mmapFile(final)
	if err != nil {
		return err
	}
	r, err := ReadGroup(b)
	if err != nil {
		unmap()
		return err
	}
	id, ok := groupIDFromName(final)
	if !ok {
		unmap()
		return fmt.Errorf("storage: cold object %q is not a group file name", name)
	}
	s.mu.Lock()
	// Promotion is idempotent: a group already visible is not added twice,
	// which would put two entries with one id in the index and unmap the same
	// file from both.
	for _, g := range s.groups {
		if g.id == id {
			s.mu.Unlock()
			unmap()
			return nil
		}
	}
	// Commit before it becomes visible, same as AppendGroup: the file is on
	// disk either way, and the manifest decides whether it is part of the
	// store.
	if err := s.man.commit([]uint64{id}, nil, nil); err != nil {
		s.mu.Unlock()
		unmap()
		return err
	}
	s.groups = append(s.groups, &groupEntry{id: id, path: final, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	s.sortGroups()
	s.mu.Unlock()
	return nil
}
