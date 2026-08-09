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
	tmp := c.path(name) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(name)) // atomic
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
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.groups[:0]
	moved := 0
	for _, g := range s.groups {
		if g.timeMax < cutoff {
			data, err := os.ReadFile(g.path)
			if err != nil {
				kept = append(kept, g)
				continue
			}
			if err := cold.Put(filepath.Base(g.path), data); err != nil {
				s.groups = append(kept, g) // keep it; surface the error
				return moved, err
			}
			os.Remove(g.path) // unlink; the mapping (if any) stays valid until unmapped
			moved++
			continue
		}
		kept = append(kept, g)
	}
	s.groups = kept
	return moved, nil
}

// Promote restores a cold group back to local disk and into the index so it can
// be queried again.
func (s *Store) Promote(name string, cold ColdStore) error {
	data, err := cold.Get(name)
	if err != nil {
		return err
	}
	final := filepath.Join(s.dir, filepath.Base(name))
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
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
	var id uint64
	fmt.Sscanf(filepath.Base(final), "group-%d.bin", &id)
	s.mu.Lock()
	s.groups = append(s.groups, &groupEntry{id: id, path: final, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	s.sortGroups()
	s.mu.Unlock()
	return nil
}
