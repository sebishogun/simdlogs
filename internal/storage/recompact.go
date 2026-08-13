package storage

import (
	"os"
	"time"
)

// Tiered storage. A group is written LZ4-compressed for fast value reads; once
// it is old enough that queries against it are rare, re-encoding its dictionaries
// with flate trades that decode speed for size. The per-block codec is flagged in
// the dict section itself, so a store can hold both kinds at once and readers need
// no change -- recompaction is purely a background rewrite.
//
// The mmap lifetime is the subtlety: a query that started before a swap still
// holds the old *Reader and reads its mapped blob, so unmapping immediately would
// segfault. Reference counting on the read path would tax every query, so a
// replaced mapping is retired and unmapped only after a grace period far longer
// than any request the server will serve.
const retireGrace = 5 * time.Minute

type retiredMap struct {
	unmap func() error
	at    time.Time
}

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
	s.mu.RLock()
	cands := make([]*groupEntry, 0, len(s.groups))
	for _, g := range s.groups {
		if g.timeMax < cutoff {
			cands = append(cands, g)
		}
	}
	s.mu.RUnlock()

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
		mb, unmap, merr := mmapFile(ge.path)
		if merr != nil {
			return groups, before, after, merr
		}
		nr, rerr := ReadGroup(mb)
		if rerr != nil {
			unmap()
			return groups, before, after, rerr
		}
		s.mu.Lock()
		old := ge.unmap
		ge.reader, ge.unmap = nr, unmap
		s.retired = append(s.retired, retiredMap{unmap: old, at: time.Now()})
		s.mu.Unlock()

		groups++
		before += oldSize
		after += int64(len(blob))
	}
	s.sweepRetired()
	return groups, before, after, nil
}

// writeGroupFile replaces path atomically: temp file, fsync, rename. A crash
// leaves either the old group or the new one, never a torn file.
func writeGroupFile(path string, blob []byte) error {
	tmp := path + ".recompact"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(blob); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// sweepRetired unmaps replaced mappings whose grace period has passed.
func (s *Store) sweepRetired() {
	now := time.Now()
	s.mu.Lock()
	keep := s.retired[:0]
	for _, r := range s.retired {
		if now.Sub(r.at) < retireGrace {
			keep = append(keep, r)
			continue
		}
		if r.unmap != nil {
			r.unmap()
		}
	}
	s.retired = keep
	s.mu.Unlock()
}
