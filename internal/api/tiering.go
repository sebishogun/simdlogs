package api

import (
	"log"
	"time"
)

// Tiered storage. Groups are written LZ4 for fast value reads; once they are old
// enough that queries against them are rare, re-encoding their dictionaries with
// flate trades that decode speed for size (~17% off a group in the storage test).
// The per-block codec is self-describing, so hot and cold groups coexist and the
// query path is unchanged -- this is the disk axis where VictoriaLogs was ahead,
// closed without giving up the inverted index that makes queries 34-490x faster.

// RecompactOlderThan re-encodes every group older than age with flate
// dictionaries, returning the groups rewritten and the bytes saved. Idempotent:
// a group with no LZ4 blocks left is skipped.
func (s *Server) RecompactOlderThan(age time.Duration, dropPostings bool) (groups int, saved int64) {
	if age <= 0 {
		return 0, 0
	}
	cutoff := time.Now().UnixNano() - int64(age)
	s.forEachTenant(func(tn *tenant) {
		n, before, after, err := tn.store.Recompact(cutoff, dropPostings)
		if err != nil {
			log.Printf("recompact: %v", err)
			return
		}
		groups += n
		saved += before - after
	})
	return groups, saved
}

// StartTiering runs RecompactOlderThan on an interval until the returned stop is
// called. A non-positive age disables it and stop is a no-op.
func (s *Server) StartTiering(age, interval time.Duration, dropPostings bool) (stop func()) {
	if age <= 0 || interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if n, saved := s.RecompactOlderThan(age, dropPostings); n > 0 {
					log.Printf("tiering: recompacted %d groups, saved %.1f MB", n, float64(saved)/1e6)
				}
			}
		}
	}()
	return func() { close(done) }
}
