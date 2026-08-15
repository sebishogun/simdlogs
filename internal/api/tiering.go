package api

import (
	"log"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
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
	return s.goBackground(interval, func() {
		if n, saved := s.RecompactOlderThan(age, dropPostings); n > 0 {
			log.Printf("tiering: recompacted %d groups, saved %.1f MB", n, float64(saved)/1e6)
		}
	})
}

// Compaction is the other axis: fewer groups rather than smaller ones.
//
// A client sending one row per request produces one group per row, and every
// query walks the group list before it touches a column, so the cost of a
// million one-row groups is paid by every query whether or not it matches
// anything. Recompaction cannot help with that -- it makes each group smaller
// and leaves the count alone.

// CompactSmallGroups merges runs of small adjacent groups in every tenant.
//
// The options are the caller's in full, with no defaults invented here: the
// right thresholds depend on the ingest shape, and a server that picked them
// silently would rewrite a store whose owner never asked for it. `main.go`
// derives them from flags, and the zero value does nothing.
func (s *Server) CompactSmallGroups(opt storage.CompactOptions) (inputs, outputs int, saved int64) {
	// Detached: a pass is file I/O measured in seconds on a large store, and
	// forEachTenant holds the lock every request needs to resolve its tenant.
	//
	// The budgets in opt are per TENANT, not per pass, because each store
	// enforces its own. A caller with many tenants sizes them accordingly;
	// there is no cross-tenant budget and this comment is here so its absence
	// is a known limit rather than a surprise.
	s.forEachTenantDetached(func(tn *tenant) {
		st, err := tn.store.CompactGroups(opt)
		// The stats are counted before the error is reported: a pass that
		// merged forty groups and then failed did merge forty groups, and
		// dropping the count because the tail failed would under-report the
		// I/O the operator is trying to bound.
		inputs += st.Inputs
		outputs += st.Outputs
		// Never negative: mergeAndCommit refuses an output at or above its
		// inputs' size, so a pass either saves bytes or does nothing. The
		// version without that refusal merged two full groups into a bigger
		// one and logged "saved -0.0 MB".
		saved += st.BytesBefore - st.BytesAfter
		if err != nil {
			log.Printf("compact: %v", err)
		}
	})
	return inputs, outputs, saved
}

// There is no StartCompaction taking an absolute cutoff. One existed, with no
// caller and no test, and an absolute cutoff is the thing a scheduled pass
// must not have.
//
// StartCompactionAfter is StartCompaction with the age bound expressed as a
// duration rather than an absolute cutoff.
//
// The cutoff is resolved at the start of every pass, not once. Computing it at
// startup ages with the process: after a day of uptime a cutoff of "an hour
// ago" is a cutoff of "twenty-five hours ago", and the pass stops excluding the
// range ingest is still appending to -- which is the one thing the age bound
// exists to do.
func (s *Server) StartCompactionAfter(opt storage.CompactOptions, age, interval time.Duration) (stop func()) {
	if opt.MinGroups <= 0 || interval <= 0 {
		return func() {}
	}
	return s.goBackground(interval, func() {
		pass := opt
		if age > 0 {
			pass.OlderThan = time.Now().UnixNano() - int64(age)
		}
		if in, out, saved := s.CompactSmallGroups(pass); in > 0 {
			log.Printf("compaction: merged %d groups into %d, saved %.1f MB",
				in, out, float64(saved)/1e6)
		}
	})
}
