package api

import "time"

// EnforceRetention drops every group whose entire time span is older than
// maxAge and returns how many were dropped. Groups are removed from the index
// before their files are deleted, so a query in flight keeps valid bytes.
func (s *Server) EnforceRetention(maxAge time.Duration) int {
	if maxAge <= 0 {
		return 0
	}
	// The horizon is recorded HERE, where the deletion decision is actually
	// made, rather than only in StartRetention. Whatever called this has just
	// declared that anything older than maxAge is deleted on this node, and
	// that is precisely the fact the adopt path needs: anti-entropy cannot tell
	// a group that was never received from one that was deliberately dropped,
	// so without it a peer offers the deleted rows back on every pass and the
	// data is queryable again until the next sweep, forever.
	s.retentionMaxAge.Store(int64(maxAge))
	cutoff := time.Now().UnixNano() - int64(maxAge)
	dropped := 0
	s.forEachTenant(func(tn *tenant) {
		dropped += tn.store.DropGroupsBefore(cutoff)
	})
	return dropped
}

// EnforceRetentionPerStream drops groups by per-stream policy: a group is kept
// until it is older than the LONGEST retention among the streams it contains
// (so no stream loses data early), using defaultAge for streams without an
// explicit policy. Requires _stream to be synthesized (SetStreamFields).
func (s *Server) EnforceRetentionPerStream(defaultAge time.Duration, perStream map[string]time.Duration) int {
	now := time.Now().UnixNano()
	dropped := 0
	s.forEachTenant(func(tn *tenant) {
		dropped += tn.store.DropGroupsWhere(func(timeMax int64, streams []string) bool {
			maxAge := defaultAge
			for _, st := range streams {
				if a, ok := perStream[st]; ok && a > maxAge {
					maxAge = a
				}
			}
			if maxAge <= 0 {
				return false // no policy applies -> keep
			}
			return timeMax < now-int64(maxAge)
		})
	})
	return dropped
}

// StartRetention enforces maxAge every interval until the returned stop is
// called. A non-positive maxAge disables retention and stop is a no-op. This
// is time-based retention, VictoriaLogs' -retentionPeriod shape.
func (s *Server) StartRetention(maxAge, interval time.Duration) (stop func()) {
	if maxAge <= 0 || interval <= 0 {
		return func() {}
	}
	// Recorded so the ADOPT path can refuse a group this node would delete on
	// its next sweep.
	//
	// Anti-entropy cannot tell a group that was never received from one that
	// was deliberately deleted -- both are simply absent -- so a retention drop
	// on one replica is a group the union still contains, and the next repair
	// pass copies the deleted rows back. The router cannot see this: retention
	// is a per-node timer and the horizon is this node's, so the node that
	// would delete the group is the only one that can refuse it.
	s.retentionMaxAge.Store(int64(maxAge))
	s.EnforceRetention(maxAge) // sweep once at startup, not only after the first tick
	return s.goBackground(interval, func() { s.EnforceRetention(maxAge) })
}
