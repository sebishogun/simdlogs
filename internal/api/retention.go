package api

import "time"

// EnforceRetention drops every group whose entire time span is older than
// maxAge and returns how many were dropped. Groups are removed from the index
// before their files are deleted, so a query in flight keeps valid bytes.
func (s *Server) EnforceRetention(maxAge time.Duration) int {
	if maxAge <= 0 {
		return 0
	}
	cutoff := time.Now().UnixNano() - int64(maxAge)
	return s.store.DropGroupsBefore(cutoff)
}

// StartRetention enforces maxAge every interval until the returned stop is
// called. A non-positive maxAge disables retention and stop is a no-op. This
// is time-based retention, VictoriaLogs' -retentionPeriod shape.
func (s *Server) StartRetention(maxAge, interval time.Duration) (stop func()) {
	if maxAge <= 0 || interval <= 0 {
		return func() {}
	}
	s.EnforceRetention(maxAge) // sweep once at startup, not only after the first tick
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				s.EnforceRetention(maxAge)
			}
		}
	}()
	return func() { close(done) }
}
