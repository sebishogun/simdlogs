package api

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
)

// fallbackTS returns a per-record timestamp source for lines without their
// own: wall-clock plus a monotonic bump so a burst ingested in the same
// nanosecond still gets distinct, ordered timestamps. Atomic because the
// parallel ingest path calls it from many goroutines.
func (s *Server) fallbackTS() func() int64 {
	return func() int64 { return time.Now().UnixNano() + atomic.AddInt64(&s.mono, 1) }
}

// insertLoki ingests a Grafana Loki push body (JSON). Loki clients expect a
// 204 on success.
func (s *Server) insertLoki(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ingest.IngestLoki(s.w, body, s.fallbackTS())
	if err := s.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// insertDatadog ingests a Datadog logs-intake body (JSON array or object).
// Datadog's intake returns 202 Accepted on success.
func (s *Server) insertDatadog(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ingest.IngestDatadog(s.w, body, s.fallbackTS())
	if err := s.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
