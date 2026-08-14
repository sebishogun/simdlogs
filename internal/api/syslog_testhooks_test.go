package api

import (
	"sync/atomic"

	"github.com/sebishogun/simdlogs/internal/query"
)

// Test-only accessors for the syslog contract tests.
//
// In a _test.go file rather than the package proper: the syslog tests need to
// see the store's row count and the error counter, and neither belongs on the
// exported surface just to make a test possible.

// defaultTenantRows counts every row the default tenant holds.
func (s *Server) defaultTenantRows() int {
	q, err := query.ParseLogsQL("*")
	if err != nil {
		return -1
	}
	q.From, q.To = 0, int64(1)<<62
	return query.Count(s.def.store, q)
}

// errorCount reads the counter /metrics exposes as vl_http_errors_total.
func (s *Server) errorCount() int64 { return atomic.LoadInt64(&s.nHTTPErrs) }
