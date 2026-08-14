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

// defaultTenantFields returns the first stored row's fields, so a test can
// assert WHAT was stored rather than only how many rows there are. A
// mis-framed syslog message stores exactly one row too -- with the octet count
// sitting in hostname -- so a count-only assertion cannot see the defect it
// was written for.
func (s *Server) defaultTenantFields() map[string]string {
	q, err := query.ParseLogsQL("*")
	if err != nil {
		return nil
	}
	q.From, q.To, q.MatAll = 0, int64(1)<<62, true
	rows := query.RunPipeline(s.def.store, q)
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]string, len(rows[0].Fields))
	for _, f := range rows[0].Fields {
		out[f.Key] = f.Value
	}
	return out
}

// errorCount reads the counter /metrics exposes as vl_http_errors_total.
func (s *Server) errorCount() int64 { return atomic.LoadInt64(&s.nHTTPErrs) }
