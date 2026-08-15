package api

import (
	"net/http"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// checkStorage refuses a write when the tenant's storage budget says so.
//
// In the middleware every insert route shares, not in each handler. There are
// six HTTP write entry points -- jsonline, logfmt, the Elasticsearch bulk
// path, Loki, Datadog, OTLP, journald and syslog-over-HTTP between them reach
// four different functions -- and a check written into each is a check that
// will be missing from the seventh. This repo has recorded the one-side-only
// shape fifteen times; a budget enforced on five of six paths is that shape
// with a storage bill attached.
//
// It runs BEFORE the body is read and parsed, so no row reaches the writer:
// rows that reach it are the writer's, and refusing a request whose rows are
// already buffered would either drop them silently or report a failure for
// rows that will be written anyway.
//
// 507 rather than 503. The condition is about storage rather than the server
// being unavailable, and an agent that retries a 503 forever against a full
// disk is a busy loop against a machine that has no room for its retries.
func (s *Server) checkStorage(spec routeSpec, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// A route with no tenant writes nothing -- the Datadog validate
		// endpoint carries the ingest role and never resolves one -- so there
		// is no budget to check and nothing to refuse.
		tn := tenantOf(r)
		if tn == nil {
			h(w, r)
			return
		}
		if err := tn.store.CheckWrite(); err != nil {
			storage.NoteRejectedWrite(err)
			s.writeErr(w, r, spec, http.StatusInsufficientStorage, err.Error())
			return
		}
		h(w, r)
	}
}
