package api

import (
	"net/http"

	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// checkStorage refuses a write when the tenant's storage budget says so.
//
// In the middleware every insert route shares, not in each handler. The mux
// registers fourteen ingest routes reaching NINE distinct handlers -- the
// eight that write (insertJSONLine, insertLogfmt, esBulk, insertLoki,
// insertDatadog, insertSyslog, insertOTLPLogs, insertJournald) plus the inline
// 200 on /insert/datadog/api/v1/validate, which resolves no tenant and so has
// no budget to check. A check written into each is a check that will be
// missing from the tenth.
//
// This comment has now been wrong twice about its own numbers -- first "six
// entry points reaching four functions", then "eight distinct handlers" --
// which is what a count nothing gates does. Both were found by counting the
// mux, which is the only way any of them was ever going to be found.
//
// The HTTP mux is not every write path. The native syslog listeners take
// bytes off a socket with no middleware anywhere near them, so they check the budget themselves -- see syslogAdmits in syslog_listen.go.
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
		// A write this node has already taken is answered "duplicate" and NOT
		// stored again.
		//
		// This is what makes a replicated write safe to retry: the router
		// cannot tell "did not commit" from "committed and the answer was
		// lost", so it retries -- and without this, every retry duplicates
		// every row on the replicas that did commit. Silently, because a log
		// store has no primary key and a duplicated line looks exactly like a
		// line that happened twice.
		//
		// Checked before the body is read, so a duplicate costs nothing.
		var writeID storage.WriteID
		if wid := r.Header.Get(HdrWriteID); wid != "" && storage.ValidWriteID(wid) {
			if tn.store.CommittedWrite(storage.WriteID(wid)) {
				w.Header().Set(HdrDuplicate, "true")
				w.Header().Set(HdrWriteID, wid)
				s.writeErr(w, r, spec, http.StatusOK,
					"simdlogs: this write id is already committed; nothing was stored")
				return
			}
			writeID = storage.WriteID(wid)
		}
		if err := tn.store.CheckWrite(); err != nil {
			storage.NoteRejectedWrite(err)
			s.writeErr(w, r, spec, http.StatusInsufficientStorage, err.Error())
			return
		}
		if writeID == "" {
			h(w, r)
			return
		}

		// A receipt says the whole request became durable. A quota refusal or
		// parser error must not record one: the router will retry that id, and a
		// false receipt would turn the retry into a duplicate success with no
		// rows stored. Capture the handler's status and flush only a success.
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		h(sw, r)
		if sw.code < 200 || sw.code >= 300 {
			return
		}
		if err := tn.w.FlushWithReceipt(writeID); err != nil {
			obs.L().Error("could not record a write receipt",
				obs.FieldEvent, "cluster.receipt_failed",
				"write_id", string(writeID), obs.FieldTenant, tn.key,
				obs.FieldErrorClass, string(obs.ClassStorage), "error", err)
		}
	}
}
