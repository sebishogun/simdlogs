package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
)

// errorFormat selects the error envelope a route answers with, so an OTLP
// client is not handed a plain-text body where it expects JSON.
type errorFormat int

const (
	errText errorFormat = iota
	errJSON
)

// routeSpec is what one endpoint accepts. A route with no spec accepts
// anything, which is only correct for the read surface.
type routeSpec struct {
	methods []string     // allowed methods; empty means any
	types   []string     // allowed media types; empty means any
	format  errorFormat  // error envelope
	limit   func() int64 // body limit override; nil uses the server's
	// deadline applies MaxQueryDuration to the request context. Off for the
	// live tail, which is long-lived by design.
	deadline bool
	// write selects the ingest concurrency budget rather than the query one.
	write bool
	// nosem exempts the route from concurrency admission entirely. Only the
	// operational surface uses it: a scraper that gets 429 under load takes
	// away the telemetry that explains the load.
	nosem bool
	// form says this route reads its parameters out of r.Form, so guard
	// parses a multipart body before the middleware chain copies the request.
	//
	// Not every route: guard read and buffered the body of routes that never
	// look at one. Measured, a 40 MiB multipart POST, server-side TotalAlloc
	// delta -- /metrics 0 -> 128 MiB, / 0 -> 128 MiB, /alerts 0 -> 128 MiB,
	// plus a temp file written and deleted per request, on three routes that
	// discard the body. And on the routes that read the body themselves --
	// /_search, /_count and /select/vector -- the parse CONSUMED it: /_count
	// with a JSON document under multipart/form-data went from 200 to 400
	// ("simdlogs: EOF") on a node and to 503 on a router.
	//
	// A route whose handler parses a form while this is false leaks a temp
	// file per request, which is the defect this whole mechanism exists to
	// stop -- so it is not a matter of getting the list right by inspection:
	// TestNoRouteLeavesAMultipartTempFileBehind posts a spilling body to every
	// registered route and fails on any file left behind.
	form bool
	// stream selects the tail budget. A live tail is an idle connection, not
	// a running query -- charging it the query budget meant a handful of open
	// tails returned 429 for every other read, /metrics included. It still
	// gets a budget of its own, because "not a query" is not "free": each
	// tail holds a connection, a goroutine and a poll timer.
	stream bool
}

// admit bounds how many requests of one class run at once.
//
// MaxConcurrentQuery and MaxConcurrentWrite were declared, defaulted and
// validated, and read by nothing: any number of concurrent queries each
// spawned their own worker pool. A rejected request gets 429 rather than
// queueing without bound, so a client learns to back off instead of timing
// out.
func (s *Server) admit(sem chan struct{}, spec routeSpec, w http.ResponseWriter, r *http.Request) (release func(), ok bool) {
	if sem == nil {
		return func() {}, true
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	default:
		s.writeErr(w, r, spec, http.StatusTooManyRequests,
			"too many concurrent requests; retry after a moment")
		return nil, false
	}
}

// countErr tallies an error response for /metrics.
func (s *Server) countErr() { atomic.AddInt64(&s.nHTTPErrs, 1) }

// writeErr answers an error in the route's envelope, counting it.
func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, spec routeSpec, code int, msg string) {
	s.countErr()
	if spec.format == errJSON {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]any{"error": msg, "status": code})
		return
	}
	http.Error(w, msg, code)
}

// writeFlushErr answers a failed durable write with the retry metadata the
// failure actually carries, rather than the flat 503 every storage failure
// used to receive.
//
// Two facts leave here that a shipper cannot work out for itself:
//
//   - Retry-After. Every write failure this server can produce is either
//     transient or needs an operator, so the status is always 503 and the
//     interval is what separates "wait a second" from "someone has to fix the
//     disk". There was a never-retry class answering 500; see
//     ingest.WriteError.Retryable for why it is gone.
//   - duplicateOnRetry. There is no idempotency key on the ingest path yet,
//     so when part of a payload reached the store and part did not, resending
//     the payload stores the landed part twice. A client that is told can
//     choose; a client that is not told cannot.
//
// Retry-After is set before the body is written, because a header set after
// WriteHeader is a header that never leaves.
func (s *Server) writeFlushErr(w http.ResponseWriter, r *http.Request, spec routeSpec, err error) {
	var we *ingest.WriteError
	if !errors.As(err, &we) {
		// Unreachable today: every call site passes Flush or FlushMark's
		// result, and both return nil or a *WriteError. Kept because "the
		// error type is always X" is a property of five call sites rather
		// than of a signature, and the alternative to this branch is a nil
		// dereference the day that stops being true.
		s.writeErr(w, r, spec, http.StatusServiceUnavailable, err.Error())
		return
	}
	after := int(we.RetryAfter() / time.Second)
	if after > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(after))
	}
	code := we.HTTPStatus()
	if spec.format != errJSON {
		// Unreachable today: all five call sites pass ndjsonSpec() or
		// otlpSpec(), both errJSON. A text envelope has nowhere to put the
		// structured facts, so they go into the message -- losing them on a
		// text route would make the answer depend on which protocol the
		// client used, which is how a fact becomes untrustworthy. Written for
		// the route that adds one, not for a route that exists.
		s.writeErr(w, r, spec, code, fmt.Sprintf("%s (retryable=%t, duplicate-on-retry=%t)",
			we.Error(), we.Retryable(), we.DuplicatesOnRetry()))
		return
	}
	s.countErr()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error":             we.Error(),
		"status":            code,
		"retryable":         we.Retryable(),
		"retryAfterSeconds": after,
		"duplicateOnRetry":  we.DuplicatesOnRetry(),
		"groupsFailed":      we.FailedGroups,
		"groupsTotal":       we.TotalGroups,
		// What the two counts count. On the parallel path they are shard
		// writers, not groups, and a client parsing the body had no way to
		// know -- the unit survived only in the message string.
		"unit": we.Units(),
	})
}

// guard enforces method, media type and body size for one route, and is the
// only place a request body becomes readable.
//
// Every ingest handler used to call io.ReadAll on r.Body with no limit, so a
// single request could hold the whole body in memory and a slow one could
// hold it indefinitely. Method was never checked, so a GET to an ingest path
// was treated as an empty POST, and Content-Type was ignored, so a client
// sending the wrong protocol got a success with zero records.
func (s *Server) guard(spec routeSpec, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(spec.methods) > 0 && !allowedMethod(spec.methods, r.Method) {
			w.Header().Set("Allow", strings.Join(spec.methods, ", "))
			s.writeErr(w, r, spec, http.StatusMethodNotAllowed,
				fmt.Sprintf("method %s not allowed; allowed: %s", r.Method, strings.Join(spec.methods, ", ")))
			return
		}
		if len(spec.types) > 0 && r.Method != http.MethodGet && r.Method != http.MethodHead {
			if ct := r.Header.Get("Content-Type"); ct != "" {
				mt, _, err := mime.ParseMediaType(ct)
				if err != nil || !allowedType(spec.types, mt) {
					s.writeErr(w, r, spec, http.StatusUnsupportedMediaType,
						fmt.Sprintf("unsupported media type %q; supported: %s", ct, strings.Join(spec.types, ", ")))
					return
				}
			}
		}
		// A query deadline. -search.maxDuration was accepted, stored and read
		// by nothing: an operator setting it got no timeout and no warning.
		// The full executor-level cancellation is task 6.1; this bounds the
		// request now, which is what the flag says it does.
		if d := s.limits.MaxQueryDuration; d > 0 && spec.deadline {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			r = r.WithContext(ctx)
		}
		// Concurrency admission, by class.
		if !spec.nosem {
			sem := s.querySem
			switch {
			case spec.write:
				sem = s.writeSem
			case spec.stream:
				sem = s.tailSem
			}
			release, ok := s.admit(sem, spec, w, r)
			if !ok {
				return
			}
			defer release()
			// Per-tenant admission, after the class limit and for every READ.
			// A process-wide limit alone lets one tenant fill it: on a shared
			// server the tenant with the most aggressive dashboard takes every
			// slot and every other tenant sees 429 for work the server had
			// room to do.
			//
			// Live tails included. They were exempt on the `!spec.stream`
			// clause, justified by the argument for exempting WRITES -- "an
			// ingest request does not hold memory for the length of a scan" --
			// which is the exact opposite of true for a tail, the longest-lived
			// read the server has. One tenant could hold every tail slot
			// indefinitely and admission would not see it.
			//
			// WRITES ARE STILL EXEMPT, AND THAT SENTENCE IS NOT WHY.
			// It has now justified two exemptions, one of which had to be
			// undone, so it is replaced here by the measured number rather than
			// left standing for a third. `/_bulk` at four fields a document
			// peaks at about 9.3x the body in live heap and churns 26x, linear
			// across 8/16/32/60 MiB bodies -- so one request at MaxBodyBytes is
			// ~600 MiB live and a GZIPPED one, bounded by MaxDecompressed
			// instead, is ~4.8 GiB. writeSem counts requests, not bytes, so the
			// ceiling on this surface is that times MaxConcurrentWrite.
			// TestTheIngestMemoryCeilingIsPriced carries the table and fails on
			// any of the three defaults moving.
			//
			// Per-tenant admission is not the lever for it: it would bound ONE
			// tenant's share and leave the process-wide product alone. The
			// lever is a byte budget across in-flight ingests, which is a new
			// limit, a new metric and a change to this block, and is not this
			// change.
			//
			// After the class semaphore, not before: that is the cheaper check
			// and the one that protects the process.
			if s.admission != nil && !spec.write {
				// The key is CLASSED, so tails and ordinary reads have
				// independent per-tenant pools.
				//
				// Two contracts meet here and one key cannot serve both. Tails
				// must not consume query slots -- a live tail is open for
				// hours by design, and a tenant with four of them must still
				// be able to run a dashboard. And a tail must not be exempt
				// from admission either: it is the longest-lived read the
				// server has, so one tenant holding every tail slot
				// indefinitely is exactly what a per-tenant limit is for. It
				// WAS exempt, on the `!spec.stream` clause, justified by the
				// argument for exempting writes -- "an ingest request does not
				// hold memory for the length of a scan" -- which is the
				// opposite of true for a tail.
				//
				// A prefix on the key gives each class its own pool of
				// MaxQueriesPerTenant, which satisfies both.
				key := tenantKeyOf(r)
				if spec.stream {
					key = "tail\x00" + key
				}
				rel, err := s.admission.Acquire(r.Context(), key)
				if err != nil {
					s.writeErr(w, r, spec, query.HTTPStatus(err), err.Error())
					return
				}
				defer rel()
			}
		}

		limit := s.limits.MaxBodyBytes
		if spec.limit != nil {
			limit = spec.limit()
		}
		if limit != config.Unlimited && r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		// MULTIPART is parsed HERE, so its temp files can be removed when the
		// request ends.
		//
		// net/http removes them itself -- finishRequest calls
		// `w.req.MultipartForm.RemoveAll()` -- but it checks the request the
		// SERVER holds, and every middleware below this line replaces the
		// request with an `r.WithContext(...)` copy. ParseMultipartForm then
		// sets MultipartForm on a copy the server never sees, so nothing ever
		// removes anything:
		//
		//	40 MiB multipart to /select/logsql/query, node and router:
		//	  /tmp/multipart-* grows by one 41,943,040-byte file per request
		//	  and they persist. 32 files = 1.25 GiB left behind.
		//
		// Bounded per request by MaxBodyBytes and unbounded in total, on a
		// server whose whole job is to run for months.
		//
		// Parsing before the copies means every copy shares this pointer, so
		// the deferred RemoveAll reaches the form the handler used. A second
		// ParseMultipartForm downstream returns nil immediately, so the
		// handlers are unchanged; a parse that FAILS leaves MultipartForm nil
		// and the error surfaces where it did before.
		if spec.form && formKind(r) == formMultipart {
			normalizeFormContentType(r)
			_ = r.ParseMultipartForm(multipartMemory)
			if r.MultipartForm != nil {
				defer r.MultipartForm.RemoveAll()
			}
		}
		h(w, r)
	}
}

// multipartMemory is how much of a multipart body is held in memory before it
// spills to a temp file -- net/http's own FormValue default. The spill is what
// the deferred RemoveAll above cleans up; this bounds how often it happens.
//
// It cannot usefully be lowered for a test: a handler's own r.FormValue calls
// ParseMultipartForm with net/http's defaultMaxMemory, also 32 MiB, and a
// handler-side parse is exactly what the leak gate has to provoke. So the gate
// pays for a body past this size rather than shrinking it.
const multipartMemory = 32 << 20

func allowedMethod(allowed []string, m string) bool {
	for _, a := range allowed {
		if a == m {
			return true
		}
	}
	// HEAD is served by a GET handler.
	if m == http.MethodHead {
		for _, a := range allowed {
			if a == http.MethodGet {
				return true
			}
		}
	}
	return false
}

func allowedType(allowed []string, mt string) bool {
	for _, a := range allowed {
		if a == mt {
			return true
		}
	}
	return false
}

// readBody reads a request body under the configured limits, decompressing
// gzip when the request declares it.
//
// Two limits apply and both are needed. MaxBytesReader bounds what arrives on
// the wire; the decompressed bound is separate because a few hundred KB of
// gzip expands to gigabytes, and a wire-only limit accepts that happily. An
// unknown Content-Encoding is rejected rather than treated as identity, which
// would store compressed bytes as if they were log lines.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, *bodyError) {
	enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	switch enc {
	case "", "identity":
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, bodyErrFor(err)
		}
		return b, nil
	case "gzip":
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, &bodyError{code: http.StatusBadRequest, msg: "malformed gzip body: " + err.Error()}
		}
		defer zr.Close()
		max := s.limits.MaxDecompressed
		var b []byte
		if max == config.Unlimited {
			b, err = io.ReadAll(zr)
		} else {
			// Read one byte past the limit: reaching it means the body is
			// over, not exactly at, the bound.
			b, err = io.ReadAll(io.LimitReader(zr, max+1))
			if err == nil && int64(len(b)) > max {
				return nil, &bodyError{code: http.StatusRequestEntityTooLarge,
					msg: fmt.Sprintf("decompressed body exceeds %d bytes", max)}
			}
		}
		if err != nil {
			return nil, bodyErrFor(err)
		}
		return b, nil
	default:
		return nil, &bodyError{code: http.StatusUnsupportedMediaType,
			msg: fmt.Sprintf("unsupported Content-Encoding %q", enc)}
	}
}

// bodyError carries the status a body failure deserves, so the handler does
// not have to guess between 400 and 413.
type bodyError struct {
	code int
	msg  string
}

func (e *bodyError) Error() string { return e.msg }

func bodyErrFor(err error) *bodyError {
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		return &bodyError{code: http.StatusRequestEntityTooLarge,
			msg: fmt.Sprintf("request body exceeds %d bytes", mbe.Limit)}
	}
	return &bodyError{code: http.StatusBadRequest, msg: err.Error()}
}

// ndjsonSpec is the shape shared by every line-oriented ingest route. The
// media types are the ones the vendors' own agents send; an empty
// Content-Type is allowed because several send none.
func ndjsonSpec() routeSpec {
	return routeSpec{
		write:   true,
		methods: []string{http.MethodPost, http.MethodPut},
		types: []string{
			"application/x-ndjson", "application/json", "text/plain",
			"application/octet-stream", "application/logfmt",
		},
		format: errJSON,
	}
}

// journaldSpec is systemd-journal-upload, which sends the journal export
// format under its own media type. It is separate from ndjsonSpec because
// accepting application/vnd.fdo.journal everywhere would let a journal blob
// be posted to the NDJSON routes, where it parses as nothing.
func journaldSpec() routeSpec {
	sp := ndjsonSpec()
	sp.types = append(sp.types, "application/vnd.fdo.journal")
	return sp
}

// lokiSpec is the Loki push surface. It differs from ndjsonSpec by one media
// type -- application/x-protobuf -- and that one type is the whole default
// configuration of Promtail, Grafana Alloy and the Grafana Agent, all of which
// send a snappy-compressed protobuf PushRequest unless told otherwise. Without
// it the route rejected the default client at the media-type gate, before any
// decoder saw the body.
func lokiSpec() routeSpec {
	return routeSpec{
		write:   true,
		methods: []string{http.MethodPost},
		types: []string{
			// Both protobuf spellings: application/x-protobuf is what Loki's
			// clients send, application/protobuf is the IANA registration and
			// what VictoriaLogs accepts.
			"application/x-protobuf", "application/protobuf",
			"application/json", "application/x-ndjson",
			"text/plain", "application/octet-stream",
		},
		format: errJSON,
	}
}

// otlpSpec is OTLP/HTTP: protobuf or JSON, and errors in JSON.
func otlpSpec() routeSpec {
	return routeSpec{
		write:   true,
		methods: []string{http.MethodPost},
		types:   []string{"application/x-protobuf", "application/json"},
		format:  errJSON,
	}
}

// readSpec is the query surface: GET or POST, any type, plain-text errors as
// the existing clients expect.
func readSpec() routeSpec {
	return routeSpec{
		methods:  []string{http.MethodGet, http.MethodPost},
		format:   errText,
		deadline: true,
		form:     true, // query, start, end, limit and the rest come from r.Form
	}
}

// rawBodySpec is a read route whose handler reads r.Body ITSELF: the two
// Elasticsearch routes and /select/vector, whose body is a JSON document
// rather than a form. Parsing a form for them consumes the body they are about
// to decode.
//
// THREE routes, not two. The first version of this comment said "the two
// Elasticsearch routes" and /select/vector went on answering 400 "EOF" for a
// multipart body it had answered 200 for before the pre-parse existed. It is
// also the one that reads parameters as well as a document, so its `start` and
// `end` moved to the URL -- see timeWindowURL.
func rawBodySpec() routeSpec {
	sp := readSpec()
	sp.form = false
	return sp
}

// staticSpec is a read route that reads neither a form nor a body -- the UI
// and the alerts page. They answer from server state, so buffering an uploaded
// body for them is pure cost.
func staticSpec() routeSpec {
	sp := readSpec()
	sp.form = false
	return sp
}

// tailSpec is the live tail: a read route with no deadline, since it is meant
// to stay open.
func tailSpec() routeSpec {
	sp := readSpec()
	sp.deadline = false
	sp.stream = true // its own budget: not a query, not unbounded either
	return sp
}

// datadogValidateSpec is the Datadog Agent's API-key probe. It is a GET --
// the agent calls GET /api/v1/validate at startup -- so wrapping it in the
// ingest spec answered the agent 405 and it reported the key as invalid.
func datadogValidateSpec() routeSpec {
	sp := ndjsonSpec()
	sp.methods = []string{http.MethodGet, http.MethodPost, http.MethodHead}
	sp.types = nil // a GET carries no body to type
	return sp
}

// specForPath is the single source of truth for which route spec a write path
// carries. The mux picks its spec per route; routeWrites hardcoded
// ndjsonSpec() for everything, so in router mode a collector's default
// protobuf OTLP got 415, journald got 415, and the Datadog key probe got 405
// -- the exact failures the per-route specs exist to prevent, reintroduced
// one layer up. One function both call is what stops them disagreeing again.
func specForPath(p string) routeSpec {
	switch p {
	case "/v1/logs", "/insert/opentelemetry/v1/logs":
		return otlpSpec()
	case "/loki/api/v1/push", "/insert/loki/api/v1/push":
		// Router mode uses THIS table, not the mux's per-route spec. Wiring
		// lokiSpec only into the mux left the whole headline fix inert behind
		// a router: a default snappy-protobuf push still got 415 there.
		return lokiSpec()
	case "/insert/journald":
		return journaldSpec()
	case "/insert/datadog/api/v1/validate":
		return datadogValidateSpec()
	case "/insert/ready":
		sp := ndjsonSpec()
		sp.methods = []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodHead}
		sp.types = nil
		return sp
	}
	return ndjsonSpec()
}

// opsSpec is the operational surface -- metrics, health details, flags. It is
// exempt from the query budget: a scraper that gets 429 under load takes away
// the telemetry that explains the load.
func opsSpec() routeSpec {
	sp := readSpec()
	sp.nosem = true
	sp.form = false // /metrics answers from server state; it reads no parameters
	return sp
}

// adminSpec is the administrative surface.
func adminSpec() routeSpec {
	// form: FALSE. This said `true` with the note "serveReplicaState reads
	// `digest` through r.FormValue" -- which was already untrue when it was
	// written, because the same change moved that read to the URL. No admin
	// handler reads a form: not serveReplicaState, serveReplicaGroup,
	// repairCluster, clusterBackup, backup, flagsHandler or
	// acknowledgeDegraded.
	//
	// It cost what the same commit had just removed from /metrics, / and
	// /alerts. A 40 MiB multipart POST, server-side TotalAlloc delta, six
	// admin routes:
	//
	//	              form:true   form:false
	//	/flags        128.2 MiB      0.2 MiB
	//	/admin/backup 128.2          0.2
	//	… and four more, identically
	//
	// /admin/acknowledge-degraded is `nosem`, deliberately exempt from
	// admission control, so that was unbounded buffering on a route chosen to
	// stay answerable under load.
	//
	// What makes this safe to assert rather than to hope: if any admin handler
	// did parse a form, it would parse it on a request copy and leak a temp
	// file per request, and TestNoRouteLeavesAMultipartTempFileBehind posts a
	// spilling body to every one of them.
	return routeSpec{methods: []string{http.MethodGet, http.MethodPost}, format: errText}
}

// replicaGroupSpec is adminSpec with the ANTI-ENTROPY body limit.
//
// The adopt POST carries one whole group, and a group is bounded by the
// compactor, not by any client's request: compaction merges up to 128Ki rows
// into one, so a group routinely exceeds Limits.MaxBodyBytes (64 MiB by
// default) without a single client write coming close. Behind the general
// guard, MaxBytesReader cut those bodies long before the handler's own 1 GiB
// ceiling could apply -- so a shard holding a large group could never converge,
// permanently, and no bound anybody configured said so.
//
// The handler's own ceiling still applies; this only stops a limit meant for
// client uploads from deciding what one node may hand another.
//
// The ceiling reads the server's replicaGroupLimit field, which defaults to
// maxRepairBytes; a test shrinks the field to exercise the boundary without a
// gigabyte fixture.
func (s *Server) replicaGroupSpec() routeSpec {
	sp := adminSpec()
	sp.limit = func() int64 { return s.replicaGroupLimit }
	sp.form = false // the adopt POST's body IS the group, streamed into the store
	return sp
}

// writeIngestErr answers a failed ingest that stored part of its batch.
//
// The plain writeErr says only what went wrong. For a parse that failed part
// way that is not enough: a client told "400, the upload is truncated" and
// nothing else re-sends the whole upload, and every record that did land is
// then stored twice. A log store has no primary key, so the duplicate is
// invisible afterwards -- it looks exactly like the line happening twice.
//
// So the error body carries the counts as well as the message. `accepted` is
// how many records are durable; a client re-sends from that offset rather than
// from the start. It is omitted when nothing was stored, so the ordinary
// failure keeps the shape it has today.
func (s *Server) writeIngestErr(
	w http.ResponseWriter, r *http.Request, spec routeSpec,
	code int, msg string, res ingest.Result,
) {
	if res.Accepted == 0 {
		s.writeErr(w, r, spec, code, msg)
		return
	}
	s.countErr()
	body := map[string]any{
		"error":  msg,
		"status": code,
		// The records before the failure are stored. Re-sending them duplicates
		// them, and a log store cannot tell a duplicate from a repeat.
		"accepted": res.Accepted,
		"rejected": res.Rejected,
	}
	// RENDERED SHORT, AND SAID SO. ingest.MaxRejectedAt is the ATTRIBUTION
	// bound and it is sized for /_bulk's action cap (1<<20), which is where a
	// missing position costs a document. This body is not that surface: it is
	// a human-and-shipper-readable error, and a JSON array of a million
	// ordinals is about 8 MB of it. The recorded list keeps its full length
	// for the caller that maps positions onto items; what goes on this wire is
	// bounded, and rejectedTruncated already means exactly "the list you have
	// is shorter than the count".
	at := res.RejectedAt
	trunc := res.RejectedTruncated
	if len(at) > maxRenderedRejectedAt {
		at, trunc = at[:maxRenderedRejectedAt], true
	}
	if len(at) > 0 {
		body["rejectedAt"] = at
	}
	if trunc {
		body["rejectedTruncated"] = true
	}
	if spec.format == errJSON {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(body)
		return
	}
	// A non-JSON route still has to carry the count somewhere a client can read
	// it, and its body is whatever that route's format is.
	//
	// UNREACHABLE TODAY, and its two neighbours in this file say so and this
	// one did not. `writeIngestErr`'s only production caller is
	// protocols.go's insert handler, which passes `ndjsonSpec()` --
	// `format: errJSON` -- so the early return above always fires. The header
	// that does reach a client is set by that same handler on its 204 routes
	// (`/insert/loki/api/v1/push`, `/insert/syslog`), which is why the
	// constant is used from there as well: one dispatch must not have two
	// spellings, or renaming the constant moves nothing on the wire. Kept
	// because a non-JSON insert route is a routeSpec away and the count would
	// otherwise vanish silently on it.
	w.Header().Set(hdrAccepted, strconv.Itoa(res.Accepted))
	http.Error(w, msg, code)
}

// maxRenderedRejectedAt bounds how many positions one error body carries. It
// is the bound ingest.MaxRejectedAt used to be, so this response's worst case
// is unchanged by the attribution bound being raised to the _bulk action cap.
const maxRenderedRejectedAt = 1 << 16

// hdrAccepted carries the durable record count on a route whose error body is
// not JSON, and on the 204 routes, whose success code forbids a body at all.
// TestAReject204ReportsTheCountsInHeaders is what makes the name load-bearing:
// without it this constant could be renamed to anything and no test moved.
const hdrAccepted = "X-Simdlogs-Accepted"
