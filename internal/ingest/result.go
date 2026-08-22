package ingest

import (
	"fmt"
	"net/http"
)

// Result is what one ingest call did.
//
// Accepted and Rejected are per record. The distinction that matters is
// between a record this server refused and a *payload* it could not read at
// all: the first is a Rejected count, the second is an error. Every parser
// used to collapse both into "0 records, no error", so a Loki push whose JSON
// did not parse was answered with 200 and a zero count -- indistinguishable
// from an empty but valid push.
type Result struct {
	Accepted int
	Rejected int
	Warnings []Warning

	// RejectedAt holds the ZERO-BASED ORDINAL of each rejected record within
	// the batch, in order.
	//
	// A count alone cannot be mapped back to a position, and a caller that
	// needs per-record statuses -- the Elasticsearch _bulk response, whose
	// items clients match to their requests BY POSITION -- then has to guess.
	// Guessing produced the worst possible answer: marking the first N
	// records failed reported the STORED document as a failure and the
	// DROPPED one as created, so a client re-sent what landed and treated
	// what vanished as delivered.
	//
	// Bounded by MaxRejectedAt: a body of a million bad lines must not become
	// a million int32s. Past the bound the ordinals stop being recorded and
	// RejectedTruncated says so, which the caller must treat as "the
	// positions are not known" rather than as "there were no more".
	RejectedAt []int32

	// RejectedTruncated reports that RejectedAt stopped short of Rejected.
	RejectedTruncated bool
}

// MaxRejectedAt bounds the recorded positions.
//
// IT IS NOT A MEMORY NICETY; IT IS THE ATTRIBUTION BOUND, and at 64Ki it was
// SMALLER THAN THE BATCH THE ONE CALLER THAT NEEDS POSITIONS CAN SEND.
// `/_bulk` accepts up to esBulkMaxActions = 1<<20 actions, so a 5,254,000-byte
// body -- inside the 64 MiB request limit, an ordinary size for a shipper --
// carrying 70,000 `index` actions of which 66,000 hold an unstorable `_time`
// crossed 64Ki and every one of the 70,000 items came back unattributed.
// Measured on this tree before this change:
//
//	HTTP 200  errors=true
//	items                 70000
//	byStatus              map[429:70000]
//	byType                map[es_rejected_execution_exception:70000]
//	rows on disk           4000
//
// A per-item 429 is RETRYABLE to every shipper, and 66,000 of those documents
// can never be stored however many times they are sent, so the pipeline never
// drains -- and each pass re-sends the 4,000 that DID land into an
// append-only store. See markBulkRejects for the rest of that measurement.
//
// So the bound is the action cap: at 1<<20 no body `/_bulk` accepts can
// exceed it, and the residual branch stops being reachable from client input
// at all. TestTheAttributionBoundCoversTheActionCap holds the two constants
// together. The cost is 4 MB of int32 in the worst case, against a request
// body limit of 64 MiB and a `_bulk` response that is itself tens of megabytes
// at that action count; the amplification is under 1x either way, which is
// what the old bound was defending against. What one route RENDERS is bounded
// separately -- see maxRenderedRejectedAt in internal/api.
const MaxRejectedAt = 1 << 20

// Reject counts a rejected record and remembers WHERE it was.
func (r *Result) Reject(ordinal int) {
	r.Rejected++
	if len(r.RejectedAt) >= MaxRejectedAt {
		r.RejectedTruncated = true
		return
	}
	r.RejectedAt = append(r.RejectedAt, int32(ordinal))
}

// Add merges another result, for a parser that runs several passes.
func (r *Result) Add(o Result) {
	r.Accepted += o.Accepted
	r.Rejected += o.Rejected
	r.Warnings = append(r.Warnings, o.Warnings...)
	// Ordinals are NOT merged: they are relative to their own pass, so
	// concatenating two passes' positions would produce numbers that index
	// nothing. A caller that needs them must not use Add.
	if len(o.RejectedAt) > 0 || o.RejectedTruncated {
		r.RejectedTruncated = true
	}
}

// Warn records a per-record problem at a BYTE OFFSET in the body. Pass
// UnknownPos when the parser does not know one -- a parser that decoded the
// body into a struct no longer knows where in the bytes a record was.
func (r *Result) Warn(offset int64, format string, args ...any) {
	if r.warnFull() {
		return
	}
	r.warn(Warning{Offset: offset, Ordinal: UnknownPos, Msg: fmt.Sprintf(format, args...)})
}

// WarnAt records a per-record problem at a RECORD ORDINAL: the record's
// zero-based position in the batch, which is what a client matches its own
// records against.
//
// IT EXISTS BECAUSE EIGHT CALL SITES PUT AN ORDINAL IN Warning.Offset, whose
// doc says byte offset. `res.Warn(int64(ordinal), ...)` in jsonline, logfmt,
// loki, lokipb, otel and options compiled, read plausibly, and recorded record
// 3 of a batch as byte 3 of the body. datadog.go is the one that got it right,
// and it got it right by passing 0 and writing down WHY -- "Warning.Offset is
// a BYTE offset, and this parser decoded JSON into a struct and no longer
// knows where in the body the entry was".
//
// Nothing read the field, which is how six wrong values survived: the API
// layer renders `w.Msg` and drops the position. A write-only field with two
// incompatible meanings in it is worse than no field, because the first reader
// gets a plausible number.
func (r *Result) WarnAt(ordinal int, format string, args ...any) {
	if r.warnFull() {
		return
	}
	r.warn(Warning{Offset: UnknownPos, Ordinal: int64(ordinal), Msg: fmt.Sprintf(format, args...)})
}

// warnFull is the bound, asked BEFORE the message is built.
//
// It used to be asked inside warn(), which is after fmt.Sprintf has run at the
// call site: every rejected record past the 32nd formatted a string, boxed its
// arguments and handed the result to a function whose first act was to drop
// it.
//
// THE COST DEPENDS ON THE SHAPE OF THE CALL, and quoting one number for all of
// them is how "48 B" came to stand beside the words "boxed its arguments" --
// 48.1 B is the shape that boxes NOTHING. Exact runtime.MemStats deltas over
// 200,000 discarded calls, three interleaved A/B pairs, identical across them:
//
//	discarded call shape                     before        after
//	WarnAt(n, "%v", tsErr)                 1 / 144.2 B   0 / 0 B
//	WarnAt(n, "filler %d", 12345)          1 /  16.0 B   0 / 0 B
//	WarnAt(n, "entry carries a timestamp   1 /  48.1 B   0 / 0 B
//	         and no storable field")
//
// The reject arms this bound exists for are the first shape: jsonline.go
// hands `("%v", tsErr)` and `("%v", vecErr)` an error whose Error() runs
// inside Sprintf. 144.2 B, not 48. A `/_bulk` at the action cap rejects up to
// 1,048,575 records and pays it for 1,048,543 of them. This is the tenets'
// zero-allocations-per-record rule on the path that ingests the most records
// in this tree; the numbers and their method are in
// TestABoundedWarningCostsNothingToDiscard.
//
// The counts are still exact: Reject() carries them, and warn() keeps the
// same check so no caller can outrun the bound another way.
func (r *Result) warnFull() bool { return len(r.Warnings) >= maxWarnings }

func (r *Result) warn(w Warning) {
	if r.warnFull() {
		return // bounded: a body of a million bad lines must not be a million strings
	}
	r.Warnings = append(r.Warnings, w)
}

// maxWarnings bounds what one request can accumulate.
const maxWarnings = 32

// UnknownPos is the Offset or Ordinal of a warning whose position the parser
// does not know. Zero is not usable for that: byte 0 and record 0 are both
// real positions, and "0" is exactly what six call sites meant by "no idea".
const UnknownPos = int64(-1)

// Warning is one per-record note and WHERE it came from. The two positions are
// separate fields because they are different numbers: a byte offset into the
// body, and a record's ordinal within the batch. Either is UnknownPos when the
// parser cannot say -- a protobuf or JSON decode knows the record and not the
// byte, a byte scanner may know the byte and not the record.
type Warning struct {
	Offset  int64 // byte offset into the request body, or UnknownPos
	Ordinal int64 // zero-based record position in the batch, or UnknownPos
	Msg     string
}

// ErrorKind classifies an ingest failure, which is what decides the status
// code. The kinds are distinct because they need different operator
// responses: an envelope error is the client's, a storage error is the
// server's and is retryable.
type ErrorKind int

const (
	// ErrEnvelope: the top-level payload is not what the protocol describes.
	ErrEnvelope ErrorKind = iota
	// ErrEncoding: the bytes could not be decoded (bad JSON, bad protobuf).
	ErrEncoding
	// ErrUnsupported: a structure this server deliberately does not accept.
	ErrUnsupported
	// ErrStorage: the records parsed but could not be persisted.
	ErrStorage
)

func (k ErrorKind) String() string {
	switch k {
	case ErrEnvelope:
		return "envelope"
	case ErrEncoding:
		return "encoding"
	case ErrUnsupported:
		return "unsupported"
	case ErrStorage:
		return "storage"
	}
	return "unknown"
}

// Error is a fatal ingest failure: nothing in the payload was accepted, or
// what was accepted could not be stored.
type Error struct {
	Kind   ErrorKind
	Offset int64
	Err    error
}

func (e *Error) Error() string {
	if e.Offset > 0 {
		return fmt.Sprintf("ingest: %s error at offset %d: %v", e.Kind, e.Offset, e.Err)
	}
	return fmt.Sprintf("ingest: %s error: %v", e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// HTTPStatus is the status this failure deserves. A storage failure is the
// server's problem and retryable; everything else is the request's and is
// not, so a shipper that retries a 400 forever is told plainly not to.
func (e *Error) HTTPStatus() int {
	switch e.Kind {
	case ErrStorage:
		return http.StatusServiceUnavailable
	case ErrUnsupported:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusBadRequest
	}
}

// envelopeErr is the common case: the payload is not this protocol.
func envelopeErr(err error) *Error { return &Error{Kind: ErrEnvelope, Err: err} }

// encodingErr is undecodable bytes.
func encodingErr(err error) *Error { return &Error{Kind: ErrEncoding, Err: err} }

// StatusFor returns the HTTP status for an ingest error, defaulting to 400
// for anything that is not a typed *Error.
func StatusFor(err error) int {
	if e, ok := err.(*Error); ok {
		return e.HTTPStatus()
	}
	return http.StatusBadRequest
}
