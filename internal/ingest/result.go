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
}

// Add merges another result, for a parser that runs several passes.
func (r *Result) Add(o Result) {
	r.Accepted += o.Accepted
	r.Rejected += o.Rejected
	r.Warnings = append(r.Warnings, o.Warnings...)
}

// Warn records a per-record problem that did not stop the batch.
func (r *Result) Warn(offset int64, format string, args ...any) {
	if len(r.Warnings) >= maxWarnings {
		return // bounded: a body of a million bad lines must not be a million strings
	}
	r.Warnings = append(r.Warnings, Warning{Offset: offset, Msg: fmt.Sprintf(format, args...)})
}

// maxWarnings bounds what one request can accumulate.
const maxWarnings = 32

// Warning is one per-record note, with the byte offset it came from where the
// parser knows it.
type Warning struct {
	Offset int64
	Msg    string
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
