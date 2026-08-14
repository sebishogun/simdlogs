package api

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/sebishogun/simdlogs/internal/config"
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
		limit := s.limits.MaxBodyBytes
		if spec.limit != nil {
			limit = spec.limit()
		}
		if limit != config.Unlimited && r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		h(w, r)
	}
}

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

// otlpSpec is OTLP/HTTP: protobuf or JSON, and errors in JSON.
func otlpSpec() routeSpec {
	return routeSpec{
		methods: []string{http.MethodPost},
		types:   []string{"application/x-protobuf", "application/json"},
		format:  errJSON,
	}
}

// readSpec is the query surface: GET or POST, any type, plain-text errors as
// the existing clients expect.
func readSpec() routeSpec {
	return routeSpec{methods: []string{http.MethodGet, http.MethodPost}, format: errText}
}

// adminSpec is the administrative surface.
func adminSpec() routeSpec {
	return routeSpec{methods: []string{http.MethodGet, http.MethodPost}, format: errText}
}
