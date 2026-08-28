package ingest

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Vector fields: which record fields are embeddings, and what an embedding is
// allowed to be.
//
// # Why the field has to be configured rather than sniffed
//
// A JSON array of numbers is not self-evidently an embedding. `[1,2,3]` might
// be a retry-delay schedule, an HTTP status sequence, or a 3-dimensional
// vector, and a store that guessed would decide the column type from whichever
// record arrived first -- so the same payload would land as a vector on an
// empty store and as text on a populated one. Configuration says which fields
// are vectors, once, for the deployment.
//
// # Why the rules are refusals and not coercions
//
// Every rule here rejects rather than repairs, and each has a specific reason:
//
//   - A dimension that disagrees with the field's established one cannot be
//     compared to anything already stored. Cosine similarity over vectors of
//     different lengths is not a smaller answer, it is undefined, and the
//     search path's `dim != len(q)` skip would silently drop the whole group.
//   - NaN poisons a similarity: `NaN > x` is false for every x, so a NaN row
//     sorts wherever the comparison happens to put it and every score computed
//     against it is NaN. One bad record makes a whole result set meaningless
//     without failing anything.
//   - +Inf/-Inf make the norm infinite and the cosine NaN, by the same route.
//   - A dimension ceiling exists because the vector is stored uncompressed and
//     scanned in full: a record claiming 100 million dimensions is an
//     allocation, not a query.
//
// A record that breaks any of them is rejected as a record -- counted in
// Result.Rejected with a reason -- rather than stored with the field dropped.
// Dropping it silently would store a log line that is invisible to the search
// it was ingested for.

// ErrVector is a record whose vector field cannot be stored. Every failure
// below wraps it, so a caller distinguishes "this record's embedding is
// unusable" from any other parse failure.
var ErrVector = errors.New("ingest: invalid vector field")

// maxVectorDim is the largest embedding this accepts, whatever the caller
// configures.
//
// 16384 covers every published text embedding by a wide margin (OpenAI's
// largest is 3072, Cohere's 1024, E5-large 1024) and is small enough that a
// record claiming the maximum is 64 KiB rather than a memory event. A hard
// ceiling on top of the configurable one, because the configurable one is a
// number an operator can get wrong once.
const maxVectorDim = 16384

// VectorFields is the set of record fields carrying embeddings, with the
// dimension each is fixed at.
//
// The dimension is part of the configuration rather than learned from the
// first record. Learned, the first record of a restarted process would define
// the column afresh -- so a deployment that started accepting 768-dimension
// vectors after a restart would silently split its corpus in two, with each
// half invisible to the other's queries.
type VectorFields map[string]int

// Dim reports the configured dimension of a field, and whether it is a vector
// field at all.
func (v VectorFields) Dim(field string) (int, bool) {
	if v == nil {
		return 0, false
	}
	d, ok := v[field]
	return d, ok
}

// ParseVectorFields reads the `-vector-fields` form: `name:dim` pairs,
// comma-separated. `embedding:768,title_vec:384`.
func ParseVectorFields(spec string) (VectorFields, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	out := VectorFields{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, dimStr, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("%w: %q is not name:dim", ErrVector, part)
		}
		name = strings.TrimSpace(name)
		if name == "" || controlField(name) || name == "_time" {
			return nil, fmt.Errorf("%w: %q is not a usable field name", ErrVector, name)
		}
		d, err := strconv.Atoi(strings.TrimSpace(dimStr))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%w: %q has no positive dimension", ErrVector, part)
		}
		if d > maxVectorDim {
			return nil, fmt.Errorf("%w: %q exceeds the %d-dimension ceiling",
				ErrVector, part, maxVectorDim)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("%w: %q is configured twice", ErrVector, name)
		}
		out[name] = d
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// ParseVector decodes one embedding from its JSON array text into dst.
//
// Appends into a caller-supplied dst rather than returning a fresh slice: this
// runs once per record on the ingest path, and a per-record allocation of a
// 768-float slice is 3 KiB of garbage per log line. The caller passes the
// flat column buffer and the vector lands directly in it.
//
// The text is the raw JSON array as it appeared -- `[0.1,-2,3e-4]`. Parsed
// here rather than by encoding/json because the JSON path already has the
// bytes and unmarshalling into []float64 allocates a slice per record and
// converts twice.
func ParseVector(dst []float32, field, text string, dim int) ([]float32, error) {
	s := strings.TrimSpace(text)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return dst, fmt.Errorf("%w: %s is not a JSON array", ErrVector, field)
	}
	s = s[1 : len(s)-1]
	start := len(dst)
	n := 0
	for _, tok := range splitTopLevel(s) {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		f, err := strconv.ParseFloat(tok, 32)
		if err != nil {
			return dst[:start], fmt.Errorf("%w: %s holds %q, which is not a number",
				ErrVector, field, tok)
		}
		// NaN and Inf are refused, not clamped. A NaN component makes every
		// score computed against the vector NaN, and NaN compares false
		// against everything -- so one bad record does not fail, it quietly
		// makes a whole result set meaningless.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return dst[:start], fmt.Errorf("%w: %s holds %s", ErrVector, field, tok)
		}
		if n == dim {
			// Counted rather than compared at the end so a record claiming a
			// million dimensions is refused after `dim+1` of them rather than
			// after allocating all of them.
			return dst[:start], fmt.Errorf("%w: %s has more than the configured %d dimensions",
				ErrVector, field, dim)
		}
		dst = append(dst, float32(f))
		n++
	}
	if n != dim {
		return dst[:start], fmt.Errorf("%w: %s has %d dimensions, the field is configured for %d",
			ErrVector, field, n, dim)
	}
	return dst, nil
}

// splitTopLevel splits a JSON array body on commas. The elements of an
// embedding are numbers, so there is no nesting to track -- and anything that
// would need tracking is not a number and fails ParseFloat, which is the
// refusal this wants anyway.
func splitTopLevel(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
