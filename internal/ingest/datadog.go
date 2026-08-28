package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

var errEmptyBody = errors.New("empty body")

// IngestDatadog ingests the Datadog logs intake body: a JSON array of log
// objects (or a single object). "message" becomes _msg, "ddtags" is split into
// comma-separated key:value fields, "timestamp"/"date" set the time (ms since
// epoch as a number, or an RFC3339/ns string), and every other attribute
// becomes a field. https://docs.datadoghq.com/api/latest/logs/ (/api/v2/logs).
func IngestDatadog(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestDatadogOpts(w, data, fallback, nil)
}

// IngestDatadogOpts is IngestDatadog with the request's field mappings applied.
func IngestDatadogOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	trim := trimSpace(data)
	if len(trim) == 0 {
		return res, envelopeErr(errEmptyBody)
	}
	var entries []map[string]json.RawMessage
	if trim[0] == '[' {
		if err := json.Unmarshal(trim, &entries); err != nil {
			// Undecodable is a failed request. It answered 200 with a zero
			// count, which is what an empty valid batch also looks like.
			return res, encodingErr(err)
		}
	} else {
		var one map[string]json.RawMessage
		if err := json.Unmarshal(trim, &one); err != nil {
			return res, encodingErr(err)
		}
		entries = []map[string]json.RawMessage{one}
	}
	fields := map[string]string{}
	for ordinal, e := range entries {
		for k := range fields {
			delete(fields, k)
		}
		var ts int64
		haveTS := false
		var tsErr error
		for k, raw := range e {
			switch k {
			case "message":
				fields["_msg"] = rawToString(raw)
			case "ddtags":
				// Datadog tags are `key:value` by convention but a BARE tag
				// with no colon is equally legal, and those were being
				// dropped on the floor: the `i > 0` test skipped them and
				// nothing recorded it, so `env,prod-canary` stored nothing at
				// all. Keyed tags become fields; the rest are kept verbatim in
				// ddtags so no tag the sender wrote is lost.
				var bare []string
				for _, kv := range strings.Split(rawToString(raw), ",") {
					kv = strings.TrimSpace(kv)
					if kv == "" {
						continue
					}
					if i := strings.IndexByte(kv, ':'); i > 0 {
						fields[kv[:i]] = kv[i+1:]
						continue
					}
					bare = append(bare, kv)
				}
				// Stored under a name the tag namespace cannot reach. Using
				// "ddtags" collided with a tag literally named ddtags: the
				// keyed value was written first and then overwritten by this
				// join, so `ddtags="ddtags:x,bare"` lost x -- the exact
				// opposite of the "no tag the sender wrote is lost" this was
				// added for.
				if len(bare) > 0 {
					fields["_ddtags"] = strings.Join(bare, ",")
				}
			case "timestamp", "date":
				t, ok, terr := ddTime(raw)
				if terr != nil {
					tsErr = terr
					continue
				}
				if ok {
					ts, haveTS = t, true
				}
			default:
				fields[k] = rawToString(raw)
			}
		}
		// An entry that produced no fields at all carries nothing to store --
		// not even a message. The old warning said "no message field", which is
		// wrong for an entry that HAS a message and nothing else (it would have
		// one field and pass), and wrong again for one carrying only a
		// timestamp (no fields, but a message is not what it is missing).
		if tsErr != nil {
			// A timestamp that parses and cannot be stored refuses the record
			// and is counted -- see ErrTimeOutOfRange.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "%v", tsErr)
			continue
		}
		if len(fields) == 0 {
			// The ordinal is the entry's position in the batch, which is what
			// a client matches its own records against. It used to be recorded
			// with no position at all, so there was nothing to match.
			//
			// The BYTE offset is unknown: this parser decoded JSON into a
			// struct and no longer knows where in the body the entry was. The
			// ordinal is known, so the warning carries that instead -- which
			// is what Result.WarnAt exists for, and what the six sites that
			// put an ordinal into Offset were reaching for.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "entry carries no storable attribute")
			continue
		}
		if !haveTS {
			ts = fallback()
		}
		if mapped {
			opts.apply(fields)
		}
		addOrReject(w, ts, fields, opts, &res, ordinal)
	}
	return res, nil
}

// ddTime reads a Datadog timestamp: a JSON number is milliseconds since epoch
// (Datadog's default), a string is parsed as ns/RFC3339 via parseTime.
//
// The millisecond scale is CHECKED rather than wrapped. `int64(f) * 1_000_000`
// overflows for any f past 9.2e12 ms (the year 2262), and the wrapped product
// is a row filed in the distant past -- the same silent misplacement
// ErrTimeOutOfRange exists to stop, arriving through the one protocol that
// carries its timestamp as a bare number.
func ddTime(raw json.RawMessage) (int64, bool, error) {
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		ms := int64(f)
		if f != f || f >= 9.3e18 || f <= -9.3e18 || ms > math.MaxInt64/1_000_000 || ms < math.MinInt64/1_000_000 {
			return 0, false, fmt.Errorf("%w: %v ms since the epoch", ErrTimeOutOfRange, f)
		}
		return ms * 1_000_000, true, nil // ms -> ns
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return parseTime(s)
	}
	return 0, false, nil
}

// rawToString renders a JSON value as a plain string: a JSON string is
// unquoted, a number or bool keeps its literal text, and an object or array is
// COMPACTED.
//
// Compacted, not passed through: the source bytes carry whatever whitespace
// and line breaks the sender happened to use, so two agents sending the same
// logical attribute stored two different values and neither matched a query
// written against the other. Compacting also matches what the OTLP path does
// with a composite attribute, so the same nested object arrives the same way
// whichever protocol carried it.
func rawToString(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	if len(raw) > 0 && (raw[0] == '{' || raw[0] == '[') {
		var buf bytes.Buffer
		if err := json.Compact(&buf, raw); err == nil {
			return buf.String()
		}
	}
	return string(raw)
}
