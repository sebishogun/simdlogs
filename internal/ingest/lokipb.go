package ingest

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
)

// Loki's protobuf push encoding.
//
// This is the encoding Promtail, Grafana Alloy and the Grafana Agent send BY
// DEFAULT: `Content-Type: application/x-protobuf` with a snappy-compressed
// logproto.PushRequest body. A store that reads only the JSON push API drops
// the default configuration's data, and the previous behaviour was worse than
// dropping it -- a snappy body is not JSON, so it failed the JSON decode and
// the agent got a 400 for a correctly-formed request.
//
// Decoded by hand, the same way the OTLP protobuf path is, so the only new
// dependency is snappy itself -- not the Loki server module, which pulls in a
// distributed database.
//
// Field numbers, from Loki's logproto (push.proto):
//
//	PushRequest:  repeated StreamAdapter streams = 1
//	StreamAdapter: string labels = 1, repeated EntryAdapter entries = 2, string hash = 3
//	EntryAdapter:  Timestamp timestamp = 1, string line = 2,
//	               repeated LabelPairAdapter structuredMetadata = 3
//	LabelPairAdapter: string name = 1, string value = 2
//	Timestamp (google.protobuf.Timestamp): int64 seconds = 1, int32 nanos = 2

var (
	errNotSnappy      = errors.New("body is not snappy-compressed: not a Loki protobuf push")
	errBadLokiProto   = errors.New("body is not decodable protobuf")
	errNoLokiStreams  = errors.New("no streams field: not a Loki protobuf push payload")
	errLokiLabelParse = errors.New("stream labels are not a Prometheus label set")
)

// maxLokiDecompressed bounds what one push may expand to. snappy's ratio on
// log text is routinely 4-6x and can be far higher on repetitive input, so a
// body that passed the wire-size limit can still be gigabytes decompressed --
// the same amplification the gzip path already guards.
const maxLokiDecompressed = 256 << 20

// IngestLokiProto ingests a snappy-compressed logproto.PushRequest, producing
// records IDENTICAL to the JSON path's: the stream's labels become fields, the
// entry's line becomes _msg, structured metadata joins the fields, and the
// entry's timestamp sets the row time.
func IngestLokiProto(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()

	n, err := snappy.DecodedLen(data)
	if err != nil {
		return res, encodingErr(errNotSnappy)
	}
	// The OPERATOR's limit when they set one, so lowering
	// -max-decompressed-bytes actually lowers this path too. The constant is
	// only the fallback for a writer with no configured bound.
	limit := w.MaxDecompressedBytes()
	if limit <= 0 {
		limit = maxLokiDecompressed
	}
	if n > limit {
		return res, encodingErr(fmt.Errorf(
			"snappy body expands to %d bytes, over the %d-byte limit", n, limit))
	}
	raw, err := snappy.Decode(nil, data)
	if err != nil {
		return res, encodingErr(errNotSnappy)
	}
	// Same reason as the OTLP path: eachField stops silently at the first
	// malformed tag, so a garbage body would decode to zero records and be
	// answered as an empty, successful push.
	if !validProtobuf(raw) {
		return res, encodingErr(errBadLokiProto)
	}

	fields := map[string]string{}
	sawStream := false
	eachField(raw, func(num, wire int, stream []byte) {
		if num != 1 || wire != 2 { // streams
			return
		}
		sawStream = true
		var labels string
		var entries [][]byte
		eachField(stream, func(n, wt int, p []byte) {
			switch {
			case n == 1 && wt == 2: // labels
				labels = string(p)
			case n == 2 && wt == 2: // entries
				entries = append(entries, p)
			}
		})
		lbl, lerr := parseLokiLabels(labels)
		if lerr != nil {
			res.Rejected += len(entries)
			// Neither position is known: the labels belong to the STREAM, not
			// to one entry, and the body is protobuf.
			res.Warn(UnknownPos, "stream labels %q: %v", first80(labels), lerr)
			return
		}
		for _, ent := range entries {
			var line string
			var seconds, nanos int64
			var meta [][2]string
			eachField(ent, func(n, wt int, p []byte) {
				switch {
				case n == 1 && wt == 2: // timestamp
					eachField(p, func(tn, tw int, tp []byte) {
						switch {
						case tn == 1 && tw == 0:
							seconds = int64(leU64(tp))
						case tn == 2 && tw == 0:
							nanos = int64(int32(leU64(tp)))
						}
					})
				case n == 2 && wt == 2: // line
					line = string(p)
				case n == 3 && wt == 2: // structuredMetadata
					var k, v string
					eachField(p, func(mn, mw int, mp []byte) {
						switch {
						case mn == 1 && mw == 2:
							k = string(mp)
						case mn == 2 && mw == 2:
							v = string(mp)
						}
					})
					// Both halves must be non-empty. proto3 omits an empty
					// string field entirely, so LabelPairAdapter{Name:"app"}
					// is the on-wire shape for an empty value -- and applying
					// it ERASED the stream label of the same name, which is
					// silent data loss dressed as an override. VictoriaLogs
					// guards both (app/vlinsert/loki/pb.go).
					if k != "" && v != "" {
						meta = append(meta, [2]string{k, v})
					}
				}
			})

			for k := range fields {
				delete(fields, k)
			}
			for k, v := range lbl {
				fields[k] = v
			}
			// Structured metadata after the labels: Loki 3.x carries
			// high-cardinality attributes here precisely because they are NOT
			// labels, and an entry's own metadata is more specific than the
			// stream's.
			for _, kv := range meta {
				fields[kv[0]] = kv[1]
			}
			fields["_msg"] = line

			// seconds is a protobuf varint the client chose, so `seconds*1e9`
			// overflows for anything past the year 2262 -- and a wrapped
			// product is a row filed in the distant past, accepted at 200 and
			// invisible to every query. Refused and counted, the same call
			// ErrTimeOutOfRange makes for every other protocol.
			ts, tsOK := lokiNanos(seconds, nanos)
			if !tsOK {
				ord := res.Accepted + res.Rejected
				res.Reject(ord)
				res.WarnAt(ord, "%v: %d s + %d ns since the epoch",
					ErrTimeOutOfRange, seconds, nanos)
				continue
			}
			if seconds == 0 && nanos == 0 {
				ts = fallback()
			}
			if mapped {
				opts.apply(fields)
			}
			addOrReject(w, ts, fields, opts, &res, res.Accepted+res.Rejected)
		}
	})
	if !sawStream && len(raw) > 0 {
		return res, envelopeErr(errNoLokiStreams)
	}
	return res, nil
}

// leU64 reads the 8-byte little-endian buffer eachField hands back for a
// varint field.
func leU64(p []byte) uint64 {
	var v uint64
	for i := 0; i < 8 && i < len(p); i++ {
		v |= uint64(p[i]) << (8 * i)
	}
	return v
}

func first80(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}

// parseLokiLabels parses the label set the protobuf encoding carries as ONE
// string -- `{app="api", env="prod"}` -- where the JSON encoding sends a map.
// Both must produce the same fields.
//
// Written to match what Loki's own client EMITS and what VictoriaLogs ACCEPTS,
// not to a tidy grammar:
//
//   - The name is everything before the first `=`, taken raw. Loki's client
//     writes the name unquoted and unescaped (clients/pkg/util/batch.go), so
//     `{app-name="x"}` and `{service.name="x"}` are exactly what goes on the
//     wire -- and a character allowlist of [A-Za-z0-9_:] rejected both, which
//     discarded every entry in the stream while still answering 204. Dots and
//     dashes are the dominant convention in OpenTelemetry and Kubernetes label
//     names, so that was most of the traffic.
//   - The value goes through strconv.Unquote, because Loki writes it with
//     strconv.Quote. The five escapes a hand-rolled switch knew are not the
//     set Quote produces: a control byte becomes \x01, a zero-width space
//     becomes \u200b, invalid UTF-8 becomes \xff, and every one of those was
//     stored with the backslash intact.
//   - The closing brace must be the LAST byte. `{app="a"}}` and
//     `{app="a"}garbage` were both accepted.
//   - An absent or empty label set is an ERROR, not an empty map. A row with
//     no identifying field at all is not something to store silently.
func parseLokiLabels(s string) (map[string]string, error) {
	s = string(trimSpace([]byte(s)))
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, errLokiLabelParse
	}
	body := s[1 : len(s)-1]
	out := map[string]string{}
	i := 0
	skipSpace := func() {
		for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
			i++
		}
	}
	for {
		skipSpace()
		if i >= len(body) {
			return out, nil
		}
		eq := strings.IndexByte(body[i:], '=')
		if eq < 0 {
			return nil, errLokiLabelParse
		}
		name := string(trimSpace([]byte(body[i : i+eq])))
		if name == "" {
			return nil, errLokiLabelParse
		}
		i += eq + 1
		skipSpace()
		if i >= len(body) || body[i] != '"' {
			return nil, errLokiLabelParse
		}
		// QuotedPrefix finds the end of the quoted string honouring escapes,
		// so a `"` or a `,` inside the value cannot end it early.
		q, err := strconv.QuotedPrefix(body[i:])
		if err != nil {
			return nil, errLokiLabelParse
		}
		val, err := strconv.Unquote(q)
		if err != nil {
			return nil, errLokiLabelParse
		}
		out[name] = val
		i += len(q)
		skipSpace()
		if i >= len(body) {
			return out, nil
		}
		if body[i] != ',' {
			return nil, errLokiLabelParse
		}
		i++
	}
}

// lokiNanos combines a protobuf Timestamp's seconds and nanos into epoch
// nanoseconds, reporting false when the result is outside the storable range.
//
// Both halves come off the wire as arbitrary integers, so neither the multiply
// nor the addition is safe on its own: `seconds*1e9` overflows past the year
// 2262, and a nanos field the sender did not normalise can carry the sum over
// the edge from a seconds value that was inside it.
func lokiNanos(seconds, nanos int64) (int64, bool) {
	if seconds > math.MaxInt64/int64(time.Second) || seconds < math.MinInt64/int64(time.Second) {
		return 0, false
	}
	ns := seconds * int64(time.Second)
	if nanos > 0 && ns > math.MaxInt64-nanos {
		return 0, false
	}
	if nanos < 0 && ns < math.MinInt64-nanos {
		return 0, false
	}
	return ns + nanos, true
}
