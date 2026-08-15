package ingest

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"unicode/utf8"
)

// The OTLP/HTTP protobuf encoding of ExportLogsServiceRequest, decoded by hand:
// a collector's otlphttp exporter sends protobuf BY DEFAULT, so a store that
// reads only the JSON encoding drops the default configuration's data while
// answering 200. Hand-rolled because this repository takes no dependencies; the
// wire format is four primitives and the message shape is stable OTLP v1.
//
// Field numbers, from opentelemetry-proto (logs/v1/logs.proto, common/v1):
//
//	ExportLogsServiceRequest: resource_logs = 1
//	ResourceLogs:  resource = 1, scope_logs = 2
//	Resource:      attributes = 1
//	ScopeLogs:     log_records = 2
//	LogRecord:     time_unix_nano = 1 (fixed64), severity_text = 3,
//	               body = 5, attributes = 6, observed_time_unix_nano = 11 (fixed64)
//	KeyValue:      key = 1, value = 2
//	AnyValue:      string = 1, bool = 2, int = 3, double = 4, bytes = 7

// IngestOTLPLogsProto ingests the protobuf encoding, producing records
// IDENTICAL to the JSON path's: resource attributes plus record attributes,
// severity_text -> severity, body -> _msg, time (or observed time) -> time.
func IngestOTLPLogsProto(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	fields := map[string]string{}
	// Protobuf is OTLP's default encoding, and this parser accepted anything:
	// a body that decoded to no records returned success, so an exporter
	// sending garbage was told its data was delivered.
	if !validProtobuf(data) {
		return res, encodingErr(errBadProtobuf)
	}
	// ordinal is the record's position across the whole payload, flat rather
	// than per resource or per scope: OTLP nests records two levels deep, and a
	// caller matching a rejection back onto what it sent counts records in
	// order. It used to be recorded with no position at all.
	ordinal := 0
	sawResourceLogs := false
	// sawLogShape is the discriminator: a LogRecord carries its timestamp as
	// a fixed64, where Metric.name and Span.trace_id at the same field number
	// are length-delimited.
	sawLogShape := false
	eachField(data, func(num int, wire int, payload []byte) {
		if num != 1 || wire != 2 { // resource_logs
			return
		}
		sawResourceLogs = true
		var resAttrs []otlpKV
		// First pass: the resource's attributes; second: the records. The
		// resource message may follow its scope_logs on the wire, so both
		// passes are needed for the order protobuf allows.
		eachField(payload, func(n, w int, p []byte) {
			if n == 1 && w == 2 { // resource
				eachField(p, func(n2, w2 int, p2 []byte) {
					if n2 == 1 && w2 == 2 { // attributes
						if k, v, ok := decodeKV(p2); ok {
							resAttrs = append(resAttrs, otlpKV{Key: k, Value: v})
						}
					}
				})
			}
		})
		eachField(payload, func(n, wt int, p []byte) {
			if n != 2 || wt != 2 { // scope_logs
				return
			}
			// InstrumentationScope (scope = 1): name = 1, version = 2,
			// attributes = 3. Read before the records, because the scope may
			// precede or follow them on the wire, and dropped entirely before
			// this -- so every record looked like it came from the
			// application rather than from the library that emitted it.
			var scopeAttrs []otlpKV
			var scopeName, scopeVersion string
			eachField(p, func(n2, w2 int, sc []byte) {
				if n2 != 1 || w2 != 2 { // scope
					return
				}
				eachField(sc, func(n3, w3 int, sp []byte) {
					switch {
					case n3 == 1 && w3 == 2:
						scopeName = string(sp)
					case n3 == 2 && w3 == 2:
						scopeVersion = string(sp)
					case n3 == 3 && w3 == 2:
						if k, v, ok := decodeKV(sp); ok {
							scopeAttrs = append(scopeAttrs, otlpKV{Key: k, Value: v})
						}
					}
				})
			})
			eachField(p, func(n2, w2 int, rec []byte) {
				if n2 != 2 || w2 != 2 { // log_records
					return
				}
				for k := range fields {
					delete(fields, k)
				}
				for _, a := range resAttrs {
					fields[a.Key] = a.Value.str()
				}
				for _, a := range scopeAttrs {
					fields[a.Key] = a.Value.str()
				}
				if scopeName != "" {
					fields["scope_name"] = scopeName
				}
				if scopeVersion != "" {
					fields["scope_version"] = scopeVersion
				}
				var ts, observed int64
				var sevNum int
				var sevText, traceID, spanID, eventName string
				var flags, dropped uint32
				isLog := false
				// wrongShape: field 1 present but length-delimited, which is
				// Metric.name or Span.trace_id rather than time_unix_nano.
				wrongShape := false
				eachField(rec, func(fn, fw int, fp []byte) {
					switch {
					case fn == 1 && fw == 1: // time_unix_nano
						ts = int64(binary.LittleEndian.Uint64(fp))
						isLog = true
					case fn == 11 && fw == 1: // observed_time_unix_nano
						observed = int64(binary.LittleEndian.Uint64(fp))
						isLog = true
					case fn == 2 && fw == 0: // severity_number (enum, varint)
						// Range-checked BEFORE the int conversion. A varint of
						// 2^63 or more converts to a NEGATIVE int, which then
						// passed a `sevNum < len(names)` test and indexed the
						// name table out of range: a remote panic from a
						// 31-byte body, and a 500 an OTLP exporter retries
						// forever.
						if v := binary.LittleEndian.Uint64(fp); v < uint64(len(severityNumberName)) {
							sevNum = int(v)
						}
						isLog = true
					case fn == 3 && fw == 2: // severity_text
						sevText = string(fp)
					case fn == 7 && fw == 0: // dropped_attributes_count
						dropped = uint32(binary.LittleEndian.Uint64(fp))
					case fn == 8 && fw == 5: // flags (fixed32)
						flags = binary.LittleEndian.Uint32(fp)
						isLog = true
					// Fields 9, 10 and 12 collide with Metric.histogram,
					// Metric.exponential_histogram and Metric.metadata, all at
					// wire type 2 (docs/wrong.md records the collision). Length
					// is the discriminator OTLP gives: a trace id is EXACTLY 16
					// bytes and a span id exactly 8, which a histogram
					// submessage is not. Without this a metrics payload posted
					// to /v1/logs invented a trace_id out of its bucket bytes.
					case fn == 9 && fw == 2 && len(fp) == 16: // trace_id: raw here, hex in JSON
						traceID = hex.EncodeToString(fp)
					case fn == 10 && fw == 2 && len(fp) == 8: // span_id
						spanID = hex.EncodeToString(fp)
					case fn == 12 && fw == 2 && utf8.Valid(fp): // event_name
						eventName = string(fp)
					case fn == 5 && fw == 2: // body
						if v, ok := decodeAnyValue(fp); ok {
							if msg := v.str(); msg != "" {
								fields["_msg"] = msg
							}
						}
					case fn == 6 && fw == 2: // attributes
						if k, v, ok := decodeKV(fp); ok {
							fields[k] = v.str()
						}
					// Four field numbers where a length-delimited value
					// cannot be a LogRecord and can be a Metric:
					//   1  Metric.name       vs LogRecord.time_unix_nano (fixed64)
					//   2  Metric.description vs LogRecord.severity_number (enum, varint)
					//   7  Metric.sum        vs LogRecord.dropped_attributes_count (varint)
					//   11 Metric.summary    vs LogRecord.observed_time_unix_nano (fixed64)
					// Span.trace_id is field 1 wire 2 and every span has one.
					// Rejecting these does not touch the legal case below --
					// a LogRecord that omits every timestamp -- because none
					// of these four is a LogRecord field of wire type 2.
					//
					// Field 3 (Metric.unit vs LogRecord.severity_text) and
					// field 5 (Metric.gauge vs LogRecord.body) are both
					// length-delimited on both sides and stay ambiguous; a
					// unit-only or gauge-only metric is still stored as a log
					// row. Recorded in docs/wrong.md.
					case fw == 2 && (fn == 1 || fn == 2 || fn == 7 || fn == 11):
						wrongShape = true
					}
				})
				// Decide per record, before storing it. A LogRecord carries
				// its timestamp as a fixed64; Metric.name and Span.trace_id
				// sit at the same field number as length-delimited values. A
				// check made only after the walk still stored the bogus rows.
				//
				// A record with NO field 1 and NO field 11 at all is legal:
				// every LogRecord field is optional, and the spec has the
				// receiver stamp observed_time_unix_nano when the producer
				// omits it -- which is what the fallback below does. Only a
				// record whose field 1 is length-delimited is another signal
				// wearing a log's field numbers.
				if wrongShape {
					res.Reject(ordinal)
					ordinal++
					res.Warn(0, "record's field 1 is not a timestamp; a metrics or traces payload, not logs")
					return
				}
				if isLog {
					sawLogShape = true
				}
				otlpRecordFields(fields, sevNum, sevText, traceID, spanID, eventName, flags, dropped)
				if ts == 0 {
					ts = observed
				}
				if ts == 0 {
					ts = fallback()
				}
				if mapped {
					opts.apply(fields)
				}
				addWithStream(w, ts, fields, opts)
				res.Accepted++
				ordinal++
			})
		})
	})
	// Discriminate logs from the other two signals. ResourceMetrics and
	// ResourceSpans use the same field numbers as ResourceLogs -- resource=1,
	// scope=2, and Metric/Span/LogRecord all sit at field 2 of the scope --
	// so a metrics or traces export posted to /v1/logs walked as logs and
	// stored a bogus row per record with a 200. Counting records could not
	// see it; the wire types can.
	//
	// LogRecord.time_unix_nano is field 1 wire 1 (fixed64), while Metric.name
	// and Span.trace_id are both field 1 wire 2. One record carrying field 1
	// or field 11 as fixed64 separates all three.
	// Fire on rejected-only, not on "no record had the log shape". Those are
	// different: a ResourceLogs carrying genuinely zero records is a legal
	// empty batch and must stay a success, and it has nothing to reject.
	if len(data) > 0 && res.Accepted == 0 {
		if !sawResourceLogs {
			return res, envelopeErr(errNoResourceLogsProto)
		}
		if res.Rejected > 0 {
			return res, envelopeErr(errNotLogRecords)
		}
		// A resource_logs that yielded no records is NOT an error. OTLP
		// requires success for a request that carries no data, exporters
		// treat 4xx as permanent, and an empty export is indistinguishable
		// on the wire from an empty metrics one anyway -- both are field 1,
		// wire 2, with nothing inside that names a signal. Rejecting it
		// bought a discrimination that does not exist and cost a retry loop
		// that does.
		_ = sawLogShape
	}
	return res, nil
}

var (
	errBadProtobuf         = errors.New("body is not decodable protobuf")
	errNoResourceLogsProto = errors.New("no resource_logs field: not an OTLP protobuf logs payload")
	errNotLogRecords       = errors.New("records carry no log timestamp: this is a metrics or traces payload, not logs")
	errNoLogRecordsProto   = errors.New("resource_logs carries no log records")
)

// validProtobuf reports whether data is a well-formed sequence of protobuf
// fields end to end. eachField stops silently at the first malformed tag,
// which is why a garbage body parsed to zero records and no error.
func validProtobuf(data []byte) bool {
	for len(data) > 0 {
		tag, n := binary.Uvarint(data)
		if n <= 0 {
			return false
		}
		data = data[n:]
		switch tag & 7 {
		case 0: // varint
			_, n := binary.Uvarint(data)
			if n <= 0 {
				return false
			}
			data = data[n:]
		case 1: // 64-bit
			if len(data) < 8 {
				return false
			}
			data = data[8:]
		case 2: // length-delimited
			l, n := binary.Uvarint(data)
			if n <= 0 {
				return false
			}
			data = data[n:]
			if uint64(len(data)) < l {
				return false
			}
			data = data[l:]
		case 5: // 32-bit
			if len(data) < 4 {
				return false
			}
			data = data[4:]
		default:
			return false
		}
	}
	return true
}

// eachField walks one protobuf message, calling fn per field with the wire type
// and payload (the value bytes for varint/fixed encodings, the message bytes
// for length-delimited). Unknown fields are skipped, malformed input stops the
// walk -- lenient the way ingest must be.
func eachField(msg []byte, fn func(num, wire int, payload []byte)) {
	for len(msg) > 0 {
		tag, n := binary.Uvarint(msg)
		if n <= 0 {
			return
		}
		msg = msg[n:]
		num, wire := int(tag>>3), int(tag&7)
		switch wire {
		case 0: // varint
			v, vn := binary.Uvarint(msg)
			if vn <= 0 {
				return
			}
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], v)
			fn(num, 0, buf[:])
			msg = msg[vn:]
		case 1: // fixed64
			if len(msg) < 8 {
				return
			}
			fn(num, 1, msg[:8])
			msg = msg[8:]
		case 2: // length-delimited
			l, ln := binary.Uvarint(msg)
			if ln <= 0 || uint64(len(msg)-ln) < l {
				return
			}
			fn(num, 2, msg[ln:ln+int(l)])
			msg = msg[ln+int(l):]
		case 5: // fixed32
			if len(msg) < 4 {
				return
			}
			fn(num, 5, msg[:4])
			msg = msg[4:]
		default:
			return // groups and reserved wire types: not in OTLP
		}
	}
}

// decodeKV reads a KeyValue message.
func decodeKV(msg []byte) (key string, val otlpValue, ok bool) {
	return decodeKVDepth(msg, 0)
}

// decodeKVDepth carries the nesting depth through a kvlist, so a KeyValue
// inside a KeyValueList inside a KeyValue cannot escape the recursion bound.
func decodeKVDepth(msg []byte, depth int) (key string, val otlpValue, ok bool) {
	if depth > maxAnyValueDepth {
		return "", otlpValue{}, false
	}
	eachField(msg, func(num, wire int, p []byte) {
		switch {
		case num == 1 && wire == 2:
			key, ok = string(p), true
		case num == 2 && wire == 2:
			if v, vok := decodeAnyValueDepth(p, depth); vok {
				val = v
			}
		}
	})
	return key, val, ok
}

// decodeAnyValue reads an AnyValue into the same struct the JSON path fills,
// so one str() renders both encodings identically.
//
// All seven kinds. Stopping at four meant bytes, array and kvlist attributes
// vanished from a protobuf export -- silently, with the record still stored
// and still answered 200, so the attribute simply was not there when someone
// went looking. bytes is base64-encoded rather than passed through raw,
// because base64 is what the JSON encoding of the same attribute carries and
// the two must produce the same row.
func decodeAnyValue(msg []byte) (otlpValue, bool) {
	return decodeAnyValueDepth(msg, 0)
}

// maxAnyValueDepth bounds the recursion. array and kvlist nest, and the
// nesting arrives from an untrusted exporter: without a limit, a crafted body
// a few hundred bytes long recurses until the goroutine stack is exhausted,
// which takes the process down rather than the request. OTLP's own semantic
// conventions do not nest attributes more than a couple of levels, so 16 is
// past anything real and far short of what breaks.
const maxAnyValueDepth = 16

func decodeAnyValueDepth(msg []byte, depth int) (otlpValue, bool) {
	var out otlpValue
	if depth > maxAnyValueDepth {
		return out, false
	}
	found := false
	eachField(msg, func(num, wire int, p []byte) {
		switch {
		case num == 1 && wire == 2: // string_value
			s := string(p)
			out.StringValue, found = &s, true
		case num == 2 && wire == 0: // bool_value
			b := binary.LittleEndian.Uint64(p) != 0
			out.BoolValue, found = &b, true
		case num == 3 && wire == 0: // int_value (varint; OTLP JSON writes it as a string)
			s := strconv.FormatInt(int64(binary.LittleEndian.Uint64(p)), 10)
			out.IntValue, found = &s, true
		case num == 4 && wire == 1: // double_value
			f := math.Float64frombits(binary.LittleEndian.Uint64(p))
			out.DoubleValue, found = &f, true
		case num == 5 && wire == 2: // array_value: ArrayValue{ repeated AnyValue values = 1 }
			arr := &otlpArray{}
			eachField(p, func(n2, w2 int, e []byte) {
				if n2 == 1 && w2 == 2 {
					if v, ok := decodeAnyValueDepth(e, depth+1); ok {
						arr.Values = append(arr.Values, v)
					}
				}
			})
			out.ArrayValue, found = arr, true
		case num == 6 && wire == 2: // kvlist_value: KeyValueList{ repeated KeyValue values = 1 }
			kvl := &otlpKvlist{}
			eachField(p, func(n2, w2 int, e []byte) {
				if n2 == 1 && w2 == 2 {
					if k, v, ok := decodeKVDepth(e, depth+1); ok {
						kvl.Values = append(kvl.Values, otlpKV{Key: k, Value: v})
					}
				}
			})
			out.KvlistValue, found = kvl, true
		case num == 7 && wire == 2: // bytes_value
			s := base64.StdEncoding.EncodeToString(p)
			out.BytesValue, found = &s, true
		}
	})
	return out, found
}

// ---- a tiny protobuf writer, only for building the test fixture ----

func pvarint(dst []byte, num int, v uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(num)<<3|0)
	return binary.AppendUvarint(dst, v)
}

func pfixed64(dst []byte, num int, v uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(num)<<3|1)
	return binary.LittleEndian.AppendUint64(dst, v)
}

func pbytes(dst []byte, num int, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(num)<<3|2)
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func pstring(dst []byte, num int, s string) []byte { return pbytes(dst, num, []byte(s)) }

func anyString(s string) []byte { return pstring(nil, 1, s) }
func anyInt(v int64) []byte     { return pvarint(nil, 3, uint64(v)) }
func anyDouble(f float64) []byte {
	return pfixed64(nil, 4, math.Float64bits(f))
}
func anyBool(b bool) []byte {
	v := uint64(0)
	if b {
		v = 1
	}
	return pvarint(nil, 2, v)
}

func kv(key string, val []byte) []byte {
	var m []byte
	m = pstring(m, 1, key)
	m = pbytes(m, 2, val)
	return m
}

// buildTestExport builds the protobuf twin of the JSON fixture in the tests:
// one resource with two attributes, two log records with every AnyValue kind.
func buildTestExport() []byte {
	var resource []byte
	resource = pbytes(resource, 1, kv("service.name", anyString("api")))
	resource = pbytes(resource, 1, kv("host", anyString("h1")))

	var rec1 []byte
	rec1 = pfixed64(rec1, 1, 1_700_000_000_000_000_000)
	rec1 = pstring(rec1, 3, "ERROR")
	rec1 = pbytes(rec1, 5, anyString("boom happened"))
	rec1 = pbytes(rec1, 6, kv("code", anyInt(500)))
	rec1 = pbytes(rec1, 6, kv("ratio", anyDouble(0.5)))
	rec1 = pbytes(rec1, 6, kv("retry", anyBool(true)))

	var rec2 []byte // no time field: observed time carries it
	rec2 = pfixed64(rec2, 11, 1_700_000_001_000_000_000)
	rec2 = pbytes(rec2, 5, anyString("second"))

	var scope []byte
	scope = pbytes(scope, 2, rec1)
	scope = pbytes(scope, 2, rec2)

	var rl []byte
	rl = pbytes(rl, 1, resource)
	rl = pbytes(rl, 2, scope)

	return pbytes(nil, 1, rl)
}

// BuildTestOTLPProtoExport is the protobuf export the tests post over HTTP; it
// lives here so the api tests need no protobuf writer of their own.
func BuildTestOTLPProtoExport() []byte { return buildTestExport() }
