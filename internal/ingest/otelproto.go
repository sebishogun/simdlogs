package ingest

import (
	"encoding/binary"
	"math"
	"strconv"
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
func IngestOTLPLogsProto(w *Writer, data []byte, fallback func() int64, opts *Options) (ingested, skipped int) {
	mapped := !opts.Empty()
	fields := map[string]string{}
	eachField(data, func(num int, wire int, payload []byte) {
		if num != 1 || wire != 2 { // resource_logs
			return
		}
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
				var ts, observed int64
				eachField(rec, func(fn, fw int, fp []byte) {
					switch {
					case fn == 1 && fw == 1: // time_unix_nano
						ts = int64(binary.LittleEndian.Uint64(fp))
					case fn == 11 && fw == 1: // observed_time_unix_nano
						observed = int64(binary.LittleEndian.Uint64(fp))
					case fn == 3 && fw == 2: // severity_text
						if len(fp) > 0 {
							fields["severity"] = string(fp)
						}
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
					}
				})
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
				ingested++
			})
		})
	})
	return ingested, skipped
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
	eachField(msg, func(num, wire int, p []byte) {
		switch {
		case num == 1 && wire == 2:
			key, ok = string(p), true
		case num == 2 && wire == 2:
			if v, vok := decodeAnyValue(p); vok {
				val = v
			}
		}
	})
	return key, val, ok
}

// decodeAnyValue reads an AnyValue into the same struct the JSON path fills,
// so one str() renders both encodings identically.
func decodeAnyValue(msg []byte) (otlpValue, bool) {
	var out otlpValue
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
