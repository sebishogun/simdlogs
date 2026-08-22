package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// otlpValue is an OTLP AnyValue: exactly one of the typed fields is set. OTLP's
// JSON encoding writes int64 as a string, so IntValue is a *string.
//
// All SEVEN kinds are represented, not the four that used to be. The protobuf
// header comment claimed bytes = 7 was decoded and decodeAnyValue had no case
// for it, so a bytes attribute -- which is what an exporter sends for a raw
// payload, a compressed body, or a binary correlation id -- was silently
// dropped from both encodings. array and kvlist are how OTLP carries any
// structured attribute at all, and those were dropped too.
type otlpValue struct {
	StringValue *string     `json:"stringValue"`
	IntValue    *string     `json:"intValue"`
	DoubleValue *float64    `json:"doubleValue"`
	BoolValue   *bool       `json:"boolValue"`
	BytesValue  *string     `json:"bytesValue"`  // base64, per OTLP's JSON mapping
	ArrayValue  *otlpArray  `json:"arrayValue"`  // {"values":[AnyValue...]}
	KvlistValue *otlpKvlist `json:"kvlistValue"` // {"values":[KeyValue...]}
}

type otlpArray struct {
	Values []otlpValue `json:"values"`
}

type otlpKvlist struct {
	Values []otlpKV `json:"values"`
}

// str renders a value as the single string a column holds.
//
// The rendering is the contract between the two encodings: the JSON and
// protobuf paths must produce byte-identical rows for the same logical
// payload, which is what TestOTLPJSONAndProtoNormalizeIdentically pins. So
// bytes is the base64 text OTLP's own JSON mapping uses (NOT the raw octets,
// which are not valid UTF-8 and would differ between the encodings), and the
// two composite kinds are compact JSON -- the form OTLP's semantic conventions
// specify for a complex attribute flattened into a single value.
func (v otlpValue) str() string { return v.strLimit(maxCompositeBytes) }

// strLimit renders with a byte budget that is SPENT as it descends.
//
// The budget is threaded rather than checked per level, and that distinction is
// the whole fix: a per-level check fires only after a child has already been
// appended, so one child could still be arbitrarily large and the total still
// multiplied out. Every level subtracts what its siblings have already spent,
// so the total is bounded once, at the top.
func (v otlpValue) strLimit(budget int) string {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return *v.IntValue
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'g', -1, 64)
	case v.BoolValue != nil:
		return strconv.FormatBool(*v.BoolValue)
	case v.BytesValue != nil:
		return *v.BytesValue
	case v.ArrayValue != nil:
		return v.ArrayValue.json(budget)
	case v.KvlistValue != nil:
		return v.KvlistValue.json(budget)
	}
	return ""
}

// maxCompositeBytes bounds the rendered size of one composite attribute.
//
// The renderer quotes each element's rendered form, so a nested composite is
// escaped once per level: length roughly DOUBLES per level of nesting, and at
// the 16-level depth bound that is 2^16. A 1,663-byte body was measured
// producing 20 MB of rendered attributes -- 12,126x -- and the shape gzips
// almost perfectly, so the 64 MiB body limit and the 512 MiB decompressed
// limit are both a long way from bounding it. maxAnyValueDepth bounds the
// STACK; this bounds the OUTPUT, and they are different exhaustions.
//
// 64 KiB is far past any real attribute and far short of what hurts.
const maxCompositeBytes = 64 << 10

// json renders an array as a compact JSON array. Hand-built rather than
// encoding/json so the element rendering goes through str() -- otherwise an
// int inside an array would serialize as a number here and as a string at the
// top level, and the two encodings' rows would diverge by nesting depth.
//
// Truncation is MARKED, not silent: a caller reading `["a",...truncated]` can
// see that the value is not the whole one.
func (a *otlpArray) json(budget int) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, e := range a.Values {
		if b.Len() >= budget {
			b.WriteString(`,"...truncated"`)
			break
		}
		if i > 0 {
			b.WriteByte(',')
		}
		// The child gets what is LEFT, so a single huge child cannot blow the
		// total the way a per-level check allowed.
		// Halved: quoteJSON escapes what the child returns, and a value that
		// is all quotes doubles under escaping. Budgeting the child the raw
		// remainder would let the QUOTED form land at twice the ceiling.
		b.WriteString(quoteJSON(e.strLimit((budget - b.Len()) / 2)))
	}
	b.WriteByte(']')
	return b.String()
}

// json renders a kvlist as a compact JSON object, keys in wire order. Wire
// order, not sorted: OTLP defines KeyValueList as ordered, both encodings
// carry that order, and re-sorting here would be this package inventing a
// canonical form the exporter did not send.
func (k *otlpKvlist) json(budget int) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range k.Values {
		if b.Len() >= budget {
			b.WriteString(`,"...":"truncated"`)
			break
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteJSON(e.Key))
		b.WriteByte(':')
		b.WriteString(quoteJSON(e.Value.strLimit((budget - b.Len()) / 2)))
	}
	b.WriteByte('}')
	return b.String()
}

// quoteJSON is encoding/json's string quoting without the reflection: the
// composite renderings are built one element at a time and this is the only
// escaping they need.
func quoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil { // a Go string always marshals
		return `""`
	}
	return string(b)
}

type otlpKV struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

// otlpLogs is the OTLP/HTTP logs export payload (ExportLogsServiceRequest) in
// its JSON encoding: resource -> scope -> log records.
//
// severityNumber is json.Number because OTLP's JSON mapping permits an enum
// as EITHER its integer or its name, and a collector configured either way is
// a configuration this store has no business rejecting.
type otlpLogs struct {
	ResourceLogs []struct {
		Resource struct {
			Attributes             []otlpKV `json:"attributes"`
			DroppedAttributesCount uint32   `json:"droppedAttributesCount"`
		} `json:"resource"`
		ScopeLogs []struct {
			Scope struct {
				Name       string   `json:"name"`
				Version    string   `json:"version"`
				Attributes []otlpKV `json:"attributes"`
			} `json:"scope"`
			LogRecords []otlpLogRecord `json:"logRecords"`
		} `json:"scopeLogs"`
	} `json:"resourceLogs"`
}

type otlpLogRecord struct {
	TimeUnixNano           string       `json:"timeUnixNano"`
	ObservedTimeUnixNano   string       `json:"observedTimeUnixNano"`
	SeverityNumber         otlpSeverity `json:"severityNumber"`
	SeverityText           string       `json:"severityText"`
	Body                   otlpValue    `json:"body"`
	Attributes             []otlpKV     `json:"attributes"`
	DroppedAttributesCount uint32       `json:"droppedAttributesCount"`
	Flags                  uint32       `json:"flags"`
	TraceID                string       `json:"traceId"` // hex in JSON, raw bytes in protobuf
	SpanID                 string       `json:"spanId"`
	EventName              string       `json:"eventName"`
}

// severityNumberName maps the OTLP SeverityNumber enum to its name, so a
// payload that sent the integer and one that sent the name store the same
// value. The names are OTLP's own (logs.proto SeverityNumber).
var severityNumberName = [...]string{
	0: "", 1: "TRACE", 2: "TRACE2", 3: "TRACE3", 4: "TRACE4",
	5: "DEBUG", 6: "DEBUG2", 7: "DEBUG3", 8: "DEBUG4",
	9: "INFO", 10: "INFO2", 11: "INFO3", 12: "INFO4",
	13: "WARN", 14: "WARN2", 15: "WARN3", 16: "WARN4",
	17: "ERROR", 18: "ERROR2", 19: "ERROR3", 20: "ERROR4",
	21: "FATAL", 22: "FATAL2", 23: "FATAL3", 24: "FATAL4",
}

// otlpSeverity accepts either JSON spelling of the SeverityNumber enum.
//
// json.Number does NOT do this -- it rejects a quoted value outright with
// "invalid number literal" -- and OTLP's JSON mapping permits the enum's name
// as a string. A collector configured that way would have had its whole
// export rejected as undecodable, which for an OTLP exporter means a retry
// loop that never succeeds.
type otlpSeverity int

func (s *otlpSeverity) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var name string
		if err := json.Unmarshal(b, &name); err != nil {
			return nil // a malformed string is UNSPECIFIED, not a failed batch
		}
		// A QUOTED INTEGER first. OTLP's JSON mapping permits an enum as its
		// number or its name, and protojson accepts a number written as a
		// string for any 64-bit field, so `"17"` is legal and used to fall
		// through to the name lookup and silently become UNSPECIFIED.
		if i, err := strconv.Atoi(strings.TrimSpace(name)); err == nil {
			if i > 0 && i < len(severityNumberName) {
				*s = otlpSeverity(i)
			}
			return nil
		}
		*s = otlpSeverity(severityNumberByName(name))
		return nil
	}
	// Anything that is not a number -- an object, an array, a bool -- is
	// UNSPECIFIED for THIS field. Returning the error failed the whole
	// document: one malformed severity in a batch of 10,000 records rejected
	// all 10,000, which for an OTLP exporter is a permanent 4xx and total data
	// loss for the batch. One bad field must cost one field.
	if b[0] != '-' && (b[0] < '0' || b[0] > '9') {
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return nil
	}
	i, err := n.Int64()
	if err != nil {
		// A non-integer severity is not a reason to fail the whole export.
		return nil
	}
	if i < 0 || int(i) >= len(severityNumberName) {
		return nil // out of range means UNSPECIFIED, not an error
	}
	*s = otlpSeverity(i)
	return nil
}

// severityNumberByName maps the enum's name, with or without OTLP's
// SEVERITY_NUMBER_ prefix, to its integer. An unrecognized name yields 0 --
// OTLP's UNSPECIFIED -- rather than an error: a severity this store does not
// know is not a reason to drop the log line the operator is trying to keep.
func severityNumberByName(name string) int {
	name = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(name)), "SEVERITY_NUMBER_")
	for i, s := range severityNumberName {
		if s != "" && s == name {
			return i
		}
	}
	return 0
}

// otlpRecordFields writes the LogRecord's non-attribute fields into fields.
// Shared by both encodings so neither can carry a field the other drops --
// which is exactly how severity_number, trace_id, span_id, flags, the dropped
// counts and event_name came to be absent from both.
//
// Empty and zero values are not written: a column of "0" for every record is
// storage spent to say nothing, and OTLP's zero values all mean "unset".
func otlpRecordFields(fields map[string]string, sevNum int, sevText, traceID, spanID, eventName string, flags, dropped uint32) {
	// > 0 and in range, not != 0: a negative sevNum passed the old test and
	// indexed severityNumberName out of range.
	if sevNum > 0 && sevNum < len(severityNumberName) {
		fields["severity_number"] = strconv.Itoa(sevNum)
		// severity is the human-facing column. severityText wins when the
		// exporter set it, since that is the string the operator chose.
		if sevText == "" {
			fields["severity"] = severityNumberName[sevNum]
		}
	}
	if sevText != "" {
		fields["severity"] = sevText
	}
	// NORMALIZED, and only if they are what they claim to be.
	//
	// OTLP/JSON carries these as hex and hex is case-insensitive, so the same
	// trace arrives upper, lower or mixed depending on whose exporter wrote it.
	// The protobuf path carries raw bytes and renders them lowercase; the JSON
	// path stored the string it was handed. The identical trace was therefore
	// stored under two spellings, and a query for one found neither half of the
	// other's records.
	//
	// Nothing validated it either: `not-hex-at-all!!` went in verbatim, so a
	// column queries treat as an identifier held arbitrary text that arrived
	// from outside the cluster. A value that is not a trace ID is dropped
	// rather than stored as one -- and the RECORD is kept, because one bad
	// field is not a reason to lose a log line.
	//
	// Doing it here rather than in the JSON decoder is what makes the two
	// encodings agree by construction: the protobuf path hands in lowercase
	// hex of exactly the right length, so it normalises to itself.
	if id, ok := normalizeIDHex(traceID, 16); ok {
		fields["trace_id"] = id
	}
	if id, ok := normalizeIDHex(spanID, 8); ok {
		fields["span_id"] = id
	}
	if eventName != "" {
		fields["event_name"] = eventName
	}
	if flags != 0 {
		fields["flags"] = strconv.FormatUint(uint64(flags), 10)
	}
	if dropped != 0 {
		fields["dropped_attributes_count"] = strconv.FormatUint(uint64(dropped), 10)
	}
}

// normalizeIDHex lowercases a hex ID of exactly wantBytes bytes, and reports
// false for anything else -- wrong length, a non-hex character, or empty.
//
// No allocation when the input is already lowercase and the right length, which
// is every record on the protobuf path and most on the JSON one.
func normalizeIDHex(s string, wantBytes int) (string, bool) {
	if len(s) != wantBytes*2 {
		return "", false
	}
	upper := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
			upper = true
		default:
			return "", false
		}
	}
	if !upper {
		return s, true
	}
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'F' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b), true
}

var errNoResourceLogs = errors.New("no resourceLogs field: not an OTLP logs payload")

// IngestOTLPLogs ingests an OpenTelemetry logs export (OTLP/HTTP, JSON): each
// log record becomes a record whose fields are the resource attributes plus
// the record's own attributes, with severityText -> severity, body -> _msg,
// and timeUnixNano (or observedTimeUnixNano) -> time.
func IngestOTLPLogs(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestOTLPLogsOpts(w, data, fallback, nil)
}

// IngestOTLPLogsOpts is IngestOTLPLogs with the request's field mappings applied.
func IngestOTLPLogsOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	var p otlpLogs
	if err := json.Unmarshal(data, &p); err != nil {
		// OTLP exporters retry on 5xx and give up on 4xx. Answering 200 for
		// an undecodable body told them the data was delivered.
		return res, encodingErr(err)
	}
	if p.ResourceLogs == nil {
		return res, envelopeErr(errNoResourceLogs)
	}
	fields := map[string]string{}
	for _, rl := range p.ResourceLogs {
		resAttrs := make(map[string]string, len(rl.Resource.Attributes))
		for _, a := range rl.Resource.Attributes {
			resAttrs[a.Key] = a.Value.str()
		}
		if rl.Resource.DroppedAttributesCount != 0 {
			resAttrs["resource_dropped_attributes_count"] =
				strconv.FormatUint(uint64(rl.Resource.DroppedAttributesCount), 10)
		}
		for _, sl := range rl.ScopeLogs {
			// Scope attributes sit between resource and record in OTLP's
			// precedence, and were dropped entirely. They are what a library
			// uses to identify itself, so losing them makes every record look
			// like it came from the application directly.
			scopeAttrs := make(map[string]string, len(sl.Scope.Attributes)+2)
			for _, a := range sl.Scope.Attributes {
				scopeAttrs[a.Key] = a.Value.str()
			}
			if sl.Scope.Name != "" {
				scopeAttrs["scope_name"] = sl.Scope.Name
			}
			if sl.Scope.Version != "" {
				scopeAttrs["scope_version"] = sl.Scope.Version
			}
			for _, lr := range sl.LogRecords {
				for k := range fields {
					delete(fields, k)
				}
				for k, v := range resAttrs {
					fields[k] = v
				}
				for k, v := range scopeAttrs {
					fields[k] = v
				}
				for _, a := range lr.Attributes {
					fields[a.Key] = a.Value.str()
				}
				otlpRecordFields(fields, int(lr.SeverityNumber),
					lr.SeverityText, lr.TraceID, lr.SpanID, lr.EventName,
					lr.Flags, lr.DroppedAttributesCount)
				if msg := lr.Body.str(); msg != "" {
					fields["_msg"] = msg
				}
				tsStr := lr.TimeUnixNano
				if tsStr == "" {
					tsStr = lr.ObservedTimeUnixNano
				}
				ts, ok, tsErr := parseTime(tsStr)
				if tsErr != nil {
					// See ErrTimeOutOfRange. OTLP reports a rejected count in
					// its partial-success body, so the exporter is told.
					ord := res.Accepted + res.Rejected
					res.Reject(ord)
					res.WarnAt(ord, "%v", tsErr)
					continue
				}
				if !ok {
					ts = fallback()
				}
				if mapped {
					opts.apply(fields)
				}
				addOrReject(w, ts, fields, opts, &res, res.Accepted+res.Rejected)
			}
		}
	}
	return res, nil
}

// OTLPPartialSuccess is the ExportLogsServiceResponse body an OTLP receiver
// returns when it accepted the request but not every record in it.
//
// The empty response `{}` means full success, which is why an accepted export
// of zero records is still a 200 with `{}` -- an exporter is entitled to send
// an empty batch. A rejected count is reported here rather than as a 4xx,
// because a 4xx tells the exporter to drop the WHOLE batch including the
// records this store did accept.
type OTLPPartialSuccess struct {
	RejectedLogRecords string `json:"rejectedLogRecords"`
	ErrorMessage       string `json:"errorMessage,omitempty"`
}

// OTLPResponse is the response envelope. partialSuccess is omitted entirely on
// full success: OTLP defines an empty message as "everything was accepted",
// and a present-but-zero partialSuccess is a different statement.
type OTLPResponse struct {
	PartialSuccess *OTLPPartialSuccess `json:"partialSuccess,omitempty"`
}

// OTLPResponseFor builds the response body for a Result. msg explains the
// rejection and is required when anything was rejected -- OTLP says the
// error_message is for human eyes and must not be parsed, but an empty one
// leaves the operator with a count and no cause.
func OTLPResponseFor(res Result, msg string) OTLPResponse {
	if res.Rejected == 0 {
		return OTLPResponse{}
	}
	if msg == "" {
		msg = "some log records could not be stored; see the server's logs"
	}
	return OTLPResponse{PartialSuccess: &OTLPPartialSuccess{
		RejectedLogRecords: strconv.Itoa(res.Rejected),
		ErrorMessage:       msg,
	}}
}
