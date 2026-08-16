package ingest

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// OTLP conformance: the JSON and protobuf encodings of the SAME logical export
// must produce the SAME stored rows.
//
// This is the property nothing checked, and its absence is why the two paths
// drifted. Before this test: neither encoding decoded bytesValue, arrayValue or
// kvlistValue -- the protobuf file's own header comment claimed "bytes = 7" and
// decodeAnyValue had no case for it -- and neither carried severity_number,
// trace_id, span_id, flags, the dropped-attribute counts, event_name, or ANY
// scope attribute. An exporter sending those got a 200 and a row without them.
//
// A collector's otlphttp exporter sends protobuf by default and JSON on
// request, so an operator who switches encodings must not see their columns
// change. That is what is asserted here, field by field.

// ---- a protobuf writer for the fixtures ----
//
// pvarint/pfixed64/pbytes already exist in otelproto.go for its own fixture;
// these add what the conformance fixtures need.

func pfixed32(dst []byte, num int, v uint32) []byte {
	dst = binary.AppendUvarint(dst, uint64(num)<<3|5)
	return binary.LittleEndian.AppendUint32(dst, v)
}

// pstring, anyString, anyInt, anyDouble, anyBool and kv already exist in
// otelproto.go for its own fixture. Only the three kinds nothing could build
// before are added here -- which is itself the tell: the fixture builder had
// no way to express bytes, array or kvlist, so no test could have caught the
// decoder not reading them.
func anyBytes(b []byte) []byte { return pbytes(nil, 7, b) }

// anyArray wraps element AnyValues in ArrayValue{ repeated AnyValue values = 1 }.
func anyArray(elems ...[]byte) []byte {
	var inner []byte
	for _, e := range elems {
		inner = pbytes(inner, 1, e)
	}
	return pbytes(nil, 5, inner)
}

// anyKvlist wraps KeyValues in KeyValueList{ repeated KeyValue values = 1 }.
func anyKvlist(pairs ...[2]any) []byte {
	var inner []byte
	for _, pair := range pairs {
		inner = pbytes(inner, 1, kv(pair[0].(string), pair[1].([]byte)))
	}
	return pbytes(nil, 6, inner)
}

// ---- the shared logical fixture ----

// otlpFixture is one export described once, then rendered into both encodings.
// Described once on purpose: a fixture written twice by hand is a fixture whose
// two halves drift, which is the defect this whole file exists to catch.
type otlpFixture struct {
	resourceAttrs []attr
	scopeName     string
	scopeVersion  string
	scopeAttrs    []attr
	records       []fixtureRecord
}

type attr struct {
	key  string
	kind string // "string" | "int" | "double" | "bool" | "bytes" | "array" | "kvlist"
	s    string
	i    int64
	d    float64
	b    bool
	raw  []byte
	arr  []attr // for "array": only the value halves are used
	kvl  []attr // for "kvlist"
}

type fixtureRecord struct {
	timeUnixNano         uint64
	observedTimeUnixNano uint64
	severityNumber       int
	severityText         string
	body                 attr
	attrs                []attr
	dropped              uint32
	flags                uint32
	traceID              []byte
	spanID               []byte
	eventName            string
}

// proto renders one attr as an AnyValue message.
func (a attr) proto() []byte {
	switch a.kind {
	case "string":
		return anyString(a.s)
	case "int":
		return anyInt(a.i)
	case "double":
		return anyDouble(a.d)
	case "bool":
		return anyBool(a.b)
	case "bytes":
		return anyBytes(a.raw)
	case "array":
		elems := make([][]byte, 0, len(a.arr))
		for _, e := range a.arr {
			elems = append(elems, e.proto())
		}
		return anyArray(elems...)
	case "kvlist":
		pairs := make([][2]any, 0, len(a.kvl))
		for _, e := range a.kvl {
			pairs = append(pairs, [2]any{e.key, e.proto()})
		}
		return anyKvlist(pairs...)
	}
	panic("unknown attr kind " + a.kind)
}

// jsonValue renders one attr as the OTLP JSON AnyValue object.
func (a attr) jsonValue() map[string]any {
	switch a.kind {
	case "string":
		return map[string]any{"stringValue": a.s}
	case "int":
		// OTLP's JSON mapping writes int64 as a STRING.
		return map[string]any{"intValue": fmt.Sprintf("%d", a.i)}
	case "double":
		return map[string]any{"doubleValue": a.d}
	case "bool":
		return map[string]any{"boolValue": a.b}
	case "bytes":
		return map[string]any{"bytesValue": base64.StdEncoding.EncodeToString(a.raw)}
	case "array":
		vals := make([]any, 0, len(a.arr))
		for _, e := range a.arr {
			vals = append(vals, e.jsonValue())
		}
		return map[string]any{"arrayValue": map[string]any{"values": vals}}
	case "kvlist":
		vals := make([]any, 0, len(a.kvl))
		for _, e := range a.kvl {
			vals = append(vals, map[string]any{"key": e.key, "value": e.jsonValue()})
		}
		return map[string]any{"kvlistValue": map[string]any{"values": vals}}
	}
	panic("unknown attr kind " + a.kind)
}

func (f otlpFixture) proto() []byte {
	var scope []byte
	scope = pstring(scope, 1, f.scopeName)
	scope = pstring(scope, 2, f.scopeVersion)
	for _, a := range f.scopeAttrs {
		scope = pbytes(scope, 3, kv(a.key, a.proto()))
	}

	var scopeLogs []byte
	scopeLogs = pbytes(scopeLogs, 1, scope)
	for _, r := range f.records {
		var rec []byte
		if r.timeUnixNano != 0 {
			rec = pfixed64(rec, 1, r.timeUnixNano)
		}
		if r.severityNumber != 0 {
			rec = pvarint(rec, 2, uint64(r.severityNumber))
		}
		if r.severityText != "" {
			rec = pstring(rec, 3, r.severityText)
		}
		rec = pbytes(rec, 5, r.body.proto())
		for _, a := range r.attrs {
			rec = pbytes(rec, 6, kv(a.key, a.proto()))
		}
		if r.dropped != 0 {
			rec = pvarint(rec, 7, uint64(r.dropped))
		}
		if r.flags != 0 {
			rec = pfixed32(rec, 8, r.flags)
		}
		if len(r.traceID) > 0 {
			rec = pbytes(rec, 9, r.traceID)
		}
		if len(r.spanID) > 0 {
			rec = pbytes(rec, 10, r.spanID)
		}
		if r.observedTimeUnixNano != 0 {
			rec = pfixed64(rec, 11, r.observedTimeUnixNano)
		}
		if r.eventName != "" {
			rec = pstring(rec, 12, r.eventName)
		}
		scopeLogs = pbytes(scopeLogs, 2, rec)
	}

	var resource []byte
	for _, a := range f.resourceAttrs {
		resource = pbytes(resource, 1, kv(a.key, a.proto()))
	}

	var resourceLogs []byte
	resourceLogs = pbytes(resourceLogs, 1, resource)
	resourceLogs = pbytes(resourceLogs, 2, scopeLogs)

	return pbytes(nil, 1, resourceLogs)
}

func (f otlpFixture) json() []byte {
	resAttrs := make([]any, 0, len(f.resourceAttrs))
	for _, a := range f.resourceAttrs {
		resAttrs = append(resAttrs, map[string]any{"key": a.key, "value": a.jsonValue()})
	}
	scopeAttrs := make([]any, 0, len(f.scopeAttrs))
	for _, a := range f.scopeAttrs {
		scopeAttrs = append(scopeAttrs, map[string]any{"key": a.key, "value": a.jsonValue()})
	}
	recs := make([]any, 0, len(f.records))
	for _, r := range f.records {
		m := map[string]any{"body": r.body.jsonValue()}
		if r.timeUnixNano != 0 {
			m["timeUnixNano"] = fmt.Sprintf("%d", r.timeUnixNano)
		}
		if r.observedTimeUnixNano != 0 {
			m["observedTimeUnixNano"] = fmt.Sprintf("%d", r.observedTimeUnixNano)
		}
		if r.severityNumber != 0 {
			m["severityNumber"] = r.severityNumber
		}
		if r.severityText != "" {
			m["severityText"] = r.severityText
		}
		if len(r.attrs) > 0 {
			as := make([]any, 0, len(r.attrs))
			for _, a := range r.attrs {
				as = append(as, map[string]any{"key": a.key, "value": a.jsonValue()})
			}
			m["attributes"] = as
		}
		if r.dropped != 0 {
			m["droppedAttributesCount"] = r.dropped
		}
		if r.flags != 0 {
			m["flags"] = r.flags
		}
		if len(r.traceID) > 0 {
			m["traceId"] = hex.EncodeToString(r.traceID)
		}
		if len(r.spanID) > 0 {
			m["spanId"] = hex.EncodeToString(r.spanID)
		}
		if r.eventName != "" {
			m["eventName"] = r.eventName
		}
		recs = append(recs, m)
	}
	b, err := json.Marshal(map[string]any{
		"resourceLogs": []any{map[string]any{
			"resource": map[string]any{"attributes": resAttrs},
			"scopeLogs": []any{map[string]any{
				"scope": map[string]any{
					"name": f.scopeName, "version": f.scopeVersion, "attributes": scopeAttrs,
				},
				"logRecords": recs,
			}},
		}},
	})
	if err != nil {
		panic(err)
	}
	return b
}

// conformanceFixture exercises every AnyValue kind and every LogRecord field
// the plan's steps 1 and 2 enumerate.
func conformanceFixture() otlpFixture {
	return otlpFixture{
		resourceAttrs: []attr{
			{key: "service.name", kind: "string", s: "checkout"},
			{key: "service.instance.count", kind: "int", i: 7},
		},
		scopeName:    "github.com/example/logger",
		scopeVersion: "1.4.2",
		scopeAttrs: []attr{
			{key: "scope.tier", kind: "string", s: "library"},
		},
		records: []fixtureRecord{{
			timeUnixNano:         1714521600000000000,
			observedTimeUnixNano: 1714521600500000000,
			severityNumber:       17, // ERROR
			severityText:         "ERROR",
			body:                 attr{kind: "string", s: "payment declined"},
			attrs: []attr{
				{key: "attr.string", kind: "string", s: "hello"},
				{key: "attr.int", kind: "int", i: -42},
				{key: "attr.double", kind: "double", d: 1.5},
				{key: "attr.bool", kind: "bool", b: true},
				{key: "attr.bytes", kind: "bytes", raw: []byte{0x00, 0xff, 0x10, 0x7f}},
				{key: "attr.array", kind: "array", arr: []attr{
					{kind: "string", s: "a"},
					{kind: "int", i: 2},
					{kind: "bool", b: false},
				}},
				{key: "attr.kvlist", kind: "kvlist", kvl: []attr{
					{key: "inner.s", kind: "string", s: "v"},
					{key: "inner.i", kind: "int", i: 9},
				}},
			},
			dropped:   3,
			flags:     1,
			traceID:   []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
			spanID:    []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8},
			eventName: "payment.declined",
		}},
	}
}

// rowsOf ingests a payload through one of the two paths and returns the stored
// rows in storeRows' comparable form. A real store, not a fake: the assertion
// is about what a query gets back, and a writer-level check would pass over a
// field the storage layer drops.
func rowsOf(t *testing.T, ingest func(*Writer) (Result, error)) ([]string, Result) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	res, ierr := ingest(w)
	if ierr != nil {
		t.Fatalf("ingest: %v", ierr)
	}
	w.Close()
	return storeRows(t, st), res
}

func TestOTLPJSONAndProtoNormalizeIdentically(t *testing.T) {
	f := conformanceFixture()
	fb := func() int64 { return 1 }

	jsonRows, jsonRes := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsOpts(w, f.json(), fb, nil)
	})
	protoRows, protoRes := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, f.proto(), fb, nil)
	})

	if jsonRes.Accepted != protoRes.Accepted {
		t.Fatalf("accepted: JSON %d, protobuf %d", jsonRes.Accepted, protoRes.Accepted)
	}
	if jsonRes.Accepted != len(f.records) {
		t.Fatalf("accepted %d records, fixture has %d", jsonRes.Accepted, len(f.records))
	}
	if len(jsonRows) != len(protoRows) {
		t.Fatalf("JSON stored %d rows, protobuf %d\nJSON:  %v\nproto: %v",
			len(jsonRows), len(protoRows), jsonRows, protoRows)
	}
	for i := range jsonRows {
		if jsonRows[i] != protoRows[i] {
			t.Errorf("row %d differs:\n  JSON:     %s\n  protobuf: %s", i, jsonRows[i], protoRows[i])
		}
	}
}

// Every AnyValue kind must actually survive into the row. Comparing the two
// encodings to each other cannot catch a kind BOTH of them drop -- which is
// precisely what happened to bytes, array and kvlist -- so the values are
// asserted against what they must be.
func TestOTLPStoresEveryAnyValueKind(t *testing.T) {
	f := conformanceFixture()
	fb := func() int64 { return 1 }

	want := map[string]string{
		"attr.string": "hello",
		"attr.int":    "-42",
		"attr.double": "1.5",
		"attr.bool":   "true",
		// base64 of {0x00,0xff,0x10,0x7f}, the encoding OTLP's JSON mapping uses.
		"attr.bytes":  base64.StdEncoding.EncodeToString([]byte{0x00, 0xff, 0x10, 0x7f}),
		"attr.array":  `["a","2","false"]`,
		"attr.kvlist": `{"inner.s":"v","inner.i":"9"}`,
	}

	for _, enc := range []struct {
		name   string
		ingest func(*Writer) (Result, error)
	}{
		{"json", func(w *Writer) (Result, error) { return IngestOTLPLogsOpts(w, f.json(), fb, nil) }},
		{"proto", func(w *Writer) (Result, error) { return IngestOTLPLogsProto(w, f.proto(), fb, nil) }},
	} {
		t.Run(enc.name, func(t *testing.T) {
			rows, _ := rowsOf(t, enc.ingest)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			got := fieldsOfRow(rows[0])
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// The LogRecord fields the plan's step 2 names, asserted by value in both
// encodings. trace_id and span_id are the interesting pair: JSON carries hex
// and protobuf carries raw bytes, so a decoder that passed either through
// unchanged would make the two encodings disagree.
func TestOTLPStoresEveryRecordField(t *testing.T) {
	f := conformanceFixture()
	fb := func() int64 { return 1 }

	want := map[string]string{
		"severity":                 "ERROR",
		"severity_number":          "17",
		"trace_id":                 "0102030405060708090a0b0c0d0e0f10",
		"span_id":                  "a1a2a3a4a5a6a7a8",
		"event_name":               "payment.declined",
		"flags":                    "1",
		"dropped_attributes_count": "3",
		"scope_name":               "github.com/example/logger",
		"scope_version":            "1.4.2",
		"scope.tier":               "library",
		"service.name":             "checkout",
		"service.instance.count":   "7",
		"_msg":                     "payment declined",
	}

	for _, enc := range []struct {
		name   string
		ingest func(*Writer) (Result, error)
	}{
		{"json", func(w *Writer) (Result, error) { return IngestOTLPLogsOpts(w, f.json(), fb, nil) }},
		{"proto", func(w *Writer) (Result, error) { return IngestOTLPLogsProto(w, f.proto(), fb, nil) }},
	} {
		t.Run(enc.name, func(t *testing.T) {
			rows, _ := rowsOf(t, enc.ingest)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			got := fieldsOfRow(rows[0])
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// severityNumber accepts either JSON spelling. A collector configured to emit
// the enum NAME must store the same thing as one emitting the integer.
func TestOTLPSeverityNumberAcceptsBothSpellings(t *testing.T) {
	for _, tc := range []struct {
		spelling string
		wantNum  string
		wantSev  string
	}{
		{`17`, "17", "ERROR"},
		{`"ERROR"`, "17", "ERROR"},
		{`"SEVERITY_NUMBER_ERROR"`, "17", "ERROR"},
		{`9`, "9", "INFO"},
		{`"INFO"`, "9", "INFO"},
		// Unknown and out of range both mean UNSPECIFIED, and must not drop
		// the record: a severity this store does not recognize is not a reason
		// to lose the log line.
		{`"NOT_A_SEVERITY"`, "", ""},
		{`9999`, "", ""},
	} {
		t.Run(tc.spelling, func(t *testing.T) {
			body := fmt.Sprintf(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[
				{"timeUnixNano":"1714521600000000000","severityNumber":%s,
				 "body":{"stringValue":"m"}}]}]}]}`, tc.spelling)
			rows, res := rowsOf(t, func(w *Writer) (Result, error) {
				return IngestOTLPLogsOpts(w, []byte(body), func() int64 { return 1 }, nil)
			})
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1 -- an unknown severity must not drop the record", res.Accepted)
			}
			got := fieldsOfRow(rows[0])
			if got["severity_number"] != tc.wantNum {
				t.Errorf("severity_number = %q, want %q", got["severity_number"], tc.wantNum)
			}
			if got["severity"] != tc.wantSev {
				t.Errorf("severity = %q, want %q", got["severity"], tc.wantSev)
			}
		})
	}
}

// severityText wins over the enum's name when both are present: it is the
// string the operator chose.
func TestOTLPSeverityTextWinsOverTheEnumName(t *testing.T) {
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[
		{"timeUnixNano":"1714521600000000000","severityNumber":17,"severityText":"fatal-ish",
		 "body":{"stringValue":"m"}}]}]}]}`
	rows, _ := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsOpts(w, []byte(body), func() int64 { return 1 }, nil)
	})
	got := fieldsOfRow(rows[0])
	if got["severity"] != "fatal-ish" {
		t.Errorf("severity = %q, want the record's own severityText", got["severity"])
	}
	if got["severity_number"] != "17" {
		t.Errorf("severity_number = %q, want 17 kept alongside it", got["severity_number"])
	}
}

// A nested composite must not be able to exhaust the stack. The bound is a
// property of an untrusted decoder, not a nicety: the nesting comes off the
// wire, and a body a few hundred bytes long can describe thousands of levels.
func TestOTLPNestedCompositeIsBounded(t *testing.T) {
	// An array nested far past the limit.
	inner := anyString("leaf")
	for i := 0; i < maxAnyValueDepth*4; i++ {
		inner = anyArray(inner)
	}
	var rec []byte
	rec = pfixed64(rec, 1, 1714521600000000000)
	rec = pbytes(rec, 5, anyString("m"))
	rec = pbytes(rec, 6, kv("deep", inner))
	var scopeLogs []byte
	scopeLogs = pbytes(scopeLogs, 2, rec)
	var rl []byte
	rl = pbytes(rl, 2, scopeLogs)
	payload := pbytes(nil, 1, rl)

	// The assertion is that this returns at all, with the record stored.
	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, payload, func() int64 { return 1 }, nil)
	})
	if res.Accepted != 1 {
		t.Fatalf("accepted %d, want 1", res.Accepted)
	}
	got := fieldsOfRow(rows[0])
	if _, ok := got["deep"]; !ok {
		t.Error("the over-nested attribute vanished entirely; it should be truncated, not dropped")
	}
}

// OTLPResponseFor is the partial_success contract. Full success is an EMPTY
// object: OTLP defines `{}` as "everything was accepted", and a
// present-but-zero partialSuccess says something different.
func TestOTLPPartialSuccessBody(t *testing.T) {
	full, err := json.Marshal(OTLPResponseFor(Result{Accepted: 5}, ""))
	if err != nil {
		t.Fatal(err)
	}
	if string(full) != "{}" {
		t.Errorf("full success body = %s, want {}", full)
	}

	partial, err := json.Marshal(OTLPResponseFor(Result{Accepted: 5, Rejected: 2}, "two records had no body"))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		PartialSuccess struct {
			RejectedLogRecords string `json:"rejectedLogRecords"`
			ErrorMessage       string `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if err := json.Unmarshal(partial, &got); err != nil {
		t.Fatalf("partial body is not decodable: %v\n%s", err, partial)
	}
	// A string, not a number: OTLP's JSON mapping writes int64 as a string,
	// and a receiver that emitted a bare number would be read wrong by a
	// strict client.
	if got.PartialSuccess.RejectedLogRecords != "2" {
		t.Errorf("rejectedLogRecords = %q, want \"2\"", got.PartialSuccess.RejectedLogRecords)
	}
	if got.PartialSuccess.ErrorMessage == "" {
		t.Error("errorMessage is empty; a count with no cause leaves the operator nothing to act on")
	}

	// A rejection with no message still gets one.
	noMsg := OTLPResponseFor(Result{Rejected: 1}, "")
	if noMsg.PartialSuccess == nil || noMsg.PartialSuccess.ErrorMessage == "" {
		t.Error("a rejection with no supplied message must still carry one")
	}
}

// ---- helpers ----

// fieldsOfRow parses storeRows' "time|k=v|k=v" line. Only the FIRST '=' of a
// field separates: a value can contain one (base64 padding, a JSON blob), and
// splitting on every '=' would silently truncate exactly the composite values
// this file exists to check.
func fieldsOfRow(row string) map[string]string {
	out := map[string]string{}
	parts := strings.Split(row, "|")
	for _, part := range parts[1:] { // parts[0] is the encoded timestamp
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

// A severity_number varint of 2^63 or more converts to a NEGATIVE int, passes
// a `sevNum < len(names)` bound test, and indexes the name table out of range.
// A 31-byte body panicked the handler; recoverPanic turned it into a 500, and
// an OTLP exporter retries a 500 forever.
func TestOTLPHugeSeverityNumberDoesNotPanic(t *testing.T) {
	for _, sev := range []uint64{
		1 << 63,    // negative once converted
		^uint64(0), // all ones
		1<<63 + 17, // negative, and 17 is a legal severity
		25,         // one past the table
		1 << 40,    // large but positive
	} {
		t.Run(fmt.Sprintf("%d", sev), func(t *testing.T) {
			var rec []byte
			rec = pfixed64(rec, 1, 1714521600000000000)
			rec = pvarint(rec, 2, sev)
			rec = pbytes(rec, 5, anyString("m"))
			var scopeLogs []byte
			scopeLogs = pbytes(scopeLogs, 2, rec)
			var rl []byte
			rl = pbytes(rl, 2, scopeLogs)
			payload := pbytes(nil, 1, rl)

			rows, res := rowsOf(t, func(w *Writer) (Result, error) {
				return IngestOTLPLogsProto(w, payload, func() int64 { return 1 }, nil)
			})
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1", res.Accepted)
			}
			// Out of range means UNSPECIFIED: no severity columns at all,
			// rather than a fabricated one or a crash.
			got := fieldsOfRow(rows[0])
			if v, ok := got["severity_number"]; ok {
				t.Errorf("severity_number = %q for an out-of-range value; want the column absent", v)
			}
			if v, ok := got["severity"]; ok {
				t.Errorf("severity = %q for an out-of-range value; want the column absent", v)
			}
		})
	}
}

// Fields 9, 10 and 12 collide with Metric.histogram, Metric.exponential_histogram
// and Metric.metadata. A metrics payload's bucket bytes were being stored as a
// trace_id. Length is the discriminator: 16 bytes for a trace id, 8 for a span
// id, per OTLP.
func TestOTLPRejectsMetricBytesAsTraceIDs(t *testing.T) {
	mk := func(field int, payload []byte) map[string]string {
		var rec []byte
		rec = pfixed64(rec, 1, 1714521600000000000)
		rec = pbytes(rec, 5, anyString("m"))
		rec = pbytes(rec, field, payload)
		var scopeLogs []byte
		scopeLogs = pbytes(scopeLogs, 2, rec)
		var rl []byte
		rl = pbytes(rl, 2, scopeLogs)
		rows, res := rowsOf(t, func(w *Writer) (Result, error) {
			return IngestOTLPLogsProto(w, pbytes(nil, 1, rl), func() int64 { return 1 }, nil)
		})
		if res.Accepted != 1 || len(rows) != 1 {
			t.Fatalf("accepted %d, %d rows", res.Accepted, len(rows))
		}
		return fieldsOfRow(rows[0])
	}

	// A histogram submessage of any length OTHER than 16 must not become a
	// trace_id. The length check narrows the collision; it does not close it,
	// and the residual is stated in docs/wrong.md rather than papered over: a
	// histogram that happens to be exactly 16 bytes is indistinguishable from
	// a trace id on the wire. What makes that residual small in practice is
	// the wrongShape check above -- every real Metric carries name at field 1,
	// wire type 2, which rejects the record before these fields are read. Only
	// a NAMELESS metric with an exactly-16-byte histogram gets through.
	histogram := []byte("\n\x14a-longer-bucket-payload")
	if len(histogram) == 16 {
		t.Fatal("fixture is exactly 16 bytes; it cannot test the length check")
	}
	if v, ok := mk(9, histogram)["trace_id"]; ok {
		t.Errorf("a %d-byte Metric.histogram became trace_id=%q", len(histogram), v)
	}
	if v, ok := mk(10, histogram)["span_id"]; ok {
		t.Errorf("a %d-byte Metric.exponential_histogram became span_id=%q", len(histogram), v)
	}
	// A genuine 16-byte trace id and 8-byte span id still store.
	tid := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if got := mk(9, tid)["trace_id"]; got != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("a real 16-byte trace id did not store: %q", got)
	}
	sid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if got := mk(10, sid)["span_id"]; got != "0102030405060708" {
		t.Errorf("a real 8-byte span id did not store: %q", got)
	}
	// A short id is not a valid id either.
	if v, ok := mk(9, []byte{1, 2, 3})["trace_id"]; ok {
		t.Errorf("a 3-byte value became trace_id=%q", v)
	}
	// event_name must be text, not a metric's binary metadata.
	if v, ok := mk(12, []byte{0xff, 0xfe, 0x00, 0x01})["event_name"]; ok {
		t.Errorf("invalid UTF-8 became event_name=%q", v)
	}
}

// One bad field must cost one field, never the batch. A severityNumber that is
// an object, an array or a bool used to return an error from UnmarshalJSON,
// which failed the WHOLE document -- so one malformed record in a batch of
// 10,000 rejected all 10,000, and for an OTLP exporter a 4xx is permanent.
func TestOTLPOneBadSeverityDoesNotFailTheBatch(t *testing.T) {
	for _, bad := range []string{`{}`, `[]`, `true`, `null`, `1.5`, `-3`, `1e2`, `9999`, `"nonsense"`} {
		t.Run(bad, func(t *testing.T) {
			var b strings.Builder
			b.WriteString(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[`)
			for i := 0; i < 20; i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				sev := "9"
				if i == 7 {
					sev = bad
				}
				fmt.Fprintf(&b, `{"timeUnixNano":"1714521600000000000","severityNumber":%s,`+
					`"body":{"stringValue":"m%d"}}`, sev, i)
			}
			b.WriteString(`]}]}]}`)

			st := openTestStore(t)
			w := NewWriter(st)
			res, err := IngestOTLPLogsOpts(w, []byte(b.String()), func() int64 { return 1 }, nil)
			w.Close()
			if err != nil {
				t.Fatalf("one bad severityNumber (%s) failed the whole 20-record batch: %v", bad, err)
			}
			if res.Accepted != 20 {
				t.Errorf("accepted %d of 20 records", res.Accepted)
			}
		})
	}
}

// A quoted integer is legal OTLP JSON -- protojson writes any 64-bit field as
// a string -- and used to fall through to the enum-name lookup and silently
// become UNSPECIFIED.
func TestOTLPSeverityNumberAcceptsAQuotedInteger(t *testing.T) {
	for _, tc := range []struct{ spelling, wantNum, wantSev string }{
		{`"17"`, "17", "ERROR"},
		{`" 9 "`, "9", "INFO"},
		{`17`, "17", "ERROR"},
		{`"9999"`, "", ""}, // quoted but out of range: UNSPECIFIED, not an error
	} {
		t.Run(tc.spelling, func(t *testing.T) {
			body := fmt.Sprintf(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[
				{"timeUnixNano":"1714521600000000000","severityNumber":%s,
				 "body":{"stringValue":"m"}}]}]}]}`, tc.spelling)
			rows, res := rowsOf(t, func(w *Writer) (Result, error) {
				return IngestOTLPLogsOpts(w, []byte(body), func() int64 { return 1 }, nil)
			})
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1", res.Accepted)
			}
			got := fieldsOfRow(rows[0])
			if got["severity_number"] != tc.wantNum {
				t.Errorf("severity_number = %q, want %q", got["severity_number"], tc.wantNum)
			}
			if got["severity"] != tc.wantSev {
				t.Errorf("severity = %q, want %q", got["severity"], tc.wantSev)
			}
		})
	}
}

// The composite renderer quotes each element's rendered form, so a nested
// composite is escaped once per level and its length roughly DOUBLES per
// level. At the 16-level depth bound that is 2^16: a 1,663-byte body was
// measured producing 20 MB of rendered attributes, and the shape gzips almost
// perfectly, so neither the 64 MiB body limit nor the 512 MiB decompressed
// limit comes near bounding it.
//
// maxAnyValueDepth bounds the STACK. This bounds the OUTPUT. They are
// different exhaustions and the first does not imply the second.
func TestOTLPCompositeRenderingIsBounded(t *testing.T) {
	// Nest to the depth bound, each level carrying a payload that escapes.
	// ONE child per level: the body stays tiny while the escaping doubles the
	// rendered length at every level. That asymmetry is the vector.
	inner := anyString(`a"b\c` + strings.Repeat("x", 64))
	for i := 0; i < maxAnyValueDepth; i++ {
		inner = anyArray(inner)
	}
	var rec []byte
	rec = pfixed64(rec, 1, 1714521600000000000)
	rec = pbytes(rec, 5, anyString("m"))
	rec = pbytes(rec, 6, kv("deep", inner))
	var scopeLogs []byte
	scopeLogs = pbytes(scopeLogs, 2, rec)
	var rl []byte
	rl = pbytes(rl, 2, scopeLogs)
	payload := pbytes(nil, 1, rl)

	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, payload, func() int64 { return 1 }, nil)
	})
	if res.Accepted != 1 {
		t.Fatalf("accepted %d, want 1", res.Accepted)
	}
	got := fieldsOfRow(rows[0])["deep"]
	if got == "" {
		t.Fatal("the attribute vanished; it should be truncated, not dropped")
	}
	// The guarantee, stated exactly: 2x the budget -- the outermost level's own
	// quoting can double what the budget admitted -- plus a small constant,
	// because each level writes its brackets and its truncation marker AFTER
	// the budget check. It is a hard ceiling, not a target: without it the
	// same 173-byte body rendered 20 MB.
	if limit := 2*maxCompositeBytes + 4096; len(got) > limit {
		t.Errorf("a %d-byte body rendered a %d-byte attribute (%.0fx), over the %d-byte ceiling",
			len(payload), len(got), float64(len(got))/float64(len(payload)), limit)
	}
	if !strings.Contains(got, "truncated") {
		t.Error("the value was cut without a marker; a caller cannot tell it is partial")
	}
	t.Logf("body %d bytes -> attribute %d bytes (%.1fx), bound %d",
		len(payload), len(got), float64(len(got))/float64(len(payload)), maxCompositeBytes)
}

// ---- the identity violations the differential could not see ----

// fieldsOfRow is why storeRows refuses a separator in a value, demonstrated.
//
// It splits on `|`, so a row whose _msg is `x|severity=ERROR` parses as TWO
// fields: `_msg=x`, and a `severity` field that IS NOT IN THE ROW. An assertion
// that the record carries severity=ERROR then passes on a record with no
// severity at all -- and every value assertion in these files goes through this
// function.
//
// No separator can be reserved: a log value holds arbitrary bytes. So the
// renderer refuses instead, and this is the behaviour it is refusing to
// produce.
func TestFieldsOfRowInventsAFieldWhenAValueCarriesTheSeparator(t *testing.T) {
	got := fieldsOfRow("TS|_msg=x|severity=ERROR")
	if got["severity"] != "ERROR" {
		t.Fatalf("the demonstration no longer demonstrates: severity=%q. If "+
			"fieldsOfRow became unambiguous, storeRows no longer needs to "+
			"refuse and this test should go with it", got["severity"])
	}
	if got["_msg"] != "x" {
		t.Errorf("_msg=%q, want the truncated %q", got["_msg"], "x")
	}
}

// Resource.dropped_attributes_count is field 2, and the protobuf path read
// only field 1.
//
// The JSON path writes `resource_dropped_attributes_count` and the protobuf
// path never decoded it, so the SAME logical export stored a different set of
// fields depending on which encoding the collector was configured for. A
// collector's otlphttp exporter sends protobuf by default and JSON on request.
//
// The conformance fixture could not see it: its builder had no way to write
// the field, so neither encoding was ever asked.
func TestResourceDroppedAttributeCountSurvivesBothEncodings(t *testing.T) {
	const dropped = 7
	const atNano = 1_700_000_000_000_000_000

	// protobuf
	var resource []byte
	resource = pbytes(resource, 1, kv("service.name", anyString("api")))
	resource = pvarint(resource, 2, dropped) // Resource.dropped_attributes_count
	var rec []byte
	rec = pfixed64(rec, 1, atNano)
	rec = pbytes(rec, 5, anyString("m"))
	var scope []byte
	scope = pbytes(scope, 2, rec)
	var rl []byte
	rl = pbytes(rl, 1, resource)
	rl = pbytes(rl, 2, scope)
	protoBody := pbytes(nil, 1, rl)

	jsonBody := fmt.Sprintf(`{"resourceLogs":[{"resource":{
		"attributes":[{"key":"service.name","value":{"stringValue":"api"}}],
		"droppedAttributesCount":%d},
		"scopeLogs":[{"logRecords":[
			{"timeUnixNano":"%d","body":{"stringValue":"m"}}]}]}]}`, dropped, atNano)

	jr, _ := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogs(w, []byte(jsonBody), func() int64 { return 0 })
	})
	pr, _ := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, protoBody, func() int64 { return 0 }, nil)
	})
	if len(jr) != 1 || len(pr) != 1 {
		t.Fatalf("json %d rows, proto %d rows", len(jr), len(pr))
	}
	jf, pf := fieldsOfRow(jr[0]), fieldsOfRow(pr[0])
	const key = "resource_dropped_attributes_count"
	if jf[key] != "7" {
		t.Fatalf("the JSON path did not store %s: %v", key, jf)
	}
	if pf[key] != jf[key] {
		t.Errorf("protobuf stored %s=%q, JSON stored %q. The same logical "+
			"export stores different fields depending on which encoding the "+
			"collector was configured for, and protobuf is the default",
			key, pf[key], jf[key])
	}
}

// An OTLP/JSON trace_id is hex, and hex is case-insensitive, so the same ID
// arrives uppercase or lowercase depending on the exporter.
//
// The protobuf path carries raw bytes and renders them lowercase. The JSON path
// stored whatever string it was given, so the identical trace stored two
// different values and a query for one found neither half of the other's
// records. Nothing validated it either: `not-hex-at-all!!` went in verbatim.
func TestATraceIDIsNormalizedTheSameWayInBothEncodings(t *testing.T) {
	const atNano = 1_700_000_000_000_000_000
	raw := []byte{0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
		0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89}
	span := []byte{0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10}

	var rec []byte
	rec = pfixed64(rec, 1, atNano)
	rec = pbytes(rec, 5, anyString("m"))
	rec = pbytes(rec, 9, raw)   // trace_id
	rec = pbytes(rec, 10, span) // span_id
	var scope []byte
	scope = pbytes(scope, 2, rec)
	var rl []byte
	rl = pbytes(rl, 1, pbytes(nil, 1, kv("service.name", anyString("api"))))
	rl = pbytes(rl, 2, scope)
	protoBody := pbytes(nil, 1, rl)

	pr, _ := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, protoBody, func() int64 { return 0 }, nil)
	})
	if len(pr) != 1 {
		t.Fatalf("proto stored %d rows", len(pr))
	}
	want := fieldsOfRow(pr[0])

	// The same IDs, spelled in UPPER case, which is what an exporter using
	// Go's %X or Java's toHexString produces.
	for _, spelling := range []struct{ name, trace, span string }{
		{"upper", strings.ToUpper(hex.EncodeToString(raw)), strings.ToUpper(hex.EncodeToString(span))},
		{"lower", hex.EncodeToString(raw), hex.EncodeToString(span)},
		{"mixed", "AbCdEf0123456789aBcDeF0123456789", "FeDcBa9876543210"},
	} {
		t.Run(spelling.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"resourceLogs":[{"resource":{
				"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
				"scopeLogs":[{"logRecords":[{"timeUnixNano":"%d",
					"body":{"stringValue":"m"},"traceId":%q,"spanId":%q}]}]}]}`,
				atNano, spelling.trace, spelling.span)
			jr, _ := rowsOf(t, func(w *Writer) (Result, error) {
				return IngestOTLPLogs(w, []byte(body), func() int64 { return 0 })
			})
			if len(jr) != 1 {
				t.Fatalf("json stored %d rows", len(jr))
			}
			got := fieldsOfRow(jr[0])
			if got["trace_id"] != want["trace_id"] {
				t.Errorf("trace_id: JSON %q, protobuf %q. The same trace stored "+
					"under two spellings means a query for one finds neither "+
					"half of the other's records", got["trace_id"], want["trace_id"])
			}
			if got["span_id"] != want["span_id"] {
				t.Errorf("span_id: JSON %q, protobuf %q", got["span_id"], want["span_id"])
			}
		})
	}
}

// A traceId that is not hex at all is not stored as if it were one.
//
// OTLP/JSON says trace_id is hex. A value that is not gets stored verbatim,
// so a field a query treats as an identifier holds arbitrary text -- including
// text that came from outside the cluster.
func TestATraceIDThatIsNotHexIsNotStoredAsOne(t *testing.T) {
	const atNano = 1_700_000_000_000_000_000
	for _, bad := range []string{
		"not-hex-at-all!!",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"0123",                             // hex, but not 16 bytes
		strings.Repeat("ab", 64),           // hex, far too long
		"0123456789abcdef0123456789abcdeg", // one bad nibble
	} {
		t.Run(bad, func(t *testing.T) {
			body := fmt.Sprintf(`{"resourceLogs":[{"resource":{
				"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
				"scopeLogs":[{"logRecords":[{"timeUnixNano":"%d",
					"body":{"stringValue":"m"},"traceId":%q}]}]}]}`, atNano, bad)
			rows, _ := rowsOf(t, func(w *Writer) (Result, error) {
				return IngestOTLPLogs(w, []byte(body), func() int64 { return 0 })
			})
			if len(rows) != 1 {
				t.Fatalf("stored %d rows", len(rows))
			}
			if got := fieldsOfRow(rows[0])["trace_id"]; got == bad {
				t.Errorf("stored trace_id=%q verbatim. OTLP/JSON says this "+
					"field is hex; storing whatever arrived puts arbitrary "+
					"external text in a column queries treat as an identifier", got)
			}
			// The record itself is kept: one bad field is not a reason to drop
			// a log line.
			if got := fieldsOfRow(rows[0])["_msg"]; got != "m" {
				t.Errorf("the record was lost over a bad trace_id: _msg=%q", got)
			}
		})
	}
}

// Past the nesting bound the two encodings still agree, and the value says it
// was cut.
//
// This started as a check that the DEPTH bound marks its truncation the way
// maxCompositeBytes marks its own. It does not -- and it turns out it cannot be
// seen to, which is the finding.
//
// A nested array renders by stringifying each level and then escaping it as a
// string value at the next, so the escaping roughly doubles the output per
// level: by about level 13 the value is past the 64 KiB budget, and the byte
// bound therefore ALWAYS fires before the 16-level depth bound can. Measured at
// 22 levels: 512 KB of escaped brackets, marked `"...truncated"` by the byte
// budget. The depth bound is a STACK guard during decode, not an output guard,
// and its cut is not separately observable through any array or kvlist fixture.
//
// The first version of this test asserted `strings.Contains(value,
// "truncated")` and passed -- on the byte budget's marker, not the depth
// bound's, which does not exist. That is the shape of a test that agrees with
// whatever the code does.
//
// So it pins what is true and checkable: both encodings produce the SAME value
// past the bound, the value is marked, and the record survives. An operator
// switching their collector's encoding must not see the attribute change.
func TestNestingPastTheBoundIsTheSameInBothEncodings(t *testing.T) {
	const depth = maxAnyValueDepth + 6
	const atNano = 1_700_000_000_000_000_000

	inner := anyString("leaf")
	for i := 0; i < depth; i++ {
		inner = anyArray(inner)
	}
	var rec []byte
	rec = pfixed64(rec, 1, atNano)
	rec = pbytes(rec, 5, anyString("m"))
	rec = pbytes(rec, 6, kv("deep", inner))
	var scope []byte
	scope = pbytes(scope, 2, rec)
	var rl []byte
	rl = pbytes(rl, 1, pbytes(nil, 1, kv("service.name", anyString("api"))))
	rl = pbytes(rl, 2, scope)
	protoBody := pbytes(nil, 1, rl)

	jsonInner := `{"stringValue":"leaf"}`
	for i := 0; i < depth; i++ {
		jsonInner = `{"arrayValue":{"values":[` + jsonInner + `]}}`
	}
	jsonBody := fmt.Sprintf(`{"resourceLogs":[{"resource":{
		"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
		"scopeLogs":[{"logRecords":[{"timeUnixNano":"%d",
			"body":{"stringValue":"m"},
			"attributes":[{"key":"deep","value":%s}]}]}]}]}`, atNano, jsonInner)

	pr, pres := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, protoBody, func() int64 { return 0 }, nil)
	})
	jr, jres := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogs(w, []byte(jsonBody), func() int64 { return 0 })
	})
	if len(pr) != 1 || len(jr) != 1 {
		t.Fatalf("proto %d rows, json %d rows", len(pr), len(jr))
	}
	if pres.Accepted != 1 || jres.Accepted != 1 {
		t.Fatalf("accepted proto=%d json=%d, want 1 each", pres.Accepted, jres.Accepted)
	}
	pv, jv := fieldsOfRow(pr[0])["deep"], fieldsOfRow(jr[0])["deep"]
	if pv == "" || jv == "" {
		t.Fatalf("the over-nested attribute vanished entirely: proto %d bytes, "+
			"json %d bytes. It is meant to be truncated, not dropped", len(pv), len(jv))
	}
	if pv != jv {
		t.Errorf("the two encodings disagree past the nesting bound: proto %d "+
			"bytes, json %d bytes. An operator switching their collector's "+
			"encoding sees the attribute change", len(pv), len(jv))
	}
	// The byte budget is what fires, and it is what marks. Bounded by it too:
	// the 64 KiB budget is on the value, and this asserts the row did not
	// somehow carry the full 512 KB.
	if !strings.Contains(pv, "truncated") {
		t.Errorf("the cut value carries no marker: %.120q", pv)
	}
	if len(pv) > 4*maxCompositeBytes {
		t.Errorf("the value is %d bytes against a %d-byte budget; the escaping "+
			"blowup is not being bounded", len(pv), maxCompositeBytes)
	}
}

// The renderer's refusal, exercised. Nothing in any fixture carries a `|`
// today, so deleting the check left the whole package green -- which is the
// state a tripwire is in right up until the day it matters.
func TestTheRowRendererRefusesAnAmbiguousField(t *testing.T) {
	for _, c := range []struct {
		key, value string
		bad        bool
	}{
		{"_msg", "x|severity=ERROR", true}, // the phantom-field case
		{"we|ird", "v", true},              // the key side
		{"_msg", "a plain message", false}, // and the control: ordinary data
		{"_msg", "an = sign is fine", false},
		{"_msg", "", false},
	} {
		err := rowRenderable(c.key, c.value)
		if c.bad && err == nil {
			t.Errorf("%q=%q was accepted; rendering it produces a row that "+
				"parses into fields nobody stored", c.key, c.value)
		}
		if !c.bad && err != nil {
			t.Errorf("%q=%q was refused: %v. Ordinary data must render",
				c.key, c.value, err)
		}
	}
}

// A GOLDEN, hand-derived from the protobuf spec rather than from this
// package's own writer.
//
// Every other protobuf assertion here is circular for the WIRE SHAPE: the
// fixture is built by pbytes/pvarint/kv/anyString in this repository and
// decoded by eachField/decodeAnyValue in this repository, so a matched pair of
// bugs -- a field number wrong in both, a wire type wrong in both -- produces a
// green suite and a decoder that cannot read what a real collector sends.
// Nothing in the package could tell the difference.
//
// These 32 bytes were written out by hand from the field numbers in
// opentelemetry/proto/logs/v1/logs.proto and common/v1/common.proto:
//
//	0a 1e                                  ExportLogsServiceRequest.resource_logs (1, LEN 30)
//	  0a 0a                                ResourceLogs.resource (1, LEN 10)
//	    0a 08                              Resource.attributes (1, LEN 8)
//	      0a 01 61                         KeyValue.key (1, LEN 1) = "a"
//	      12 03                            KeyValue.value (2, LEN 3)
//	        0a 01 62                       AnyValue.string_value (1, LEN 1) = "b"
//	  12 10                                ResourceLogs.scope_logs (2, LEN 16)
//	    12 0e                              ScopeLogs.log_records (2, LEN 14)
//	      09 0100000000000000              LogRecord.time_unix_nano (1, I64) = 1
//	      2a 03                            LogRecord.body (5, LEN 3)
//	        0a 01 6d                       AnyValue.string_value (1, LEN 1) = "m"
//
// Two assertions, and the pair is what closes the loop: the DECODER must read
// these bytes, and the fixture BUILDER must produce exactly them. Either alone
// leaves the other side free to drift.
const otlpGoldenHex = "0a1e0a0a0a080a01611203" + "0a01621210120e09" +
	"01000000000000002a030a016d"

func TestTheProtobufDecoderReadsBytesThisPackageDidNotWrite(t *testing.T) {
	golden, err := hex.DecodeString(otlpGoldenHex)
	if err != nil {
		t.Fatalf("the golden is not hex: %v", err)
	}
	if len(golden) != 32 {
		t.Fatalf("the golden is %d bytes, and the derivation above says 32", len(golden))
	}
	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestOTLPLogsProto(w, golden, func() int64 { return 0 }, nil)
	})
	if res.Accepted != 1 || len(rows) != 1 {
		t.Fatalf("accepted %d, stored %d rows, want 1 each: the decoder cannot "+
			"read a message written from the spec", res.Accepted, len(rows))
	}
	got := fieldsOfRow(rows[0])
	if got["a"] != "b" {
		t.Errorf(`resource attribute a=%q, want "b"`, got["a"])
	}
	if got["_msg"] != "m" {
		t.Errorf(`_msg=%q, want "m"`, got["_msg"])
	}
}

func TestTheFixtureBuilderProducesTheGoldenBytes(t *testing.T) {
	var resource []byte
	resource = pbytes(resource, 1, kv("a", anyString("b")))
	var rec []byte
	rec = pfixed64(rec, 1, 1)
	rec = pbytes(rec, 5, anyString("m"))
	var scope []byte
	scope = pbytes(scope, 2, rec)
	var rl []byte
	rl = pbytes(rl, 1, resource)
	rl = pbytes(rl, 2, scope)
	built := pbytes(nil, 1, rl)

	if got := hex.EncodeToString(built); got != otlpGoldenHex {
		t.Errorf("the fixture builder does not produce the spec's bytes:\n"+
			"  built  %s\n  golden %s\n"+
			"Every other protobuf test in this file uses the builder and the "+
			"decoder together, so a field number or wire type wrong in BOTH is "+
			"invisible to all of them. This is the assertion that is not "+
			"circular.", got, otlpGoldenHex)
	}
}
