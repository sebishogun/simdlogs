package ingest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

const jsonExport = `{"resourceLogs":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"api"}},
  {"key":"host","value":{"stringValue":"h1"}}]},
 "scopeLogs":[{"logRecords":[
  {"timeUnixNano":"1700000000000000000","severityText":"ERROR",
   "body":{"stringValue":"boom happened"},
   "attributes":[{"key":"code","value":{"intValue":"500"}},
                 {"key":"ratio","value":{"doubleValue":0.5}},
                 {"key":"retry","value":{"boolValue":true}}]},
  {"observedTimeUnixNano":"1700000001000000000","body":{"stringValue":"second"}}
 ]}]}]}`

// storeRows renders every stored record as "time|k=v;k=v" lines, sorted.
// storeRows renders every stored row as `<time>|k=v|k=v...`, sorted.
//
// It REFUSES a key or value containing the separator, which is the whole
// reason this note exists. fieldsOfRow reverses the rendering by splitting on
// `|`, so a value like `x|severity=ERROR` did not merely truncate the field it
// belonged to -- it INVENTED a `severity` field, and an assertion that the row
// carries severity=ERROR then passed on a row that has no severity at all.
//
// Splitting cannot be made safe: a log value can hold any byte, so no
// separator is reserved. What can be made safe is the fixture. A test that
// genuinely needs a `|` in a value has to compare structured fields instead,
// and this failure is where it finds that out -- loudly, rather than by
// quietly agreeing with it.
func storeRows(t *testing.T, st *storage.Store) []string {
	t.Helper()
	q, err := query.ParseLogsQL("*")
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To, q.MatAll = 0, int64(1)<<62, true
	var out []string
	for _, r := range query.RunPipeline(st, q) {
		fs := make([]string, 0, len(r.Fields))
		for _, f := range r.Fields {
			if err := rowRenderable(f.Key, f.Value); err != nil {
				t.Fatal(err)
			}
			fs = append(fs, f.Key+"="+f.Value)
		}
		sort.Strings(fs)
		out = append(out, strings.Join(append([]string{time60(r.Time)}, fs...), "|"))
	}
	sort.Strings(out)
	return out
}

func time60(t int64) string {
	b := make([]byte, 0, 20)
	return string(binary.AppendVarint(b, t))
}

// TestOTLPProtoMatchesJSON is the contract: the two encodings of the same
// export must store IDENTICAL records. The JSON path is the reference; a
// protobuf decode that differs is wrong however plausible.
func TestOTLPProtoMatchesJSON(t *testing.T) {
	stJSON, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stJSON.Close()
	wJSON := NewWriter(stJSON)
	if nRes, _ := IngestOTLPLogs(wJSON, []byte(jsonExport), func() int64 { return 42 }); nRes.Accepted != 2 {
		t.Fatalf("JSON path ingested %d records, want 2", nRes.Accepted)
	}
	wJSON.Close()

	stPB, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stPB.Close()
	wPB := NewWriter(stPB)
	if nRes, _ := IngestOTLPLogsProto(wPB, BuildTestOTLPProtoExport(), func() int64 { return 42 }, nil); nRes.Accepted != 2 {
		t.Fatalf("protobuf path ingested %d records, want 2", nRes.Accepted)
	}
	wPB.Close()

	j, p := storeRows(t, stJSON), storeRows(t, stPB)
	if len(j) != len(p) {
		t.Fatalf("JSON stored %d rows, protobuf %d", len(j), len(p))
	}
	for i := range j {
		if j[i] != p[i] {
			t.Errorf("row %d differs:\n  json:  %s\n  proto: %s", i, j[i], p[i])
		}
	}
}

// TestOTLPProtoMalformed: a truncated or garbage body must not panic and must
// not invent records.
func TestOTLPProtoMalformed(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()
	full := BuildTestOTLPProtoExport()
	for n := 0; n < len(full); n += 3 {
		IngestOTLPLogsProto(w, full[:n], func() int64 { return 1 }, nil)
	}
	IngestOTLPLogsProto(w, []byte("not protobuf at all"), func() int64 { return 1 }, nil)
}

// Protobuf is OTLP's default encoding, and this parser used to accept
// anything: a body that decoded to no records returned success, so an
// exporter sending garbage -- or aimed at the wrong signal path -- was told
// its data was delivered.
func TestOTLPProtoRejectsUndecodableAndEmptyPayloads(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, c := range []struct {
		name string
		body []byte
	}{
		{"pure garbage", []byte{0xff, 0xff, 0xff, 0xff}},
		{"truncated length prefix", []byte{0x0a, 0x05, 0xff}},
		{"json mislabelled as protobuf", []byte(`{"resourceLogs":[]}`)},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := NewWriter(st)
			defer w.Close()
			res, err := IngestOTLPLogsProto(w, c.body, func() int64 { return 1 }, nil)
			if err == nil {
				t.Fatalf("accepted with %d records and no error", res.Accepted)
			}
			if res.Accepted != 0 {
				t.Fatalf("reported %d records accepted from a rejected body", res.Accepted)
			}
		})
	}

	// A well-formed envelope carrying no records is a success too, not just
	// an empty body. OTLP requires it, exporters treat 4xx as permanent, and
	// ExportMetricsServiceRequest has the same field 1 / wire 2 shape, so
	// there is nothing in an empty envelope to discriminate on. A metrics
	// payload with actual data points is still rejected -- by its records,
	// which is where the signal lives (TestOTLPProtoRejectsMetricsAndTraces).
	for _, empty := range [][]byte{
		{0x0a, 0x02, 0x12, 0x00}, // ResourceLogs{ScopeLogs{}}
		{0x0a, 0x00},             // ResourceLogs{}
	} {
		w := NewWriter(st)
		res, err := IngestOTLPLogsProto(w, empty, func() int64 { return 1 }, nil)
		if err != nil {
			t.Fatalf("an empty export was rejected: %v", err)
		}
		if res.Accepted != 0 || res.Rejected != 0 {
			t.Fatalf("empty export reported %d accepted, %d rejected", res.Accepted, res.Rejected)
		}
		w.Close()
	}

	// An empty body stays a success: it is a valid empty export.
	w := NewWriter(st)
	defer w.Close()
	if _, err := IngestOTLPLogsProto(w, nil, func() int64 { return 1 }, nil); err != nil {
		t.Fatalf("an empty export was rejected: %v", err)
	}
}

// pbField builds one protobuf field.
func pbField(num, wire int, payload []byte) []byte {
	out := binary.AppendUvarint(nil, uint64(num)<<3|uint64(wire))
	switch wire {
	case 1:
		return append(out, payload...) // caller supplies 8 bytes
	case 2:
		out = binary.AppendUvarint(out, uint64(len(payload)))
		return append(out, payload...)
	}
	return append(out, payload...)
}

func fixed64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// Metrics and traces posted to /v1/logs must be refused, not stored as logs.
//
// ResourceMetrics, ResourceSpans and ResourceLogs use the same field numbers
// -- resource=1, scope=2, and Metric/Span/LogRecord all at field 2 of the
// scope -- so counting records cannot tell them apart: a metrics export
// walked as logs and stored one bogus row per metric with a 200. The wire
// type of field 1 does separate them: a LogRecord's time_unix_nano is a
// fixed64 where Metric.name and Span.trace_id are length-delimited.
func TestOTLPProtoRejectsMetricsAndTraces(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// The three signals nest identically:
	//   Export*ServiceRequest{ resource_* = 1 }
	//   Resource*{ resource = 1, scope_* = 2 }
	//   Scope*{ scope = 1, metrics/spans/log_records = 2 }
	// so only the leaf's field-1 wire type differs.

	// Metric{ name: "cpu" } -- field 1 length-delimited.
	resourceMetrics := pbField(1, 2, pbField(2, 2, pbField(2, 2, pbField(1, 2, []byte("cpu")))))

	// Span{ trace_id: 16 bytes } -- field 1 length-delimited.
	resourceSpans := pbField(1, 2, pbField(2, 2, pbField(2, 2, pbField(1, 2, make([]byte, 16)))))

	// LogRecord{ time_unix_nano } -- field 1 fixed64.
	resourceLogs := pbField(1, 2, pbField(2, 2, pbField(2, 2,
		pbField(1, 1, fixed64(1_700_000_000_000_000_000)))))

	for _, c := range []struct {
		name    string
		body    []byte
		wantErr bool
	}{
		{"metrics export", resourceMetrics, true},
		{"traces export", resourceSpans, true},
		{"logs export", resourceLogs, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := NewWriter(st)
			defer w.Close()
			res, err := IngestOTLPLogsProto(w, c.body, func() int64 { return 1 }, nil)
			if c.wantErr {
				if err == nil {
					t.Fatalf("accepted, storing %d rows", res.Accepted)
				}
				if res.Accepted != 0 {
					t.Fatalf("stored %d rows from a rejected payload", res.Accepted)
				}
				return
			}
			if err != nil {
				t.Fatalf("a real logs export was rejected: %v", err)
			}
			if res.Accepted != 1 {
				t.Fatalf("accepted %d records, want 1", res.Accepted)
			}
		})
	}
}

// rowRenderable rejects a key or value that would make the rendered row
// ambiguous. Separate from storeRows so it can be tested directly: inside a
// t.Fatal nothing can reach it, and a refusal nothing exercises is one nobody
// knows still works.
func rowRenderable(key, value string) error {
	if strings.ContainsRune(key, '|') || strings.ContainsRune(value, '|') {
		return fmt.Errorf("field %q=%q contains the row separator. fieldsOfRow "+
			"splits on it, so this row would parse into fields that are not in "+
			"it -- including a whole field nobody stored. Compare the "+
			"structured fields for this fixture instead", key, value)
	}
	return nil
}

// A time_unix_nano PAST 2262 IS REFUSED, NOT WRAPPED INTO THE PAST.
//
// A proto3 `fixed64` is UNSIGNED. `int64(binary.LittleEndian.Uint64(fp))` was
// raw, so every instant after 2262-04-11 -- a legal value of this field, and
// what a producer with a broken clock or a nanosecond/millisecond unit mixup
// sends -- became a NEGATIVE nanosecond count and the record was stored,
// counted and filed before 1970.
//
// This is `/v1/logs` with `Content-Type: application/x-protobuf`, which is the
// OpenTelemetry Collector's DEFAULT encoding. The round that fixed the JSON
// encoding of the same export (otel.go, through parseTime) left the file
// beside it -- commit a92b638's finding again, "two OTLP encodings of one
// export were storing different things", which is why this asserts the two
// encodings AGREE rather than just that the protobuf one refuses.
func TestAnUnstorableProtobufTimestampIsRefused(t *testing.T) {
	// 2^63 exactly: one nanosecond past MaxInt64, and the smallest fixed64
	// this must refuse. The old conversion turned it into MinInt64.
	const past2262 = uint64(1) << 63

	for _, tc := range []struct {
		name               string
		field              int // 1 = time_unix_nano, 11 = observed_time_unix_nano
		ts                 uint64
		accepted, rejected int
		// stored is separate from accepted because every query window in this
		// tree is half-open: a row AT MaxInt64 is accepted and stored and no
		// `[from, to)` can name it. That is a pre-existing property of the
		// window, not of this refusal, and folding the two numbers together
		// would have reported the boundary control as a lost row.
		stored int
	}{
		{"time_unix_nano at 2^63", 1, past2262, 0, 1, 0},
		{"time_unix_nano at MaxUint64", 1, ^uint64(0), 0, 1, 0},
		{"observed_time_unix_nano at 2^63", 11, past2262, 0, 1, 0},
		// The controls. MaxInt64 is the last storable nanosecond and must
		// still be accepted: a refusal one comparison wide the wrong way would
		// satisfy every row above.
		{"time_unix_nano at MaxInt64 (control)", 1, uint64(math.MaxInt64), 1, 0, 0},
		{"time_unix_nano one ns below MaxInt64 (control)", 1, uint64(math.MaxInt64) - 1, 1, 0, 1},
		{"an ordinary instant (control)", 1, 1_700_000_000_000_000_000, 1, 0, 1},
		{"observed_time_unix_nano at MaxInt64 (control)", 11, uint64(math.MaxInt64), 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec []byte
			rec = pfixed64(rec, tc.field, tc.ts)
			rec = pbytes(rec, 5, anyString("body"))
			var scope []byte
			scope = pbytes(scope, 2, rec)
			var rl []byte
			rl = pbytes(rl, 2, scope)
			body := pbytes(nil, 1, rl)

			st, err := storage.OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			w := NewWriter(st)
			res, err := IngestOTLPLogsProto(w, body, func() int64 { return 42 }, nil)
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			if res.Accepted != tc.accepted || res.Rejected != tc.rejected {
				t.Errorf("accepted=%d rejected=%d, want %d/%d.\n"+
					"A fixed64 is UNSIGNED: an instant past 2262 is a legal wire "+
					"value, and converting it raw files the row before 1970.",
					res.Accepted, res.Rejected, tc.accepted, tc.rejected)
			}
			// The WIDEST window, not storeRows': storeRows stops at 1<<62
			// (the year 2116) and the MaxInt64 controls are stored above it,
			// so reusing it would have reported a stored row as missing.
			if got := len(rowsOverTheWholeDomain(t, st)); got != tc.stored {
				t.Errorf("the store holds %d rows over the whole int64 domain, want %d",
					got, tc.stored)
			}
		})
	}
}

// THE TWO ENCODINGS OF ONE EXPORT MUST AGREE ABOUT AN UNSTORABLE TIMESTAMP.
//
// The JSON path reads `timeUnixNano` as a decimal string through parseTime;
// the protobuf path reads a fixed64. Same export, same instant, and until this
// round the JSON one refused it while the protobuf one stored it before 1970.
func TestTheTwoOTLPEncodingsAgreeOnAnUnstorableTimestamp(t *testing.T) {
	const ns = "9223372036854775808" // 2^63: one past MaxInt64

	stJSON, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stJSON.Close()
	wJSON := NewWriter(stJSON)
	jRes, err := IngestOTLPLogs(wJSON, []byte(
		`{"resourceLogs":[{"scopeLogs":[{"logRecords":[`+
			`{"timeUnixNano":"`+ns+`","body":{"stringValue":"body"}}]}]}]}`),
		func() int64 { return 42 })
	if err != nil {
		t.Fatal(err)
	}
	wJSON.Close()

	var rec []byte
	rec = pfixed64(rec, 1, uint64(1)<<63)
	rec = pbytes(rec, 5, anyString("body"))
	var scope []byte
	scope = pbytes(scope, 2, rec)
	var rl []byte
	rl = pbytes(rl, 2, scope)

	stPB, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stPB.Close()
	wPB := NewWriter(stPB)
	pRes, err := IngestOTLPLogsProto(wPB, pbytes(nil, 1, rl), func() int64 { return 42 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	wPB.Close()

	if jRes.Accepted != pRes.Accepted || jRes.Rejected != pRes.Rejected {
		t.Errorf("JSON accepted=%d rejected=%d, protobuf accepted=%d rejected=%d.\n"+
			"One export, two encodings, two different answers about the same instant.",
			jRes.Accepted, jRes.Rejected, pRes.Accepted, pRes.Rejected)
	}
	if jRes.Rejected != 1 {
		t.Errorf("the JSON path accepted an instant past MaxInt64: %+v", jRes)
	}
	if j, p := storeRows(t, stJSON), storeRows(t, stPB); len(j) != len(p) {
		t.Errorf("JSON stored %d rows, protobuf %d: %v vs %v", len(j), len(p), j, p)
	}
}

// TestUvarintMatchesBinaryAcrossTheBoundary: the helper must decode exactly
// what binary.Uvarint does. The values cross the boundary where a varint
// stops being its own byte, plus the two's-complement negatives a signed
// int32/int64 field (severity_number, Loki nanos, int attrs) encodes as a
// 10-byte varint.
func TestUvarintMatchesBinaryAcrossTheBoundary(t *testing.T) {
	for _, v := range []uint64{0, 1, 7, 127, 128, 129, 200, 255, 300, 1000,
		65535, 1 << 20, 1<<32 - 1, 1 << 32, 1 << 63, 1<<63 + 1, ^uint64(0)} {
		enc := binary.AppendUvarint(nil, v)
		if got := uvarint(enc); got != v {
			t.Fatalf("uvarint(%x) = %d, want %d", enc, got, v)
		}
	}
	// The signed path: proto3 encodes a negative int32/int64 as the uint64
	// two's-complement cast, and the call sites decode then cast. The cast
	// must round-trip the wire value.
	for _, s := range []int64{-1, -128, -129, -1 << 31, -1 << 63, math.MinInt64} {
		enc := binary.AppendUvarint(nil, uint64(s))
		if got := int64(uvarint(enc)); got != s {
			t.Fatalf("int64(uvarint(%x)) = %d, want %d", enc, got, s)
		}
	}
	// The int32 path (Loki Timestamp.nanos): the wire carries the int32
	// sign-extended, so a value past int32 truncates on the wire -- MinInt64
	// as an int32 field IS zero, and the receiver's int32 cast must recover
	// what the client sent.
	for _, s := range []int64{-1, -128, -129, -1 << 31, math.MinInt32, math.MaxInt32} {
		enc := binary.AppendUvarint(nil, uint64(int32(s)))
		if got := int64(int32(uvarint(enc))); got != s {
			t.Fatalf("int64(int32(uvarint(%x))) = %d, want %d", enc, got, s)
		}
	}
}

// rowsOverTheWholeDomain is storeRows' window widened to every instant an int64
// nanosecond count can hold. storeRows uses [0, 1<<62), which stops in 2116 --
// fine for its fixtures and wrong for a timestamp near MaxInt64, where a row
// that IS stored reads as a row that is not.
func rowsOverTheWholeDomain(t *testing.T, st *storage.Store) []query.Row {
	t.Helper()
	q, err := query.ParseLogsQL("*")
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To, q.MatAll = math.MinInt64, math.MaxInt64, true
	return query.RunPipeline(st, q)
}

// TestEachFieldVarintZeroAlloc: the wire-type-0 path must allocate nothing.
//
// The old shape re-encoded every varint field's value into a local [8]byte
// buffer whose address escaped into the callback ("moved to heap: buf" in
// -gcflags=-m), so eachField allocated once per varint field. Loki entries
// carry seconds+nanos varints; OTLP records carry severity_number and the
// dropped counts -- a per-field allocation on the default encodings of both
// protocols. AllocsPerRun over a message of NOTHING but varint fields
// isolates exactly that allocation: no protocol allocation (strings, maps,
// slices) is inside the measured call, so the count is the field loop's own.
func TestEachFieldVarintZeroAlloc(t *testing.T) {
	var msg []byte
	for i := 0; i < 64; i++ {
		msg = pvarint(msg, 1, uint64(i+1))
	}
	fn := func(num, wire int, payload []byte) {}
	if got := testing.AllocsPerRun(100, func() { eachField(msg, fn) }); got != 0 {
		t.Fatalf("eachField allocates %.0f objects per 64-varint-field walk (want 0): "+
			"the wire-type-0 payload is re-encoded into an escaping buffer, once per field", got)
	}
}

// TestEachFieldHandsRawVarintBytes pins the wire-type-0 payload contract: the
// callback receives the RAW varint bytes from the message, not a re-encoded
// 8-byte little-endian image. The values cross the boundary where a varint
// stops being its own byte -- that is where a decode reading the wrong shape
// goes wrong (docs/wrong.md entry 105).
func TestEachFieldHandsRawVarintBytes(t *testing.T) {
	for _, v := range []uint64{1, 127, 128, 129, 200, 255, 65535, 1 << 32, 1<<63 + 1, ^uint64(0)} {
		var msg []byte
		msg = pvarint(msg, 3, v)
		var payload []byte
		eachField(msg, func(num, wire int, p []byte) {
			if num != 3 || wire != 0 {
				t.Fatalf("callback saw field %d wire %d, want 3/0", num, wire)
			}
			payload = append(payload[:0], p...)
		})
		want := binary.AppendUvarint(nil, v)
		if !bytes.Equal(payload, want) {
			t.Fatalf("value %d: callback payload %x, want the raw varint %x", v, payload, want)
		}
	}
}
