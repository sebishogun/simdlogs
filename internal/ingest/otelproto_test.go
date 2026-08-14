package ingest

import (
	"encoding/binary"
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
