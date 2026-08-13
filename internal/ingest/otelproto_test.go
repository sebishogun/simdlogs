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
	if n, _ := IngestOTLPLogs(wJSON, []byte(jsonExport), func() int64 { return 42 }); n != 2 {
		t.Fatalf("JSON path ingested %d records, want 2", n)
	}
	wJSON.Close()

	stPB, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stPB.Close()
	wPB := NewWriter(stPB)
	if n, _ := IngestOTLPLogsProto(wPB, BuildTestOTLPProtoExport(), func() int64 { return 42 }, nil); n != 2 {
		t.Fatalf("protobuf path ingested %d records, want 2", n)
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
