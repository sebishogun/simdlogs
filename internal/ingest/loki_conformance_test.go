package ingest

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/snappy"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Loki conformance: the JSON push API and the snappy-protobuf PushRequest are
// the same protocol in two encodings, and Promtail / Grafana Alloy / the
// Grafana Agent all send the PROTOBUF one by default.
//
// Before this: the protobuf encoding was not read at all. A default-configured
// agent's push is a snappy blob, which is not JSON, so it failed the JSON
// decode and the agent was answered 400 -- and the route's media-type gate did
// not even list application/x-protobuf, so it was rejected before that. The
// third element of a JSON entry, Loki 3.x's structured metadata, was
// documented as "which we ignore" and dropped: a trace id sent there was
// accepted with a 204 and then was not in the store.

// ---- protobuf fixture writer for logproto.PushRequest ----

func lokiEntry(seconds, nanos int64, line string, meta ...[2]string) []byte {
	var ts []byte
	if seconds != 0 {
		ts = pvarint(ts, 1, uint64(seconds))
	}
	if nanos != 0 {
		ts = pvarint(ts, 2, uint64(nanos))
	}
	var ent []byte
	if len(ts) > 0 {
		ent = pbytes(ent, 1, ts)
	}
	ent = pstring(ent, 2, line)
	for _, kv := range meta {
		var pair []byte
		pair = pstring(pair, 1, kv[0])
		pair = pstring(pair, 2, kv[1])
		ent = pbytes(ent, 3, pair)
	}
	return ent
}

func lokiStream(labels string, entries ...[]byte) []byte {
	var st []byte
	st = pstring(st, 1, labels)
	for _, e := range entries {
		st = pbytes(st, 2, e)
	}
	return st
}

func lokiPushProto(streams ...[]byte) []byte {
	var req []byte
	for _, st := range streams {
		req = pbytes(req, 1, st)
	}
	return snappy.Encode(nil, req)
}

// The two encodings of ONE logical push must store identical rows.
func TestLokiJSONAndProtoNormalizeIdentically(t *testing.T) {
	const (
		secs  = 1714521600
		nanos = 123456789
	)
	line := "payment declined"

	jsonBody := fmt.Sprintf(`{"streams":[{
		"stream":{"app":"checkout","env":"prod"},
		"values":[["%d%09d", %q, {"trace_id":"abc123","span_id":"def456"}]]}]}`,
		secs, nanos, line)

	protoBody := lokiPushProto(lokiStream(`{app="checkout", env="prod"}`,
		lokiEntry(secs, nanos, line,
			[2]string{"trace_id", "abc123"}, [2]string{"span_id", "def456"})))

	fb := func() int64 { return 1 }
	jsonRows, jres := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestLokiOpts(w, []byte(jsonBody), fb, nil)
	})
	protoRows, pres := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestLokiProto(w, protoBody, fb, nil)
	})

	if jres.Accepted != 1 || pres.Accepted != 1 {
		t.Fatalf("accepted: JSON %d, protobuf %d, want 1 each", jres.Accepted, pres.Accepted)
	}
	if len(jsonRows) != len(protoRows) {
		t.Fatalf("JSON stored %d rows, protobuf %d", len(jsonRows), len(protoRows))
	}
	for i := range jsonRows {
		if jsonRows[i] != protoRows[i] {
			t.Errorf("row %d differs:\n  JSON:     %s\n  protobuf: %s", i, jsonRows[i], protoRows[i])
		}
	}
}

// Structured metadata reaches the store from BOTH encodings. Comparing the two
// against each other cannot catch a field both of them drop, which is what
// happened here, so the values are asserted.
func TestLokiStoresStructuredMetadata(t *testing.T) {
	const secs, nanos = 1714521600, 0
	jsonBody := fmt.Sprintf(`{"streams":[{"stream":{"app":"api"},
		"values":[["%d000000000","hello",{"trace_id":"t-1","user":"u-9"}]]}]}`, secs)
	protoBody := lokiPushProto(lokiStream(`{app="api"}`,
		lokiEntry(secs, nanos, "hello",
			[2]string{"trace_id", "t-1"}, [2]string{"user", "u-9"})))

	for _, enc := range []struct {
		name   string
		ingest func(*Writer) (Result, error)
	}{
		{"json", func(w *Writer) (Result, error) {
			return IngestLokiOpts(w, []byte(jsonBody), func() int64 { return 1 }, nil)
		}},
		{"proto", func(w *Writer) (Result, error) {
			return IngestLokiProto(w, protoBody, func() int64 { return 1 }, nil)
		}},
	} {
		t.Run(enc.name, func(t *testing.T) {
			rows, _ := rowsOf(t, enc.ingest)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			got := fieldsOfRow(rows[0])
			for k, v := range map[string]string{
				"app": "api", "trace_id": "t-1", "user": "u-9", "_msg": "hello",
			} {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// Structured metadata is MORE specific than the stream's labels, so it wins on
// a key collision. Both encodings must agree on that, or the same push stores
// different things depending on the agent's encoding.
func TestLokiStructuredMetadataOverridesLabels(t *testing.T) {
	const secs = 1714521600
	jsonBody := fmt.Sprintf(`{"streams":[{"stream":{"app":"from-label"},
		"values":[["%d000000000","m",{"app":"from-metadata"}]]}]}`, secs)
	protoBody := lokiPushProto(lokiStream(`{app="from-label"}`,
		lokiEntry(secs, 0, "m", [2]string{"app", "from-metadata"})))

	for _, enc := range []struct {
		name   string
		ingest func(*Writer) (Result, error)
	}{
		{"json", func(w *Writer) (Result, error) {
			return IngestLokiOpts(w, []byte(jsonBody), func() int64 { return 1 }, nil)
		}},
		{"proto", func(w *Writer) (Result, error) {
			return IngestLokiProto(w, protoBody, func() int64 { return 1 }, nil)
		}},
	} {
		t.Run(enc.name, func(t *testing.T) {
			rows, _ := rowsOf(t, enc.ingest)
			if got := fieldsOfRow(rows[0])["app"]; got != "from-metadata" {
				t.Errorf("app = %q, want from-metadata", got)
			}
		})
	}
}

// parseLokiLabels is the piece with no JSON counterpart: the protobuf encoding
// carries the whole label set as one Prometheus-syntax string. Every case here
// is a shape a real agent emits.
func TestParseLokiLabels(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    map[string]string
		wantErr bool
	}{
		{`{app="api"}`, map[string]string{"app": "api"}, false},
		{`{app="api", env="prod"}`, map[string]string{"app": "api", "env": "prod"}, false},
		{`{app="api",env="prod"}`, map[string]string{"app": "api", "env": "prod"}, false},
		{`{ app = "api" }`, map[string]string{"app": "api"}, false},
		{`{}`, map[string]string{}, false},
		// An ABSENT label set is an error, not an empty map. This test used to
		// pin the opposite while its own comment below stated the rule -- a row
		// with no identifying field at all is not something to store silently,
		// and VictoriaLogs errors on both (app/vlinsert/loki/pb.go).
		{``, nil, true},
		{`   `, nil, true},
		// Escapes: a log line in a label value is not hypothetical.
		{`{msg="a\"b"}`, map[string]string{"msg": `a"b`}, false},
		{`{msg="line1\nline2"}`, map[string]string{"msg": "line1\nline2"}, false},
		{`{p="c:\\tmp"}`, map[string]string{"p": `c:\tmp`}, false},
		// An unknown escape is an ERROR. The value goes through strconv.Unquote
		// now, because Loki writes it with strconv.Quote -- and Quote never
		// emits \q. The old hand-rolled switch invented a "keep both bytes"
		// rule that neither Loki nor VictoriaLogs has, while silently
		// corrupting the escapes Quote DOES emit (\x01, \u200b, \xff).
		{`{p="a\qb"}`, nil, true},
		// The escapes Quote actually produces, which the old switch mangled.
		{`{p="ctrl\x01char"}`, map[string]string{"p": "ctrl\x01char"}, false},
		{`{p="zero\u200bwidth"}`, map[string]string{"p": "zero\u200bwidth"}, false},
		// Label names as Loki's own client writes them: raw and unquoted, so
		// dots and dashes arrive as-is. Rejecting these discarded every entry
		// in the stream behind a 204.
		{`{app-name="x"}`, map[string]string{"app-name": "x"}, false},
		{`{service.name="x"}`, map[string]string{"service.name": "x"}, false},
		{`{k8s.pod.name="p-1"}`, map[string]string{"k8s.pod.name": "p-1"}, false},
		// Trailing content after the closing brace is not a label set.
		{`{app="a"}}`, nil, true},
		{`{app="a"}garbage`, nil, true},
		// A value containing the separators must not end the parse early.
		{`{msg="a,b}c{d"}`, map[string]string{"msg": "a,b}c{d"}, false},
		// Prometheus label names allow underscores and colons.
		{`{a_b:c="v"}`, map[string]string{"a_b:c": "v"}, false},
		// Malformed: each must be an error, not a silently empty label set,
		// because an empty one stores a row with no identifying fields at all.
		{`{app=api}`, nil, true},         // unquoted value
		{`{app="api"`, nil, true},        // unterminated set
		{`{app="api`, nil, true},         // unterminated value
		{`{="v"}`, nil, true},            // no name
		{`app="api"`, nil, true},         // no braces
		{`{app="a" env="b"}`, nil, true}, // missing comma
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseLokiLabels(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("no error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLokiLabels: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// A body that is not snappy, or is snappy over garbage, must be an error --
// never zero records and a 204, which is what tells a misconfigured agent its
// data was delivered.
func TestLokiProtoRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"not snappy", []byte("this is plain text, not a snappy block")},
		{"snappy over garbage", snappy.Encode(nil, []byte{0xff, 0xff, 0xff, 0xff, 0xff})},
		{"empty", nil},
		{"truncated snappy", snappy.Encode(nil, lokiStream(`{a="b"}`))[:3]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			w := NewWriter(st)
			res, err := IngestLokiProto(w, tc.body, func() int64 { return 1 }, nil)
			w.Close()
			if err == nil && res.Accepted == 0 {
				t.Errorf("no error AND no records: a malformed push was answered as an empty success")
			}
			if res.Accepted != 0 {
				t.Errorf("invented %d records from a malformed body", res.Accepted)
			}
		})
	}
}

// A stream whose labels do not parse rejects ITS entries and reports them --
// it must not fail the whole push, since the other streams in the same body
// are unaffected and their data is still wanted.
func TestLokiProtoBadLabelsRejectOneStreamOnly(t *testing.T) {
	body := lokiPushProto(
		lokiStream(`{good="yes"}`, lokiEntry(1714521600, 0, "kept")),
		lokiStream(`not a label set`, lokiEntry(1714521600, 0, "dropped")),
	)
	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestLokiProto(w, body, func() int64 { return 1 }, nil)
	})
	if res.Accepted != 1 {
		t.Errorf("accepted %d, want 1 (the good stream)", res.Accepted)
	}
	if res.Rejected != 1 {
		t.Errorf("rejected %d, want 1 (the bad stream's entry)", res.Rejected)
	}
	if len(res.Warnings) == 0 {
		t.Error("no warning recorded; the operator has no way to see what was dropped")
	}
	if len(rows) != 1 || fieldsOfRow(rows[0])["_msg"] != "kept" {
		t.Errorf("stored rows = %v, want just the good stream's entry", rows)
	}
}

// A snappy body that expands past the limit is refused on its DECLARED length,
// before the allocation -- a check made after decompressing is not a check.
func TestLokiProtoRefusesOversizedDecompression(t *testing.T) {
	// A snappy block's header is the decoded length as a varint. Forge one
	// that claims more than the limit without carrying the bytes.
	var hdr []byte
	hdr = binary.AppendUvarint(hdr, uint64(maxLokiDecompressed)+1)
	hdr = append(hdr, 0x00, 'x') // one literal byte, nowhere near the claim

	st := openTestStore(t)
	w := NewWriter(st)
	_, err := IngestLokiProto(w, hdr, func() int64 { return 1 }, nil)
	w.Close()
	if err == nil {
		t.Fatal("no error for a body declaring more than the decompression limit")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("error %q does not name the limit", err)
	}
}

// The timestamp: seconds and nanos combine, and an entry with neither falls
// back rather than storing the epoch.
func TestLokiProtoTimestamp(t *testing.T) {
	body := lokiPushProto(lokiStream(`{a="b"}`,
		lokiEntry(1714521600, 250000000, "with-time"),
		lokiEntry(0, 0, "no-time"),
	))
	const fallbackTS = 999999999
	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestLokiProto(w, body, func() int64 { return fallbackTS }, nil)
	})
	if res.Accepted != 2 {
		t.Fatalf("accepted %d, want 2", res.Accepted)
	}
	var sawWithTime, sawFallback bool
	for _, r := range rows {
		switch fieldsOfRow(r)["_msg"] {
		case "with-time":
			sawWithTime = true
		case "no-time":
			sawFallback = true
		}
	}
	if !sawWithTime || !sawFallback {
		t.Errorf("rows = %v, want both entries stored", rows)
	}
}

// openTestStore is the two-line store setup the other tests spell out inline.
func openTestStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}
