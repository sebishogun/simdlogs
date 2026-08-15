package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/query"
)

// benchLine is one NDJSON row of the shape a storage node emits: a timestamp,
// a message, and the stream fields a real deployment carries.
var benchLine = []byte(`{"_time":"2026-06-01T12:34:56.789012345Z","_msg":"GET /api/v1/users 200 12ms",` +
	`"level":"info","service":"api-gateway","host":"ip-10-0-3-91","region":"eu-west-1",` +
	`"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","status":200,"duration_ms":12.4,"cached":false}`)

func BenchmarkJSONLineToRow(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchLine)))
	for i := 0; i < b.N; i++ {
		row := jsonLineToRow(benchLine)
		if len(row.Fields) == 0 {
			b.Fatal("no fields")
		}
	}
}

// decoderRow is the encoding/json implementation this path used, kept as the
// specification the scanner is differentially tested against. It is the
// reference in the sense internal/ref is: slower, obviously correct, and the
// thing a disagreement is judged against.
func decoderRow(line []byte) query.Row {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return rawRow(line)
	}
	row := query.Row{NoTime: true}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return rawRow(line)
		}
		key, ok := kt.(string)
		if !ok {
			return rawRow(line)
		}
		vt, err := dec.Token()
		if err != nil {
			return rawRow(line)
		}
		val := decoderScalar(vt)
		if key == "_time" && row.NoTime {
			if t, terr := time.Parse(time.RFC3339Nano, val); terr == nil {
				row.Time, row.NoTime = t.UnixNano(), false
				continue
			}
		}
		row.Fields = append(row.Fields, query.Field{Key: key, Value: val})
	}
	return row
}

func decoderScalar(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}

func BenchmarkJSONLineToRowDecoder(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchLine)))
	for i := 0; i < b.N; i++ {
		row := decoderRow(benchLine)
		if len(row.Fields) == 0 {
			b.Fatal("no fields")
		}
	}
}

// rowsEqual compares two decoded rows field by field, including order.
func rowsEqual(a, b query.Row) bool {
	if a.Time != b.Time || a.NoTime != b.NoTime || len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

func showRow(r query.Row) string {
	s := fmt.Sprintf("time=%d notime=%v", r.Time, r.NoTime)
	for _, f := range r.Fields {
		s += fmt.Sprintf(" %q=%q", f.Key, f.Value)
	}
	return s
}

// The scanner answers what the encoding/json decoder answered.
//
// Every case is a line a storage node could emit or a malformed one a
// coordinator could receive, and the decoder is the specification: a
// disagreement means the fast path changed a row.
func TestRowScannerMatchesTheDecoder(t *testing.T) {
	cases := []string{
		`{}`,
		`{"a":"b"}`,
		`{"_msg":"hello"}`,
		string(benchLine),
		// _time in its several shapes.
		`{"_time":"2026-06-01T12:00:00Z","_msg":"x"}`,
		`{"_time":"2026-06-01T12:00:00.5Z","_msg":"x"}`,
		`{"_time":"not a time","_msg":"x"}`,
		`{"_time":123,"_msg":"x"}`,
		`{"_msg":"x","_time":"2026-06-01T12:00:00Z"}`,
		`{"_time":"2026-06-01T12:00:00Z","_time":"2026-06-02T12:00:00Z"}`,
		`{"_time":"bad","_time":"2026-06-02T12:00:00Z"}`,
		// Scalars.
		`{"n":0,"m":-1,"f":1.5,"e":1e10,"E":1.5E-3,"big":123456789012345678901234567890}`,
		`{"t":true,"f":false,"z":null}`,
		// Strings and escapes.
		`{"k":""}`,
		`{"k":"a\"b"}`,
		`{"k":"a\\b"}`,
		`{"k":"a\/b"}`,
		`{"k":"tab\there"}`,
		`{"k":"nl\nhere"}`,
		`{"k":"\b\f\r"}`,
		`{"k":"Aé€"}`,
		`{"k":"😀"}`,           // surrogate pair: an emoji
		`{"k":"\ud800"}`,      // lone high surrogate
		`{"k":"\udc00"}`,      // lone low surrogate
		`{"k":"\ud800A"}`,     // high surrogate then a non-surrogate
		`{"k":"héllo wörld"}`, // raw UTF-8
		`{"esc\"key":"v"}`,
		`{"_key":"v"}`,
		// Whitespace.
		` { "a" : "b" , "c" : 1 } `,
		"{\n\"a\":\t\"b\"\n}",
		// Not an object, or malformed: rawRow either way.
		``,
		`   `,
		`null`,
		`"just a string"`,
		`[1,2,3]`,
		`{`,
		`{"a"`,
		`{"a":`,
		`{"a":}`,
		`{"a":1,}`,
		`{"a":1,`,
		`{"a":1,"b":`,
		`{"a"`,
		`{,"a":1}`,
		`{"a" "b"}`,
		`{"a":1}{"b":2}`,
		`{"a":1} trailing`,
		`{"a":01}`,
		`{"a":1.}`,
		`{"a":.5}`,
		`{"a":1e}`,
		`{"a":-}`,
		`{"a":tru}`,
		`{"a":"unterminated}`,
		`{"a":"bad \q escape"}`,
		`{"a":"bad \u00 escape"}`,
		"{\"a\":\"raw\tcontrol\"}",
		`{1:"a"}`,
		`not json at all`,
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			checkRowScanner(t, []byte(in))
		})
	}
}

// checkRowScanner is the contract, applied to one line.
//
// For a valid JSON OBJECT the scanner must produce exactly what the decoder
// produced. For anything else it must produce rawRow -- a definite answer,
// rather than "whatever encoding/json happened to do", which is what made the
// decoder return a plausible row for a truncated line.
func checkRowScanner(t *testing.T, line []byte) {
	t.Helper()
	got := jsonLineToRow(line)
	if json.Valid(line) && startsWithBrace(line) {
		if hasNested(line) {
			return // covered by TestANestedValueIsItsRawText
		}
		want := decoderRow(line)
		if !rowsEqual(want, got) {
			t.Errorf("input %q is a valid JSON object\n  decoder: %s\n  scanner: %s",
				line, showRow(want), showRow(got))
		}
		return
	}
	if !rowsEqual(rawRow(line), got) {
		t.Errorf("input %q is not a valid JSON object, so it must decode to the whole "+
			"line as _msg\n  want: %s\n   got: %s",
			line, showRow(rawRow(line)), showRow(got))
	}
}

// startsWithBrace reports whether a valid JSON document is an object.
func startsWithBrace(line []byte) bool {
	i := skipWS(line, 0)
	return i < len(line) && line[i] == '{'
}

// A nested value is the one place the scanner deliberately differs, and it
// differs by being right.
//
// The decoder's token stream returned Delim('{') for the value, which
// jsonScalar rendered as "{", and then read the nested object's own keys as if
// they were the row's -- so `{"a":{"b":1}}` decoded to a row with fields a="{"
// and b="1". That row does not exist anywhere.
func TestANestedValueIsItsRawText(t *testing.T) {
	for _, tc := range []struct{ in, key, val string }{
		{`{"a":{"b":1}}`, "a", `{"b":1}`},
		{`{"a":[1,2,3]}`, "a", `[1,2,3]`},
		{`{"a":{"b":{"c":"}"}}}`, "a", `{"b":{"c":"}"}}`},
		{`{"a":["x\"y"]}`, "a", `["x\"y"]`},
	} {
		got := jsonLineToRow([]byte(tc.in))
		if len(got.Fields) != 1 || got.Fields[0].Key != tc.key || got.Fields[0].Value != tc.val {
			t.Errorf("%s decoded to %s, want one field %q=%q",
				tc.in, showRow(got), tc.key, tc.val)
		}
		// And the decoder really did produce the row that never existed, so
		// this is a fix rather than a preference.
		if old := decoderRow([]byte(tc.in)); len(old.Fields) == 1 && old.Fields[0].Value == tc.val {
			t.Errorf("%s: the decoder already handled this; the comment claiming "+
				"otherwise is wrong", tc.in)
		}
	}
}

// A row decoded from a line does not alias the line, which the merge reuses.
func TestADecodedRowDoesNotAliasTheLineBuffer(t *testing.T) {
	line := []byte(`{"_msg":"original","level":"info"}`)
	row := jsonLineToRow(line)
	for i := range line {
		line[i] = 'X'
	}
	if row.Fields[0].Value != "original" || row.Fields[1].Value != "info" {
		t.Errorf("overwriting the line changed the row: %s", showRow(row))
	}
}

func FuzzJSONLineToRow(f *testing.F) {
	for _, s := range []string{
		`{}`, `{"a":"b"}`, string(benchLine), `{"k":"😀"}`,
		`{"_time":"2026-06-01T12:00:00Z"}`, `{"a":1e10}`, `[1,2]`, `{`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		// A line with a newline in it is not one NDJSON line, and neither
		// implementation is asked to handle it.
		for _, c := range line {
			if c == '\n' {
				t.Skip()
			}
		}
		checkRowScanner(t, line)
	})
}

// hasNested reports whether a line contains a composite value, which is the
// one shape the two implementations answer differently.
func hasNested(line []byte) bool {
	depth, inStr := 0, false
	for i := 0; i < len(line); i++ {
		switch {
		case inStr && line[i] == '\\':
			i++
		case line[i] == '"':
			inStr = !inStr
		case inStr:
		case line[i] == '{' || line[i] == '[':
			depth++
			if depth > 1 {
				return true
			}
		case line[i] == '}' || line[i] == ']':
			depth--
		}
	}
	return false
}

// Two allocations per row, and a gate so it stays that way.
//
// This path was 147 allocations and 4448 bytes for the ten-field row above --
// an encoding/json Decoder, its 512-byte buffer, and an `any` box plus a string
// per token, per row, on the merge path of every clustered query with a
// coordinator half. The two that remain are the byte buffer every key and value
// aliases and the exactly-sized Field slice.
func TestRowScannerAllocatesTwicePerRow(t *testing.T) {
	got := testing.AllocsPerRun(200, func() {
		row := jsonLineToRow(benchLine)
		if len(row.Fields) != 9 {
			t.Fatalf("want 9 fields (10 minus the lifted _time), got %d", len(row.Fields))
		}
	})
	if got > 2 {
		t.Errorf("%v allocations per row, want 2: the merge decodes one of these "+
			"per row per clustered query with a coordinator half", got)
	}
}

// The Field slice is sized from the line, not guessed, so a row neither grows
// its slice nor reserves space it will not use.
func TestTheFieldSliceIsSizedFromTheLine(t *testing.T) {
	for _, tc := range []struct {
		line string
		want int
	}{
		{`{"a":"b"}`, 1},
		{`{"a":"b","c":"d"}`, 2},
		{string(benchLine), 9},
		{`{"a":{"b":1,"c":2},"d":3}`, 2}, // commas inside a nested value do not count
		{`{"a":"x,y,z"}`, 1},             // nor inside a string
	} {
		row := jsonLineToRow([]byte(tc.line))
		if len(row.Fields) != tc.want {
			t.Errorf("%s: %d fields, want %d", tc.line, len(row.Fields), tc.want)
			continue
		}
		if c := cap(row.Fields); c != tc.want && c != tc.want+1 {
			// +1 because a lifted _time is counted and then removed.
			t.Errorf("%s: %d fields in a slice of capacity %d", tc.line, tc.want, c)
		}
	}
}
