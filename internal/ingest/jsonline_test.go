package ingest

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A JSON boolean is stored as what it is, and a large integer keeps its digits.
//
// Both were silent losses on the ingest path. simdjson's Value.Int() returns 0
// for every kind that is not a Number, so the Bool branch tested a value that
// was always 0 and EVERY boolean -- true and false alike -- was stored as
// "false": `v:=true` matched no row ever ingested and `v:=false` matched all of
// them, at HTTP 200 with {"ingested":1,"skipped":0}. Value.Bool() sits four
// lines below Value.Int() in the same file and was called nowhere.
//
// Numbers went through float64, so 9007199254740993 -- one past the last
// integer a float64 holds exactly -- was stored as ...992. Snowflake ids, trace
// ids and epoch-nanosecond timestamps are all in that range.
func TestJSONBooleansAndLargeIntegersSurviveIngest(t *testing.T) {
	for _, tc := range []struct{ name, line, key, want string }{
		{"true", `{"_msg":"a","v":true}`, "v", "true"},
		{"false", `{"_msg":"a","v":false}`, "v", "false"},
		{"2^53+1", `{"_msg":"a","id":9007199254740993}`, "id", "9007199254740993"},
		{"2^63-1", `{"_msg":"a","id":9223372036854775807}`, "id", "9223372036854775807"},
		{"negative", `{"_msg":"a","id":-9007199254740993}`, "id", "-9007199254740993"},
		{"small int", `{"_msg":"a","n":42}`, "n", "42"},
		{"real float", `{"_msg":"a","n":1.5}`, "n", "1.5"},
		{"exponent", `{"_msg":"a","n":1e3}`, "n", "1000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, err := storage.OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()

			if _, _, err := IngestJSONLinesParallelCfg(st, []byte(tc.line+"\n"),
				monotonic(), ParallelConfig{}, nil); err != nil {
				t.Fatalf("ingest: %v", err)
			}
			got, ok := readBackField(t, st, tc.key)
			if !ok {
				t.Fatalf("%s: field %q is not in the store at all", tc.line, tc.key)
			}
			if got != tc.want {
				t.Errorf("%s stored %s=%q, want %q", tc.line, tc.key, got, tc.want)
			}
		})
	}
}

// readBackField returns the first value stored for a field, from the store
// rather than from the parser -- the round trip is the property, not the
// intermediate map.
func readBackField(t *testing.T, st *storage.Store, key string) (string, bool) {
	t.Helper()
	sn, err := st.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer sn.Close()
	for _, g := range sn.Groups {
		if !g.ColumnExists(key) {
			continue
		}
		if v, ok := g.DictValueAt(key, 0); ok {
			return v, true
		}
	}
	return "", false
}
