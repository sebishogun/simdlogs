package query

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// A BARE pack packs the row as it will be SERIALIZED, including the three
// fields the query layer cannot see.
//
// `_time`, `_stream` and `_stream_id` are synthesized at serialization
// (appendRowJSON), and pack_json runs before that -- so the packed value was
// short while the row beside it was right, and a client reading `p` got a
// different record from a client reading the row, out of one response.
// Measured against the staged victoria-logs binary:
//
//	VL  p keys  [_msg _stream _stream_id _time lvl svc]
//	SL  p       {"_msg":"hello","_stream":"{svc=\"api\"}","lvl":"info","svc":"api"}
//
// Asserted on the KEY SET rather than on the bytes: VL emits
// `_time, _stream_id, _stream, _msg, …` and this server emits `_time, <fields>,
// _stream, _stream_id`, and matching that order means matching VL's internal
// field ordering, which is not something this server has.
func TestABarePackCarriesTheSynthesizedFields(t *testing.T) {
	row := Row{
		Time: 1786000000000000000,
		Fields: []Field{
			{Key: "_msg", Value: "hello"},
			{Key: "_stream", Value: `{svc="api"}`},
			{Key: "lvl", Value: "info"},
			{Key: "svc", Value: "api"},
		},
	}
	got := packKeys(t, packJSON(row, nil))
	want := []string{"_msg", "_stream", "_stream_id", "_time", "lvl", "svc"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("a bare pack carries %v, want %v", got, want)
	}

	// The id is DERIVED from the stream, so it is the id a `_stream_id:` filter
	// would match -- not a placeholder.
	var m map[string]string
	if err := json.Unmarshal([]byte(packJSON(row, nil)), &m); err != nil {
		t.Fatal(err)
	}
	if m["_stream_id"] != StreamID(`{svc="api"}`) {
		t.Errorf("_stream_id is %q, want %q -- a packed id that does not match the "+
			"stream is one no filter finds", m["_stream_id"], StreamID(`{svc="api"}`))
	}
	if m["_time"] == "" {
		t.Error("a bare pack over a row with a timestamp carries no _time")
	}
}

// A PROJECTED pack and a STATS pack carry only their own fields.
//
// The first fix for the bare pack synthesized the empty stream for every row,
// which put `_stream` and `_stream_id` onto rows that never had one -- the
// mirror image of the defect, on the two shapes the record had already
// measured as correct:
//
//	VL   * | fields lvl | pack_json as p   ->  p {"lvl":"info"}
//	then                                       p {"lvl":"info","_stream":"{}",
//	                                              "_stream_id":"0000…55b5"}
//
// A full record is always in SOME stream, which is why serialization
// synthesizes one. A projection is not a record.
func TestAProjectedPackCarriesOnlyItsOwnFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  Row
		want []string
	}{
		{
			name: "projection that dropped _time and _stream",
			row:  Row{NoTime: true, Fields: []Field{{Key: "lvl", Value: "info"}}},
			want: []string{"lvl"},
		},
		{
			name: "stats row",
			row: Row{NoTime: true, Fields: []Field{
				{Key: "svc", Value: "api"}, {Key: "n", Value: "1"},
			}},
			want: []string{"n", "svc"},
		},
		{
			name: "projection that KEPT _time",
			row: Row{Time: 1786000000000000000, Fields: []Field{
				{Key: "_time", Value: "2026-08-16T03:00:00Z"},
				{Key: "_msg", Value: "hello"},
			}},
			want: []string{"_msg", "_time"},
		},
		{
			// An empty `_stream` value is NOT a stream: the store materializes
			// the column for a whole group, so a row that never carried one
			// comes back with "" once any row in its flush did.
			name: "row whose _stream column is empty",
			row: Row{NoTime: true, Fields: []Field{
				{Key: "_stream", Value: ""}, {Key: "lvl", Value: "info"},
			}},
			want: []string{"_stream", "lvl"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := packKeys(t, packJSON(tc.row, nil))
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("packed %v, want %v", got, tc.want)
			}
		})
	}
}

// An EXPLICIT field list packs exactly that list, synthesized fields included
// only if named.
func TestAnExplicitFieldListIsUnchanged(t *testing.T) {
	row := Row{
		Time:   1786000000000000000,
		Fields: []Field{{Key: "_msg", Value: "hello"}, {Key: "_stream", Value: `{svc="api"}`}},
	}
	got := packKeys(t, packJSON(row, []string{"_msg"}))
	if strings.Join(got, ",") != "_msg" {
		t.Errorf("an explicit list packed %v, want [_msg]", got)
	}
}

// pack_logfmt follows the same rule as pack_json.
func TestPackLogfmtCarriesTheSameFields(t *testing.T) {
	row := Row{
		Time: 1786000000000000000,
		Fields: []Field{
			{Key: "_msg", Value: "hello"},
			{Key: "_stream", Value: `{svc="api"}`},
		},
	}
	out := packLogfmt(row, nil)
	for _, k := range []string{"_time=", "_msg=", "_stream=", "_stream_id="} {
		if !strings.Contains(out, k) {
			t.Errorf("a bare pack_logfmt does not carry %s: %s", k, out)
		}
	}
	if out := packLogfmt(Row{NoTime: true, Fields: []Field{{Key: "lvl", Value: "info"}}}, nil); out != "lvl=info" {
		t.Errorf("a projected pack_logfmt is %q, want %q", out, "lvl=info")
	}
}

func packKeys(t *testing.T, packed string) []string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal([]byte(packed), &m); err != nil {
		t.Fatalf("the packed value does not parse: %v: %s", err, packed)
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
