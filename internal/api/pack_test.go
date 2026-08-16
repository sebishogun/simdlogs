package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// `pack_json` with no field list packs the row's fields, under any pipe chain.
//
// The field-collection walk that decides which columns the scan materializes has
// nothing to add for a pack-all -- there is no field list to add -- so any
// PROJECTING pipe in the same chain cleared MatAll and the scan read nothing for
// it. Every row packed to `{}`, at HTTP 200: a plausible answer, correctly
// shaped, empty.
//
// It cannot be satisfied from a projected column set, because the set it needs
// is "all of them". The MatAll decision now says so. A projecting pipe BEFORE
// the pack still narrows the rows first -- `fields a, b | pack_json` packs a and
// b -- because MatAll governs what the SCAN reads, not what a pipe has already
// thrown away; that direction is asserted here too, so the fix cannot be "pack
// everything always".

func queryPacked(t *testing.T, ts *httptest.Server, q string) []map[string]any {
	t.Helper()
	code, lines, raw := queryRows(t, ts, q)
	if code != 200 {
		t.Fatalf("query %s: %d %.200s", q, code, raw)
	}
	var out []map[string]any
	for _, ln := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("query %s: %v (line %q)", q, err, ln)
		}
		out = append(out, m)
	}
	return out
}

func TestPackAllPacksTheRowUnderAProjectingPipe(t *testing.T) {
	node := realShard(t, []string{
		`{"_time":"2024-01-01T00:00:00Z","_msg":"one","svc":"api","level":"info"}`,
		`{"_time":"2024-01-01T00:00:01Z","_msg":"two","svc":"web","level":"warn"}`,
	})

	for _, tc := range []struct {
		name, query string
		wantKeys    []string
	}{
		// A pack-all with a projecting pipe AFTER it. This packed {}.
		{"pack then fields", `* | pack_json as packed | fields packed`,
			[]string{"svc", "level", "_msg"}},
		// And with one BEFORE it: the projection has already narrowed the row,
		// so the pack sees exactly what survived -- not everything.
		{"fields then pack", `* | fields svc | pack_json as packed`,
			[]string{"svc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := queryPacked(t, node, tc.query)
			if len(rows) == 0 {
				t.Fatalf("%s returned no rows", tc.query)
			}
			for i, r := range rows {
				packed, _ := r["packed"].(string)
				if packed == "" || packed == "{}" {
					t.Fatalf("row %d packed to %q -- the scan materialized nothing for a "+
						"pack-all (row was %v)", i, packed, r)
				}
				var got map[string]any
				if err := json.Unmarshal([]byte(packed), &got); err != nil {
					t.Fatalf("row %d packed to unparseable JSON %q: %v", i, packed, err)
				}
				for _, k := range tc.wantKeys {
					if _, ok := got[k]; !ok {
						t.Errorf("row %d packed %q, which is missing %q", i, packed, k)
					}
				}
			}
			// The narrowing direction: `fields svc | pack_json` must NOT have
			// picked up the fields the projection removed.
			if tc.name == "fields then pack" {
				for i, r := range rows {
					var got map[string]any
					json.Unmarshal([]byte(r["packed"].(string)), &got)
					for _, k := range []string{"level", "_msg"} {
						if _, ok := got[k]; ok {
							t.Errorf("row %d packed %v, which includes %q -- the projection "+
								"before the pack was ignored", i, got, k)
						}
					}
				}
			}
		})
	}
}

// A pack-all does not turn a pipeline into a full-record select.
//
// q.MatAll carries TWO meanings: "the scan reads every column" and "this is
// full-record output", and the API layer reads the second as "synthesize the
// _stream/_stream_id pair onto every row" (appendRowJSON's withStream). Setting
// MatAll to feed a pack-all therefore put `_stream:"{}"` and a _stream_id onto
// the output of
//
//   - | stats by (svc) count() n | pack_json as p
//
// two fields those rows had never carried, on a query that is not a record
// select. The packed VALUE was correct throughout -- the leak was beside it.
//
// q.MatCols is the half that is only about what the scan reads.
//
// SCOPE, measured against victoria-logs at v1.x rather than reasoned about.
// This test's three rows all carry a PROJECTING pipe, which is the whole set
// where the fix applies. A BARE pack still takes MatAll = true, and that is
// correct -- VictoriaLogs returns the full record for it:
//
//	VL  * | pack_json as p
//	    {"_msg":"hello","_stream":"{svc=\"api\"}","_stream_id":"0000…aa07…",
//	     "_time":"2026-08-16T03:00:00Z","lvl":"info",
//	     "p":"{\"_time\":…,\"_stream_id\":…,\"_stream\":…,\"_msg\":…,
//	           \"lvl\":…,\"svc\":…}","svc":"api"}
//
//	VL  * | fields lvl | pack_json as p   ->  {"lvl":"info","p":"{\"lvl\":\"info\"}"}
//	VL  * | stats by (svc) count() n | pack_json as p
//	                                      ->  {"svc":"api","n":"1","p":"{\"svc\":…,\"n\":…}"}
//
// The two projected shapes match this server exactly. The bare one does not,
// and not in the direction this test is about: VL's `p` CONTAINS `_time`,
// `_stream_id` and `_stream`, and this server's does not, because the pack runs
// in the query layer while those fields are synthesized at serialization
// (appendRowJSON). So the row is right and the packed value is short -- the
// mirror image of the defect fixed here. Making them agree means synthesizing
// the pair before the pipes run, which changes what every pipe sees; task #437.
func TestAPackAllDoesNotSynthesiseStreamFields(t *testing.T) {
	node := realShard(t, []string{
		`{"_time":"2024-01-01T00:00:00Z","_msg":"one","svc":"api"}`,
		`{"_time":"2024-01-01T00:00:01Z","_msg":"two","svc":"web"}`,
	})
	for _, q := range []string{
		`* | stats by (svc) count() n | pack_json as p`,
		`* | fields svc | pack_json as p`,
		`* | pack_json as p | fields p`,
	} {
		t.Run(q, func(t *testing.T) {
			rows := queryPacked(t, node, q)
			if len(rows) == 0 {
				t.Fatalf("%s returned no rows", q)
			}
			for i, r := range rows {
				for _, leaked := range []string{"_stream", "_stream_id"} {
					if _, ok := r[leaked]; ok {
						t.Errorf("row %d carries %s, which this query never asked for: %v",
							i, leaked, r)
					}
				}
				if p, _ := r["p"].(string); p == "" || p == "{}" {
					t.Errorf("row %d packed to %q -- the fix must not cost the pack its "+
						"input (row %v)", i, p, r)
				}
			}
		})
	}
}
