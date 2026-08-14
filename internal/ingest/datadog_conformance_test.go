package ingest

import (
	"fmt"
	"testing"
)

// Datadog logs-intake conformance.
//
// The intake accepts a JSON ARRAY of entries or a SINGLE object, and agents
// send both: the Agent batches into an array, a curl-based shipper or a Lambda
// forwarder frequently sends one object. Both shapes, and the reserved
// attributes each carries, are asserted here rather than assumed.
//
// Three defects this pins, all of which were silent:
//   - a bare `ddtags` entry with no colon was dropped entirely, so
//     `ddtags=env,prod-canary` stored nothing
//   - a nested object attribute kept its SOURCE BYTES, whitespace included, so
//     the same logical attribute from two agents stored two different values
//   - an entry with no storable attribute was rejected with the warning "entry
//     has no message field", which is not what was wrong with it

// ddRows ingests a Datadog body and returns the stored rows.
func ddRows(t *testing.T, body string) ([]string, Result) {
	t.Helper()
	return rowsOf(t, func(w *Writer) (Result, error) {
		return IngestDatadogOpts(w, []byte(body), func() int64 { return 1 }, nil)
	})
}

// An array of entries and a single object must behave identically for the same
// entry -- the array form is just a batch.
func TestDatadogArrayAndSingleRecordAgree(t *testing.T) {
	entry := `{"message":"hello","ddsource":"nginx","hostname":"h1","service":"web","status":"info","timestamp":1714521600000}`

	single, sres := ddRows(t, entry)
	array, ares := ddRows(t, "["+entry+"]")

	if sres.Accepted != 1 || ares.Accepted != 1 {
		t.Fatalf("accepted: single %d, array %d, want 1 each", sres.Accepted, ares.Accepted)
	}
	if len(single) != 1 || len(array) != 1 {
		t.Fatalf("rows: single %d, array %d, want 1 each", len(single), len(array))
	}
	if single[0] != array[0] {
		t.Errorf("the same entry stored differently:\n  single: %s\n  array:  %s", single[0], array[0])
	}

	got := fieldsOfRow(single[0])
	for k, v := range map[string]string{
		"_msg": "hello", "ddsource": "nginx", "hostname": "h1",
		"service": "web", "status": "info",
	} {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

// ddtags: keyed tags become fields, and BARE tags are kept rather than dropped.
func TestDatadogDDTags(t *testing.T) {
	for _, tc := range []struct {
		name, tags string
		want       map[string]string
	}{
		{"keyed", "env:prod,team:payments",
			map[string]string{"env": "prod", "team": "payments"}},
		{"bare only", "canary,beta",
			map[string]string{"_ddtags": "canary,beta"}},
		{"mixed", "env:prod,canary,team:payments",
			map[string]string{"env": "prod", "team": "payments", "_ddtags": "canary"}},
		// A value containing a colon keeps everything after the FIRST one:
		// `url:https://x` is a real tag and must not become `url` = `https`.
		{"colon in value", "url:https://example.com/a:b",
			map[string]string{"url": "https://example.com/a:b"}},
		{"empty segments", "env:prod,,,team:x",
			map[string]string{"env": "prod", "team": "x"}},
		{"whitespace", " env:prod , canary ",
			map[string]string{"env": "prod", "_ddtags": "canary"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, res := ddRows(t, fmt.Sprintf(`{"message":"m","ddtags":%q}`, tc.tags))
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1", res.Accepted)
			}
			got := fieldsOfRow(rows[0])
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// A nested value is stored COMPACTED, so the same logical attribute sent with
// different formatting stores the same string. Before this it kept the source
// bytes and a pretty-printed agent and a compact one disagreed.
func TestDatadogNestedValuesAreCompacted(t *testing.T) {
	pretty := "{\"message\":\"m\",\"http\":{\n  \"method\": \"GET\",\n  \"status\": 200\n}}"
	compact := `{"message":"m","http":{"method":"GET","status":200}}`

	pRows, _ := ddRows(t, pretty)
	cRows, _ := ddRows(t, compact)
	if len(pRows) != 1 || len(cRows) != 1 {
		t.Fatalf("rows: pretty %d, compact %d", len(pRows), len(cRows))
	}
	pv := fieldsOfRow(pRows[0])["http"]
	cv := fieldsOfRow(cRows[0])["http"]
	if pv != cv {
		t.Errorf("the same object stored two ways:\n  pretty:  %q\n  compact: %q", pv, cv)
	}
	if want := `{"method":"GET","status":200}`; cv != want {
		t.Errorf("http = %q, want %q", cv, want)
	}

	// An array attribute goes the same way.
	aRows, _ := ddRows(t, "{\"message\":\"m\",\"tags\":[ \"a\" , \"b\" ]}")
	if got, want := fieldsOfRow(aRows[0])["tags"], `["a","b"]`; got != want {
		t.Errorf("tags = %q, want %q", got, want)
	}
}

// The timestamp: a number is milliseconds since epoch (Datadog's default), a
// string goes through the shared parser, and an entry with neither falls back.
func TestDatadogTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantTS     int64
	}{
		{"ms number", `{"message":"m","timestamp":1714521600000}`, 1714521600000 * 1_000_000},
		{"date alias", `{"message":"m","date":1714521600000}`, 1714521600000 * 1_000_000},
		{"rfc3339 string", `{"message":"m","timestamp":"2024-05-01T00:00:00Z"}`, 1714521600 * 1_000_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			w := NewWriter(st)
			res, err := IngestDatadogOpts(w, []byte(tc.body), func() int64 { return 42 }, nil)
			if err != nil {
				t.Fatalf("ingest: %v", err)
			}
			w.Close()
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1", res.Accepted)
			}
			rows := storeRows(t, st)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			if got := time60(tc.wantTS); !hasPrefix(rows[0], got) {
				t.Errorf("row %q does not carry the expected timestamp", rows[0])
			}
		})
	}
}

// A malformed body is an error, not a 202 with zero records. An agent told
// "accepted" for a body nothing could read has no reason to retry or alert.
func TestDatadogRejectsMalformed(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"empty", ""},
		{"whitespace only", "   \n\t "},
		{"truncated array", `[{"message":"m"}`},
		{"truncated object", `{"message":`},
		{"not JSON", `message=hello`},
		{"array of scalars", `["a","b"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			w := NewWriter(st)
			res, err := IngestDatadogOpts(w, []byte(tc.body), func() int64 { return 1 }, nil)
			w.Close()
			if err == nil && res.Accepted == 0 && res.Rejected == 0 {
				t.Errorf("no error and no counts: a malformed body was answered as an empty success")
			}
			if res.Accepted != 0 {
				t.Errorf("invented %d records from a malformed body", res.Accepted)
			}
		})
	}
}

// An entry with nothing storable is rejected AND reported, with a warning that
// says what was actually wrong.
func TestDatadogEmptyEntryIsReported(t *testing.T) {
	rows, res := ddRows(t, `[{"message":"kept"},{},{"timestamp":1714521600000}]`)
	if res.Accepted != 1 {
		t.Errorf("accepted %d, want 1", res.Accepted)
	}
	if res.Rejected != 2 {
		t.Errorf("rejected %d, want 2", res.Rejected)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("no warning; the operator cannot see what was dropped")
	}
	for _, w := range res.Warnings {
		if contains(w.Msg, "no message field") {
			t.Errorf("warning still blames a missing message: %q", w.Msg)
		}
	}
	if len(rows) != 1 || fieldsOfRow(rows[0])["_msg"] != "kept" {
		t.Errorf("stored %v, want just the good entry", rows)
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
