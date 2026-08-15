package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Golden response SHAPES for the frozen routes.
//
// # What is frozen and what is not
//
// The shape, not the data. A golden file holding an actual response would fail
// on any change to the fixture, to a timestamp, or to a generated stream id --
// so it would be regenerated constantly and would stop meaning anything. What
// is pinned is the set of KEYS at each level and the JSON type of each value,
// which is what a client parses against.
//
// A field appearing is a compatible change and is allowed here with a note; a
// field DISAPPEARING or changing type breaks every client that reads it, and
// that is what this catches.
//
// # Why goldens rather than assertions in code
//
// The contract is data, and data in a file can be read by a person deciding
// whether a change is breaking. An assertion spread across handler tests
// cannot be read that way, and the question at release time is exactly "what
// did the response shape used to be".

// shapeOf reduces a JSON document to its structure: keys and value types, with
// arrays collapsed to their first element's shape.
//
// Collapsed because an array's LENGTH is data, not shape. A response holding
// three rows and one holding four are the same contract.
func shapeOf(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range t {
			out[k] = shapeOf(val)
		}
		return out
	case []any:
		if len(t) == 0 {
			return []any{}
		}
		// The first element's shape stands for the array. A response whose
		// elements disagree in shape is itself a defect, and this would hide
		// it -- so the elements are checked against each other first.
		first := shapeOf(t[0])
		for i := 1; i < len(t); i++ {
			if !sameShape(first, shapeOf(t[i])) {
				return []any{"MIXED ELEMENT SHAPES"}
			}
		}
		return []any{first}
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case nil:
		return "null"
	}
	return fmt.Sprintf("%T", v)
}

func sameShape(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

// goldenRoutes are the response shapes frozen for v1.
func goldenRoutes() []surfaceRoute {
	const q = "query=%2A"
	return []surfaceRoute{
		{path: "/select/logsql/hits", method: "GET",
			query: q + "&step=1h&start=2026-06-01T00:00:00Z&end=2026-06-02T00:00:00Z"},
		{path: "/select/logsql/facets", method: "GET", query: q},
		{path: "/select/logsql/field_names", method: "GET", query: q},
		{path: "/select/logsql/field_values", method: "GET", query: q + "&field=level"},
		{path: "/select/logsql/streams", method: "GET", query: q},
		{path: "/select/logsql/stream_ids", method: "GET", query: q},
		{path: "/select/logsql/stats_query", method: "GET",
			query: q + "%20%7C%20stats%20count%28%29%20c"},
		{path: "/select/logsql/stats_query_range", method: "GET",
			query: q + "%20%7C%20stats%20count%28%29%20c&step=1h"},
		{path: "/_search", method: "POST", body: `{"query":{"match_all":{}}}`,
			ctype: "application/json"},
		{path: "/_count", method: "POST", body: `{"query":{"match_all":{}}}`,
			ctype: "application/json"},
	}
}

func goldenName(path string) string {
	return strings.TrimPrefix(strings.ReplaceAll(strings.Trim(path, "/"), "/", "_"), "select_") + ".json"
}

// TestFrozenResponseShapes compares each route's shape against its golden.
//
// Regenerate deliberately, never as a reflex:
//
//	SIMDLOGS_WRITE_GOLDEN=1 go test ./internal/api -run TestFrozenResponseShapes
//
// A regeneration is a decision that the contract changed. Doing it to make a
// red build green is how a breaking change ships.
func TestFrozenResponseShapes(t *testing.T) {
	node := realShard(t, corpus(1)[0])
	dir := filepath.Join("testdata", "golden")
	write := os.Getenv("SIMDLOGS_WRITE_GOLDEN") != ""
	if write {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	for _, rt := range goldenRoutes() {
		name := goldenName(rt.path)
		seen[name] = true
		t.Run(rt.path, func(t *testing.T) {
			code, body := surfaceCall(t, node, rt)
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			var doc any
			if err := json.Unmarshal([]byte(body), &doc); err != nil {
				t.Fatalf("the response is not JSON: %v: %.200s", err, body)
			}
			got, err := json.MarshalIndent(shapeOf(doc), "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')

			path := filepath.Join(dir, name)
			if write {
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s", path)
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("no golden for this route (%v). A frozen route with no "+
					"golden is a contract nobody recorded; regenerate with "+
					"SIMDLOGS_WRITE_GOLDEN=1 and review the diff", err)
			}
			if string(got) != string(want) {
				t.Fatalf("the response shape changed.\n--- frozen ---\n%s\n--- now ---\n%s\n"+
					"A key disappearing or changing type breaks every client that "+
					"reads it. If the change is intended, regenerate with "+
					"SIMDLOGS_WRITE_GOLDEN=1 and say so in the commit", want, got)
			}
		})
	}

	// A golden with no route is a contract for something that no longer
	// exists, and it would sit there passing forever.
	if ents, err := os.ReadDir(dir); err == nil {
		var orphans []string
		for _, e := range ents {
			if !seen[e.Name()] {
				orphans = append(orphans, e.Name())
			}
		}
		sort.Strings(orphans)
		if len(orphans) > 0 && !write {
			t.Errorf("goldens with no route: %v. Either the route was removed -- "+
				"which is a breaking change -- or the golden is stale", orphans)
		}
	}
}

// The error envelopes, both of them, frozen as they are.
//
// The read surface answers PLAIN TEXT and the ingest surface answers JSON.
// That is deliberate, not an inconsistency to tidy: readSpec is errText
// "as the existing clients expect", because the read surface is a drop-in for
// VictoriaLogs and its clients parse text. Changing either would break
// somebody, so both are pinned here and the split is written down -- a reader
// finding two error formats in one server should be able to tell a decision
// from an accident.
func TestFrozenErrorEnvelopes(t *testing.T) {
	node := realShard(t, nil)

	t.Run("a read error is plain text", func(t *testing.T) {
		resp, err := http.Get(node.URL + "/select/logsql/query?query=" +
			strings.Repeat("%28", 200))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Fatalf("a malformed query answered %d", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
			t.Errorf("Content-Type is %q; the read surface answers text/plain errors "+
				"for drop-in compatibility", ct)
		}
		var doc any
		if json.Unmarshal(b, &doc) == nil {
			t.Errorf("the read error parsed as JSON. If the envelope is being "+
				"changed, that is a breaking change for every client parsing text: "+
				"%.200s", b)
		}
		if !strings.Contains(string(b), "simdlogs") {
			t.Errorf("the error does not identify the server: %.200s", b)
		}
	})

	t.Run("an ingest error is JSON with error and status", func(t *testing.T) {
		resp, err := http.Post(node.URL+"/insert/journald",
			"application/vnd.fdo.journal", strings.NewReader("MESSAGE\n\xff\xff"))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Fatalf("a truncated upload answered %d", resp.StatusCode)
		}
		var doc map[string]any
		if err := json.Unmarshal(b, &doc); err != nil {
			t.Fatalf("the ingest error is not JSON: %v: %.200s", err, b)
		}
		for _, k := range []string{"error", "status"} {
			if _, ok := doc[k]; !ok {
				t.Errorf("the ingest error envelope has no %q: %.200s", k, b)
			}
		}
		if st, ok := doc["status"].(float64); !ok || int(st) != resp.StatusCode {
			t.Errorf("the body says status %v and the HTTP status is %d: a client "+
				"branching on either gets a different answer", doc["status"], resp.StatusCode)
		}
	})
}
