package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The SQL contract.
//
// The same failure as the Elasticsearch surface, in a second query language:
// the parser stopped after LIMIT and ignored whatever followed. `LIMIT 5
// OFFSET 10` silently dropped the OFFSET, `HAVING count > 1` silently dropped
// the HAVING, a JOIN silently answered about one table -- each returning a
// different result set than the one asked for, in a response that looks
// entirely normal.

func sqlServer(t *testing.T, n int) *httptest.Server {
	t.Helper()
	docs := make([]map[string]string, 0, n)
	for i := 0; i < n; i++ {
		lvl := "info"
		if i%4 == 0 {
			lvl = "error"
		}
		docs = append(docs, map[string]string{
			"_msg": fmt.Sprintf("line %d", i), "level": lvl, "app": "svc",
		})
	}
	return esServer(t, docs...)
}

func runSQL(t *testing.T, ts *httptest.Server, q string) (int, []string, string) {
	t.Helper()
	code, body := bodyOf(t, ts.URL+"/select/sql?query="+url.QueryEscape(q))
	if code != 200 {
		return code, nil, body
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return code, lines, body
}

// The supported subset answers.
func TestSQLSupportedSubsetAnswers(t *testing.T) {
	ts := sqlServer(t, 40)
	for _, tc := range []struct {
		name string
		q    string
		rows int
	}{
		{"select all", "SELECT * FROM logs", 40},
		{"projection", "SELECT level FROM logs LIMIT 5", 5},
		{"where", "SELECT * FROM logs WHERE level = 'error'", 10},
		{"limit", "SELECT * FROM logs LIMIT 3", 3},
		{"group by", "SELECT level, count(*) FROM logs GROUP BY level", 2},
		{"order by", "SELECT level FROM logs ORDER BY level LIMIT 4", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, lines, raw := runSQL(t, ts, tc.q)
			if code != 200 {
				t.Fatalf("%d: %s", code, raw)
			}
			if len(lines) != tc.rows {
				t.Fatalf("%d rows, want %d: %s", len(lines), tc.rows, raw)
			}
		})
	}
}

// Anything the subset does not cover is a 400 naming the token, never a
// silently different answer.
func TestSQLUnsupportedSyntaxIs400(t *testing.T) {
	ts := sqlServer(t, 20)
	for _, tc := range []struct{ name, q string }{
		{"offset", "SELECT * FROM logs LIMIT 5 OFFSET 10"},
		{"having", "SELECT level, count(*) FROM logs GROUP BY level HAVING count(*) > 1"},
		{"join", "SELECT * FROM logs JOIN other ON logs.id = other.id"},
		{"union", "SELECT * FROM logs UNION SELECT * FROM logs"},
		{"trailing garbage", "SELECT * FROM logs WHERE level = 'error' NONSENSE"},
		{"not a select", "DELETE FROM logs"},
		{"no from", "SELECT *"},
		{"bad limit", "SELECT * FROM logs LIMIT abc"},
		{"unsupported operator", "SELECT * FROM logs WHERE level REGEXP 'e.*'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, raw := runSQL(t, ts, tc.q)
			if code != 400 {
				t.Fatalf("%d, want 400: %s", code, raw)
			}
		})
	}
	// And the refusal for a leftover clause names what it choked on, so a
	// client can fix the query rather than guess.
	_, _, raw := runSQL(t, ts, "SELECT * FROM logs LIMIT 5 OFFSET 10")
	if !strings.Contains(raw, "OFFSET") && !strings.Contains(raw, "offset") {
		t.Errorf("the refusal does not name the clause: %s", raw)
	}
}

// A SQL query is subject to the same row budget as every other read, and the
// refusal is the typed one rather than the generic timeout.
func TestSQLObeysTheRowBudget(t *testing.T) {
	srv, ts := streamServerWith(t, 200, 20)
	_ = srv
	code, body := bodyOf(t, ts.URL+"/select/sql?query="+
		url.QueryEscape("SELECT * FROM logs ORDER BY seq"))
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("an over-budget SQL query returned %d, want 413: %.300s", code, body)
	}
	// And one bounded by its own LIMIT answers.
	code, body = bodyOf(t, ts.URL+"/select/sql?query="+
		url.QueryEscape("SELECT * FROM logs LIMIT 5"))
	if code != 200 {
		t.Fatalf("a bounded SQL query returned %d: %.300s", code, body)
	}
	if n := strings.Count(strings.TrimSpace(body), "\n") + 1; n != 5 {
		t.Fatalf("%d rows for LIMIT 5", n)
	}
}
