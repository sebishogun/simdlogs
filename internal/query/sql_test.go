package query

import "testing"

func TestSQLTranslate(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT * FROM logs WHERE level = 'error' LIMIT 10", "level:=error | limit 10"},
		{"SELECT service, count(*) FROM logs GROUP BY service ORDER BY count(*) DESC LIMIT 5",
			"* | stats by (service) count() as count | sort by (count) desc | limit 5"},
		{"SELECT * FROM logs WHERE _msg LIKE '%timeout%'", "_msg:~timeout"},
		{"SELECT * FROM logs WHERE status >= 500 AND service = 'api'", "status:>=500 and service:=api"},
	}
	for _, c := range cases {
		got, err := TranslateSQL(c.sql)
		if err != nil {
			t.Fatalf("%q: %v", c.sql, err)
		}
		if got != c.want {
			t.Errorf("%q\n  got  %q\n  want %q", c.sql, got, c.want)
		}
	}
}

func TestSQLQuery(t *testing.T) {
	s := statsStore(t) // service a:3, b:2
	q, err := ParseSQL("SELECT service, count(*) FROM logs GROUP BY service ORDER BY count(*) DESC")
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	rows := RunPipeline(s, q)
	if len(rows) != 2 || rowField(rows[0], "service") != "a" || rowField(rows[0], "count") != "3" {
		t.Fatalf("SQL group-by = %v want a:3 first", rows)
	}
}
