package query

import "testing"

// Flat conjunctions (and single predicates) lower to Query.Preds.
func TestParseLogsQLPreds(t *testing.T) {
	cases := []struct {
		in    string
		preds int
		kind  PredKind
		val   string
	}{
		{`level:=error`, 1, Eq, "error"},
		{`level:error`, 1, Eq, "error"},
		{`_msg:~"timeout"`, 1, Contains, "timeout"},
		{`_msg:~"id=[0-9]+"`, 1, Regexp, "id=[0-9]+"},
		{`service:*api*`, 1, Contains, "api"},
		{`path:/api*`, 1, Prefix, "/api"},
		{`latency:>100`, 1, Gt, ""},
		{`code:<=404`, 1, Le, ""},
		{`status:in(200,404,500)`, 1, In, ""},
		{`timeout`, 1, Contains, "timeout"}, // bare word -> _msg substring
		{`level:=error AND service:=auth`, 2, Eq, "error"},
		{`level:=error service:=auth`, 2, Eq, "error"},
		{``, 0, Eq, ""},
		{`*`, 0, Eq, ""},
	}
	for _, c := range cases {
		q, err := ParseLogsQL(c.in)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		if len(q.Preds) != c.preds {
			t.Fatalf("%q: %d preds want %d (filter=%v)", c.in, len(q.Preds), c.preds, q.Filter != nil)
		}
		if c.preds > 0 && (q.Preds[0].Kind != c.kind || q.Preds[0].Value != c.val) {
			t.Fatalf("%q: pred %v/%q want %v/%q", c.in, q.Preds[0].Kind, q.Preds[0].Value, c.kind, c.val)
		}
	}
}

// OR / NOT / parentheses build a Query.Filter tree.
func TestParseLogsQLFilter(t *testing.T) {
	cases := []struct {
		in string
		op ExprOp
	}{
		{`level:=error OR level:=warn`, OpOr},
		{`NOT level:=debug`, OpNot},
		{`service:=auth AND (level:=error OR level:=warn)`, OpAnd},
	}
	for _, c := range cases {
		q, err := ParseLogsQL(c.in)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		if q.Filter == nil {
			t.Fatalf("%q: expected a filter tree, got preds", c.in)
		}
		if q.Filter.Op != c.op {
			t.Fatalf("%q: root op %v want %v", c.in, q.Filter.Op, c.op)
		}
	}
	// Malformed input errors rather than silently matching.
	for _, bad := range []string{
		`level:`, `(level:=error`, `latency:>abc`, `_stream:{service=~"("}`,
	} {
		if _, err := ParseLogsQL(bad); err == nil {
			t.Fatalf("%q: expected a parse error", bad)
		}
	}
}
