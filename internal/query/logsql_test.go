package query

import "testing"

func TestParseLogsQL(t *testing.T) {
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
			t.Fatalf("%q: %d preds want %d", c.in, len(q.Preds), c.preds)
		}
		if c.preds > 0 {
			if q.Preds[0].Kind != c.kind || q.Preds[0].Value != c.val {
				t.Fatalf("%q: pred %v/%q want %v/%q", c.in, q.Preds[0].Kind, q.Preds[0].Value, c.kind, c.val)
			}
		}
	}
	if _, err := ParseLogsQL("noColon"); err == nil {
		t.Fatal("expected error for a predicate without a colon")
	}
}
