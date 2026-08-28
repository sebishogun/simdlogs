package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// WHERE THE TEXT SPLIT AND THE PARSE DISAGREE, exactly.
//
// pipeSegments walks the query text with its own idea of quoting and nesting;
// the lexer has a different one. `'` is the clearest case -- the walker treats
// it as a quote and the lexer does not -- so a filter containing an apostrophe
// makes the walker swallow every following `|` and return ONE segment.
// planQuery must then push nothing down, which the next test checks; what THIS
// one pins is which queries land in which case, because that is the input the
// refusal is decided from.
//
// EVERY ROW ASSERTS. This test was called TestTheTextSplitAgreesWithTheParse
// and could not fail: the agreement it was named for was a `t.Logf`, and a
// query that did not parse was a `t.Skip` -- two of the twelve rows skipping on
// every run, forever, green. A gate whose only two outcomes are "log" and
// "skip" is a gate that has retired itself; docs/wrong.md entries 79, 117, 118,
// 123 and 128 record five more of this shape, and this one sat in the file
// written to catch the planner defect it is named after.
//
// The three outcomes are spelled out per row, so a query that changes case --
// which is a change in what the planner will do with it -- is red here.
func TestWhichQueriesSplitTheWayTheyParse(t *testing.T) {
	const (
		agrees      = "the split matches the parse"
		disagrees   = "the split does NOT match the parse, so the planner must refuse"
		unparseable = "the lexer rejects it before any of this matters"
	)
	for _, tc := range []struct{ q, want string }{
		{`*`, agrees},
		{`* | stats count() c`, agrees},
		{`_msg:"a|b" | limit 1`, agrees},
		{`_msg:~"x|y" | stats count() n`, agrees},
		{`* | filter level:=error | sort by (_time) | limit 5`, agrees},
		// The apostrophe family: the walker quotes on `'` and the lexer does
		// not, so everything after it is swallowed into one segment.
		{`don't | stats count() c`, disagrees},
		{`level:err' | stats count() c`, disagrees},
		{`_msg:it's | fields _msg | limit 3`, disagrees},
		// An unbalanced bracket, and a backtick, do the same through the
		// walker's nesting rules.
		{`_msg:a] | stats count() c`, disagrees},
		{"a` | limit 1", disagrees},
		// Rejected by the lexer. These USED TO SKIP, which is why the two of
		// them are named here: a query that stops parsing becomes a 400 and
		// leaves this file, and nothing would have said so.
		{`a) | limit 1`, unparseable},
		{`a} | limit 1`, unparseable},
	} {
		t.Run(tc.q, func(t *testing.T) {
			parsed, err := query.ParseLogsQL(tc.q)
			if err != nil {
				if tc.want != unparseable {
					t.Fatalf("does not parse (%v), and this row says %q", err, tc.want)
				}
				return
			}
			if tc.want == unparseable {
				t.Fatalf("parses into %d pipes, and this row says it does not parse",
					len(parsed.Pipes))
			}
			segs := pipeSegments(tc.q)
			got := agrees
			if len(segs) != len(parsed.Pipes)+1 {
				got = disagrees
			}
			if got != tc.want {
				t.Fatalf("%d segments for %d pipes: %s\nthis row says: %s",
					len(segs), len(parsed.Pipes), got, tc.want)
			}
		})
	}
}

// When the split disagrees with the parse, the query is REFUSED.
//
// The obvious fallback -- push nothing down and send the head as the filter --
// is the defect itself: when the walker swallows the pipes there is only one
// segment and it is the whole query, so every shard runs the entire pipeline
// and the coordinator runs it again. A refusal is visible; a wrong number is
// not.
func TestADisagreeingSplitIsRefused(t *testing.T) {
	node := realShard(t, corpus(1)[0])
	_, cluster := routerServer(t, node.URL)

	for _, tc := range []struct {
		q          string
		wantRefuse bool
	}{
		{`* | fields level | sort by (level)`, false},
		{`_msg:"a|b" | limit 1`, false},
		{`_msg:it's | fields level | sort by (level)`, true},
		{"a` | fields level | limit 1", true},
	} {
		t.Run(tc.q, func(t *testing.T) {
			code, _, raw := queryRows(t, cluster, tc.q)
			refused := code == http.StatusBadRequest
			if refused != tc.wantRefuse {
				t.Fatalf("status %d (refused=%v), expected refused=%v: %.250s",
					code, refused, tc.wantRefuse, raw)
			}
			if refused && !strings.Contains(raw, "disagree") {
				t.Errorf("the refusal does not explain itself: %.250s", raw)
			}
		})
	}
}
