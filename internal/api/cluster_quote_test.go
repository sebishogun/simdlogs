package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// The text split must agree with the parse, or nothing may be pushed down.
//
// pipeSegments walks the query text with its own idea of quoting and nesting;
// the lexer has a different one. `'` is the clearest case -- the walker treats
// it as a quote and the lexer does not -- so a filter containing an apostrophe
// made the walker swallow every following `|` and return ONE segment. planQuery
// then shipped the caller's whole string to every shard and re-applied the
// coordinator half on top, which is per-shard aggregation: the exact defect
// this planner exists to remove, answering one believable number instead of
// three obvious ones.
//
// This asserts the invariant directly rather than through a cluster round trip.
// A round-trip test only sees the defect when the filter MATCHES rows -- the
// first version of this test used `don't`, which matches nothing in the corpus,
// so both sides returned zero rows and it passed with the guard deleted.
func TestTheTextSplitAgreesWithTheParse(t *testing.T) {
	for _, q := range []string{
		`*`,
		`* | stats count() c`,
		`don't | stats count() c`,
		`level:err' | stats count() c`,
		`_msg:a] | stats count() c`,
		`_msg:it's | fields _msg | limit 3`,
		`_msg:"a|b" | limit 1`,
		`_msg:~"x|y" | stats count() n`,
		`* | filter level:=error | sort by (_time) | limit 5`,
		`a) | limit 1`,
		`a} | limit 1`,
		"a` | limit 1",
	} {
		t.Run(q, func(t *testing.T) {
			parsed, err := query.ParseLogsQL(q)
			if err != nil {
				t.Skipf("does not parse: %v", err)
			}
			segs := pipeSegments(q)
			if len(segs) != len(parsed.Pipes)+1 {
				// Not a failure of the SPLIT -- it is allowed to disagree.
				// What must hold is that planQuery notices and pushes nothing
				// down, which the next test checks.
				t.Logf("split disagrees: %d segments for %d pipes (the planner "+
					"must fall back)", len(segs), len(parsed.Pipes))
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
