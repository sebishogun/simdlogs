package query

import (
	"testing"
	"time"
)

// A PIPE FILTER'S TIME BOUNDS DO NOT NARROW THE SCAN.
//
// A mid-pipe filter runs after whatever precedes it, so its bounds are not
// bounds on the rows the scan must read: an aggregate before it can emit rows
// whose timestamps fall outside them. `resolveTimePreds` resolves pipe-borne
// `_time:` predicates and deliberately does not feed them into the narrowing.
//
// ASSERTED ON THE WINDOW, because the window is the claim.
//
// The first version of this test drove `* | stats count() c | filter _time:5m`
// over HTTP and asserted the body did not contain `"1"`. It could not fail: the
// stats row is `NoTime`, so the trailing `_time:` filter drops it whether the
// count is 1 or 2 and the body is empty under both. Three assertions, every one
// satisfied by the degraded answer -- and feeding the pipe bounds into the
// narrowing left the entire repository green.
func TestAPipeFilterDoesNotNarrowTheScanWindow(t *testing.T) {
	now := time.Now().UnixNano()
	const wide = int64(1) << 62

	for _, tc := range []struct {
		name       string
		query      string
		wantNarrow bool
	}{
		// A QUERY-level relative filter narrows: that is the optimisation.
		{"in the query", `_time:5m`, true},
		{"in the query, one-sided", `_time:>=5m`, true},
		// A PIPE-level one must not, however it is spelled.
		{"in a filter pipe", `* | filter _time:5m`, false},
		{"in a filter pipe, one-sided", `* | filter _time:>=5m`, false},
		{"in a filter pipe after an aggregate",
			`* | stats count() c | filter _time:5m`, false},
		{"in a filter pipe, absolute",
			`* | filter _time:[2020-01-01T00:00:00Z, 2030-01-01T00:00:00Z]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseLogsQL(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			q.From, q.To = 0, wide
			q.SetNow(now)
			resolveTimePreds(q)

			narrowed := q.From != 0 || q.To != wide
			if narrowed != tc.wantNarrow {
				t.Errorf("%q narrowed the scan window to [%d,%d) (from [0,%d)); "+
					"narrowed=%v, want %v. A mid-pipe filter's bounds are not "+
					"bounds on the scan -- an aggregate before it can emit rows "+
					"whose timestamps fall outside them",
					tc.query, q.From, q.To, wide, narrowed, tc.wantNarrow)
			}
		})
	}
}

// AND THE PIPE'S PREDICATE IS STILL RESOLVED, which is the other half.
//
// Not narrowing the window must not mean not resolving: a relative pred that
// reaches the row evaluator with `Rel` still set matches nothing, which is the
// defect the resolve was added for. Asserting only the window would be
// satisfied by a `resolveTimePipes` that does nothing at all.
func TestAPipeFiltersOwnPredicateIsStillResolved(t *testing.T) {
	now := time.Now().UnixNano()
	q, err := ParseLogsQL(`* | filter _time:5m`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	q.SetNow(now)
	resolveTimePreds(q)

	var seen int
	for _, p := range q.Pipes {
		fp, ok := p.(*FilterPipe)
		if !ok {
			continue
		}
		walkLeaves(fp.Expr, func(pr *Pred) {
			if pr.Kind != TimeRange {
				return
			}
			seen++
			if pr.Rel {
				t.Error("the pipe's `_time:` pred still has Rel set, so its " +
					"T1/T2 are offsets rather than instants and the row " +
					"evaluator matches nothing")
			}
			if pr.T2 < now-int64(time.Hour) || pr.T2 > now+int64(time.Hour) {
				t.Errorf("the pipe's pred resolved to an end of %d, which is "+
					"not near the request's now of %d", pr.T2, now)
			}
		})
	}
	if seen != 1 {
		t.Fatalf("found %d time preds in the pipe, want 1", seen)
	}
}

// walkLeaves visits every leaf predicate of an expression tree.
func walkLeaves(e *Expr, fn func(*Pred)) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		fn(&e.Pred)
	case OpNot:
		walkLeaves(e.Child, fn)
	default:
		for _, k := range e.Kids {
			walkLeaves(k, fn)
		}
	}
}
