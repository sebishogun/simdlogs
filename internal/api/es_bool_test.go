package api

import (
	"strings"
	"testing"
)

// THE `bool` CLAUSE'S TWO EMPTY CASES: an arm that filters nothing, and a
// `should` beside a `must`.
//
// Both are silent wrong answers of the same kind the strict decoder exists to
// prevent -- a structurally valid 200 carrying a document set the client did
// not ask for -- and both are the shapes Kibana emits most.

// esBoolDocs is the four-document corpus: one error, one warn, two info.
func esBoolDocs() []map[string]string {
	return []map[string]string{
		{"_msg": "a", "level": "error"},
		{"_msg": "b", "level": "warn"},
		{"_msg": "c", "level": "info"},
		{"_msg": "d", "level": "info"},
	}
}

// A CHILD THAT TRANSLATES TO "NO FILTER" MEANS SOMETHING; DROPPING IT MEANT
// THE OPPOSITE.
//
// `{"match_all":{}}`, `{}` and `{"bool":{}}` all map to nil -- no filter, i.e.
// every document. The `must_not` and `should` loops appended only non-nil
// children, so such an arm vanished:
//
//	query                                              got   ES
//	must_not: [match_all]                                4    0
//	must_not: [{}]                                       4    0
//	must_not: [bool {}]                                  4    0
//	should:   [match_all, term level=error]              1    4
//
// `must_not: [{"match_all":{}}]` is the canonical Elasticsearch spelling of
// "match nothing", and Kibana emits it whenever a filter pill is negated to
// nothing. It answered every document in the index.
//
// A nil child matches EVERY document. Under `must_not` its negation therefore
// matches NONE; under `should` the union with it is EVERY document.
func TestANilESBoolChildMeansMatchAllUnderShouldAndMatchNoneUnderMustNot(t *testing.T) {
	ts := esServer(t, esBoolDocs()...)
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		{"must_not match_all", `{"query":{"bool":{"must_not":[{"match_all":{}}]}},"size":100}`, 0},
		{"must_not empty clause", `{"query":{"bool":{"must_not":[{}]}},"size":100}`, 0},
		{"must_not empty bool", `{"query":{"bool":{"must_not":[{"bool":{}}]}},"size":100}`, 0},
		{"must_not match_all beside a real must", `{"query":{"bool":{"must":[{"term":{"level":"error"}}],"must_not":[{"match_all":{}}]}},"size":100}`, 0},
		{"must_not match_all beside a real must_not", `{"query":{"bool":{"must_not":[{"term":{"level":"error"}},{"match_all":{}}]}},"size":100}`, 0},
		{"should match_all beside a term", `{"query":{"bool":{"should":[{"match_all":{}},{"term":{"level":"error"}}]}},"size":100}`, 4},
		{"should match_all alone", `{"query":{"bool":{"should":[{"match_all":{}}]}},"size":100}`, 4},
		{"should empty clause beside a term", `{"query":{"bool":{"should":[{},{"term":{"level":"error"}}]}},"size":100}`, 4},

		// THE CONTROLS. Without them a build that answered 0 to every
		// `must_not` and 4 to every `should` would pass every row above.
		{"must_not a real term (control)", `{"query":{"bool":{"must_not":[{"term":{"level":"error"}}]}},"size":100}`, 3},
		{"must_not two real terms (control)", `{"query":{"bool":{"must_not":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}},"size":100}`, 2},
		{"must match_all (control)", `{"query":{"bool":{"must":[{"match_all":{}}]}},"size":100}`, 4},
		{"should two real terms (control)", `{"query":{"bool":{"should":[{"term":{"level":"info"}},{"term":{"level":"error"}}]}},"size":100}`, 3},
		{"should one real term (control)", `{"query":{"bool":{"should":[{"term":{"level":"error"}}]}},"size":100}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %.300s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("%s answered total=%d, want %d.\nAn arm that filters "+
					"nothing is not an arm that is not there: under `must_not` "+
					"it means match nothing, under `should` it means match "+
					"everything.", tc.body, total, tc.total)
			}
		})
	}
}

// `minimum_should_match` DEFAULTS TO 0 BESIDE A `must` OR `filter`, AND TO 1
// ALONE.
//
// `should` was ANDed with the rest of the bool unconditionally, so a `should`
// beside a `must` narrowed a query Elasticsearch widens:
//
//	query                                                    got   ES
//	must: [term level=error], should: [range gte 2030]         0    1
//	filter: [term level=info], should: [term level=error]      0    2
//
// That is Kibana's own shape -- the filter pills go in `filter`, the search bar
// goes in `should` -- so the search bar turned a working dashboard empty.
//
// And an explicit `"minimum_should_match": 0` was a 400: the exact value ES
// defaults to, refused in the exact shape the default was being got wrong.
func TestMinimumShouldMatchDefaultsToZeroBesideAMustOrFilter(t *testing.T) {
	ts := esServer(t, esBoolDocs()...)
	for _, tc := range []struct {
		name, body string
		total      int
	}{
		// The default beside a must/filter is 0: the should clause is
		// optional and does not narrow the answer.
		{"must plus an unsatisfiable should", `{"query":{"bool":{"must":[{"term":{"level":"error"}}],"should":[{"range":{"@timestamp":{"gte":"2030-01-01T00:00:00Z"}}}]}},"size":100}`, 1},
		{"filter plus a should", `{"query":{"bool":{"filter":[{"term":{"level":"info"}}],"should":[{"term":{"level":"error"}}]}},"size":100}`, 2},
		{"must plus a should that would widen", `{"query":{"bool":{"must":[{"term":{"level":"error"}}],"should":[{"term":{"level":"info"}}]}},"size":100}`, 1},
		{"must_not plus a should (no must/filter, so the default is 1)", `{"query":{"bool":{"must_not":[{"term":{"level":"warn"}}],"should":[{"term":{"level":"error"}}]}},"size":100}`, 1},

		// The default with no must/filter is 1: the should union is the whole
		// filter.
		{"should alone", `{"query":{"bool":{"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}},"size":100}`, 2},

		// EXPLICIT VALUES. 0 is the value ES defaults to and was a 400.
		//
		// AND AN EXPLICIT 0 DOES NOT MAKE A PURE DISJUNCTION MATCH EVERYTHING.
		// This row asserted 4 -- every document in the index -- for a query
		// naming one term. Lucene's rule is about REQUIRED clauses: with no
		// `must` and no `filter`, at least one `should` arm must still match,
		// whatever the number says. Measured against ES: 1, not 4. The gate
		// and the code were wrong together, which is why nothing was red.
		{"explicit 0 with no must or filter still needs an arm", `{"query":{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}}]}},"size":100}`, 1},
		{"explicit 0 over two arms is their union", `{"query":{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}},"size":100}`, 2},
		{"explicit 0 beside a must_not, which is not a required clause", `{"query":{"bool":{"minimum_should_match":0,"must_not":[{"term":{"level":"warn"}}],"should":[{"term":{"level":"error"}}]}},"size":100}`, 1},
		{"explicit 0 with no should arms at all (control)", `{"query":{"bool":{"minimum_should_match":0,"must":[{"term":{"level":"error"}}]}},"size":100}`, 1},
		{"explicit 0 beside a must", `{"query":{"bool":{"minimum_should_match":0,"must":[{"term":{"level":"info"}}],"should":[{"term":{"level":"error"}}]}},"size":100}`, 2},
		{"explicit 1 alone", `{"query":{"bool":{"minimum_should_match":1,"should":[{"term":{"level":"error"}}]}},"size":100}`, 1},
		{"explicit 1 beside a must makes the should REQUIRED", `{"query":{"bool":{"minimum_should_match":1,"must":[{"term":{"level":"error"}}],"should":[{"term":{"level":"info"}}]}},"size":100}`, 0},
		{"explicit 1 beside a must, satisfiable", `{"query":{"bool":{"minimum_should_match":1,"must":[{"term":{"level":"error"}}],"should":[{"term":{"level":"error"}},{"term":{"level":"info"}}]}},"size":100}`, 1},

		// NESTED. EVERY ROW ABOVE IS A SINGLE-LEVEL bool, AND THE LIFT IS NOT
		// A TOP-LEVEL RULE.
		//
		// `esMinShouldMatch` takes `*esBool` and nothing else -- no boolean
		// context -- because Lucene's rule is about the clause's OWN required
		// arms, not about where the clause sits. Passing the surrounding
		// `lift` into it and gating the 0 -> 1 promotion on positive
		// conjunctive position compiles and leaves `go test ./...` GREEN,
		// because all thirteen rows above are top level and the top level IS
		// lift=true. Measured under exactly that mutation:
		//
		//	query                                            mutant   ES
		//	should: [bool{should:[error], msm 0}]               4       1
		//	must_not: [bool{should:[error], msm 0}]             0       3
		//
		// The first is es.go's own named worst case -- "a dropped filter
		// returns MORE documents than the client asked for, in a response that
		// is structurally valid" -- because a nested bool whose only arm is
		// discarded translates to nil, and nil under `should` means match_all.
		// The second is its mirror: nil under `must_not` means match none.
		{"a nested bool with msm 0 under a should", `{"query":{"bool":{"should":[{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 1},
		{"a nested bool with msm 0 under a must_not", `{"query":{"bool":{"must_not":[{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 3},
		{"a nested bool with msm 0 under a must", `{"query":{"bool":{"must":[{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 1},
		{"a nested bool with msm 0 under a filter", `{"query":{"bool":{"filter":[{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}}]}},"size":100}`, 2},
		{"a nested bool with msm 0 two levels down, under a should", `{"query":{"bool":{"should":[{"bool":{"must":[{"bool":{"minimum_should_match":0,"should":[{"term":{"level":"warn"}}]}}]}}]}},"size":100}`, 1},
		{"a nested bool whose own must makes 0 the ES default", `{"query":{"bool":{"must_not":[{"bool":{"minimum_should_match":0,"must":[{"term":{"level":"info"}}],"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 2},
		// The nested CONTROLS: the same shapes with the msm absent, so a build
		// that ignored `minimum_should_match` entirely would still have to
		// answer these -- and they pin that nesting itself is not the thing
		// under test.
		{"a nested bool with no msm under a should (control)", `{"query":{"bool":{"should":[{"bool":{"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 1},
		{"a nested bool with no msm under a must_not (control)", `{"query":{"bool":{"must_not":[{"bool":{"should":[{"term":{"level":"error"}}]}}]}},"size":100}`, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, total, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 200 {
				t.Fatalf("%d, want 200: %.300s", code, raw)
			}
			if total != tc.total {
				t.Fatalf("%s answered total=%d, want %d.\nES makes `should` "+
					"optional when a `must` or `filter` is present "+
					"(minimum_should_match defaults to 0) and required when it "+
					"is not (defaults to 1).", tc.body, total, tc.total)
			}
		})
	}
}

// A `minimum_should_match` THIS BUILD CANNOT EXPRESS IS STILL A 400 NAMING IT.
//
// "at least 2 of N" is not expressible as an AND/OR/NOT tree without
// enumerating the combinations, so it is refused with the value in the message
// -- never answered as though it were 1, which is what the client would get
// silently if the value were ignored. The refusal is narrower than it was: 0
// and 1 are answered now, and only the values that need a counting operator
// are refused.
func TestAnUnsupportedMinimumShouldMatchIsRefusedWithItsValue(t *testing.T) {
	ts := esServer(t, esBoolDocs()...)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"two of three", `{"query":{"bool":{"minimum_should_match":2,"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}},{"term":{"level":"info"}}]}},"size":100}`, "minimum_should_match=2"},
		{"negative", `{"query":{"bool":{"minimum_should_match":-1,"should":[{"term":{"level":"error"}},{"term":{"level":"warn"}}]}},"size":100}`, "minimum_should_match=-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _, raw := esSearchRaw(t, ts, tc.body)
			if code != 400 {
				t.Fatalf("%d, want 400: %.300s", code, raw)
			}
			if !strings.Contains(raw, tc.want) {
				t.Fatalf("the refusal does not name the value (%q): %.300s", tc.want, raw)
			}
		})
	}
}
