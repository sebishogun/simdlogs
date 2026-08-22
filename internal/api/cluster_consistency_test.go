package api

import (
	"net/http"
	"sort"
	"testing"
)

// The ANSWER must not depend on where a stats pipe sits in the pipeline.
//
// This asserted that both orders were REFUSED. The refusal was the thing that
// depended on pipe order -- the check fired only when a stats pipe was first
// past the split point, so `* | stats avg(n) a` was 400 and
// `* | sort by (n) | stats avg(n) a` was 200 -- and the fix at the time was to
// make both 400. Both run the aggregate once at the coordinator over the merged
// rows, which is what a single node does, so both were answerable all along and
// the consistent refusal was consistently over-strict.
//
// What is asserted now is the property the original defect was really about: a
// client rewriting a query in a way that changes nothing about its meaning gets
// the same number back. And it must be the NODE's number, or "consistent"
// would be satisfied by two identical wrong answers.
func TestTheAnswerDoesNotDependOnPipeOrder(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, pair := range [][2]string{
		{`* | stats avg(n) a`, `* | sort by (n) | stats avg(n) a`},
		{`* | stats quantile(0.5, n) p`, `* | sort by (n) | stats quantile(0.5, n) p`},
		{`* | stats count_uniq(user) u`, `* | fields user | stats count_uniq(user) u`},
	} {
		t.Run(pair[0], func(t *testing.T) {
			c0, r0, b0 := queryRows(t, cluster, pair[0])
			c1, r1, b1 := queryRows(t, cluster, pair[1])
			if c0 != c1 {
				t.Fatalf("the same aggregate answered %d in one pipe order and %d in "+
					"another:\n  %q -> %.150s\n  %q -> %.150s",
					c0, c1, pair[0], b0, pair[1], b1)
			}
			if c0 != http.StatusOK {
				t.Fatalf("expected both to be answered, got %d: %.200s", c0, b0)
			}
			if !equalSets(sortedCopy(r0), sortedCopy(r1)) {
				t.Fatalf("the same aggregate gave different numbers in two pipe "+
					"orders:\n  %q -> %v\n  %q -> %v", pair[0], r0, pair[1], r1)
			}
			// And it is the single node's number, not merely a stable one.
			cs, rs, bs := queryRows(t, single, pair[0])
			// FATAL, not Skipf. The header says this test exists to check the
			// cluster's number is "the single node's number, not merely a
			// stable one" -- and a skip when the node cannot answer retires
			// exactly that, silently, in a green run. If the node stops
			// answering a query in this table, that is the finding.
			if cs != http.StatusOK {
				t.Fatalf("the single node answered %d for %q, so the cluster's "+
					"number cannot be checked against it and the only property "+
					"left is that the cluster agrees with itself: %.150s",
					cs, pair[0], bs)
			}
			if !equalSets(sortedCopy(rs), sortedCopy(r0)) {
				t.Fatalf("cluster and single node disagree:\n  single:  %v\n  cluster: %v",
					rs, r0)
			}
		})
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
