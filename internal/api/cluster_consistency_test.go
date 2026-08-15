package api

import (
	"net/http"
	"strings"
	"testing"
)

// The refusal must not depend on where a stats pipe sits in the pipeline.
//
// The check fired only when a stats pipe was first past the split point, so the
// same aggregate was refused in one pipe order and answered in another:
// `* | stats avg(n) a` was 400 and `* | sort by (n) | stats avg(n) a` was 200.
// Both run the aggregate once at the coordinator, so the second answer was
// correct and the first refusal over-strict -- and the inconsistency was worse
// than either, because whichever a client tried first became the one believed.
func TestTheRefusalDoesNotDependOnPipeOrder(t *testing.T) {
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	for _, pair := range [][2]string{
		{`* | stats avg(n) a`, `* | sort by (n) | stats avg(n) a`},
		{`* | stats quantile(0.5, n) p`, `* | sort by (n) | stats quantile(0.5, n) p`},
		{`* | stats count_uniq(user) u`, `* | fields user | stats count_uniq(user) u`},
	} {
		t.Run(pair[0], func(t *testing.T) {
			c0, _, b0 := queryRows(t, cluster, pair[0])
			c1, _, b1 := queryRows(t, cluster, pair[1])
			if c0 != c1 {
				t.Fatalf("the same aggregate answered %d in one pipe order and %d in "+
					"another:\n  %q -> %.150s\n  %q -> %.150s",
					c0, c1, pair[0], b0, pair[1], b1)
			}
			if c0 != http.StatusBadRequest {
				t.Errorf("expected both to be refused, got %d", c0)
			}
			if !strings.Contains(b1, "shards") {
				t.Errorf("the second refusal does not explain itself: %.150s", b1)
			}
		})
	}
}
