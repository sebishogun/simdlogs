package api

import (
	"net/http"
	"strings"
	"testing"
)

// Every stats surface refuses the same aggregates.
//
// federatedMatrix summed whatever the shards returned while its own comment
// claimed it was restricted to additive aggregates: two shards averaging 10
// answered 20, and a quantile was answered where the other two endpoints
// refused it. One binary answering the same aggregate three ways depending on
// the endpoint is worse than any single wrong answer, because whichever one a
// client tried first becomes the one they trust.
func TestEveryStatsSurfaceRefusesTheSameAggregates(t *testing.T) {
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	surfaces := []struct{ name, path string }{
		{"query", "/select/logsql/query?query="},
		{"stats_query", "/select/logsql/stats_query?query="},
		{"stats_query_range", "/select/logsql/stats_query_range?step=1h&query="},
	}
	nonMergeable := []string{
		`* | stats avg(n) a`,
		`* | stats quantile(0.5, n) p`,
		`* | stats count_uniq(user) u`,
	}
	for _, q := range nonMergeable {
		for _, sf := range surfaces {
			t.Run(sf.name+" "+q, func(t *testing.T) {
				code, body := chaosGet(t, cluster.URL+sf.path+urlEscape(q))
				if code != http.StatusBadRequest {
					t.Fatalf("%s answered %d for %q, want 400: summing shards for "+
						"this aggregate produces a number that looks right and is "+
						"not: %.200s", sf.name, code, q, body)
				}
				if !strings.Contains(body, "shards") {
					t.Errorf("the refusal does not explain itself: %.200s", body)
				}
			})
		}
	}

	// And a mergeable one is still answered on all three.
	for _, sf := range surfaces {
		t.Run(sf.name+" count", func(t *testing.T) {
			code, body := chaosGet(t, cluster.URL+sf.path+urlEscape(`* | stats count() c`))
			if code != 200 {
				t.Fatalf("%s refused a count: %d %.200s", sf.name, code, body)
			}
		})
	}
}
