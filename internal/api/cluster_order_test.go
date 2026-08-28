package api

import (
	"fmt"
	"strings"
	"testing"
)

// A projecting pipe strips _time, so every merged row shares a sort key.
//
// rowLineTime returns 0 for a line with no `"_time":"`, which is every row
// after `| fields`. The sort then left them in the order the fan-out goroutines
// happened to finish: `* | fields _msg | limit 5` over three shards returned
// shard 2's first five rows, because shard 2's chunk landed in the slice first.
func TestAProjectedResultIsOrderedLikeASingleNode(t *testing.T) {
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL)
	single := realShard(t, corpus(1)[0])

	for _, q := range []string{
		`* | fields _msg`,
		`* | fields level, user`,
		`* | delete _msg`,
	} {
		for _, lim := range []string{"", "&limit=5"} {
			t.Run(q+lim, func(t *testing.T) {
				path := "/select/logsql/query?query=" + urlEscape(q) + lim
				_, sRows := rowsAt(t, single, path)
				_, cRows := rowsAt(t, cluster, path)
				if len(sRows) != len(cRows) {
					t.Fatalf("%d rows on one node, %d across three", len(sRows), len(cRows))
				}
				for i := range sRows {
					if sRows[i] != cRows[i] {
						t.Fatalf("row %d differs:\n  single:  %s\n  cluster: %s\n"+
							"the merge order is not the single node's",
							i, sRows[i], cRows[i])
					}
				}
			})
		}
	}
}

// Identical requests must return identical answers.
//
// sort.Slice is not stable, so rows sharing a timestamp came back in an order
// that varied run to run: 40 requests produced two distinct four-row answers.
// With `limit` that is different ROWS, not merely a different order.
func TestIdenticalRequestsReturnIdenticalAnswers(t *testing.T) {
	// Every row at the same timestamp, so the tiebreak is the only thing
	// deciding the order.
	var parts [3][]string
	for i := 0; i < 24; i++ {
		parts[i%3] = append(parts[i%3], fmt.Sprintf(
			`{"_time":"2026-06-01T12:00:00Z","_msg":"tied-%02d"}`, i))
	}
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL)

	for _, path := range []string{
		"/select/logsql/query?query=%2A&limit=4",
		"/select/logsql/query?query=%2A",
		"/select/logsql/query?query=%2A%20%7C%20fields%20_msg&limit=6",
	} {
		t.Run(path, func(t *testing.T) {
			_, first := rowsAt(t, cluster, path)
			if len(first) == 0 {
				t.Fatal("no rows, so this proves nothing")
			}
			for i := 0; i < 40; i++ {
				_, got := rowsAt(t, cluster, path)
				if strings.Join(got, "\n") != strings.Join(first, "\n") {
					t.Fatalf("request %d returned a different answer:\n  first: %v\n"+
						"  now:   %v", i, first, got)
				}
			}
		})
	}
}
