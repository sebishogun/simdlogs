package api

import "testing"

// A PIPE AFTER THE AGGREGATE runs once, over the merged result.
//
// `needsExactStats` inspected only the pipes BEFORE the aggregate, so anything
// following the stats pipe took the federated path -- where the whole query,
// that pipe included, goes to every shard. Each shard applied it to its own
// groups and the coordinator merged what came back without applying it again.
// Every one of these answered HTTP 200 with a different result from the node,
// measured over three shards:
//
//	| stats by (_msg) count() c | limit 2                  node 2   cluster 6
//	| stats by (_msg) count() c | sort by (_msg) | limit 2 node 2   cluster 6
//	| stats by (_msg) count() c | offset 25                node 5   cluster 0
//	| stats by (user) count() c | limit 2                  node 2   cluster 4
//	| stats by (_msg) count() c | top 2 by (c)             1 each, different value
//	| stats by (_msg) count() c | uniq by (c)              1 each, different value
//
// `limit 2` is two groups PER SHARD. `offset 25` skips 25 groups per shard and
// no shard holds 25, so a query answering five series on a node answers NONE
// across a cluster -- the shape that looks like "no data" rather than like a
// bug.
//
// `| stats count() c | limit 1` agreed before the fix and still does: one group
// makes `limit 1` a no-op. That is arithmetic, not a rule, and it is why the
// defect looked absent -- it is kept below as the case that must not be the
// only one tested.
func TestAPipeAfterTheAggregateAgreesAcrossACluster(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(3)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL,
		realShard(t, parts[2]).URL)

	const win = "?start=2026-06-01T11:00:00Z&end=2026-06-01T13:00:00Z"
	for _, q := range []string{
		`* | stats by (_msg) count() c | limit 2`,
		`* | stats by (_msg) count() c | sort by (_msg) | limit 2`,
		`* | stats by (_msg) count() c | sort by (c) | limit 3`,
		`* | stats by (_msg) count() c | offset 25`,
		`* | stats by (_msg) count() c | offset 2 | limit 2`,
		`* | stats by (user) count() c | limit 2`,
		`* | stats by (_msg) count() c | top 2 by (c)`,
		`* | stats by (_msg) count() c | uniq by (c)`,
		// The no-op that agreed all along. Present so a future change that
		// re-narrows the rule has to keep the ones above green too.
		`* | stats count() c | limit 1`,
		// And a bare aggregate, which must not regress while fixing the rest.
		`* | stats by (level) count() c`,
	} {
		t.Run(q, func(t *testing.T) {
			p := "/select/logsql/stats_query" + win + "&query=" + urlEscape(q)
			codeS, bodyS := chaosGet(t, single.URL+p)
			codeC, bodyC := chaosGet(t, cluster.URL+p)
			if codeS != 200 {
				t.Fatalf("the node answered %d: %.200s", codeS, bodyS)
			}
			if codeC != codeS {
				t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
			}
			gotS, gotC := statsSet(t, bodyS), statsSet(t, bodyC)
			if len(gotS) == 0 {
				t.Fatalf("the node produced no series, so this compares two "+
					"empty answers and proves nothing: %.200s", bodyS)
			}
			if !equalSets(gotS, gotC) {
				t.Errorf("node and cluster disagree\n  node    (%d): %v\n"+
					"  cluster (%d): %v", len(gotS), gotS, len(gotC), gotC)
			}
		})
	}
}

// A `_time:` filter that CROSSES a bucket boundary prices each bucket on its
// own overlap with the filter.
//
// `TestTheExactStatsSurfacesPriceTheScannedWindow` uses a 30-second filter
// inside a one-hour bucket, so per-bucket narrowing and range-wide narrowing
// coincide and it cannot tell them apart. A reviewer hoisted the narrowing out
// of the loop -- giving every bucket the range's window -- and the ENTIRE
// suite stayed green while a filter spanning two buckets went from the node's
// [1, 1] to [0.909..., 0.0909...].
//
// rate() is the aggregate that can see this: it divides by the window width,
// so a bucket priced on the wrong width is a wrong number rather than a
// missing one. count() cannot -- it does not divide -- which is why the
// count() rows of the older tests proved nothing about the window.
func TestABucketCrossingTimeFilterIsPricedPerBucket(t *testing.T) {
	all := corpus(1)
	single := realShard(t, all[0])
	parts := corpus(2)
	cluster := router(t, realShard(t, parts[0]).URL, realShard(t, parts[1]).URL)

	// Buckets are [12:00:00,12:00:10) and [12:00:10,12:00:20). The filter runs
	// 12:00:00 to 12:00:10, so it fills the first and misses the second -- it
	// cannot be satisfied by any single-bucket window.
	q := `_time:[2026-06-01T12:00:00Z, 2026-06-01T12:00:10Z] | sort by (n) | stats rate() r`
	p := "/select/logsql/stats_query_range?step=10s" +
		"&start=2026-06-01T12:00:00Z&end=2026-06-01T12:00:20Z&query=" + urlEscape(q)

	codeS, bodyS := chaosGet(t, single.URL+p)
	codeC, bodyC := chaosGet(t, cluster.URL+p)
	if codeS != 200 {
		t.Fatalf("the node answered %d: %.250s", codeS, bodyS)
	}
	if codeC != codeS {
		t.Fatalf("node %d, cluster %d: %.250s", codeS, codeC, bodyC)
	}
	gotS, gotC := statsSet(t, bodyS), statsSet(t, bodyC)
	if len(gotS) == 0 {
		t.Fatalf("the node produced no series, so this compares two empty "+
			"answers: %.250s", bodyS)
	}
	if !equalSets(gotS, gotC) {
		t.Errorf("node and cluster price the crossing filter differently\n"+
			"  node:    %v\n  cluster: %v", gotS, gotC)
	}
}
