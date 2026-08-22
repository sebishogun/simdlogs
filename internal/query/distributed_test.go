package query

import (
	"strings"
	"testing"
)

// Pipe classification. The default matters most: a pipe this table has not
// been taught about must run at the COORDINATOR, because the alternative
// default -- assume row-local -- is how a newly added pipe silently starts
// returning per-shard answers.
func TestPipeClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pipe  Pipe
		class PipeClass
	}{
		{"fields", &FieldsPipe{}, PipeRowLocal},
		{"rename", &RenamePipe{}, PipeRowLocal},
		{"delete", &DeletePipe{}, PipeRowLocal},
		{"format", &FormatPipe{}, PipeRowLocal},
		{"filter", &FilterPipe{}, PipeRowLocal},
		{"math", &MathPipe{}, PipeRowLocal},

		{"sort", &SortPipe{}, PipeGlobalOrder},
		{"limit", &LimitPipe{}, PipeGlobalOrder},
		{"offset", &OffsetPipe{}, PipeGlobalOrder},
		{"tail", &TailPipe{}, PipeGlobalOrder},
		{"top", &TopPipe{}, PipeGlobalOrder},
		{"uniq", &UniqPipe{}, PipeGlobalOrder},

		{"join", &JoinPipe{}, PipeCoordinatorOnly},
		{"union", &UnionPipe{}, PipeCoordinatorOnly},
		{"stream_context", &StreamContextPipe{}, PipeCoordinatorOnly},
		{"sample", &SamplePipe{}, PipeCoordinatorOnly},

		{"stats count", &StatsPipe{Aggs: []Agg{{Kind: AggCount}}}, PipeMergeableAggregate},
		{"stats sum", &StatsPipe{Aggs: []Agg{{Kind: AggSum}}}, PipeMergeableAggregate},
		{"stats min/max", &StatsPipe{Aggs: []Agg{{Kind: AggMin}, {Kind: AggMax}}},
			PipeMergeableAggregate},
		{"stats quantile", &StatsPipe{Aggs: []Agg{{Kind: AggQuantile}}}, PipeCoordinatorOnly},
		{"stats avg", &StatsPipe{Aggs: []Agg{{Kind: AggAvg}}}, PipeCoordinatorOnly},
		{"stats count_uniq", &StatsPipe{Aggs: []Agg{{Kind: AggCountUniq}}}, PipeCoordinatorOnly},
		{"stats mixed", &StatsPipe{Aggs: []Agg{{Kind: AggCount}, {Kind: AggQuantile}}},
			PipeCoordinatorOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyPipe(tc.pipe); got != tc.class {
				t.Fatalf("%T is %s, want %s", tc.pipe, got, tc.class)
			}
		})
	}
}

// The non-mergeable aggregates each explain themselves, because the router
// puts the reason in a 400 and a caller has to be able to act on it.
func TestNonMergeableAggregatesExplainThemselves(t *testing.T) {
	for _, tc := range []struct {
		kind AggKind
		want string
	}{
		{AggQuantile, "median of medians"},
		{AggAvg, "average of averages"},
		{AggCountUniq, "double-counts"},
		{AggUniq, "double-counts"},
		{AggHistogram, "histogram"},
		{AggRate, "window coverage"},
	} {
		got := NonMergeableReason([]Agg{{Kind: tc.kind}})
		if got == "" {
			t.Errorf("kind %d has no explanation", tc.kind)
			continue
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("kind %d says %q, expected it to mention %q", tc.kind, got, tc.want)
		}
	}
	// And the mergeable ones say nothing.
	if got := NonMergeableReason([]Agg{{Kind: AggCount}, {Kind: AggSum}}); got != "" {
		t.Errorf("count+sum is mergeable but reported %q", got)
	}
}

// The plan splits at the first non-row-local pipe, and everything from there
// runs at the coordinator.
func TestPlanSplitsAtTheFirstNonRowLocalPipe(t *testing.T) {
	// `fields a, b` does not keep _time, so it stops the push-down even though
	// it is row-local: the coordinator merges in time order and reads that time
	// from the row, so a shard that projected it away leaves every merged row
	// sharing the key 0 and the order becomes goroutine completion order. The
	// query below keeps _time, so the split is the one this test is about.
	q, err := ParseLogsQL(`* | fields _time, a, b | rename a as c | sort by (c) | limit 5`)
	if err != nil {
		t.Fatal(err)
	}
	plan := PlanDistributed(q.Pipes)
	if plan.Reject != "" {
		t.Fatalf("rejected: %s", plan.Reject)
	}
	if len(plan.ShardPipes) != 2 {
		t.Fatalf("%d shard pipes, want 2 (fields, rename)", len(plan.ShardPipes))
	}
	if len(plan.CoordinatorPipes) != 2 {
		t.Fatalf("%d coordinator pipes, want 2 (sort, limit)", len(plan.CoordinatorPipes))
	}

	// A row-local pipe AFTER a global one still runs at the coordinator: once
	// the rows have been reordered or cut, applying anything on a shard is
	// applying it to the wrong set.
	q2, _ := ParseLogsQL(`* | sort by (t) | fields a`)
	plan2 := PlanDistributed(q2.Pipes)
	if len(plan2.ShardPipes) != 0 {
		t.Fatalf("%d shard pipes after a sort, want 0", len(plan2.ShardPipes))
	}
	if len(plan2.CoordinatorPipes) != 2 {
		t.Fatalf("%d coordinator pipes, want 2", len(plan2.CoordinatorPipes))
	}
}

// A bare filter is entirely distributable: nothing runs at the coordinator but
// the merge.
func TestABareFilterNeedsNoCoordinatorWork(t *testing.T) {
	q, _ := ParseLogsQL(`level:=error`)
	plan := PlanDistributed(q.Pipes)
	if len(plan.ShardPipes) != 0 || len(plan.CoordinatorPipes) != 0 {
		t.Fatalf("%+v", plan)
	}
	if ok, why := Distributable(q.Pipes); !ok {
		t.Fatalf("a bare filter is not distributable: %s", why)
	}
}

// A quantile is PLANNED for the coordinator, not refused.
//
// It used to be refused, and the reason given was true of the wrong thing: a
// quantile of a union is not any function of the shards' quantiles, so MERGING
// per-shard quantiles produces a number that looks like a latency and is not
// one. The planner does not merge them. A StatsPipe is never row-local, so it
// lands past the split point and runs once at the coordinator over the merged
// rows -- the same rows, through the same pipe, as a single node.
//
// The pipe must be at the COORDINATOR, not merely unrefused: this fails if a
// later pushdown puts it on the shards without also restoring the refusal.
func TestAQuantileIsPlannedForTheCoordinator(t *testing.T) {
	q, err := ParseLogsQL(`* | stats quantile(0.99, duration) p99`)
	if err != nil {
		t.Skipf("this build does not parse quantile: %v", err)
	}
	plan := PlanDistributed(q.Pipes)
	if plan.Reject != "" {
		t.Fatalf("a quantile the coordinator computes over merged rows was refused: %s",
			plan.Reject)
	}
	if len(plan.ShardPipes) != 0 {
		t.Fatalf("the stats pipe was pushed to the shards (%d of them); a shard-computed "+
			"quantile cannot be merged and the refusal has to come back with the "+
			"pushdown", len(plan.ShardPipes))
	}
	if len(plan.CoordinatorPipes) != 1 {
		t.Fatalf("the stats pipe is not at the coordinator: %+v", plan)
	}
}

// The refusal fires for a stats pipe on a SHARD, which is the case it names.
//
// Nothing reaches it through PlanDistributed today, and that is the point: the
// rule is "where does this aggregate run", so it is tested where it is decided
// rather than through a request that cannot produce it. Without this, a
// pushdown could land and the refusal it needs would be dead code that still
// compiles.
func TestARefusalStillFiresForAShardSideAggregate(t *testing.T) {
	q, err := ParseLogsQL(`* | stats quantile(0.99, duration) p99`)
	if err != nil {
		t.Skipf("this build does not parse quantile: %v", err)
	}
	why := rejectReason(q.Pipes) // as if the planner had pushed it down
	if why == "" {
		t.Fatal("a quantile computed on a shard and merged here was accepted; " +
			"the median of medians is not the median")
	}
	if !strings.Contains(why, "sketch") {
		t.Errorf("the refusal does not say what would fix it: %s", why)
	}
	// And an additive aggregate is still fine on a shard.
	sum, err := ParseLogsQL(`* | stats sum(n) s`)
	if err != nil {
		t.Fatal(err)
	}
	if why := rejectReason(sum.Pipes); why != "" {
		t.Errorf("a sum has a mergeable partial state and was refused: %s", why)
	}
}

// A projection that drops _time is not pushed to the shards.
//
// It is row-local in shape, and pushing it down breaks the merge: the
// coordinator orders shard rows by the time it reads from each row, so a shard
// that projected _time away leaves every row sharing the key 0 and the order
// becomes whichever fan-out goroutine finished first.
func TestAProjectionThatDropsTimeStaysAtTheCoordinator(t *testing.T) {
	for _, tc := range []struct {
		q          string
		wantShard  int
		wantCoord  int
		reasonNote string
	}{
		{`* | fields _msg | sort by (_msg)`, 0, 2, "fields without _time"},
		{`* | fields _time, _msg | sort by (_msg)`, 1, 1, "fields keeping _time"},
		{`* | delete _time | sort by (_msg)`, 0, 2, "delete of _time"},
		{`* | delete _msg | sort by (level)`, 1, 1, "delete of another field"},
		{`* | rename a as b | sort by (b)`, 1, 1, "rename touches no _time"},
	} {
		t.Run(tc.reasonNote, func(t *testing.T) {
			q, err := ParseLogsQL(tc.q)
			if err != nil {
				t.Skipf("does not parse: %v", err)
			}
			plan := PlanDistributed(q.Pipes)
			if len(plan.ShardPipes) != tc.wantShard {
				t.Fatalf("%d shard pipes, want %d (%s)",
					len(plan.ShardPipes), tc.wantShard, tc.reasonNote)
			}
			if len(plan.CoordinatorPipes) != tc.wantCoord {
				t.Fatalf("%d coordinator pipes, want %d",
					len(plan.CoordinatorPipes), tc.wantCoord)
			}
		})
	}
}

// `rejectReason` is unreachable BY CONSTRUCTION, and this asserts the
// construction rather than the result.
//
// Two existing tests assert `Distributable(...) == (true, "")` for queries
// including non-mergeable aggregates, which reads as coverage of the refusal
// and is a tautology: `rejectReason` reads `ShardPipes`, `PlanDistributed`
// appends to `ShardPipes` only for `PipeRowLocal`, and `ClassifyPipe` never
// returns `PipeRowLocal` for a `*StatsPipe`. So no StatsPipe can be in the
// slice `rejectReason` walks, and "" is the only answer it has.
//
// A test that asserts the answer passes forever and says nothing. This asserts
// the two facts the answer rests on, so the day a stats pipe becomes
// push-downable -- which is the change that makes the refusal live, and a
// wrong quantile if it is not there -- this fails and names what changed.
func TestRejectReasonIsUnreachableByConstruction(t *testing.T) {
	// FACT ONE: ClassifyPipe never calls a StatsPipe row-local. If it did, a
	// non-mergeable aggregate would be pushed to the shards and the
	// coordinator would combine per-shard quantiles -- the median of medians.
	// Parsed rather than hand-built, so the Agg shape cannot drift out from
	// under the assertion.
	for _, q := range []string{
		`* | stats count() c`,
		`* | stats sum(n) s`,
		`* | stats avg(n) a`,
		`* | stats quantile(0.5, n) p`,
		`* | stats uniq(user) u`,
		`* | stats count_uniq(user) cu`,
	} {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		for _, p := range pq.Pipes {
			sp, ok := p.(*StatsPipe)
			if !ok {
				continue
			}
			if got := ClassifyPipe(sp); got == PipeRowLocal {
				t.Errorf("ClassifyPipe(%q's stats pipe) = PipeRowLocal. A stats "+
					"pipe in ShardPipes makes rejectReason live -- and until it "+
					"is wired to a caller, it makes a non-mergeable aggregate "+
					"merge wrongly instead of being refused", q)
			}
		}
	}

	// FACT TWO: PlanDistributed puts nothing but row-local pipes in ShardPipes.
	for _, q := range []string{
		`* | stats quantile(0.5, n) p`,
		`* | stats uniq(user) u`,
		`* | filter level:error | stats avg(n) a`,
		`* | sort by (n) | stats count() c`,
	} {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		plan := PlanDistributed(pq.Pipes)
		for i, p := range plan.ShardPipes {
			if _, isStats := p.(*StatsPipe); isStats {
				t.Errorf("%q put a StatsPipe at ShardPipes[%d]; rejectReason "+
					"is now reachable and every branch on it is now live", q, i)
			}
			if ClassifyPipe(p) != PipeRowLocal {
				t.Errorf("%q put a non-row-local pipe at ShardPipes[%d]", q, i)
			}
		}
	}
}
