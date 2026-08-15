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
	q, err := ParseLogsQL(`* | fields a, b | rename a as c | sort by (c) | limit 5`)
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

// A quantile is refused rather than answered. A quantile of a union is not any
// function of the shards' quantiles, so merging them produces a number that
// looks like a latency and is not one.
func TestAQuantileQueryIsRejected(t *testing.T) {
	q, err := ParseLogsQL(`* | stats quantile(0.99, duration) p99`)
	if err != nil {
		t.Skipf("this build does not parse quantile: %v", err)
	}
	plan := PlanDistributed(q.Pipes)
	if plan.Reject == "" {
		t.Fatal("a quantile across shards was accepted; the median of medians is not the median")
	}
	if !strings.Contains(plan.Reject, "sketch") {
		t.Errorf("the refusal does not say what would fix it: %s", plan.Reject)
	}
}
