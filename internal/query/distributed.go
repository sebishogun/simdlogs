package query

import "fmt"

// Distributed execution: which pipes may run on a shard, and which must not.
//
// # The failure this replaces
//
// The router sent the WHOLE query to every shard and concatenated the final
// rows. For a filter that is right. For anything else it is not, and the
// wrongness is silent:
//
//   - `* | stats count() n` ran a complete aggregation per shard, so a
//     three-shard cluster answered three rows, each a shard-local count. A
//     client reading `n` from the first row got a third of the answer.
//   - `* | sort by (t) | limit 10` returned each shard's top ten concatenated
//     -- thirty rows, and the true global top ten only by luck.
//   - `* | uniq by (user)` returned each shard's distinct users, so a user
//     present on two shards appeared twice and "distinct" meant nothing.
//
// Every one of those produces a plausible number. That is what makes it worth
// a planner rather than a warning in a document.
//
// # The classification
//
// A pipe is one of four things, and the class decides where it runs.
//
// The planner is deliberately CONSERVATIVE at the boundary: the shard part is
// the longest row-local PREFIX, and everything from the first non-row-local
// pipe onward runs at the coordinator over the merged rows.
//
// It does NOT push aggregates down. An earlier version of this paragraph
// described a pushdown "applied only when the aggregate is the whole remaining
// pipeline", and PlanDistributed has never done that -- a mergeable count or
// sum still runs once at the coordinator over the merged rows. That is correct
// and slower than it could be; putting a partial state on the wire is the
// change that would make it faster, and a subtly wrong partial state is a
// number nobody can spot.

// PipeClass is where a pipe may run.
type PipeClass uint8

const (
	// PipeRowLocal transforms each row independently of every other row, so a
	// shard can apply it to its own rows and the merged result is identical.
	// Filters and rewriters.
	PipeRowLocal PipeClass = iota

	// PipeMergeableAggregate produces a partial state a coordinator can
	// combine: count, sum, min, max, and average as sum-plus-count.
	PipeMergeableAggregate

	// PipeGlobalOrder needs every row before it can emit its first: sort,
	// limit, offset, tail, top, uniq. A shard cannot know whether its own
	// best row is the cluster's.
	PipeGlobalOrder

	// PipeCoordinatorOnly must run once, at the coordinator, over the merged
	// rows -- because it runs a subquery, reaches across streams, or has no
	// mergeable partial state.
	PipeCoordinatorOnly
)

func (c PipeClass) String() string {
	switch c {
	case PipeRowLocal:
		return "row-local"
	case PipeMergeableAggregate:
		return "mergeable-aggregate"
	case PipeGlobalOrder:
		return "global-order"
	}
	return "coordinator-only"
}

// ClassifyPipe reports where a pipe may run.
//
// The default is PipeCoordinatorOnly. A pipe this function has not been taught
// about runs at the coordinator, which is slower and correct -- and the
// alternative default, "assume it is row-local", is exactly how a new pipe
// would silently start returning per-shard answers.
// Four of the cases below return what the final default returns. They are not
// redundant and they are not a gate: they are where the reason for each pipe is
// written down, and a test that asserted their return value would pass with the
// case deleted. What makes the default unreachable is
// TestEveryPipeIsClassifiedExplicitly, which takes the set of pipes from the
// package source and requires a case for each -- so a pipe added to the
// language cannot silently become coordinator-only.
func ClassifyPipe(p Pipe) PipeClass {
	switch pp := p.(type) {
	// Row-local: each output row is a function of one input row.
	case *FieldsPipe, *RenamePipe, *DeletePipe, *CopyPipe, *FilterPipe,
		*FormatPipe, *ExtractPipe, *ExtractRegexpPipe, *MathPipe, *LenPipe,
		*UnpackJSONPipe, *UnpackLogfmtPipe, *UnpackSyslogPipe, *UnpackWordsPipe,
		*ReplacePipe, *HashPipe, *DecolorizePipe, *PackPipe, *DropEmptyPipe,
		*JSONArrayLenPipe, *CollapseNumsPipe, *UnrollPipe:
		return PipeRowLocal

	// Sampling is row-local in shape and NOT distributable in meaning: each
	// shard sampling 10% of its own rows gives 10% of the cluster, but the
	// selection is per shard and a caller asking for a sample of the cluster
	// gets a stratified one instead. Coordinator.
	case *SamplePipe:
		return PipeCoordinatorOnly

	case *StatsPipe:
		if mergeableAggs(pp.Aggs) {
			return PipeMergeableAggregate
		}
		return PipeCoordinatorOnly

	// These need the whole result set to be right.
	case *SortPipe, *LimitPipe, *OffsetPipe, *TailPipe, *TopPipe, *UniqPipe,
		*RankPipe:
		return PipeGlobalOrder

	// Subqueries and cross-row reach: one execution, at the coordinator.
	case *JoinPipe, *UnionPipe, *StreamContextPipe:
		return PipeCoordinatorOnly

	// Introspection answers about the store it runs on. Merged by their own
	// endpoints, not through the row pipeline.
	case *FieldValuesPipe, *FieldNamesPipe, *FacetsPipe, *BlocksCountPipe,
		*BlockStatsPipe:
		return PipeCoordinatorOnly
	}
	return PipeCoordinatorOnly
}

// mergeableAggs reports whether every aggregate in a stats pipe has a partial
// state a coordinator can combine.
//
// Additive or extremal only. An average is mergeable because it is a sum and a
// count -- but only if the SHARDS return both, which means the coordinator
// must rewrite the pipe rather than merge the averages. Averaging averages is
// the classic wrong answer: three shards reporting 10, 20 and 30 over 1, 1 and
// 1000 rows average to 20 and the true mean is 29.97.
func mergeableAggs(aggs []Agg) bool {
	for i := range aggs {
		switch aggs[i].Kind {
		case AggCount, AggSum, AggMin, AggMax, AggSumLen, AggCountEmpty:
		default:
			// Quantiles, uniq, count_uniq, histogram, values, rate and the
			// row_min/row_max family are all either non-additive or need a
			// sketch this does not have. See NonMergeableReason.
			return false
		}
	}
	return true
}

// NonMergeableReason explains why an aggregate cannot be distributed, in terms
// a caller can act on.
//
// Quantiles are the important one. A quantile of a union is not any function
// of the shards' quantiles -- the median of medians is not the median -- so
// merging them produces a number that looks like a latency and is not one. The
// fix is a mergeable sketch (t-digest or DDSketch) with a documented error
// bound; until that exists and is tested against the exact single-node answer,
// the router REFUSES rather than returning a plausible wrong percentile.
// Silently wrong latency numbers are how capacity decisions get made badly.
func NonMergeableReason(aggs []Agg) string {
	for i := range aggs {
		switch aggs[i].Kind {
		case AggCount, AggSum, AggMin, AggMax, AggSumLen, AggCountEmpty:
			continue
		case AggQuantile:
			return "quantile() cannot be merged across shards: a quantile of a union is not " +
				"any function of the shards' quantiles (the median of medians is not the " +
				"median). It needs a mergeable sketch with a documented error bound, which " +
				"this build does not have, so it is refused rather than answered wrongly"
		case AggAvg:
			return "avg() is not merged as an average of averages, which is wrong whenever " +
				"the shards hold different row counts. Use sum() and count() and divide, " +
				"which the router does merge"
		case AggUniq, AggCountUniq:
			return "uniq()/count_uniq() cannot be merged across shards without an HLL " +
				"sketch on the wire: summing per-shard distinct counts double-counts every " +
				"value present on more than one shard"
		case AggHistogram:
			return "histogram() cannot be merged across shards in this build"
		case AggRate, AggRateSum:
			return "rate() is computed over the query window per shard and cannot be summed " +
				"without knowing each shard's window coverage"
		default:
			return fmt.Sprintf("this aggregate (kind %d) has no mergeable partial state",
				aggs[i].Kind)
		}
	}
	return ""
}

// A Plan is how one query is split between the shards and the coordinator.
type Plan struct {
	// ShardPipes run on every shard, over that shard's own rows.
	ShardPipes []Pipe
	// CoordinatorPipes run once, at the coordinator, over the merged rows.
	CoordinatorPipes []Pipe
	// Reject is non-empty when the query cannot be answered correctly across
	// shards at all. The router refuses with it rather than returning a
	// plausible wrong number.
	Reject string
}

// PlanDistributed splits a pipeline.
//
// The split point is the FIRST pipe that is not row-local. Everything before
// it runs on the shards, because a row-local pipe applied per shard and then
// merged gives the same rows as applied after merging -- and doing it on the
// shards is where the data already is. Everything from that point on runs at
// the coordinator, over the merged rows.
//
// This is the correctness-first fallback the plan calls for: it is never
// wrong, and it is slower than it could be for an aggregate that could have
// been pushed down. Pushing one down means putting its partial state on the
// wire, and a partial state that is subtly wrong is a number nobody can spot.
func PlanDistributed(pipes []Pipe) Plan {
	var plan Plan
	for i, p := range pipes {
		class := ClassifyPipe(p)
		// A pipe that strips _time stops the push-down, however row-local it
		// is.
		//
		// The coordinator merges shard rows in TIME ORDER, and it reads that
		// time from the row. A `fields` or `delete` applied on the shards
		// removes it, so every merged row shares the key 0 and the order
		// becomes whichever fan-out goroutine finished first: `* | fields _msg
		// | limit 5` over three shards returned shard 2's first five rows.
		//
		// Run at the coordinator instead, the projection happens after the
		// merge, which is where a single node does it too -- it projects after
		// its scan, not before.
		if class == PipeRowLocal && stripsTime(p) {
			class = PipeCoordinatorOnly
		}
		if class == PipeRowLocal {
			plan.ShardPipes = append(plan.ShardPipes, p)
			continue
		}
		plan.CoordinatorPipes = append(plan.CoordinatorPipes, pipes[i:]...)
		plan.Reject = rejectReason(plan.CoordinatorPipes)
		return plan
	}
	plan.Reject = rejectReason(plan.CoordinatorPipes)
	return plan
}

// rejectReason explains why a pipeline cannot be answered across shards, or ""
// when it can.
//
// It scans EVERY coordinator pipe, not only the one at the split point. The
// check used to fire only when a stats pipe happened to be first past the
// split, so the same aggregate was refused in one pipe order and answered in
// another:
//
//   - | stats avg(n) a              400, "average of averages"
//   - | sort by (n) | stats avg(n) a   200, {"a":"14.5"}
//
// Both run the aggregate once, at the coordinator, over the merged rows -- so
// the second answer was correct and the first refusal was over-strict. That
// inconsistency is worse than either policy: whichever a client tried first
// became the one they believed.
//
// The refusal is kept rather than dropped because it is not always
// over-strict: an aggregate the coordinator computes over merged rows is fine,
// and one a shard computes and the coordinator sums is not. Which of those
// happens is decided by the pushdown, and the pushdown is what may change.
func rejectReason(coord []Pipe) string {
	for _, p := range coord {
		sp, ok := p.(*StatsPipe)
		if !ok {
			continue
		}
		if why := NonMergeableReason(sp.Aggs); why != "" {
			return why
		}
	}
	return ""
}

// Distributable reports whether a pipeline can be answered across shards. It
// is Plan.Reject == "" and exists so a caller can ask without building a plan.
func Distributable(pipes []Pipe) (bool, string) {
	p := PlanDistributed(pipes)
	return p.Reject == "", p.Reject
}

// stripsTime reports whether a pipe removes _time from the rows it emits.
//
// Only the two projections can: `fields` keeps a named set, and `delete` drops
// one. Every other row-local pipe adds or rewrites fields and leaves _time
// alone. A pipe added later that can remove it belongs here -- the cost of
// missing one is a merge ordered by goroutine completion, which looks like data
// in a plausible but wrong order.
func stripsTime(p Pipe) bool {
	switch t := p.(type) {
	case *FieldsPipe:
		for _, f := range t.Keep {
			if f == "_time" {
				return false
			}
		}
		return true
	case *DeletePipe:
		for _, f := range t.Drop {
			if f == "_time" {
				return true
			}
		}
	}
	return false
}
