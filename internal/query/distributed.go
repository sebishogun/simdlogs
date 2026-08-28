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
// the router does not merge them at all -- it fetches the rows and computes the
// quantile once, which is exact and moves every matching row. This string is
// what a caller is told when an aggregate CANNOT be answered that way either,
// which today means only a pushed-down one. Silently wrong latency numbers are
// how capacity decisions get made badly.
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
		plan.Reject = rejectReason(plan.ShardPipes)
		return plan
	}
	plan.Reject = rejectReason(plan.ShardPipes)
	return plan
}

// rejectReason explains why a pipeline cannot be answered across shards, or ""
// when it can.
//
// It reads the SHARD half of the plan, because that is the only half where a
// non-mergeable aggregate is wrong.
//
// The rule is about WHERE the aggregate runs, not which aggregate it is. A
// shard computing a quantile of its own rows and a coordinator combining those
// quantiles gives a number that is not the cluster's quantile -- the median of
// medians is not the median. A coordinator computing that same quantile ONCE
// over every shard's merged rows gives exactly what a single node gives, from
// the same rows through the same pipe, and there is nothing left to merge.
//
// It used to read the coordinator half, and so refused the second case as if
// it were the first. That was over-strict against the planner as it exists:
// PlanDistributed splits at the first non-row-local pipe and a StatsPipe is
// never row-local, so no stats pipe has ever been pushed to a shard -- every
// aggregate already ran at the coordinator over merged rows, and the refusal
// was declining to return an answer this code had computed correctly. Measured
// on two shards holding 2500 rows each: with the refusal lifted, avg,
// quantile, uniq, count_uniq and histogram were byte-identical to the single
// node's, grouped and ungrouped.
//
// The check stays rather than being deleted because the pushdown is what may
// change. The day a mergeable partial state goes on the wire, a StatsPipe
// appears in ShardPipes and this refuses the aggregates that cannot survive
// the trip -- which is the condition it was always trying to name.
//
// It scans every pipe of that half, not only the one at the split point. The
// check once fired only when a stats pipe happened to be first past the split,
// so one pipe order was refused and another answered:
//
//   - | stats avg(n) a                 400, "average of averages"
//   - | sort by (n) | stats avg(n) a   200, {"a":"14.5"}
//
// Whichever a client tried first became the one they believed.
// PROVABLY ALWAYS "" as the planner stands, and kept for that reason rather
// than in spite of it.
//
// `ClassifyPipe` returns PipeMergeableAggregate or PipeCoordinatorOnly for a
// *StatsPipe and never PipeRowLocal; `PlanDistributed` appends to ShardPipes
// only for PipeRowLocal. So no StatsPipe can be in `shard` and the loop cannot
// return non-"". Everything that branches on it -- `Plan.OK`, `Distributable`,
// the 400 in cluster_plan.go -- is dead, and two tests in distributed_test.go
// assert a tautology.
//
// It stays because it is the check that becomes live the moment a stats pipe
// becomes push-downable, which is a change somebody will make: a shard
// computing a quantile of its own rows and a coordinator combining those
// quantiles is a wrong number, and this is where that gets refused. Deleting
// it would remove the guard along with the dead branch.
//
// What is NOT acceptable is a test that asserts it returns "" and calls that
// coverage. See `TestRejectReasonIsUnreachableByConstruction`, which asserts
// the REASON it is unreachable and fails if the classifier changes.
func rejectReason(shard []Pipe) string {
	for _, p := range shard {
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

// ChangesRowCount reports that a pipe's output can hold a different number of
// rows than its input.
//
// It exists for ONE question: whether a pipe may run on a shard when the
// endpoint's `limit` is set. `limit` is LastN, and a node applies it to the
// scan -- before every pipe. A shard that filtered first has already discarded
// rows the bound would have kept, and the router cannot put them back:
//
//   - | filter level:info | sort by (_time)   with &limit=5
//     node     nothing, when the newest five are all level=error
//     cluster  five info rows, at HTTP 200
//
// A pipe that maps rows one-to-one is safe: each shard's newest n contains the
// cluster's newest n, so pushing it down with the bound is both correct and
// cheaper.
//
// THE DEFAULT IS TRUE. A pipe this switch does not name is treated as unsafe to
// push down, which costs a shard's full match set and never an answer. The
// opposite default would make a pipe added to the language silently eligible,
// and the failure would be a short answer at 200.
func ChangesRowCount(p Pipe) bool {
	switch p.(type) {
	case *FieldsPipe, *RenamePipe, *DeletePipe, *CopyPipe, *FormatPipe,
		*ExtractPipe, *ExtractRegexpPipe, *MathPipe, *LenPipe,
		*UnpackJSONPipe, *UnpackLogfmtPipe, *UnpackSyslogPipe, *UnpackWordsPipe,
		*ReplacePipe, *HashPipe, *DecolorizePipe, *PackPipe,
		*JSONArrayLenPipe, *CollapseNumsPipe:
		// One row in, one row out: fields are added, renamed, rewritten or
		// dropped, and no row is.
		return false
	}
	// Named for the reader rather than left to the default, because these are
	// the row-local pipes it is actually about: `filter` and `drop_empty`
	// remove rows, `unroll` replaces one row with as many as its array holds
	// (zero, for an empty one).
	return true
}

// MergeOp is how a coordinator combines one aggregate's per-shard values.
type MergeOp uint8

const (
	// MergeSum adds: count, sum, sum_len, count_empty. The union's value is
	// the sum of the shards' values because the shards hold disjoint rows.
	MergeSum MergeOp = iota
	// MergeMin and MergeMax take the extreme. min of mins IS the min; the sum
	// of mins is a number in the right units and on the right axis, which is
	// what makes it dangerous.
	MergeMin
	MergeMax
)

// MergeOps maps each output name of a query's final stats pipe to the operator
// that combines it across shards.
//
// mergeableAggs has always listed AggMin and AggMax as mergeable, and the
// comment above it says "Additive OR EXTREMAL only" -- but every federated
// stats merge added unconditionally, so `stats min(n)` over two shards holding
// 100-105 and 106-111 answered 206 rather than 100. That is not a plausible
// wrong answer, it is an arithmetically impossible one (no row has n=206), and
// it came back HTTP 200.
//
// ok is false when raw does not parse or does not end in a stats pipe, which
// tells a merge to refuse rather than guess an operator.
func MergeOps(raw string) (ops map[string]MergeOp, ok bool) {
	q, err := ParseLogsQL(raw)
	if err != nil {
		return nil, false
	}
	var sp *StatsPipe
	for i := range q.Pipes {
		if s, isStats := q.Pipes[i].(*StatsPipe); isStats {
			sp = s
		}
	}
	if sp == nil {
		return nil, false
	}
	ops = make(map[string]MergeOp, len(sp.Aggs))
	for i := range sp.Aggs {
		op, known := mergeOpFor(sp.Aggs[i].Kind)
		if !known {
			// mergeableAggs refuses the query before it reaches a merge, so a
			// kind arriving here means the two lists have drifted apart.
			// Refusing keeps them from disagreeing silently.
			return nil, false
		}
		ops[sp.Aggs[i].Alias] = op
	}
	return ops, true
}

func mergeOpFor(k AggKind) (MergeOp, bool) {
	switch k {
	case AggCount, AggSum, AggSumLen, AggCountEmpty:
		return MergeSum, true
	case AggMin:
		return MergeMin, true
	case AggMax:
		return MergeMax, true
	}
	return 0, false
}

// Combine applies op to a running value and the next shard's value. first says
// whether acc holds a shard's value yet, which matters for the extremal
// operators: a zero accumulator would win every min over positive values.
func (op MergeOp) Combine(acc, v float64, first bool) float64 {
	if first {
		return v
	}
	switch op {
	case MergeMin:
		if v < acc {
			return v
		}
		return acc
	case MergeMax:
		if v > acc {
			return v
		}
		return acc
	}
	return acc + v
}

// HasStatsPipe reports whether raw parses and contains a stats pipe.
//
// It is what decides which SHAPE a storage node's /select/logsql/stats_query
// answer has: with a stats pipe it is the Prometheus instant vector, without
// one StatsQueryInstant fails and the `by=` fallback emits this repository's
// own `{"stats":[{value,hits}]}`. A router switching on `by=` instead decoded
// the wrong shape for every grouped stats query.
func HasStatsPipe(raw string) bool {
	q, err := ParseLogsQL(raw)
	if err != nil {
		return false
	}
	for i := range q.Pipes {
		if _, isStats := q.Pipes[i].(*StatsPipe); isStats {
			return true
		}
	}
	return false
}
