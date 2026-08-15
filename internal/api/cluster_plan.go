package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
)

// Planning a LogsQL query across shards.
//
// # What the router did
//
// It sent the whole query string to every shard and concatenated the final
// rows. For a bare filter that is right, and for everything else it is wrong
// in a way that produces a plausible number:
//
//   - `* | stats count() n` aggregated per shard, so three shards answered
//     three rows each holding a third of the count.
//   - `* | sort by (t) | limit 10` returned each shard's top ten
//     concatenated -- thirty rows, the true top ten only by luck.
//   - `* | uniq by (user)` returned each shard's distinct users, so anyone
//     active on two shards appeared twice.
//
// # What it does now
//
// The pipeline is split. The longest ROW-LOCAL prefix is sent to the shards --
// filters and rewriters, which give the same answer applied before or after a
// merge, and which are cheapest where the data already is. Everything from the
// first non-row-local pipe onward is applied ONCE, here, over the merged rows.
//
// A query whose aggregate has no mergeable partial state is refused with the
// reason, not answered. See query.NonMergeableReason: the important case is
// quantiles, where a median of medians is not a median and the wrong answer
// still looks like a latency.

// planQuery splits the request's query and returns the string to send to the
// shards plus the pipes to apply here.
//
// The shard query IS the caller's text, cut at a pipe boundary -- the head plus
// as many segments as the planner kept.
//
// This comment used to claim the opposite ("not the caller's string with pipes
// trimmed textually"), which is the stronger and wrong version: rebuilding from
// the parse would need a printer for every pipe in the language, kept exactly
// in step with the parser, and that printer does not exist. What the code does
// is cut text and then CHECK the cut against the parse -- one segment per pipe
// plus the head -- refusing when they disagree. That is a weaker guarantee than
// the comment promised and it is the one the code makes.
func (s *Server) planQuery(w http.ResponseWriter, r *http.Request) (shardQuery string, coord []query.Pipe, ok bool) {
	raw := r.FormValue("query")
	if strings.TrimSpace(raw) == "" {
		raw = "*"
	}
	q, err := query.ParseLogsQL(raw)
	if err != nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, err.Error())
		return "", nil, false
	}
	plan := query.PlanDistributed(q.Pipes)
	if plan.Reject != "" {
		obs.L().Warn("refused a query that cannot be answered across shards",
			obs.FieldEvent, "cluster.plan_rejected",
			obs.FieldRoute, r.URL.Path, "reason", plan.Reject)
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, fmt.Sprintf(
			"simdlogs: this query cannot be answered correctly across shards. %s. "+
				"Run it against a single storage node, or rewrite it", plan.Reject))
		return "", nil, false
	}

	// The shard query is the filter plus the first N pipe segments, where N is
	// how many pipes the planner kept.
	//
	// A COUNT is enough because the planner only ever keeps a prefix: the nth
	// kept pipe is the nth text segment after the head, by construction. That
	// is also why it only keeps a prefix -- a planner that reordered or took
	// from the middle could not name its pipes in the original text, and
	// re-serialising a parsed pipe would mean writing a printer for every pipe
	// in the language and keeping it exactly in step with the parser.
	segs := pipeSegments(raw)

	// The split is CHECKED against the parse before it is trusted.
	//
	// pipeSegments walks the text with its own idea of quoting and nesting.
	// The lexer has a different one -- it does not treat `'` as a quote, for
	// instance -- so a filter containing an apostrophe made the walker swallow
	// every following `|`, leaving one segment. planQuery then shipped the
	// CALLER'S WHOLE STRING to every shard and re-applied the coordinator half
	// on top:
	//
	//   query=don't | stats count() c   3 shards x 10 rows
	//     single node  {"c":"30"}
	//     cluster      {"c":"3"}        each shard counted its own 10
	//
	// which is precisely the per-shard aggregation this planner exists to
	// remove, returning one believable number instead of three obvious ones.
	//
	// Rather than teach one walker to agree with a lexer it cannot see, the
	// disagreement is DETECTED: a correct split has exactly one segment per
	// pipe plus the head. When it does not, nothing is pushed down and the
	// whole pipeline runs at the coordinator -- always correct, sometimes
	// slower, and never a wrong number.
	if len(segs) != len(q.Pipes)+1 {
		// REFUSED, not worked around.
		//
		// The obvious fallback -- push nothing down and send segs[0] as the
		// filter -- is the defect itself: when the walker swallowed the pipes,
		// segs[0] IS the whole query, so every shard runs the entire pipeline
		// and the coordinator runs it again. There is no filter text to fall
		// back to, because extracting it needs the same walk that just failed.
		//
		// Deriving it from the parse instead would need a printer for every
		// pipe in the language, kept exactly in step with the parser -- which
		// is the thing this design avoided by counting segments in the first
		// place. So the router refuses, as it already does for an aggregate it
		// cannot merge: a query it cannot plan is not a query it may guess at.
		obs.L().Warn("refused a query whose text does not split the way it parses",
			obs.FieldEvent, "cluster.plan_text_mismatch", obs.FieldRoute, r.URL.Path,
			"segments", len(segs), "pipes", len(q.Pipes))
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: this query cannot be split across shards. Its text contains "+
				"a quote or bracket character that the pipe splitter treats as "+
				"grouping and the parser does not, so the two disagree about where "+
				"the pipes are ("+strconv.Itoa(len(segs))+" text segments for "+
				strconv.Itoa(len(q.Pipes))+" parsed pipes). Quote the value, or run "+
				"this against a single storage node")
		return "", nil, false
	}

	shardQuery = segs[0]
	for i := 1; i <= len(plan.ShardPipes) && i < len(segs); i++ {
		shardQuery += " | " + segs[i]
	}
	return shardQuery, plan.CoordinatorPipes, true
}

// pipeSegments splits a query's text into its top-level pipe segments, with
// the same quote and nesting awareness as queryHead.
func pipeSegments(raw string) []string {
	var out []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case quote != 0:
			if c == '\\' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			depth--
		case c == '|' && depth == 0:
			out = append(out, strings.TrimSpace(raw[start:i]))
			start = i + 1
		}
	}
	return append(out, strings.TrimSpace(raw[start:]))
}

// shardQueryURL rewrites the request's query parameter for the shards.
func shardQueryURL(r *http.Request, shardQuery string, coordPipes []query.Pipe) string {
	vals, _ := url.ParseQuery(r.URL.RawQuery)
	vals.Set("query", shardQuery)
	// `limit` is the CLIENT's bound on the final answer, and it only replaced
	// `query`.
	//
	// So the endpoint's limit travelled to every shard and truncated that
	// shard's rows before the coordinator half ever ran -- and the coordinator
	// branch never read it either. Both halves were wrong at once:
	//
	//   * | stats count() c   &limit=5, 3 shards x 10 rows
	//       single node 30, cluster 15   -- each shard counted its first 5
	//   * | sort by (n)       &limit=5
	//       single node 5 rows, cluster 15 -- three times what was asked for,
	//                                        and a different five
	//
	// When there is a coordinator half, the shards must return everything that
	// matches and the bound is applied once, here, over the merged rows. With
	// no coordinator half the shard limit is exactly right and stays.
	if len(coordPipes) > 0 {
		vals.Del("limit")
	}
	return vals.Encode()
}

// applyCoordinatorPipes runs the non-distributable half of the plan over the
// merged rows.
//
// Under the same budget every other read obeys: the merged set is the whole
// cluster's matching rows, which is the largest thing this process handles, and
// a pipeline over it with no ceiling is the one place a router falls over
// rather than a storage node.
func (s *Server) applyCoordinatorPipes(
	r *http.Request, rows []query.Row, pipes []query.Pipe,
) ([]query.Row, error) {
	q := &query.Query{Pipes: pipes, MatAll: true}
	// The returned flag is not read: applyQueryBudget binds the query, so
	// every stop() records a reason and q.StopErr() below is the whole signal.
	// It was assigned to `_` with no explanation, which reads as a hole.
	s.applyQueryBudget(r, q)
	out := query.ApplyPipes(q, rows)
	if err := q.StopErr(); err != nil {
		return nil, err
	}
	return out, nil
}

// endpointLimit is the `limit` query parameter, or 0 when absent or unusable.
//
// A separate helper because two places need the same reading of it and they
// used to disagree: the shard request forwarded it verbatim and the coordinator
// ignored it entirely.
func endpointLimit(r *http.Request) int {
	v := r.FormValue("limit")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// limitBoundsOutput reports whether the endpoint's `limit` should bound this
// pipeline's OUTPUT rows at the coordinator.
//
// The rule is measured against a single node, not assumed, because `limit` does
// not mean one thing:
//
//	| stats count() c      &limit=5   1 row,  c=30    -- not bounded
//	| stats by (level) ... &limit=2   3 rows          -- not bounded
//	| uniq by (user)       &limit=4   7 rows          -- not bounded
//	| sort by (_msg)       &limit=5   5 rows          -- bounded
//	| fields level         &limit=7   7 rows          -- bounded
//
// What separates them is whether a pipe REDUCES: stats and uniq derive their
// row count from distinct values rather than from the input, and on a single
// node the limit reaches the scan and never the aggregate. sort and fields emit
// one row per input row, so bounding the output is the same as bounding the
// input and the client sees exactly N.
//
// q.Limit does not express this -- the engine feeds it to the scan, and the
// coordinator has no scan -- so the predicate is written here with the
// measurements above as its test. If a pipe is added whose reduction is not
// stats or uniq, this is the list that needs it.
func limitBoundsOutput(pipes []query.Pipe) bool {
	for _, p := range pipes {
		switch p.(type) {
		case *query.StatsPipe, *query.UniqPipe:
			return false
		}
	}
	return true
}

// withoutLimits clones a request with the result-shaping parameters removed, so
// a shard returns everything that matches and the bound is applied once.
//
// `limit` and `max_values_per_field` shape the ANSWER, and an answer is the
// cluster's. Forwarded to the shards they shape each shard's contribution
// instead, and a top-N built from truncated lists silently omits any value that
// is popular across the cluster and unremarkable on every shard.
//
// The cost is real and is the right trade: a shard returns its whole
// distribution rather than its top N. That is bounded by the field's
// cardinality, which the storage node already bounds with its own defaults, and
// the alternative is an answer that is wrong in a way nobody can see.
//
// DELETING a parameter is not the same as removing the bound. A handler that
// reads it with a non-zero default -- facets does, with DefaultFacetLimit --
// gets the default instead of "unlimited", so the merge still sums shard-local
// top-Ns. Callers whose parameter has a capping default pass an explicit
// unlimited value through unlimited[] instead of relying on absence.
//
// ok is false when the query string will not parse. This used to `return r`,
// silently forwarding the caller's `limit` to every shard -- so one malformed
// unrelated parameter (`&x=%zz`) turned a correct cluster top-N into a merge of
// shard-local top-Ns, and the value that was #1 cluster-wide and #11 on each
// shard vanished from a 200 response.
func withoutLimits(r *http.Request, unlimited map[string]string) (*http.Request, bool) {
	vals, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, false
	}
	vals.Del("limit")
	vals.Del("max_values_per_field")
	for k, v := range unlimited {
		vals.Set(k, v)
	}
	out := r.Clone(r.Context())
	out.URL.RawQuery = vals.Encode()
	return out, true
}

// refuseUnparseableQuery answers the caller when withoutLimits could not read
// the query string, and reports whether the caller should stop.
func (s *Server) refuseUnparseableQuery(w http.ResponseWriter, r *http.Request) {
	s.writeErr(w, r, readSpec(), http.StatusBadRequest,
		"simdlogs: this request's query string could not be parsed, so the "+
			"router cannot strip the per-shard result limits from it. Forwarding "+
			"it unchanged would build the cluster's top values out of each shard's "+
			"own top values, which silently drops anything popular across the "+
			"cluster and unremarkable on each shard, so it is refused instead")
}
