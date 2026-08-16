package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
)

// The router surfaces that were reading the router's own empty store.
//
// A select-router holds no data. Only a handler that KNOWS it is a router fans
// out; every other one runs against that empty local store and answers 200 with
// nothing. `{"facets":[]}` and `{"count":0}` are indistinguishable from a
// cluster that genuinely holds no matching data, so nothing reports a problem
// and a dashboard shows an empty panel.
//
// Found by TestNoRouterReadSilentlyReadsTheEmptyLocalStore, which sends the
// same request to a router and to a storage node holding the data and fails
// when the storage node answers with something and the router answers with
// nothing. Counting `len(s.backends) > 0` branches would have listed the
// handlers that DO federate; it cannot list the ones nobody remembered.

// federatedFacets merges per-field value counts across shards.
//
// Hits sum per (field, value) -- a field value present on three shards is one
// value with the three counts added, not three entries. Both limits then apply
// to the MERGED list: each shard applied its own before answering, and applying
// them again here is what makes "the top 10 values" the cluster's top 10 rather
// than the first shard's.
func (s *Server) federatedFacets(w http.ResponseWriter, r *http.Request) {
	// Without the limits, for the reason in federatedValueCounts: a shard that
	// truncated to its own top N can never contribute a value that is popular
	// across the cluster and unremarkable on each shard.
	// `limit=0` rather than deleting the parameter.
	//
	// facets reads it as intParam(r, "limit", DefaultFacetLimit), so ABSENT
	// means 10, not unlimited -- deleting it left every shard truncating to its
	// own top 10 and the merge summing those. `keep_const_fields=1` for the
	// same reason: a field that is constant on one shard and varied across the
	// cluster is dropped by each shard before the coordinator can see it.
	shardReq, ok := withoutLimits(r, map[string]string{
		"limit": "0",
		// The CALLER's value, forwarded -- not zero.
		//
		// Deleting it left every shard at its own default of 1000, so a field
		// with 1200 distinct values per shard was dropped by all of them and a
		// caller asking for 5000 got {"facets":[]}. Sending "0" fixed that and
		// was worse: `limit` is a RESULT shape and unlimited is cheap, but
		// max_values_per_field is a CARDINALITY bound and removing it makes
		// timeFacet materialize every matching row -- `_time` has roughly one
		// distinct value per row in a log store. Measured on one shard:
		//
		//	  40,000 rows   3.4 MB body    27.9 ms   +30 MiB
		//	 160,000 rows   13.6 MB body  114.7 ms  +127 MiB
		//	 640,000 rows   54.9 MB body   482 ms   +496 MiB
		//
		// 85.8 B/row, dead linear, on the default dashboard path -- and
		// peerMaxBodyBytes is 256 MiB, so above ~3.1M rows in the window every
		// cluster facets request FAILS, after each shard has allocated ~2.4 GiB
		// building a body the router discards. The answer it produced kept one
		// field.
		//
		// Forwarding the caller's value fixes the original defect without
		// removing the bound: a caller asking 5000 gets 5000 on every shard, a
		// caller asking nothing gets the shard default on every shard, and the
		// coordinator applies the same number again over the union.
		"max_values_per_field": maxValuesParam(r),
		"keep_const_fields":    "1",
	})
	if !ok {
		s.refuseUnparseableQuery(w, r)
		return
	}
	bodies, w, ok := s.fanOutChecked(w, shardReq, "/select/logsql/facets", nil)
	if !ok {
		return
	}
	// Field order is first-seen across shards, and values within a field sort
	// by count. Without the explicit order this answered in map order, which
	// changes per process -- the same defect as the storage column order.
	type fieldAcc struct {
		hits  map[string]int
		order []string
	}
	byField := map[string]*fieldAcc{}
	var fieldOrder []string
	for _, a := range bodies {
		var v struct {
			Facets []query.FieldFacet `json:"facets"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		for _, f := range v.Facets {
			fa := byField[f.FieldName]
			if fa == nil {
				fa = &fieldAcc{hits: map[string]int{}}
				byField[f.FieldName] = fa
				fieldOrder = append(fieldOrder, f.FieldName)
			}
			for _, val := range f.Values {
				if _, seen := fa.hits[val.FieldValue]; !seen {
					fa.order = append(fa.order, val.FieldValue)
				}
				fa.hits[val.FieldValue] += val.Hits
			}
		}
	}

	// `limit` truncates VALUES WITHIN A FIELD, which is what it does on a
	// storage node (introspect.go: `if limit > 0 && len(vc) > limit`). It used
	// to truncate FIELDS here, so `?limit=2` answered five fields of top-2
	// values on one node and two fields of every value on a cluster -- the same
	// parameter, two meanings, and the fields anyone is actually faceting on
	// missing from the cluster answer.
	limit := intParam(r, "limit", query.DefaultFacetLimit)
	maxValues := intParam(r, "max_values_per_field", query.DefaultFacetMaxValues)
	// The CALLER's keep_const_fields, not the one forced on the shards.
	keepConst := r.FormValue("keep_const_fields") == "1"
	out := make([]query.FieldFacet, 0, len(fieldOrder))
	for _, name := range fieldOrder {
		fa := byField[name]
		vals := make([]query.FacetValue, 0, len(fa.order))
		for _, v := range fa.order {
			vals = append(vals, query.FacetValue{FieldValue: v, Hits: fa.hits[v]})
		}
		// facetKeep, WHOLE, over the union: `distinct == 0 || (max > 0 &&
		// distinct > max)` drops it, and then `distinct > 1 || keepConst`
		// decides. Both halves, applied here rather than at the shards,
		// because a field can be under the cap on each shard and over it on
		// the union, and constant on each shard and varied across it.
		//
		// The const half was missing, and shipping only the cardinality half
		// was a REGRESSION: the shards are sent keep_const_fields=1 so a field
		// that is constant on one shard survives to be judged here, and
		// nothing here judged it. Measured on two nodes holding six rows each
		// against a single node holding all twelve, `?query=*`: the cluster
		// returned 7 fields (_msg _stream _stream_id _time level n service) and
		// the node returned 4. Every field constant over the queried window --
		// _stream and _stream_id whenever the filter selects one stream, host
		// on a single-host cluster, the field just filtered on -- appeared in
		// a cluster facet list and not a node's.
		if !facetKeepUnion(len(vals), maxValues, keepConst) {
			continue
		}
		sort.Slice(vals, func(i, j int) bool {
			if vals[i].Hits != vals[j].Hits {
				return vals[i].Hits > vals[j].Hits
			}
			return vals[i].FieldValue < vals[j].FieldValue
		})
		if limit > 0 && len(vals) > limit {
			vals = vals[:limit]
		}
		out = append(out, query.FieldFacet{FieldName: name, Values: vals})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"facets": out})
}

// maxValuesParam is the caller's max_values_per_field as a string, or the
// storage node's own default when absent -- the value forwarded to the shards.
func maxValuesParam(r *http.Request) string {
	return strconv.Itoa(intParam(r, "max_values_per_field", query.DefaultFacetMaxValues))
}

// facetKeepUnion is query.facetKeep over the merged distribution. It is
// duplicated rather than exported because the rule belongs to the engine and
// the coordinator has to apply the same one to a set the engine never sees.
func facetKeepUnion(distinct, maxPerField int, keepConst bool) bool {
	if distinct == 0 || (maxPerField > 0 && distinct > maxPerField) {
		return false
	}
	return distinct > 1 || keepConst
}

// vectorSeries is one entry of the Prometheus instant-vector envelope.
type vectorSeries struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

// federatedVector merges an instant vector across shards.
//
// The plain (`by=`-less) stats_query branch decoded `{"count":N}` from each
// backend. No backend emits that: the handler answers the Prometheus vector
// envelope, so `v.Count` was always the zero value and the router answered
// `{"count":0}` for every query against any cluster, however much data the
// shards held. A confident zero.
//
// Values sum per metric label set, which is correct for exactly the aggregates
// PlanDistributed calls mergeable. A query whose aggregate is not one of those
// is refused here for the same reason it is refused there: summing shard
// quantiles produces a number that looks like a latency and is not one.
func (s *Server) federatedVector(w http.ResponseWriter, r *http.Request) {
	if !s.rejectNonMergeableStats(w, r) {
		return
	}
	bodies, w, ok := s.fanOutChecked(w, r, "/select/logsql/stats_query", nil)
	if !ok {
		return
	}
	// The operator per output name, from the query. Every federated stats merge
	// used to ADD unconditionally, and mergeableAggs lists min and max as
	// mergeable, so `stats min(n)` over two shards holding 100-105 and 106-111
	// answered 206. No row has n=206; the value is not merely wrong, it is
	// impossible, and it came back 200.
	// ops is nil for a query with NO stats pipe, where there is no aggregate to
	// get wrong and summing is the only defined behaviour. When there IS one,
	// the operator comes from it and an output the pipe does not name is
	// refused.
	ops, opsOK := query.MergeOps(r.FormValue("query"))
	if query.HasStatsPipe(r.FormValue("query")) && !opsOK {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: this query's aggregates cannot be combined across shards. "+
				"The router reads from the stats pipe whether each output is summed "+
				"or taken extremally, and this build does not know for at least one "+
				"of them, so the query is refused rather than summed by default")
		return
	}
	if !opsOK {
		ops = nil
	}
	type acc struct {
		metric map[string]string
		stamp  any
		val    float64
		seen   bool
		op     query.MergeOp
	}
	byLabels := map[string]*acc{}
	var order []string
	// unparseable counts shard values this router could not read. A sum missing
	// one of its terms is not a smaller sum, it is a different number, and the
	// caller has to be told.
	unparseable := 0
	for _, a := range bodies {
		var v struct {
			Data struct {
				Result []vectorSeries `json:"result"`
			} `json:"data"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		for _, se := range v.Data.Result {
			key := labelKey(se.Metric)
			a := byLabels[key]
			if a == nil {
				m := se.Metric
				if m == nil {
					m = map[string]string{}
				}
				// The output name is __name__, which is the aggregate's alias.
				// A series whose name is not in the query's stats pipe is one
				// this router cannot combine: refusing beats picking an
				// operator for it.
				op, known := query.MergeSum, true
				if ops != nil {
					op, known = ops[m["__name__"]]
				}
				if !known {
					s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
						"simdlogs: a shard returned a series named %q, which is not an "+
							"output of this query's stats pipe, so the router does not "+
							"know whether to sum it or take its extreme",
						m["__name__"]))
					return
				}
				a = &acc{metric: m, stamp: se.Value[0], op: op}
				byLabels[key] = a
				order = append(order, key)
			}
			f, err := strconv.ParseFloat(fmt.Sprint(se.Value[1]), 64)
			if err != nil {
				// The comment here said "skipping it silently would understate
				// the total, so it is reported as an incomplete answer
				// instead", and the code was a bare `continue` -- so a shard
				// reporting an unparseable value was dropped and the answer
				// came back 200 with no partial marker at all. Now it is what
				// the comment always said it was.
				obs.L().Warn("a shard reported a value this router cannot parse",
					obs.FieldEvent, "cluster.vector_unparseable",
					obs.FieldRoute, r.URL.Path, "value", fmt.Sprint(se.Value[1]))
				unparseable++
				continue
			}
			a.val = a.op.Combine(a.val, f, !a.seen)
			a.seen = true
		}
	}
	res := make([]vectorSeries, 0, len(order))
	for _, k := range order {
		a := byLabels[k]
		res = append(res, vectorSeries{
			Metric: a.metric,
			// A string, as the Prometheus wire format requires: a client that
			// parses it expects one.
			Value: [2]any{a.stamp, strconv.FormatFloat(a.val, 'f', -1, 64)},
		})
	}
	if unparseable > 0 {
		s.writeErr(w, r, adminSpec(), http.StatusBadGateway, fmt.Sprintf(
			"simdlogs: %d shard value(s) could not be read, so this total would be "+
				"missing terms. A sum short of a term is a different number, not a "+
				"smaller one, and a min or max short of a term is a different "+
				"extreme", unparseable))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "vector", "result": res},
	})
}

// rejectNonMergeableStats refuses a stats query whose aggregate has no
// mergeable partial state, and reports whether the caller may continue.
//
// The same rule the LogsQL planner applies, at the endpoints that take a stats
// query directly. Without it these two endpoints would sum what must not be
// summed while /select/logsql/query refused the identical aggregate -- the same
// query answered two ways by one binary.
func (s *Server) rejectNonMergeableStats(w http.ResponseWriter, r *http.Request) bool {
	raw := r.FormValue("query")
	if raw == "" {
		return true
	}
	q, err := query.ParseLogsQL(raw)
	if err != nil {
		return true // the shards will reject it with the parse error
	}
	for _, p := range q.Pipes {
		sp, isStats := p.(*query.StatsPipe)
		if !isStats {
			continue
		}
		if why := query.NonMergeableReason(sp.Aggs); why != "" {
			s.writeErr(w, r, readSpec(), http.StatusBadRequest, fmt.Sprintf(
				"simdlogs: this query cannot be answered correctly across shards. %s. "+
					"Run it against a single storage node, or rewrite it", why))
			return false
		}
	}
	return true
}

// federatedSQL answers a SQL select for the cluster, or refuses.
//
// SQL parses to the same Query as LogsQL, so the same classification applies:
// a pipeline that is entirely row-local gives the same rows applied per shard
// and merged as applied after merging, and anything else does not.
//
// Where LogsQL splits the pipeline, SQL is refused. The split needs the shard
// half re-expressed as a query string, and for LogsQL that is a count of pipe
// segments in the original text. SQL has no such correspondence -- GROUP BY,
// ORDER BY and LIMIT are clauses of one statement, not appended stages -- so
// building the shard half would mean either a SQL printer kept exactly in step
// with the parser, or text surgery on a query language. Refusing names the
// endpoint that does plan, which answers the same question.
func (s *Server) federatedSQL(w http.ResponseWriter, r *http.Request) {
	q, err := query.ParseSQL(r.FormValue("query"))
	if err != nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, err.Error())
		return
	}
	plan := query.PlanDistributed(q.Pipes)
	if len(plan.CoordinatorPipes) > 0 {
		reason := plan.Reject
		if reason == "" {
			reason = "it aggregates, orders or limits across the whole result set, " +
				"which cannot be done shard by shard"
		}
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, fmt.Sprintf(
			"simdlogs: this SQL query cannot be answered across shards: %s. "+
				"Use /select/logsql/query, which splits a pipeline between the "+
				"shards and the router, or run this against a single storage node",
			reason))
		return
	}
	// Row-local throughout: every shard answers for its own rows and the merge
	// is a concatenation in time order, which is what federatedSelect does.
	s.federatedRows(w, r, "/select/sql")
}

// refuseInRouterMode answers a surface this build cannot federate, and reports
// whether the caller should stop.
//
// The alternative is what these did: run against the router's own empty store
// and answer successfully with nothing. A live tail streamed forever and never
// yielded a row; a vector search returned no neighbours. Both look exactly like
// a cluster in which nothing matched, and a caller has no way to tell.
//
// 501 rather than 400: the request is well-formed and this node cannot serve
// it. A client that sees 400 retries with a different query, which will not
// help; 501 says the endpoint is not implemented on the node it reached, and a
// caller can act on that by asking a storage node directly.
func (s *Server) refuseInRouterMode(w http.ResponseWriter, r *http.Request, what, why string) bool {
	if len(s.backends) == 0 {
		return false
	}
	s.writeErr(w, r, readSpec(), http.StatusNotImplemented, fmt.Sprintf(
		"simdlogs: %s is not available on a select-router in this build: %s. "+
			"Send this request to a storage node instead", what, why))
	return true
}
