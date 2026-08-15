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
	bodies, w, ok := s.fanOutChecked(w, withoutLimits(r), "/select/logsql/facets", nil)
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

	maxFields := intParam(r, "limit", query.DefaultFacetLimit)
	maxValues := intParam(r, "max_values_per_field", query.DefaultFacetMaxValues)
	out := make([]query.FieldFacet, 0, len(fieldOrder))
	for _, name := range fieldOrder {
		fa := byField[name]
		vals := make([]query.FacetValue, 0, len(fa.order))
		for _, v := range fa.order {
			vals = append(vals, query.FacetValue{FieldValue: v, Hits: fa.hits[v]})
		}
		sort.Slice(vals, func(i, j int) bool {
			if vals[i].Hits != vals[j].Hits {
				return vals[i].Hits > vals[j].Hits
			}
			return vals[i].FieldValue < vals[j].FieldValue
		})
		if maxValues > 0 && len(vals) > maxValues {
			vals = vals[:maxValues]
		}
		out = append(out, query.FieldFacet{FieldName: name, Values: vals})
	}
	if maxFields > 0 && len(out) > maxFields {
		out = out[:maxFields]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"facets": out})
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
	type acc struct {
		metric map[string]string
		stamp  any
		sum    float64
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
				a = &acc{metric: m, stamp: se.Value[0]}
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
			a.sum += f
		}
	}
	res := make([]vectorSeries, 0, len(order))
	for _, k := range order {
		a := byLabels[k]
		res = append(res, vectorSeries{
			Metric: a.metric,
			// A string, as the Prometheus wire format requires: a client that
			// parses it expects one.
			Value: [2]any{a.stamp, strconv.FormatFloat(a.sum, 'f', -1, 64)},
		})
	}
	if unparseable > 0 {
		s.writeErr(w, r, adminSpec(), http.StatusBadGateway, fmt.Sprintf(
			"simdlogs: %d shard value(s) could not be read, so this total would be "+
				"missing terms. A sum short of a term is a different number, not a "+
				"smaller one", unparseable))
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
