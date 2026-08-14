package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
)

// The Elasticsearch search surface -- the feature VictoriaLogs does not
// have, so ELK clients and Grafana's ES datasource work against this and
// not against it. A log-relevant subset of the query DSL (bool/term/
// terms/range/exists) maps onto the same planner; the time-range-to-
// partition mapping is automatic because range on a time field feeds the
// group skip.
type esQuery struct {
	Query esClause `json:"query"`
	Size  int      `json:"size"`
}

type esClause struct {
	Bool   *esBool            `json:"bool,omitempty"`
	Term   map[string]any     `json:"term,omitempty"`
	Range  map[string]esRange `json:"range,omitempty"`
	Exists *esExists          `json:"exists,omitempty"`
}

type esBool struct {
	Must   []esClause `json:"must,omitempty"`
	Filter []esClause `json:"filter,omitempty"`
}

type esRange struct {
	Gte any `json:"gte,omitempty"`
	Lt  any `json:"lt,omitempty"`
	Lte any `json:"lte,omitempty"`
}

type esExists struct {
	Field string `json:"field"`
}

func (s *Server) esSearch(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedESSearch(w, r)
		return
	}
	var body esQuery
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q := esToQuery(body.Query)
	if body.Size > 0 {
		q.Limit = body.Size
	}
	q.MatAll = true // ES _source expects the whole document, not just filter fields
	esStopped := s.applyQueryBudget(r, q)
	rows := query.Run(s.tn(r).store, q)
	if s.queryStopped(w, r, esStopped) {
		return
	}
	hits := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		src := map[string]any{"@timestamp": time.Unix(0, row.Time).UTC().Format(time.RFC3339Nano)}
		for _, f := range row.Fields {
			src[f.Key] = f.Value
		}
		hits = append(hits, map[string]any{"_source": src})
	}
	// Elasticsearch clients switch on Content-Type; this answered text/plain
	// for a JSON document, and so does its federated twin unless both are
	// changed together.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"hits": map[string]any{
			"total": map[string]any{"value": len(rows), "relation": "eq"},
			"hits":  hits,
		},
	})
}

func (s *Server) esCount(w http.ResponseWriter, r *http.Request) {
	if len(s.backends) > 0 {
		s.federatedESCount(w, r)
		return
	}
	var body esQuery
	json.NewDecoder(r.Body).Decode(&body)
	q := esToQuery(body.Query)
	stopped := s.applyQueryBudget(r, q)
	n := query.Count(s.tn(r).store, q)
	if s.queryStopped(w, r, stopped) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"count": n})
}

// esToQuery maps the DSL subset onto the planner's Query. A range on a
// time-typed field becomes the time window (the partition skip); term and
// exists become predicates.
func esToQuery(c esClause) *query.Query {
	q := &query.Query{To: int64(1) << 62}
	var walk func(esClause)
	walk = func(c esClause) {
		if c.Bool != nil {
			for _, m := range c.Bool.Must {
				walk(m)
			}
			for _, m := range c.Bool.Filter {
				walk(m)
			}
		}
		for field, val := range c.Term {
			q.Preds = append(q.Preds, query.Pred{Field: field, Kind: query.Eq, Value: toStr(val)})
		}
		for field, rg := range c.Range {
			if field == "@timestamp" || field == "_time" {
				if t, ok := esTime(rg.Gte); ok {
					q.From = t
				}
				if t, ok := esTime(rg.Lt); ok {
					q.To = t
				} else if t, ok := esTime(rg.Lte); ok {
					q.To = t + 1
				}
			}
			// non-time ranges on dict columns are Phase 7 (numeric columns).
		}
	}
	walk(c)
	return q
}

func esTime(v any) (int64, bool) {
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano(), true
		}
	}
	return 0, false
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "true"
		}
		return "false"
	}
	return ""
}

// esBulk ingests the Elasticsearch _bulk NDJSON: alternating action and
// document lines. The action ({"index":{...}} / "create" / "update" /
// "delete") is dropped -- delete carries no document, the rest are followed
// by the doc line, which is ingested like jsonline (@timestamp counts as the
// timestamp). So Filebeat/Logstash/Fluentd/OTel-ES exporters point here
// unchanged.
func (s *Server) esBulk(w http.ResponseWriter, r *http.Request) {
	body, berr := s.readBody(w, r)
	if berr != nil {
		s.writeErr(w, r, ndjsonSpec(), berr.code, berr.msg)
		return
	}
	// Parse the actions rather than stripping them: the response must carry one
	// item per ACTION, in order, because Elasticsearch clients match items to
	// their requests by position. See esbulk.go for the four defects the
	// strip-and-forget approach produced.
	ops, perr := parseBulk(body)
	if perr != "" {
		// The alternation is lost, so nothing after the bad line can be
		// attributed. Elasticsearch answers 400 for the request here too.
		s.writeErr(w, r, ndjsonSpec(), http.StatusBadRequest, perr)
		return
	}
	docs := bulkDocs(ops, body)

	tn := s.tn(r)
	fallback := tn.fallbackTS()
	opts := ingestOptions(r)
	var ing, skip int
	if len(docs) >= ingest.MinParallelBytes {
		var werr error
		ing, skip, werr = ingest.IngestJSONLinesParallelCfg(tn.store, docs, fallback, s.parallelCfg(), &opts)
		if werr != nil {
			s.failIngest(w, werr, ing, skip, len(body))
			return
		}
	} else {
		// Mark before adding: the tenant's buffer is shared, so only
		// FlushMark can report on the rows this request contributed.
		mark := tn.w.Mark()
		res, perr := ingest.IngestJSONLinesOpts(tn.w, docs, fallback, &opts)
		ing, skip = res.Accepted, res.Rejected
		if perr != nil {
			s.countRows(ing, skip, len(body))
			s.writeErr(w, r, ndjsonSpec(), ingest.StatusFor(perr), perr.Error())
			return
		}
		if err := tn.w.FlushMark(mark); err != nil {
			// A closed writer dropped the rows silently, so counting them
			// would inflate the ingested total by rows that were never
			// stored. Every other flush failure did buffer them.
			if !errors.Is(err, ingest.ErrWriterClosed) {
				s.countRows(ing, skip, len(body))
			}
			s.writeErr(w, r, ndjsonSpec(), http.StatusServiceUnavailable, err.Error())
			return
		}
	}

	s.countRows(ing, skip, len(body))

	// The ingester reports only a COUNT of rejects, which cannot be mapped back
	// to a position. Every document handed to it was already checked to be a
	// JSON object by parseBulk, so a reject here is a failure of a different
	// kind: report it on the items that could have produced it rather than
	// claiming 201 for all of them.
	if skip > 0 {
		marked := 0
		for i := range ops {
			if ops[i].doc != nil && ops[i].errType == "" && marked < skip {
				ops[i].status = 500
				ops[i].errType = "server_error"
				ops[i].errMsg = "the document was not stored; its position within the batch is not attributable"
				marked++
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	out := make([]byte, 0, 48+64*len(ops))
	out = append(out, `{"took":0,"errors":`...)
	out = strconv.AppendBool(out, bulkHasError(ops))
	out = appendBulkItems(out, ops)
	out = append(out, "}\n"...)
	w.Write(out)
}
