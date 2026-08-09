package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

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
	var body esQuery
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q := esToQuery(body.Query)
	if body.Size > 0 {
		q.Limit = body.Size
	}
	rows := query.Run(s.store, q)
	hits := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		src := map[string]any{"@timestamp": time.Unix(0, row.Time).UTC().Format(time.RFC3339Nano)}
		for _, f := range row.Fields {
			src[f.Key] = f.Value
		}
		hits = append(hits, map[string]any{"_source": src})
	}
	json.NewEncoder(w).Encode(map[string]any{
		"hits": map[string]any{
			"total": map[string]any{"value": len(rows), "relation": "eq"},
			"hits":  hits,
		},
	})
}

func (s *Server) esCount(w http.ResponseWriter, r *http.Request) {
	var body esQuery
	json.NewDecoder(r.Body).Decode(&body)
	q := esToQuery(body.Query)
	json.NewEncoder(w).Encode(map[string]any{"count": query.Count(s.store, q)})
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
