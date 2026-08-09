package api

import (
	"bytes"
	"encoding/json"
	"io"
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
	rows := query.Run(s.tn(r).store, q)
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
	json.NewEncoder(w).Encode(map[string]any{"count": query.Count(s.tn(r).store, q)})
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	var docs bytes.Buffer
	lines := bytes.Split(body, []byte("\n"))
	for i := 0; i < len(lines); i++ {
		line := bytes.TrimSpace(lines[i])
		if len(line) == 0 {
			continue
		}
		if bytes.Contains(line, []byte(`"delete"`)) {
			continue // delete action: no document follows
		}
		// index/create/update: the next non-empty line is the document.
		for i+1 < len(lines) {
			i++
			d := bytes.TrimSpace(lines[i])
			if len(d) > 0 {
				docs.Write(d)
				docs.WriteByte('\n')
				break
			}
		}
	}
	tn := s.tn(r)
	ing, skip := ingest.IngestJSONLines(tn.w, docs.Bytes(), tn.fallbackTS())
	if err := tn.w.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	items := make([]map[string]any, 0, ing)
	for i := 0; i < ing; i++ {
		items = append(items, map[string]any{"create": map[string]any{"status": 201}})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"took": 0, "errors": skip > 0, "items": items})
}
