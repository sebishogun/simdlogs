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
	if len(s.backends) > 0 {
		s.federatedESCount(w, r)
		return
	}
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

// stripBulkActions compacts an Elasticsearch _bulk body down to just its
// documents, in place. Actions alternate with documents (except "delete",
// which carries none), so the write cursor always trails the read cursor and
// the result aliases the input buffer.
func stripBulkActions(body []byte) []byte {
	w, r := 0, 0
	wantDoc := false
	for r < len(body) {
		nl := bytes.IndexByte(body[r:], '\n')
		var line []byte
		if nl < 0 {
			line, nl = body[r:], len(body)-r
		} else {
			line = body[r : r+nl]
		}
		start := r
		r += nl + 1
		if t := bytes.TrimSpace(line); len(t) == 0 {
			continue
		}
		if !wantDoc {
			// An action line. Only "delete" is self-contained; every other
			// action is followed by the document it applies to.
			wantDoc = !bytes.Contains(line, []byte(`"delete"`))
			continue
		}
		wantDoc = false
		w += copy(body[w:], body[start:start+nl])
		body[w] = '\n'
		w++
	}
	return body[:w]
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
	// Strip the action lines in place. A document is never longer than the input
	// already consumed at that point, so the compacted NDJSON can overwrite the
	// head of the same buffer -- no second copy of a multi-megabyte bulk body.
	docs := stripBulkActions(body)

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
		ing, skip = ingest.IngestJSONLinesOpts(tn.w, docs, fallback, &opts)
		if err := tn.w.Flush(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	s.countRows(ing, skip, len(body))

	// One item per document, the shape Elasticsearch clients parse to decide
	// whether to retry. Written directly: a bulk of 200k documents would
	// otherwise build 400k maps for the reflective encoder to walk.
	const itemOK = `{"create":{"status":201}}`
	w.Header().Set("Content-Type", "application/json")
	out := make([]byte, 0, 32+(len(itemOK)+1)*ing)
	out = append(out, `{"took":0,"errors":`...)
	out = strconv.AppendBool(out, skip > 0)
	out = append(out, `,"items":[`...)
	for i := 0; i < ing; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, itemOK...)
	}
	out = append(out, "]}\n"...)
	w.Write(out)
}
