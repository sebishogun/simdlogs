package api

import (
	"encoding/json"
	"errors"
	"fmt"
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
	From  int      `json:"from"`
}

// esClause is the supported subset. Every field a client can send is named
// here, and the decoder is strict, so anything NOT named is a 400 rather than
// a clause that is silently ignored.
//
// Silent ignoring is the failure this replaces, and it is the worst kind an
// ES surface can have: a dropped filter returns MORE documents than the client
// asked for, in a response that is structurally valid. `terms`, `must_not`,
// `should`, `exists`, `match`, `wildcard` and every non-time `range` were all
// parsed-and-dropped -- `exists` was even in the struct, decoded, and never
// read. A client filtering `status:error` and getting every log line back has
// no way to tell that from a store where everything is an error.
type esClause struct {
	Bool   *esBool             `json:"bool,omitempty"`
	Term   map[string]any      `json:"term,omitempty"`
	Terms  map[string][]any    `json:"terms,omitempty"`
	Range  map[string]esRange  `json:"range,omitempty"`
	Exists *esExists           `json:"exists,omitempty"`
	Match  map[string]any      `json:"match,omitempty"`
	Prefix map[string]esPrefix `json:"prefix,omitempty"`
	// MatchAll is `{"match_all": {}}`, which every ES client sends for "no
	// filter". Accepted and mapped to nothing.
	MatchAll *struct{} `json:"match_all,omitempty"`
}

type esBool struct {
	Must    []esClause `json:"must,omitempty"`
	Filter  []esClause `json:"filter,omitempty"`
	MustNot []esClause `json:"must_not,omitempty"`
	Should  []esClause `json:"should,omitempty"`
	// MinimumShouldMatch is rejected unless it is 1, which is what `should`
	// means on its own. Any other value changes the semantics of the whole
	// clause, and answering it as if it were 1 is a wrong answer.
	MinimumShouldMatch *int `json:"minimum_should_match,omitempty"`
}

type esRange struct {
	Gte any `json:"gte,omitempty"`
	Gt  any `json:"gt,omitempty"`
	Lt  any `json:"lt,omitempty"`
	Lte any `json:"lte,omitempty"`
}

type esExists struct {
	Field string `json:"field"`
}

type esPrefix struct {
	Value any `json:"value,omitempty"`
}

// errESUnsupported is a DSL clause this server does not implement. It is a 400
// with the clause named, never a filter dropped on the floor.
var errESUnsupported = errors.New("simdlogs: unsupported Elasticsearch query clause")

func (s *Server) esSearch(w http.ResponseWriter, r *http.Request) {
	if len(s.backendList()) > 0 {
		s.federatedESSearch(w, r)
		return
	}
	body, err := decodeESQuery(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q, err := esToQuery(body.Query)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	q.MatAll = true // ES _source expects the whole document, not just filter fields

	// The scan is NOT limited to `size`. hits.total.value is documented as the
	// number of matching documents, and it used to be len(rows) AFTER size had
	// been pushed into the scan as a Limit -- so a search with size=10 over a
	// million matches answered `"total": 10`, and every ES client renders that
	// as "10 results". The page is taken after the count, from the same scan.
	esStopped := s.applyQueryBudget(r, q)
	rows := query.Run(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, esStopped, q) {
		return
	}
	total := len(rows)

	// from/size, in that order, over the whole result. Applied here rather
	// than in the scan for the same reason: the scan's job is the total.
	if body.From > 0 {
		if body.From >= len(rows) {
			rows = nil
		} else {
			rows = rows[body.From:]
		}
	}
	size := body.Size
	if size <= 0 {
		size = esDefaultSize
	}
	if len(rows) > size {
		rows = rows[:size]
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
			"total": map[string]any{"value": total, "relation": "eq"},
			"hits":  hits,
		},
	})
}

// esDefaultSize is Elasticsearch's own default page size. A search with no
// `size` used to return every matching document, which is not what any ES
// client expects and not what the reference does.
const esDefaultSize = 10

func (s *Server) esCount(w http.ResponseWriter, r *http.Request) {
	if len(s.backendList()) > 0 {
		s.federatedESCount(w, r)
		return
	}
	body, err := decodeESQuery(r)
	if err != nil {
		// The decode error used to be discarded entirely, so a malformed body
		// counted the whole store.
		http.Error(w, err.Error(), 400)
		return
	}
	q, err := esToQuery(body.Query)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	stopped := s.applyQueryBudget(r, q)
	n := query.Count(s.tn(r).store, q)
	if s.queryStoppedErr(w, r, stopped, q) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"count": n})
}

// decodeESQuery reads the body STRICTLY. An unknown field is a 400.
//
// json.Decode ignores unknown fields by default, which on a query DSL means
// every clause the server does not implement becomes "match all". Strictness
// is the only way a client learns its filter was not applied.
func decodeESQuery(r *http.Request) (esQuery, error) {
	var body esQuery
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return esQuery{}, fmt.Errorf("simdlogs: %w", err)
	}
	if body.Size < 0 || body.From < 0 {
		return esQuery{}, fmt.Errorf("simdlogs: size and from must not be negative")
	}
	return body, nil
}

// esToQuery maps the supported DSL subset onto the planner's Query, and
// REFUSES the rest.
//
// The output is an Expr tree rather than the flat implicit-AND Preds list,
// because must_not and should are not expressible as an AND of predicates --
// and a mapping that could not express them was the reason they were dropped.
// A range on a time field still becomes the window (the partition skip), which
// is what makes an ES time filter as cheap here as a LogsQL one.
func esToQuery(c esClause) (*query.Query, error) {
	q := &query.Query{To: int64(1) << 62}
	e, err := esClauseToExpr(c, q)
	if err != nil {
		return nil, err
	}
	q.Filter = e
	return q, nil
}

// esClauseToExpr converts one clause. Time ranges are lifted onto q rather
// than becoming predicates, so they drive the group skip.
func esClauseToExpr(c esClause, q *query.Query) (*query.Expr, error) {
	var kids []*query.Expr
	add := func(e *query.Expr) {
		if e != nil {
			kids = append(kids, e)
		}
	}

	if c.Bool != nil {
		if c.Bool.MinimumShouldMatch != nil && *c.Bool.MinimumShouldMatch != 1 {
			return nil, fmt.Errorf("%w: minimum_should_match=%d (only 1 is supported)",
				errESUnsupported, *c.Bool.MinimumShouldMatch)
		}
		for _, sub := range append(append([]esClause{}, c.Bool.Must...), c.Bool.Filter...) {
			e, err := esClauseToExpr(sub, q)
			if err != nil {
				return nil, err
			}
			add(e)
		}
		for _, sub := range c.Bool.MustNot {
			e, err := esClauseToExpr(sub, q)
			if err != nil {
				return nil, err
			}
			if e != nil {
				add(&query.Expr{Op: query.OpNot, Child: e})
			}
		}
		if len(c.Bool.Should) > 0 {
			var or []*query.Expr
			for _, sub := range c.Bool.Should {
				e, err := esClauseToExpr(sub, q)
				if err != nil {
					return nil, err
				}
				if e != nil {
					or = append(or, e)
				}
			}
			if len(or) == 1 {
				add(or[0])
			} else if len(or) > 1 {
				add(&query.Expr{Op: query.OpOr, Kids: or})
			}
		}
	}

	for field, val := range c.Term {
		add(leaf(query.Pred{Field: field, Kind: query.Eq, Value: toStr(val)}))
	}
	for field, vals := range c.Terms {
		set := make([]string, 0, len(vals))
		for _, v := range vals {
			set = append(set, toStr(v))
		}
		// An empty `terms` matches nothing in Elasticsearch, and mapping it to
		// an empty In set would have matched everything.
		add(leaf(query.Pred{Field: field, Kind: query.In, Values: set}))
	}
	for field, v := range c.Match {
		// `match` on a keyword field is term equality; this store has no
		// analyzed text fields, so that is the whole meaning it can have here.
		// Mapped rather than refused because every ES client sends it, and
		// substring semantics would be a different query than the client asked
		// for.
		add(leaf(query.Pred{Field: field, Kind: query.Eq, Value: toStr(v)}))
	}
	for field, p := range c.Prefix {
		add(leaf(query.Pred{Field: field, Kind: query.Prefix, Value: toStr(p.Value)}))
	}
	if c.Exists != nil {
		if c.Exists.Field == "" {
			return nil, fmt.Errorf("%w: exists with no field", errESUnsupported)
		}
		// NOT (field == ""). A column this store does not hold reads as the
		// empty value for every row, so "has a non-empty value" is exactly
		// what exists means here. It was decoded and never read before, so
		// `exists` matched every document.
		add(&query.Expr{Op: query.OpNot,
			Child: leaf(query.Pred{Field: c.Exists.Field, Kind: query.Eq, Value: ""})})
	}

	for field, rg := range c.Range {
		if field == "@timestamp" || field == "_time" {
			if t, ok := esTime(rg.Gte); ok {
				q.From = t
			} else if t, ok := esTime(rg.Gt); ok {
				q.From = t + 1
			}
			if t, ok := esTime(rg.Lt); ok {
				q.To = t
			} else if t, ok := esTime(rg.Lte); ok {
				q.To = t + 1
			}
			continue
		}
		// A non-time range is REFUSED, not dropped. Dropping it returned every
		// document to a client that asked for a bounded set -- and the comment
		// that said "Phase 7" documented the gap for a reader of this file and
		// for nobody sending the query.
		return nil, fmt.Errorf(
			"%w: range on %q (only @timestamp/_time ranges are supported)",
			errESUnsupported, field)
	}

	switch len(kids) {
	case 0:
		return nil, nil // match_all, or an empty query: no filter
	case 1:
		return kids[0], nil
	default:
		return &query.Expr{Op: query.OpAnd, Kids: kids}, nil
	}
}

func leaf(p query.Pred) *query.Expr { return &query.Expr{Op: query.OpLeaf, Pred: p} }

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
	var rejectedAt []int32
	truncated := false
	if len(docs) >= ingest.MinParallelBytes {
		var werr error
		ing, skip, werr = ingest.IngestJSONLinesParallelCfg(tn.store, docs, fallback, s.parallelCfg(), &opts)
		if werr != nil {
			s.failIngest(w, werr, ing, skip, len(body))
			return
		}
		// The parallel path shards the body, so a shard's ordinals are
		// relative to its own shard. Reporting them as batch positions would
		// be the same guess with extra steps, so the positions are declared
		// UNKNOWN and markBulkRejects reports every candidate rather than
		// picking wrong ones.
		truncated = skip > 0
	} else {
		// Mark before adding: the tenant's buffer is shared, so only
		// FlushMark can report on the rows this request contributed.
		mark := tn.w.Mark()
		res, perr := ingest.IngestJSONLinesOpts(tn.w, docs, fallback, &opts)
		ing, skip = res.Accepted, res.Rejected
		rejectedAt, truncated = res.RejectedAt, res.RejectedTruncated
		if perr != nil {
			s.countRows(ing, skip, len(body))
			s.writeErr(w, r, ndjsonSpec(), ingest.StatusFor(perr), perr.Error())
			return
		}
		if err := tn.w.FlushMark(mark); err != nil {
			// Rows added after Close are dropped silently; rows added
			// before it were flushed by Close and are durable. This
			// under-counts the second case, which is the safe side --
			// see insertJSONLine for the same trade.
			if !errors.Is(err, ingest.ErrWriterClosed) {
				s.countRows(ing, skip, len(body))
			}
			s.writeFlushErr(w, r, ndjsonSpec(), err)
			return
		}
	}

	s.countRows(ing, skip, len(body))

	markBulkRejects(ops, skip, rejectedAt, truncated)

	w.Header().Set("Content-Type", "application/json")
	// Capped for the same reason the op presize is: len(ops) reaches the
	// action cap, and 64 bytes per item is 64 MB reserved up front. append
	// grows it for a bulk that genuinely needs it.
	outCap := 48 + 64*len(ops)
	if outCap > 1<<22 {
		outCap = 1 << 22
	}
	out := make([]byte, 0, outCap)
	out = append(out, `{"took":0,"errors":`...)
	out = strconv.AppendBool(out, bulkHasError(ops))
	out = appendBulkItems(out, ops)
	out = append(out, "}\n"...)
	w.Write(out)
}
