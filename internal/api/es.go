package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
)

// The Elasticsearch search surface lets ELK clients and Grafana's ES
// datasource query this store. The supported log-relevant DSL subset is bool
// (must/filter/must_not/should), term, terms, range, exists, match, prefix and
// match_all. The filtering clauses map onto the same planner; match_all is the
// explicit no-filter case. A positive range on a time field also narrows the
// scan window and feeds the group skip.
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
	// MinimumShouldMatch defaults to 0 beside a Must or Filter and to 1
	// otherwise, which is Elasticsearch's rule; see esMinShouldMatch. A value
	// that needs a counting operator is refused with the value named, because
	// answering it as if it were 1 is a wrong answer with nothing to say so.
	MinimumShouldMatch *int `json:"minimum_should_match,omitempty"`
}

type esRange struct {
	Gte any `json:"gte,omitempty"`
	Gt  any `json:"gt,omitempty"`
	Lt  any `json:"lt,omitempty"`
	Lte any `json:"lte,omitempty"`
	// Format and TimeZone are accepted because real clients send them on every
	// time range -- Grafana's ES datasource stamps `"format":"epoch_millis"`
	// unconditionally -- and the strict decoder otherwise refused the whole
	// request: 400 `unknown field "format"` for every Grafana dashboard query.
	// Format names the spellings the bounds use (`||`-separated, ES style);
	// TimeZone applies to spellings that carry no zone of their own. A format
	// this build cannot honour is a 400 naming it, not a guess: under
	// `basic_date` the value "20260101" means 2026-01-01, and reading it as a
	// bare epoch number would place it somewhere else entirely.
	Format   string `json:"format,omitempty"`
	TimeZone string `json:"time_zone,omitempty"`
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
// A range on a time field in positive conjunctive position still becomes the
// window (the partition skip), which is what makes an ES time filter as cheap
// here as a LogsQL one; under must_not/should it becomes a time predicate,
// because the window cannot express a negation or a union.
func esToQuery(c esClause) (*query.Query, error) {
	// ToSet even though the end is the no-end sentinel: this function RESOLVED
	// it, and a `_time:` filter may narrow that (to < q.To) but must not widen
	// past it. Left unset, the filter's end replaced the sentinel outright.
	// From starts at MinInt64, NOT at the epoch.
	//
	// The lift below is `if from > q.From`, so a From of 0 could only ever be
	// raised: an ES `range` naming a far-past `gte` -- which saturates to
	// MinInt64, the same as every other surface -- left the window starting at
	// 1970 and the documents this store holds between 1677-09-21 and the epoch
	// were unreachable through /_search by any query at all, `match_all`
	// included. Same blind spot as the LogsQL `_time:` clamp, reached a third
	// way; the zero value of the struct field was the whole cause.
	q := &query.Query{From: defaultWindowFrom, To: defaultWindowTo, ToSet: true}
	// One `now` for the whole body, so two `now-5m` bounds in one query cannot
	// resolve against two clock reads.
	e, err := esClauseToExpr(c, q, true, time.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	q.Filter = e
	return q, nil
}

// esClauseToExpr converts one clause. In a POSITIVE conjunctive context
// (lift=true) time ranges are lifted onto q's window rather than becoming
// predicates, so they drive the group skip.
//
// Under `must_not` or `should` they must NOT be lifted: the window is an AND
// over the whole query, so lifting a negated range applied it un-negated --
// `must_not: [range @timestamp < 2000]` answered the rows BEFORE 2000 instead
// of the rows after -- and lifting two `should` alternatives intersected a
// union, so complementary ranges answered nothing instead of everything. Both
// at HTTP 200. In those contexts the range becomes an ordinary TimeRange
// predicate leaf, which the boolean tree then negates or unions correctly; it
// no longer prunes whole groups, which is a cost, not a wrong answer.
func esClauseToExpr(c esClause, q *query.Query, lift bool, now int64) (*query.Expr, error) {
	var kids []*query.Expr
	add := func(e *query.Expr) {
		if e != nil {
			kids = append(kids, e)
		}
	}

	if c.Bool != nil {
		msm, err := esMinShouldMatch(c.Bool)
		if err != nil {
			return nil, err
		}
		for _, sub := range append(append([]esClause{}, c.Bool.Must...), c.Bool.Filter...) {
			e, err := esClauseToExpr(sub, q, lift, now)
			if err != nil {
				return nil, err
			}
			add(e)
		}
		for _, sub := range c.Bool.MustNot {
			// lift=false: a time range under a NOT is not a bound on the scan
			// window -- see the function comment.
			e, err := esClauseToExpr(sub, q, false, now)
			if err != nil {
				return nil, err
			}
			// A NIL CHILD IS NOT AN ABSENT CHILD. nil means "this clause is no
			// filter", i.e. it matches every document -- `{"match_all":{}}`,
			// `{}`, `{"bool":{}}` -- and the negation of that matches NONE.
			// Dropping the arm made it mean the opposite: `must_not:
			// [{"match_all":{}}]` is Elasticsearch's canonical spelling of
			// "match nothing" and Kibana emits it whenever a negated filter
			// pill empties out, and this answered every document in the index
			// at 200.
			add(&query.Expr{Op: query.OpNot, Child: esOrMatchAll(e)})
		}
		if len(c.Bool.Should) > 0 {
			var or []*query.Expr
			for _, sub := range c.Bool.Should {
				// lift=false: `should` is a union, and the window is an AND.
				//
				// PARSED EVEN WHEN IT WILL BE DISCARDED below. An unsupported
				// clause under an optional `should` is still a clause this
				// server cannot honour, and accepting it silently because the
				// arm happens not to constrain the answer is the same silent
				// drop the strict decoder exists to prevent.
				e, err := esClauseToExpr(sub, q, false, now)
				if err != nil {
					return nil, err
				}
				// The mirror of the must_not case: an arm that filters nothing
				// matches EVERY document, so the union with it is every
				// document. Dropping it left the union to the other arms and
				// narrowed a query that asked to be widened.
				or = append(or, esOrMatchAll(e))
			}
			// msm == 0: the should clauses are OPTIONAL and do not constrain
			// matching at all (in Elasticsearch they only contribute to the
			// score, which this surface does not compute).
			if msm == 1 {
				if len(or) == 1 {
					add(or[0])
				} else {
					add(&query.Expr{Op: query.OpOr, Kids: or})
				}
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
			// EVERY bound is parsed or the query is refused. This used to be
			// `if t, ok := esTime(rg.Gte); ok { ... }`, and esTime read
			// RFC3339 and nothing else -- so epoch millis (what Grafana and
			// Kibana send), epoch seconds, `now-5m`, a zoneless datetime and a
			// bare date all fell through with the bound never applied, at 200,
			// with the response structurally valid. That is the exact failure
			// this file's header says it replaces; docs/wrong.md entry 124
			// holds the measurements.
			from, to, err := esTimeRange(rg, now)
			if err != nil {
				return nil, err
			}
			if lift {
				// NARROWING, never assignment. Assignment made a second range
				// clause on the same field widen the first one away, and made
				// `gte` shadow a stricter `gt` in the same clause; ES applies
				// every bound, so the effective window is the intersection.
				if from > q.From {
					q.From = from
				}
				if to < q.To {
					q.To = to
				}
				continue
			}
			add(leaf(query.Pred{Field: "_time", Kind: query.TimeRange, T1: from, T2: to}))
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

// esOrMatchAll turns the "no filter" nil into the node that says so.
//
// An empty conjunction matches every row (evalExpr sets all bits and intersects
// nothing; exprMatchesRow returns true over no children), which is what nil
// means everywhere else in this file -- at the TOP level a nil filter is simply
// no filter. Under `must_not` and `should` the difference is the whole answer,
// so the meaning has to be written down rather than left to the absence of a
// node.
func esOrMatchAll(e *query.Expr) *query.Expr {
	if e != nil {
		return e
	}
	return &query.Expr{Op: query.OpAnd}
}

// esMinShouldMatch resolves how many `should` arms must match.
//
// ELASTICSEARCH'S DEFAULT IS NOT 1. It is 0 when the same bool carries a `must`
// or a `filter`, and 1 only when it does not -- so `should` beside a `must` is
// OPTIONAL. This ANDed it in unconditionally, which is Kibana's own shape (the
// filter pills go in `filter`, the search bar goes in `should`) and turned a
// working dashboard empty the moment anything was typed into the bar:
//
//	must:   [term level=error], should: [range gte 2030]   0 documents, ES gives 1
//	filter: [term level=info],  should: [term level=error] 0 documents, ES gives 2
//
// And an explicit `"minimum_should_match": 0` was refused with a 400 -- the
// exact value ES defaults to, rejected in the exact shape whose default was
// being got wrong.
//
// AND AN EXPLICIT 0 DOES NOT MAKE A PURE DISJUNCTION MATCH EVERYTHING. That is
// the half this function got wrong when it learned to accept 0 at all. Lucene's
// rule is about REQUIRED clauses, not about the number: a bool with no `must`
// and no `filter` still needs at least one `should` arm to match, whatever
// minimum_should_match says, because a document matching none of the optional
// clauses does not match the query at all. `must_not` is not a required clause
// and does not satisfy it. Measured on four documents (one error, one warn, two
// info), every one at HTTP 200:
//
//	query                                                 got   ES
//	should: [error],            msm 0                       4    1
//	should: [error, warn],      msm 0                       4    2
//	must_not: [warn], should: [error], msm 0                3    1
//	should: [error], msm absent                (control)    1    1
//	must: [info], should: [error], msm 0       (control)    2    2
//
// which is es.go's own header failure -- "a dropped filter returns MORE
// documents than the client asked for, in a response that is structurally
// valid" -- reached through the value the previous round added support for.
// Before that round the shape was a loud 400.
//
// A value needing a counting operator ("at least 2 of these 5") is still a 400
// naming it: an AND/OR/NOT tree cannot express it without enumerating the
// combinations, and answering it as though it were 1 would be a wrong answer
// with nothing to say so. That refusal is now narrow -- the two values real
// clients send are answered. Negative values ("all but one") reach the same
// refusal rather than being folded into 1, because `-1` over three arms means
// two of them and answering one is the silent wrong answer.
//
// ONLY THE INTEGER SPELLING IS ACCEPTED. Elasticsearch also takes a STRING --
// `"1"`, `"0"`, a percentage `"75%"`, and a combination `"2<-25%"` -- and the
// JSON number `1.0`; every one of those is a 400 from the strict decoder here,
// naming the field. That is loud rather than wrong, and it is a real gap for a
// client that sends the string form.
func esMinShouldMatch(b *esBool) (int, error) {
	// A `must` or a `filter` is a REQUIRED clause. `must_not` is not: it
	// removes documents, it does not supply the match a pure disjunction needs.
	required := len(b.Must) > 0 || len(b.Filter) > 0
	msm := 1
	if required {
		msm = 0
	}
	if b.MinimumShouldMatch != nil {
		msm = *b.MinimumShouldMatch
		if msm == 0 && !required && len(b.Should) > 0 {
			msm = 1
		}
	}
	if len(b.Should) > 0 && msm != 0 && msm != 1 {
		return 0, fmt.Errorf("%w: minimum_should_match=%d (0 and 1 are supported; "+
			"a count needs an operator this planner does not have)",
			errESUnsupported, msm)
	}
	return msm, nil
}

// esTimeRange resolves one range clause's bounds into a half-open [from, to)
// nanosecond window. An absent side is math.MinInt64 / math.MaxInt64; a bound
// that cannot be read is an ERROR, never a bound dropped -- entry 124 measured
// six spellings silently ignored at 200, every one answering MORE documents
// than the client asked for.
func esTimeRange(rg esRange, now int64) (from, to int64, err error) {
	loc, err := esRangeZone(rg.TimeZone)
	if err != nil {
		return 0, 0, err
	}
	formats, err := esRangeFormats(rg.Format)
	if err != nil {
		return 0, 0, err
	}
	from, to = math.MinInt64, math.MaxInt64
	seen := false
	bound := func(v any, name string) (int64, bool, error) {
		if v == nil {
			return 0, false, nil
		}
		t, err := esTimeValue(v, formats, loc, now)
		if err != nil {
			return 0, false, fmt.Errorf("simdlogs: range bound %q: %w", name, err)
		}
		seen = true
		return t, true, nil
	}
	// BOTH lower bounds when both are given, and both upper: ES applies every
	// bound it is handed, so the effective window is the intersection. The old
	// code read `gte` and only fell through to `gt` when `gte` was absent or
	// unreadable, which dropped a stricter `gt` on the floor.
	if t, ok, e := bound(rg.Gte, "gte"); e != nil {
		return 0, 0, e
	} else if ok && t > from {
		from = t
	}
	if t, ok, e := bound(rg.Gt, "gt"); e != nil {
		return 0, 0, e
	} else if ok {
		if t != math.MaxInt64 { // a saturated bound is already +infinity
			t++
		}
		if t > from {
			from = t
		}
	}
	if t, ok, e := bound(rg.Lt, "lt"); e != nil {
		return 0, 0, e
	} else if ok && t < to {
		to = t
	}
	if t, ok, e := bound(rg.Lte, "lte"); e != nil {
		return 0, 0, e
	} else if ok {
		if t != math.MaxInt64 {
			t++
		}
		if t < to {
			to = t
		}
	}
	if !seen {
		// `{"range":{"@timestamp":{}}}` bounds nothing. Treating it as
		// match-all would make a typo'd bound name (caught by the strict
		// decoder) and a forgotten one (not decodable at all) look identical
		// to a filter that worked.
		return 0, 0, fmt.Errorf("%w: a range on a time field names no bound "+
			"(gte/gt/lt/lte)", errESUnsupported)
	}
	return from, to, nil
}

// esRangeZone reads a range clause's time_zone: a fixed offset ("+05:30") or
// an IANA name ("Europe/London"). It applies only to spellings that carry no
// zone of their own, which is what it means in Elasticsearch.
func esRangeZone(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	if len(tz) == 6 && (tz[0] == '+' || tz[0] == '-') && tz[3] == ':' {
		hh, err1 := strconv.Atoi(tz[1:3])
		mm, err2 := strconv.Atoi(tz[4:6])
		if err1 == nil && err2 == nil && hh <= 23 && mm <= 59 {
			off := (hh*60 + mm) * 60
			if tz[0] == '-' {
				off = -off
			}
			return time.FixedZone(tz, off), nil
		}
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: time_zone %q", errESUnsupported, tz)
	}
	return loc, nil
}

// esRangeFormats validates a range clause's format list (`||`-separated).
// Empty means "no format named": layouts plus magnitude-inferred epoch
// numbers, the same rule the HTTP time params use. A name this build cannot
// honour is refused OUTRIGHT, before any bound is read: guessing would parse
// the value under a different calendar than the client named.
func esRangeFormats(format string) ([]string, error) {
	if format == "" {
		return nil, nil
	}
	parts := strings.Split(format, "||")
	for _, p := range parts {
		switch p {
		case "epoch_millis", "epoch_second",
			"strict_date_optional_time", "date_optional_time",
			"dateOptionalTime", "strict_date_optional_time_nanos",
			"strict_date_time", "date_time",
			"strict_date", "date":
		default:
			return nil, fmt.Errorf("%w: range format %q", errESUnsupported, p)
		}
	}
	return parts, nil
}

// esLayoutsZoned are the spellings that carry their own zone; esLayoutsLocal
// are the ones time_zone (default UTC) completes. Go's Parse accepts a
// fractional second wherever the seconds field appears, so ".123" needs no
// layout of its own.
var esLayoutsZoned = []string{time.RFC3339Nano, time.RFC3339}
var esLayoutsLocal = []string{
	"2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02", "2006-01", "2006",
}

// esTimeValue reads one bound. With no format named it accepts the date
// layouts, bare epoch numbers with the unit inferred from magnitude
// (unixToNanos -- the rule every HTTP time param here follows), and `now`
// date math. With formats named it accepts exactly those.
func esTimeValue(v any, formats []string, loc *time.Location, now int64) (int64, error) {
	switch x := v.(type) {
	case float64:
		return esTimeNumber(x, formats)
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, fmt.Errorf("empty time value")
		}
		// Date math first: `now` is date math in ES whatever the format says,
		// and an anchor form is refused by esDateMath with its own message.
		if strings.HasPrefix(s, "now") {
			return esDateMath(s, now)
		}
		if strings.Contains(s, "||") {
			return 0, fmt.Errorf("%w: date-math anchor %q (only `now`-anchored "+
				"expressions are supported)", errESUnsupported, s)
		}
		if len(formats) == 0 {
			// THE DATE LAYOUTS COME FIRST, which is Elasticsearch's own order:
			// a date field's default format is
			// `strict_date_optional_time||epoch_millis`, so a STRING is tried
			// as a date and only then as an epoch number.
			//
			// ParseInt ran first here, so `{"gte":"2030"}` was read as 2030
			// SECONDS since the epoch -- 1970-01-01T00:33:50Z -- and matched
			// every document, where ES parses the bare year and answers none.
			// The `"2006"` layout three lines below could not be reached by any
			// value it accepts, since a bare year is all digits.
			//
			// The epoch spellings are untouched: `time.Parse` with a 4-digit
			// year layout rejects the extra digits of a 10- or 13-digit epoch
			// value, so those still fall through to the magnitude rule.
			for _, l := range esLayoutsZoned {
				if t, err := time.Parse(l, s); err == nil {
					return satNanos(t), nil
				}
			}
			for _, l := range esLayoutsLocal {
				if t, err := time.ParseInLocation(l, s, loc); err == nil {
					return satNanos(t), nil
				}
			}
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				return unixToNanos(n), nil
			}
			return 0, fmt.Errorf("cannot read %q as a time (RFC3339, a zoneless "+
				"date/datetime, a bare epoch number, or `now` date math)", s)
		}
		for _, f := range formats {
			switch f {
			case "epoch_millis":
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					return satScale(n, int64(time.Millisecond)), nil
				}
			case "epoch_second":
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					return satScale(n, int64(time.Second)), nil
				}
			case "strict_date", "date":
				if t, err := time.ParseInLocation("2006-01-02", s, loc); err == nil {
					return satNanos(t), nil
				}
			default: // the date_optional_time / date_time family
				for _, l := range esLayoutsZoned {
					if t, err := time.Parse(l, s); err == nil {
						return satNanos(t), nil
					}
				}
				for _, l := range esLayoutsLocal {
					if t, err := time.ParseInLocation(l, s, loc); err == nil {
						return satNanos(t), nil
					}
				}
			}
		}
		return 0, fmt.Errorf("cannot read %q under format %q", s, strings.Join(formats, "||"))
	}
	return 0, fmt.Errorf("cannot read %v (%T) as a time", v, v)
}

// esTimeNumber reads a JSON number bound. Numbers big enough to be epoch
// nanoseconds do not survive a float64 (53 bits of mantissa against 1.7e18),
// but nobody sends nanoseconds as a JSON number for exactly that reason;
// millis -- what ES itself means by a numeric date -- are exact until the year
// 287396.
func esTimeNumber(x float64, formats []string) (int64, error) {
	// A float64 beyond int64's range converts implementation-defined; treat it
	// as the infinity it is. 9.1e18 is just under MaxInt64.
	//
	// THIS GUARD IS ON THE RAW VALUE, and the fractional branch below scales by
	// 1e9 -- so it never covered that branch, whose product overflows nine
	// orders of magnitude sooner. satFloatNanos guards the product.
	if x > 9.1e18 {
		return math.MaxInt64, nil
	}
	if x < -9.1e18 {
		return math.MinInt64, nil
	}
	if x != math.Trunc(x) {
		// A fractional number is seconds-with-a-fraction, as parseTimeParam
		// reads it. No ES format produces one, so refuse it under a named
		// format rather than guess the unit.
		if len(formats) == 0 {
			ns, ok := satFloatNanos(x)
			if !ok {
				return 0, fmt.Errorf("cannot read %v as a time", x)
			}
			return ns, nil
		}
		return 0, fmt.Errorf("cannot read %v under format %q", x, strings.Join(formats, "||"))
	}
	n := int64(x)
	if len(formats) == 0 {
		return unixToNanos(n), nil
	}
	for _, f := range formats {
		switch f {
		case "epoch_millis":
			return satScale(n, int64(time.Millisecond)), nil
		case "epoch_second":
			return satScale(n, int64(time.Second)), nil
		}
	}
	return 0, fmt.Errorf("cannot read the number %d under format %q "+
		"(no epoch format named)", n, strings.Join(formats, "||"))
}

// esDateMath resolves a `now`-anchored expression: `now` plus any chain of
// `+N<unit>` / `-N<unit>` with the count optional (`now-d` is one day). Units
// are ES's: s m h H d w M y, with M and y calendar-aware. Rounding (`/d`) is
// REFUSED with its message rather than approximated: `now/d` names midnight,
// and answering from a different instant is the silent wrong answer this
// surface exists to not give.
func esDateMath(s string, now int64) (int64, error) {
	rest := s[len("now"):]
	tn := now
	for rest != "" {
		switch rest[0] {
		case '/':
			return 0, fmt.Errorf("%w: date-math rounding in %q -- spell the "+
				"instant out instead", errESUnsupported, s)
		case '+', '-':
			neg := rest[0] == '-'
			rest = rest[1:]
			nd := 0
			n := int64(0)
			for nd < len(rest) && rest[nd] >= '0' && rest[nd] <= '9' {
				// The guard only stops n growing; satScale below handles any
				// value it reaches.
				if n < 1<<30 {
					n = n*10 + int64(rest[nd]-'0')
				}
				nd++
			}
			if nd == 0 {
				n = 1
			}
			if nd >= len(rest) {
				return 0, fmt.Errorf("date-math %q ends without a unit", s)
			}
			unit := rest[nd]
			rest = rest[nd+1:]
			// The arithmetic SATURATES instead of wrapping, on the int64
			// nanosecond domain. `time.Duration(n) * time.Minute` wraps for n
			// past 2.5e8 minutes and `t.UnixNano()` wraps past the year 2262,
			// and a wrapped bound lands in the PAST -- `gte now+300y` would
			// match everything instead of nothing. A saturated bound is the
			// infinity that instant means on that side.
			switch unit {
			case 's', 'm', 'h', 'H', 'd', 'w':
				var u int64
				switch unit {
				case 's':
					u = int64(time.Second)
				case 'm':
					u = int64(time.Minute)
				case 'h', 'H':
					u = int64(time.Hour)
				case 'd':
					u = 24 * int64(time.Hour)
				case 'w':
					u = 7 * 24 * int64(time.Hour)
				}
				off := satScale(n, u)
				if neg {
					if tn < math.MinInt64+off {
						tn = math.MinInt64
					} else {
						tn -= off
					}
				} else {
					if tn > math.MaxInt64-off {
						tn = math.MaxInt64
					} else {
						tn += off
					}
				}
			case 'M', 'y':
				// Calendar-aware, through time.Time. Months are capped at
				// 3,000 years' worth BEFORE the AddDate: the result already
				// saturates past 2262/1678, so the cap loses nothing and
				// keeps the int conversion and AddDate in range.
				months := n
				if unit == 'y' {
					months = satScale(n, 12)
				}
				if months > 12*3000 {
					months = 12 * 3000
				}
				if neg {
					months = -months
				}
				tn = satNanos(time.Unix(0, tn).UTC().AddDate(0, int(months), 0))
			default:
				return 0, fmt.Errorf("date-math %q: unknown unit %q", s, string(unit))
			}
		default:
			return 0, fmt.Errorf("cannot read %q as date math", s)
		}
	}
	return tn, nil
}

// satNanos is t.UnixNano() saturated to the instants int64 nanoseconds can
// hold (1677-09-21 .. 2262-04-11). `gte 9999-01-01` overflowed the conversion
// and wrapped into the PAST, turning a bound that should match nothing into one
// that matched everything; an instant beyond the range behaves as the infinity
// it is on that side.
//
// ONE DEFINITION for the whole tree, in `internal/query`, because the LogsQL
// `_time:` parser converts the same instants and the two answering differently
// is how the same window gets two answers depending on which language spelled
// it. This wrapper exists so the file that explains the rule keeps explaining
// it; it used to be a year test (`> 2260`, `< 1680`) that gave away two
// representable years at each end.
func satNanos(t time.Time) int64 { return query.SatNanos(t) }

// satScale is n*unit saturated instead of wrapped, the same rule unixToNanos
// applies. Also one definition, in internal/query.
func satScale(n, unit int64) int64 { return query.SatScale(n, unit) }

// satFloatNanos is `f * 1e9` -- fractional epoch seconds -- saturated on the
// way to int64.
//
// The guard it replaces capped the RAW float and then multiplied, so the
// PRODUCT still overflowed: 13000000000.5 is under the 9.1e18 cap and
// 1.3e19 nanoseconds is not. int64 of an out-of-range float64 is
// implementation-defined, and on amd64 it is MinInt64 -- so a far-FUTURE
// bound became minus infinity and the two directions of the comparison
// swapped: `gte` matched every document and `lte` matched none.
//
// NaN is not a time and gets no answer here; the caller reports the value as
// unreadable. Both infinities are exactly the bound they saturate to.
func satFloatNanos(f float64) (int64, bool) {
	if math.IsNaN(f) {
		return 0, false
	}
	ns := f * 1e9
	// 2^63 as a float64: the smallest value at or above which the int64
	// conversion is out of range. MinInt64 is exactly representable, so the
	// low side is a plain `<`.
	const overMaxInt64 = 9223372036854775808.0
	switch {
	case ns >= overMaxInt64:
		return math.MaxInt64, true
	case ns < -overMaxInt64:
		return math.MinInt64, true
	}
	return int64(ns), true
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
		// THE POSITIONS SURVIVE THE SHARDING.
		//
		// This called `IngestJSONLinesParallelCfg`, which returns three
		// integers, and then set `truncated = skip > 0` -- declaring the
		// positions UNKNOWN for every batch over 1 MiB, so markBulkRejects
		// marked EVERY item. One unstorable `_time` in 20,871 `index` actions
		// therefore answered 20,871 items at 400 with 20,870 rows on disk,
		// and a 4xx is permanent to Beats, Logstash and Fluentd. The shard
		// chunks are contiguous and in body order, so the ordinals rebase
		// exactly; IngestJSONLinesParallelResult does it.
		res, werr := ingest.IngestJSONLinesParallelResult(tn.store, docs, fallback, s.parallelCfg(tn.w), &opts)
		ing, skip = res.Accepted, res.Rejected
		if werr != nil {
			s.failIngest(w, werr, ing, skip, len(body))
			return
		}
		rejectedAt, truncated = res.RejectedAt, res.RejectedTruncated
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

	if !markBulkRejects(ops, skip, rejectedAt, truncated) {
		// THE ITEMS ARRAY IS NOT WRITTEN RATHER THAN WRITTEN WRONG. The
		// ingester rejected some of these documents and not others and could
		// not say which, so every per-item status available is false about
		// part of the batch -- 400 records a stored document as permanently
		// lost, 429 tells a shipper to re-send documents that can never be
		// stored. The previous answer was the 429; measured, 70,000 items at
		// 429 over 4,000 stored rows, a pipeline that never drains and 4,000
		// duplicates per retry pass.
		//
		// Reachable only from a server-side inconsistency now:
		// ingest.MaxRejectedAt is esBulkMaxActions, so the positions cannot
		// run out on any body parseBulk accepts. The counts are already in the
		// metrics above, and the message carries what IS known so an operator
		// can reconcile.
		s.writeErr(w, r, ndjsonSpec(), http.StatusInternalServerError,
			"the batch was ingested and its per-item statuses could not be built: "+
				strconv.Itoa(skip)+" of "+strconv.Itoa(len(bulkCandidates(ops)))+
				" documents were rejected and their positions were not attributable, "+
				"so no item status in this response would be true. "+
				strconv.Itoa(ing)+" documents are stored; re-sending this batch "+
				"duplicates them, and this store is append-only and does not "+
				"deduplicate.")
		return
	}

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
