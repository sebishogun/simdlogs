package api

import (
	"context"
	"fmt"
	"mime"
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
	// A MISSING query is refused, exactly as a single node refuses it.
	//
	// This defaulted to `*`, and parseRequest's own comment says why that is
	// wrong: "The reference requires `query` on every select endpoint and
	// rejects a request without one. Defaulting to match-all answered a
	// client's bug with the entire store." A router did the opposite of the
	// node it fronts:
	//
	//	GET /select/logsql/query           single 400   router 200, whole store
	//	GET /select/logsql/query?query=    single 400   router 200, whole store
	//	POST form, body junk=1             single 400   router 200, shard asked `*`
	//
	// It is also the amplifier under every "the shards were asked the empty
	// query" defect: on this route a dropped filter was not a smaller answer,
	// it was the entire store returned at HTTP 200.
	raw := r.FormValue("query")
	if strings.TrimSpace(raw) == "" {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, errMissingQuery.Error())
		return "", nil, false
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

// planKeysCtx marks the parameters the ROUTER'S PLAN has already resolved for
// a shard request, so the caller's form cannot put them back.
//
// withFormInURL merges form keys that are not already in the shard URL. That
// rule cannot tell "the caller never sent this" from "the plan DELETED this",
// and a deleted key is exactly the case that matters: shardQueryURL removes
// `limit` when there is a coordinator half, because the shards must return
// everything and the bound is applied once over the merged rows. Over a POST
// the caller's limit was not in the URL, so it was not skipped, and the merge
// put it straight back. Measured, three shards of ten rows, `&limit=5`:
//
//   - | stats count() c    single node 30, cluster GET 30, cluster POST 15
//   - | stats by (level)   10/10/10,       10/10/10,       5/5/5
//
// HTTP 200 on all of them. Deleting from r.Form as well would fix `limit` and
// leave the next deleted parameter to be found the same way, so the plan
// records WHICH keys it owns and withFormInURL skips those, set or deleted.
type planKeysCtx struct{}

// withPlanKeys returns r marked as having these parameters resolved by the plan.
func withPlanKeys(r *http.Request, keys ...string) *http.Request {
	owned := map[string]struct{}{}
	if prev, ok := r.Context().Value(planKeysCtx{}).(map[string]struct{}); ok {
		for k := range prev {
			owned[k] = struct{}{}
		}
	}
	for _, k := range keys {
		owned[k] = struct{}{}
	}
	return r.WithContext(context.WithValue(r.Context(), planKeysCtx{}, owned))
}

// planOwns reports whether the plan has resolved this parameter already.
func planOwns(r *http.Request, key string) bool {
	owned, _ := r.Context().Value(planKeysCtx{}).(map[string]struct{})
	_, ok := owned[key]
	return ok
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
	// Neither flag, and that is deliberate now rather than incidental.
	//
	// This used to set MatAll: true. ApplyPipes reads neither flag -- it runs
	// over rows it is HANDED, so there is no scan to widen and no output shape
	// to choose -- so it was inert. It is removed rather than left because
	// MatAll's meaning changed: it is now "full-RECORD output", which the API
	// reads as "synthesize _stream/_stream_id onto every row". The coordinator
	// writes with withStream=false so nothing happened, and the day that
	// changes an inert true would put the pair back on merged stats rows.
	q := &query.Query{Pipes: pipes}
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
	// PARSE the form before cloning, so there is something to delete from.
	//
	// The first attempt at this deleted the two keys from out.Form and
	// out.PostForm after the clone -- and on a POST that nothing has parsed yet
	// both are nil, so it deleted nothing and withFormInURL later parsed the
	// body fresh and put the caller's limit back. A test asserting on the two
	// final ANSWERS stayed green through that, because two differently
	// truncated shard-local lists can sum to the same visible number; a test
	// asserting on what the SHARD RECEIVES failed immediately.
	//
	// A body that will not parse is not a request that can be planned. The
	// caller turns false into a 400 (refuseUnparseableQuery), which is the same
	// answer withFormInURL gives for the same reason.
	// Only for a FORM body. ParseForm reads the body for any POST/PUT and runs
	// mime.ParseMediaType first, so a malformed Content-Type failed here on a
	// body this function was never going to read -- and only on the six routes
	// that reach withoutLimits. The same request answered 200 on the other six:
	//
	//	POST <route>?query=*   Content-Type: text/plain; charset
	//	  query, hits, facets, stats_query, stats_query_range, sql   -> 200
	//	  field_names, field_values, streams, stream_ids,
	//	  stream_field_names, stream_field_values                    -> 400
	//
	// facets escaped only by argument-evaluation order: maxValuesParam(r) primes
	// ParseForm and swallows the error before this runs. Gating on the content
	// type makes all twelve agree, and agree with a single node.
	if isFormPost(r) {
		normalizeFormContentType(r)
		if err := parseFormBody(r); err != nil {
			// A form this node cannot parse -- but NOT necessarily a bad query
			// string. ParseForm also fails on a malformed Content-Type, because
			// parsePostForm runs mime.ParseMediaType first, and the caller's
			// 400 then told an operator their query string was unreadable when
			// it was fine. Measured: `Content-Type: text/plain; charset` on
			// /select/logsql/field_values, whose query string parses perfectly.
			//
			// The URL query is re-checked here so the answer names the half
			// that is actually wrong.
			return nil, false
		}
	}
	vals.Del("limit")
	vals.Del("max_values_per_field")
	for k, v := range unlimited {
		vals.Set(k, v)
	}
	out := r.Clone(r.Context())
	out.URL.RawQuery = vals.Encode()
	// NOT marked with withPlanKeys, measured rather than assumed.
	//
	// withFormInURL no longer skips a key merely because it is in the shard
	// URL, so the obvious move is to mark these the way federatedSelect marks
	// `query`. Adding that marking reddens nothing: the deletion from the
	// clone's Form and PostForm below is what protects them, because a key
	// absent from r.Form cannot be re-added by the merge.
	//
	// Third time this session that "mark it too, for safety" turned out to be
	// inert. An inert guard reads as a load-bearing one.

	// The PARSED FORM as well, not only the query string.
	//
	// On a POST these parameters are in the body, so deleting them from
	// RawQuery deleted nothing and withFormInURL merged them straight back.
	// Measured, three shards, 30 rows, `query=*&field=user&limit=2`:
	//
	//	GET  200 {"values":[{"value":"u0","hits":5},{"value":"u1","hits":5}]}
	//	POST 200 {"values":[{"value":"u0","hits":4},{"value":"u1","hits":4},
	//	                    {"value":"u2","hits":2},{"value":"u3","hits":2}]}
	//
	// u0 has five hits across the cluster and the POST answer said four: each
	// shard truncated to its own top 2 and the coordinator summed the truncated
	// lists. HTTP 200, a smaller number, nothing to tell it apart from a
	// correct answer. That is what deleting a limit from the shard request
	// exists to prevent, and it only worked over GET.
	//
	// Request.Clone copies Form and PostForm, so the clone carries the caller's
	// parsed values and they have to be removed from BOTH -- r.Form is the
	// union the peer reads and r.PostForm is what re-parsing would rebuild it
	// from.
	// NOT marked plan-owned here.
	//
	// A marking would be inert: this function DELETES both keys from out.Form
	// and out.PostForm below, so they never reach `extra` in the first place --
	// removing the marking fails no test, and a comment claiming it stops
	// withFormInURL re-adding them would be describing the deletion's work.
	// The marking that IS load-bearing is federatedSelect's, where the plan
	// deletes from the URL and cannot reach the form.
	for _, f := range []url.Values{out.Form, out.PostForm} {
		if f == nil {
			continue
		}
		f.Del("limit")
		f.Del("max_values_per_field")
		for k, v := range unlimited {
			f.Set(k, v)
		}
	}
	return out, true
}

// isFormPost reports whether r carries a form body this router will read.
//
// MULTIPART COUNTS. A single node reads it -- r.FormValue calls
// ParseMultipartForm -- and this returned false for it, so the router never
// folded a multipart form into the shard URL and the shards were asked nothing:
//
//	curl -F 'query=*' <node>    200 on all twelve federated reads
//	curl -F 'query=*' <router>  200 on one, 5xx on eleven
//
// Reading what the node reads is the whole contract of a router.
func isFormPost(r *http.Request) bool {
	return formKind(r) != formNone
}

type formEncoding int

const (
	formNone formEncoding = iota
	formURLEncoded
	formMultipart
)

func formKind(r *http.Request) formEncoding {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		return formNone
	}
	ct := r.Header.Get("Content-Type")
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	switch strings.TrimSpace(ct) {
	case "application/x-www-form-urlencoded":
		return formURLEncoded
	case "multipart/form-data":
		return formMultipart
	}
	return formNone
}

// parseFormBody parses r's form, whichever of the two encodings it is.
//
// ParseForm does NOT parse a multipart body -- it returns nil having populated
// r.Form from the query string alone, which is indistinguishable from success
// and is why a multipart request reached the shards with no parameters.
func parseFormBody(r *http.Request) error {
	if formKind(r) == formMultipart {
		// 32 MiB, net/http's own default for FormValue. Anything larger spills
		// to a temp file, which this path does not want; the parameters a
		// select endpoint takes are query strings, not uploads.
		if err := r.ParseMultipartForm(32 << 20); err == nil {
			return nil
		}
		// A multipart body that will not parse is IGNORED, not fatal -- which
		// is what a single node does, because FormValue discards
		// ParseMultipartForm's error and reads r.Form. The URL query is then
		// the whole request, and if it carries no `query` the request is
		// refused for THAT, which is the reason a node gives.
		return r.ParseForm()
	}
	return r.ParseForm()
}

// normalizeFormContentType drops unparseable parameters from a form request's
// Content-Type, in place, so ParseForm's error is about the BODY and nothing
// else.
//
// `application/x-www-form-urlencoded; charset` -- jQuery's default spelling
// with the value lost -- is a valid media type with an invalid parameter.
// mime.ParseMediaType returns BOTH the correct media type and
// ErrInvalidMediaParameter, so net/http's parsePostForm reads and parses the
// body perfectly and then returns the header's error anyway. withoutLimits
// refused a form it had just parsed correctly, and only on the routes that
// reach it -- so gating on the content type moved the six-vs-six split to the
// neighbouring content type rather than removing it. Measured, body
// `query=%2A&field=level&limit=2`, `Content-Type: ...urlencoded; charset`:
//
//	/select/logsql/query        200  shard: field=level limit=2 query=*
//	/select/logsql/field_values 400  shard never asked
//
// Excusing the error instead -- errors.Is(err, mime.ErrInvalidMediaParameter)
// and carry on -- is the version that looks right and is not. parsePostForm
// keeps the header's error and DISCARDS url.ParseQuery's, so a request that is
// malformed in both halves reports only the header:
//
//	ct "...urlencoded; charset", body "query=%zz&limit=2"
//	  err = mime: invalid media parameter        the body error is gone
//	  PostForm = map[limit:[2]]                  `query` is gone with it
//
// That is a dropped query forwarded at HTTP 200. Correcting the header first
// keeps the precedence: ParseForm then fails on `%zz` and the request is
// refused, and refuseUnparseableQuery names the body, which is the half that
// is wrong.
//
// In place, and not restored afterwards. isFormPost already reads this header
// by stripping at the first `;`, so the router has decided what it means; the
// clone and the error path both then see the header the router acted on.
func normalizeFormContentType(r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err == nil || mt == "" {
		return
	}
	r.Header.Set("Content-Type", mt)
}

// refuseUnparseableQuery answers the caller when withoutLimits could not read
// the query string, and reports whether the caller should stop.
func (s *Server) refuseUnparseableQuery(w http.ResponseWriter, r *http.Request) {
	// WHICH half is unreadable, checked rather than assumed.
	//
	// This said "query string" flat, and a malformed FORM BODY is the common
	// case. The Content-Type branch below is kept as a bound rather than as the
	// case it was written for: withoutLimits no longer calls ParseForm unless
	// the content type IS a form, so a malformed one is not an error here at
	// all -- it just means there is no form to read.
	what := "request body"
	if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
		what = "query string"
	} else if ct := r.Header.Get("Content-Type"); ct != "" {
		if _, _, err := mime.ParseMediaType(ct); err != nil {
			what = fmt.Sprintf("Content-Type header (%q)", ct)
		}
	}
	s.writeErr(w, r, readSpec(), http.StatusBadRequest,
		"simdlogs: this request's "+what+" could not be parsed, so the "+
			"router cannot strip the per-shard result limits from it. Forwarding "+
			"it unchanged would build the cluster's top values out of each shard's "+
			"own top values, which silently drops anything popular across the "+
			"cluster and unremarkable on each shard, so it is refused instead")
}
