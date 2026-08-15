package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
// The shard query is rebuilt from the FILTER plus the row-local prefix. It is
// not the caller's string with pipes trimmed textually: a text edit of a query
// language is how a subtly different query gets sent, and the parse is already
// done.
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
	shardQuery = segs[0]
	for i := 1; i <= len(plan.ShardPipes) && i < len(segs); i++ {
		shardQuery += " | " + segs[i]
	}
	return shardQuery, plan.CoordinatorPipes, true
}

// queryHead is everything before the first pipe: the filter.
//
// LogsQL's pipe separator is `|` at the top level, and a `|` can appear inside
// a quoted string or a parenthesised subquery. This walks the text rather than
// splitting on the byte, because splitting would cut `_msg:~"a|b"` in half and
// send half a regex to every shard.
func queryHead(raw string) string {
	depth := 0
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
			return strings.TrimSpace(raw[:i])
		}
	}
	return strings.TrimSpace(raw)
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
func shardQueryURL(r *http.Request, shardQuery string) string {
	vals, _ := url.ParseQuery(r.URL.RawQuery)
	vals.Set("query", shardQuery)
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
	stopped := s.applyQueryBudget(r, q)
	_ = stopped
	out := query.ApplyPipes(q, rows)
	if err := q.StopErr(); err != nil {
		return nil, err
	}
	return out, nil
}

// jsonLineToRow decodes one NDJSON row back into the engine's Row.
//
// A round trip: the storage node encoded a Row to JSON and the coordinator
// decodes it to apply pipes. That is the cost of running a pipe over merged
// rows, and it is paid only when a query has a coordinator half -- a bare
// filter never decodes.
//
// # Field order is preserved, not sorted
//
// The order fields come back in is the order they go out in, because a client
// reading NDJSON sees it. Sorting the keys here -- which the first version did,
// for "determinism" -- meant a clustered read returned the same row with its
// fields rearranged relative to a single-node read of the same data. The order
// IS deterministic without sorting: it is the order the storage node emitted,
// which is the order the row was ingested in.
//
// # _time and its absence
//
// _time is lifted back out of the fields, because the engine carries it
// separately and a pipe that sorts by time reads Row.Time rather than a field
// named _time. A line with NO _time is a row that genuinely has none -- a stats
// result, or a projection that dropped it -- and it is marked NoTime so the
// re-encode does not invent a 1970 timestamp for it.
func jsonLineToRow(line []byte) query.Row {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return rawRow(line)
	}
	row := query.Row{NoTime: true}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return rawRow(line)
		}
		key, ok := kt.(string)
		if !ok {
			return rawRow(line)
		}
		vt, err := dec.Token()
		if err != nil {
			return rawRow(line)
		}
		val := jsonScalar(vt)
		if key == "_time" && row.NoTime {
			if t, terr := time.Parse(time.RFC3339Nano, val); terr == nil {
				row.Time, row.NoTime = t.UnixNano(), false
				continue
			}
		}
		row.Fields = append(row.Fields, query.Field{Key: key, Value: val})
	}
	return row
}

// rawRow is what a line this coordinator cannot decode becomes.
//
// A line that fails to parse here is one this cluster's own storage nodes
// encoded, so it is a bug rather than bad input -- but dropping the row
// silently would turn a formatting bug into a missing-data bug, which is far
// harder to notice. It comes through carrying its raw text, where it is
// visible.
func rawRow(line []byte) query.Row {
	return query.Row{Fields: []query.Field{{Key: "_msg", Value: string(line)}}}
}

// jsonScalar renders one decoded JSON scalar as the string the engine holds.
//
// Every field in this engine is a string; the storage nodes encode them as JSON
// strings, so this is normally the identity. It handles the other scalars
// because a hand-written peer or a future encoder emitting a bare number should
// not silently become an empty field.
func jsonScalar(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}
