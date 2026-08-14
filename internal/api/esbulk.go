package api

import (
	"bytes"
	"strconv"
)

// Elasticsearch _bulk action semantics.
//
// A _bulk body alternates an ACTION line with a SOURCE line, except for
// `delete`, which carries none. The previous implementation dropped every
// action line and fed the rest to the NDJSON ingester, which produced four
// defects, all silent:
//
//   - An `update` action's source line is a WRAPPER -- {"doc":{...}} or
//     {"script":{...}} -- not a document. It was stored as an EMPTY log row
//     carrying only a synthesized timestamp: the ingester drops object-valued
//     fields, so the wrapper's single field vanished and what landed was a row
//     with no content at all. (An earlier version of this comment said the row
//     carried a field named "doc". Measured against the pre-2.5 code, it did
//     not -- the row was empty.)
//   - A `delete` was swallowed with no item and no error, so a client asking
//     for a deletion got a success and no indication it did not happen.
//   - The response emitted one item per INGESTED DOCUMENT rather than one per
//     input action. Elasticsearch clients match items to their actions BY
//     POSITION, so any rejection shifted every subsequent status onto the
//     wrong document.
//   - The action was detected with bytes.Contains(line, `"delete"`), so
//     indexing into an index NAMED delete -- {"index":{"_index":"delete"}} --
//     was read as a delete action, and the document line that followed was
//     then read as an action line. That desynchronizes the rest of the body.
//
// This store is APPEND-ONLY. `index` and `create` are the supported
// operations; `update` and `delete` are rejected per item, explicitly, rather
// than being silently dropped or half-applied.

// bulkOp is one action line and the source line that belongs to it.
type bulkOp struct {
	op    string // index | create | update | delete
	index string // _index from the action metadata, echoed back in the item
	id    string // _id, likewise
	doc   []byte // the source line, nil when the action carries none

	status  int    // the item's HTTP-style status
	errType string // non-empty when this item failed
	errMsg  string
}

// esBulkMaxActions bounds how many items one request can produce. The response
// is one item per action, so an unbounded body is an unbounded response; the
// body limit already bounds the input, and this bounds what that input can
// make the server build.
const esBulkMaxActions = 1 << 20

// bulkPresize is the INITIAL reservation: a fixed size a client cannot steer.
//
// Sized to a typical bulk rather than the largest possible one. 4096 ops is
// ~458 KB and covers the batch sizes agents actually send (Filebeat defaults
// to 50, Logstash to 125, Fluentd to 1000), so the common case is one
// allocation. A genuinely huge bulk grows past it through append, whose
// doubling is amortized O(1) and costs ~18 reallocations for a 200k-action
// body -- nothing against parsing 200k actions.
//
// The number matters because it is paid per REQUEST: at the default
// MaxConcurrentWrite of 32, a 7 MB reservation would be 234 MB of memory held
// for bulks that mostly do not need it.
const bulkPresize = 4096

// parseBulk splits a _bulk body into its actions, in order.
//
// Every action produces exactly one bulkOp -- that is what keeps the response
// positionally aligned with the request.
//
// An UNPARSEABLE ACTION LINE fails the whole request instead, and that is not
// a shortcut. The body's meaning is carried entirely by the alternation of
// action and source lines; once a line cannot be identified as an action, the
// parser no longer knows whether the next line is a source or another action,
// and every item after it would be a guess. Elasticsearch returns 400 for the
// request in exactly this case. Reporting a per-item error and resyncing would
// be inventing a recovery the format does not support.
//
// The failures that DO stay per-item are the ones where the alternation is
// still known: an unsupported operation, a missing source line, a source that
// is not an object.
func parseBulk(body []byte) ([]bulkOp, string) {
	// Presized to a CAP, not to an estimate taken from the body.
	//
	// The estimate was bytes.Count(body, '\n')/2, which the client controls
	// freely: a body of nothing but newlines yields ZERO actions and reserved
	// one bulkOp per two bytes. At 112 bytes per op that is 56 bytes reserved
	// per body byte -- 3.5 GiB for a 64 MiB body of newlines, 28 GiB for the
	// 512 MiB decompressed limit, from a gzip POST of about half a megabyte.
	// esBulkMaxActions did not bound it, because the reservation happened
	// before the loop.
	//
	// It was also a second full pass over the body whose only consumer was
	// that estimate, and the parse loop below finds every newline anyway.
	// append grows past the cap for a genuinely large bulk, which is the case
	// that should pay for the copying rather than every hostile one.
	ops := make([]bulkOp, 0, bulkPresize)
	rest := body
	for len(rest) > 0 {
		if len(ops) >= esBulkMaxActions {
			// Silently dropping the rest and answering 200 is the same silent
			// loss this task exists to remove: reachable at 14.7 MB, well
			// inside the body limit, with no error anywhere.
			return nil, "bulk request contains more than " +
				strconv.Itoa(esBulkMaxActions) + " actions"
		}
		line, tail := nextLine(rest)
		rest = tail
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		op, meta, err := parseBulkAction(line)
		if err != "" {
			return nil, err
		}
		cur := bulkOp{op: op, index: meta.Index, id: meta.ID}

		// delete carries no source line.
		if op == "delete" {
			cur.status = 400
			cur.errType = "illegal_argument_exception"
			cur.errMsg = "delete is not supported: this is an append-only log store"
			ops = append(ops, cur)
			continue
		}

		// Every other action needs its source line.
		docLine, tail2 := nextLine(rest)
		for len(bytes.TrimSpace(docLine)) == 0 && len(tail2) > 0 {
			docLine, tail2 = nextLine(tail2)
		}
		rest = tail2
		if len(bytes.TrimSpace(docLine)) == 0 {
			cur.status = 400
			cur.errType = "illegal_argument_exception"
			cur.errMsg = "action is not followed by a source line"
			ops = append(ops, cur)
			continue
		}

		if op == "update" {
			// The source is {"doc":...} or {"script":...}, a partial-update
			// instruction rather than a document. Storing it produced an EMPTY
			// row: the ingester drops object-valued fields, so the wrapper's
			// only field vanished and what landed carried nothing but a
			// synthesized timestamp.
			cur.status = 400
			cur.errType = "illegal_argument_exception"
			cur.errMsg = "update is not supported: this is an append-only log store"
			ops = append(ops, cur)
			continue
		}

		// index and create: the source must be a JSON object, checked here so
		// the failing item can be named. A body handed to the ingester
		// wholesale reports only a COUNT of rejects, which cannot be mapped
		// back to a position.
		if !isJSONObject(docLine) {
			cur.status = 400
			cur.errType = "mapper_parsing_exception"
			cur.errMsg = "source line is not a JSON object"
			ops = append(ops, cur)
			continue
		}
		cur.doc = docLine
		cur.status = 201
		ops = append(ops, cur)
	}
	return ops, ""
}

func nextLine(b []byte) (line, rest []byte) {
	i := bytes.IndexByte(b, '\n')
	if i < 0 {
		return b, nil
	}
	return b[:i], b[i+1:]
}

// bulkMeta is the action's metadata object.
type bulkMeta struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

// parseBulkAction reads one action line: a JSON object with exactly one key,
// and that key names the operation. Parsed, not substring matched, so an index
// NAMED "delete" cannot be read as a delete action.
//
// ALLOCATION-FREE on the success path. The obvious spelling --
// json.Unmarshal into a map[string]json.RawMessage -- builds a map PER ACTION,
// which for a 200k-document bulk is 200k maps for the reflective decoder to
// populate and the GC to collect. An action line is a fixed, tiny shape, so
// the key is found by a byte scan and the two metadata fields are read the
// same way. Only the returned strings escape, and only when the fields are
// present.
func parseBulkAction(line []byte) (op string, meta bulkMeta, errMsg string) {
	t := bytes.TrimSpace(line)
	if len(t) < 2 || t[0] != '{' || t[len(t)-1] != '}' {
		return "", meta, "action line is not a JSON object"
	}
	inner := bytes.TrimSpace(t[1 : len(t)-1])
	if len(inner) == 0 {
		return "", meta, "action line has no operation key"
	}
	key, rest, ok := jsonKey(inner)
	if !ok {
		return "", meta, "action line has no operation key"
	}
	switch string(key) { // no allocation: comparing a []byte against a constant
	case "index":
		op = "index"
	case "create":
		op = "create"
	case "update":
		op = "update"
	case "delete":
		op = "delete"
	default:
		return "", meta, "unknown bulk action " + strconv.Quote(string(key))
	}

	val := bytes.TrimSpace(rest)
	// A second key means this is not a well-formed action line. Detected by
	// looking for a comma AFTER the metadata object rather than by decoding.
	if len(val) > 0 && !bytes.Equal(val, []byte("null")) {
		if val[0] != '{' {
			return "", bulkMeta{}, "action metadata is not an object"
		}
		end := matchBrace(val)
		if end < 0 {
			return "", bulkMeta{}, "action metadata is not an object"
		}
		if tail := bytes.TrimSpace(val[end+1:]); len(tail) > 0 {
			return "", bulkMeta{}, "action line must have exactly one key naming the operation"
		}
		meta.Index = jsonStringField(val[:end+1], "_index")
		meta.ID = jsonStringField(val[:end+1], "_id")
	}
	return op, meta, ""
}

// jsonKey reads a leading "key": from an object's interior and returns the key
// bytes and what follows the colon. No allocation: the key aliases the input.
func jsonKey(b []byte) (key, rest []byte, ok bool) {
	if len(b) == 0 || b[0] != '"' {
		return nil, nil, false
	}
	end := scanString(b)
	if end < 0 {
		return nil, nil, false
	}
	key = b[1:end]
	after := bytes.TrimSpace(b[end+1:])
	if len(after) == 0 || after[0] != ':' {
		return nil, nil, false
	}
	return key, after[1:], true
}

// scanString returns the index of the closing quote of the string starting at
// b[0], honouring backslash escapes, or -1.
func scanString(b []byte) int {
	for i := 1; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++ // skip the escaped byte
		case '"':
			return i
		}
	}
	return -1
}

// matchBrace returns the index of the '}' closing the object at b[0], skipping
// over strings so a brace inside a value cannot end it early, or -1.
func matchBrace(b []byte) int {
	depth := 0
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '"':
			e := scanString(b[i:])
			if e < 0 {
				return -1
			}
			i += e
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// jsonStringField reads a top-level string field out of a small JSON object
// without decoding the whole thing. Returns "" when absent or not a string.
// Only the returned value allocates, and only when the field is present --
// which for _index and _id is what actually has to reach the response.
func jsonStringField(obj []byte, name string) string {
	if len(obj) < 2 {
		return ""
	}
	b := bytes.TrimSpace(obj[1 : len(obj)-1])
	for len(b) > 0 {
		key, rest, ok := jsonKey(b)
		if !ok {
			return ""
		}
		val := bytes.TrimSpace(rest)
		if len(val) == 0 {
			return ""
		}
		var end int
		switch val[0] {
		case '"':
			end = scanString(val)
			if end < 0 {
				return ""
			}
			if string(key) == name {
				return string(val[1:end])
			}
		case '{', '[':
			end = matchBracket(val)
			if end < 0 {
				return ""
			}
		default:
			end = bytes.IndexByte(val, ',') - 1
			if end < 0 {
				end = len(val) - 1
			}
		}
		b = bytes.TrimSpace(val[end+1:])
		if len(b) > 0 && b[0] == ',' {
			b = bytes.TrimSpace(b[1:])
		}
	}
	return ""
}

// matchBracket is matchBrace for either bracket kind.
func matchBracket(b []byte) int {
	open, close := b[0], byte('}')
	if open == '[' {
		close = ']'
	}
	depth := 0
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '"':
			e := scanString(b[i:])
			if e < 0 {
				return -1
			}
			i += e
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// isJSONObject is a STRUCTURAL check: the line opens and closes as an object.
//
// Deliberately not json.Valid, which is a second full pass over every document
// in the body -- the ingester parses them all anyway, so that pass would
// double the cost of a large bulk to learn something it is about to learn. The
// cheap check is what catches the case that actually matters here, an `update`
// wrapper or a scalar where a document belongs; a document that opens as an
// object and is malformed inside is caught by the ingester and reported as an
// unattributable reject.
func isJSONObject(line []byte) bool {
	t := bytes.TrimSpace(line)
	return len(t) >= 2 && t[0] == '{' && t[len(t)-1] == '}'
}

// appendBulkItems writes the response items, one per action, in order.
//
// Written directly rather than through the reflective encoder: a bulk of 200k
// documents would otherwise build 200k maps for it to walk.
func appendBulkItems(out []byte, ops []bulkOp) []byte {
	out = append(out, `,"items":[`...)
	for i, o := range ops {
		if i > 0 {
			out = append(out, ',')
		}
		// An action whose own line did not parse has no operation name;
		// Elasticsearch reports those under "index".
		name := o.op
		if name == "" {
			name = "index"
		}
		out = append(out, '{', '"')
		out = append(out, name...)
		out = append(out, `":{`...)
		if o.index != "" {
			out = append(out, `"_index":`...)
			out = quoted(out, o.index)
			out = append(out, ',')
		}
		if o.id != "" {
			out = append(out, `"_id":`...)
			out = quoted(out, o.id)
			out = append(out, ',')
		}
		out = append(out, `"status":`...)
		out = strconv.AppendInt(out, int64(o.status), 10)
		if o.errType != "" {
			out = append(out, `,"error":{"type":`...)
			out = quoted(out, o.errType)
			out = append(out, `,"reason":`...)
			out = quoted(out, o.errMsg)
			out = append(out, '}')
		}
		out = append(out, '}', '}')
	}
	return append(out, ']')
}

// quoted appends s as a JSON string, quotes included. The package's
// appendJSONString escapes the CONTENT only, which every other caller wants
// because it is building a key or a value into a larger literal.
func quoted(out []byte, s string) []byte {
	out = append(out, '"')
	out = appendJSONString(out, s)
	return append(out, '"')
}

// bulkDocs compacts the source lines into the FRONT of the body buffer, so the
// ingester sees one contiguous NDJSON batch and keeps its parallel path.
//
// In place, with no second buffer: each doc line is preceded in the input by
// at least its own action line, so the write cursor always trails the read
// cursor and a multi-megabyte bulk is not copied twice. o.doc aliases body, so
// the copy is a move toward the front of the same array.
func bulkDocs(ops []bulkOp, body []byte) []byte {
	w := 0
	for _, o := range ops {
		if o.doc == nil {
			continue
		}
		w += copy(body[w:], o.doc)
		body[w] = '\n'
		w++
	}
	return body[:w]
}

// bulkHasError reports whether any item failed, which is the top-level
// `errors` flag every Elasticsearch client switches on.
func bulkHasError(ops []bulkOp) bool {
	for _, o := range ops {
		if o.errType != "" {
			return true
		}
	}
	return false
}

// markBulkRejects maps the ingester's rejected records onto their items.
//
// The ingester's docs are the doc-carrying ops in order, so ordinal k is the
// k-th of them. Getting this wrong is not a cosmetic error: the first attempt
// marked the FIRST n doc-carrying items 500, which reported the STORED
// document as failed and the DROPPED one as created. Elastic's own client
// matches items positionally and retries anything over 201, so it re-sent what
// had landed -- a duplicate, in an append-only store -- and recorded what had
// vanished as delivered.
//
// When the positions are NOT known (the parallel path shards the body, so a
// shard's ordinals index nothing at batch scale), every candidate item is
// marked instead of guessing at n of them. Over-reporting causes duplicates,
// which a caller can reconcile; under-reporting causes loss, which it cannot.
func markBulkRejects(ops []bulkOp, rejected int, rejectedAt []int32, truncated bool) {
	if rejected <= 0 {
		return
	}
	// The doc-carrying ops, in the order the ingester saw them.
	idx := make([]int, 0, len(ops))
	for i := range ops {
		if ops[i].doc != nil && ops[i].errType == "" {
			idx = append(idx, i)
		}
	}

	if !truncated && len(rejectedAt) == rejected {
		for _, ord := range rejectedAt {
			if int(ord) < 0 || int(ord) >= len(idx) {
				truncated = true // an ordinal that indexes nothing: fall back
				break
			}
		}
		if !truncated {
			for _, ord := range rejectedAt {
				i := idx[ord]
				ops[i].status = 500
				ops[i].errType = "server_error"
				ops[i].errMsg = "the document was not stored"
			}
			return
		}
	}

	// Positions unknown. Say so on every candidate rather than choosing.
	msg := "the document may not have been stored: " +
		strconv.Itoa(rejected) + " of " + strconv.Itoa(len(idx)) +
		" documents in this batch were rejected and their positions are not known"
	for _, i := range idx {
		ops[i].status = 500
		ops[i].errType = "server_error"
		ops[i].errMsg = msg
	}
}
