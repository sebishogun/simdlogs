package api

import (
	"encoding/json"
	"time"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	"github.com/sebishogun/simdlogs/internal/query"
)

// jsonLineToRow decodes one NDJSON row back into the engine's Row.
//
// A round trip: the storage node encoded a Row to JSON and the coordinator
// decodes it to apply pipes. That is the cost of running a pipe over merged
// rows, and it is paid only when a query has a coordinator half -- a bare
// filter never decodes.
//
// # Two allocations per row, not one per token
//
// This was an encoding/json Decoder over a bytes.Reader, and it cost 147
// allocations and 4448 bytes for a ten-field row: a Decoder, its 512-byte read
// buffer, and an `any` box plus a string per token, per row, on the merge path
// of every clustered query with a coordinator half.
//
// The scanner below allocates one byte slice sized at len(line) and one Field
// slice. Keys and values are strings aliasing that one buffer via
// unsafe.String.
//
// # Why the aliasing is sound, which is NOT because the buffer cannot grow
//
// It can. An earlier version of this comment said the buffer "never
// reallocates" because unescaping only shrinks, and that was false in one
// direction: a `\uXXXX` escape can encode up to three bytes from six, so it
// shrinks, but the buffer is a fixed cap and a caller could still exceed it in
// a shape nobody has constructed. Resting on "it never grows" is the licence
// somebody needs to hoist an unsafe.String above its appends or reuse the
// buffer across rows, and both of those ARE unsafe.
//
// What actually makes it sound, and stays true whether or not the buffer
// grows: every string is formed by bufStr AFTER its own appends are finished,
// so the bytes behind it never change; append COPIES on growth, so a string
// over the old array keeps its own bytes and the GC keeps that array alive
// through the interior pointer; and the buffer is per row and never handed to a
// caller as a slice. The line itself is NOT aliased, because it comes from a
// reused read buffer.
//
// So the two-allocation figure is the common case, not a guarantee. It is what
// TestRowScannerAllocatesTwicePerRow measures for a ten-field row, and a row
// whose content exceeds len(line) would pay one more.
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
// re-encode does not invent a 1970 timestamp for it. An UNPARSEABLE _time stays
// a field: it is data the row carries, and dropping it would lose it silently.
//
// # Anything that is not a flat JSON object is the whole line as _msg
//
// The contract, which is stricter than the Decoder's and pinned by
// TestRowScannerMatchesTheDecoder:
//
//   - A line that is a valid JSON object decodes to the same row the Decoder
//     produced, field for field and in the same order.
//   - Anything else -- not valid JSON, valid JSON that is not an object, or a
//     valid object with bytes after it -- is the whole line as _msg.
//
// The Decoder was looser in three ways this does not reproduce, all of which
// turn a broken line into a plausible row:
//
//   - A TRUNCATED object returned the fields read so far, so `{"a":1,"b":` was
//     a row with one field and no sign the rest was cut off. `{` alone was an
//     empty row.
//   - TRAILING BYTES were ignored: `{"a":1} garbage` and `{"a":1}{"b":2}` both
//     decoded to the first object and dropped the rest of the line.
//   - A NESTED value was flattened by the token stream: `{"a":{"b":1}}` became
//     a field `a` with value "{" followed by a field `b` -- a row that exists
//     nowhere. A nested value is now the raw JSON text of that value, which is
//     the one case here that is not simply rawRow.
//
// # Bytes are preserved, which is where this deliberately differs from encoding/json
//
// encoding/json COERCES invalid UTF-8 in a string to U+FFFD. This does not,
// because the line did not come from encoding/json: the storage node encodes a
// row with appendJSONString, which passes every byte >= 0x80 through
// unchanged, and /insert/logfmt stores a raw 0x80 without rejecting it. An
// earlier version of this scanner matched encoding/json here, and the result
// was that the same query answered `bad-\x80-byte` from a node and
// `bad-\xef\xbf\xbd-byte` from a cluster -- different bytes, HTTP 200, no
// marker, and anything doing an exact-match or a checksum against the ingested
// payload got a different answer from a cluster than from a node.
//
// The round trip is the contract: what a shard encoded is what comes back.
func jsonLineToRow(line []byte) query.Row {
	i := skipWS(line, 0)
	if i >= len(line) || line[i] != '{' {
		return rawRow(line)
	}
	i++

	// One buffer for every key and value byte in the row, sized so the common
	// case never grows. Growth is SAFE if it happens -- append copies, and a
	// string already formed over the old array keeps its bytes and keeps that
	// array alive -- so this is a sizing hint and not an invariant anything
	// depends on.
	buf := make([]byte, 0, len(line))
	row := query.Row{NoTime: true}

	// first distinguishes the opening `{` from a `,`. It is NOT len(row.Fields)
	// > 0: a lifted _time adds no field, so a row starting with _time would
	// then look like its first pair and the comma before the second key would
	// be read as the start of a key. That is what the differential against the
	// decoder caught -- every ordinary row, whose first field is _time, fell
	// back to rawRow.
	first := true
	for {
		i = skipWS(line, i)
		if i >= len(line) {
			return rawRow(line) // truncated
		}
		if line[i] == '}' {
			if skipWS(line, i+1) != len(line) {
				return rawRow(line) // bytes after the object
			}
			return row
		}
		if !first {
			if line[i] != ',' {
				return rawRow(line)
			}
			i = skipWS(line, i+1)
			if i >= len(line) || line[i] == '}' {
				return rawRow(line) // trailing comma
			}
		}
		first = false

		key, ni, ok := scanJSONString(line, i, &buf)
		if !ok {
			return rawRow(line)
		}
		i = skipWS(line, ni)
		if i >= len(line) || line[i] != ':' {
			return rawRow(line)
		}
		val, ni, ok := scanJSONValue(line, skipWS(line, i+1), &buf)
		if !ok {
			return rawRow(line)
		}
		i = ni

		if key == "_time" && row.NoTime {
			if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
				row.Time, row.NoTime = t.UnixNano(), false
				continue
			}
		}
		if row.Fields == nil {
			// Sized exactly, from a count taken over the line rather than
			// guessed: a fixed cap either grows (a third allocation for a
			// ten-field row) or over-reserves on every stats row, and the
			// coordinator holds every merged row at once. The count costs one
			// pass over ~200 bytes and is taken only once a field exists, so a
			// line that fails to parse never pays for it.
			row.Fields = make([]query.Field, 0, countFields(line))
		}
		row.Fields = append(row.Fields, query.Field{Key: key, Value: val})
	}
}

// countFields is the number of top-level pairs in a JSON object: an upper
// bound on the fields a row will carry, since a lifted _time removes one. It
// is a size hint, so a wrong answer on a malformed line costs nothing.
func countFields(line []byte) int {
	depth, n := 0, 0
	for i := 0; i < len(line); {
		switch line[i] {
		case '"':
			ni, ok := skipStringRaw(line, i)
			if !ok {
				return n + 1
			}
			i = ni
			continue
		case '{', '[':
			depth++
			if depth == 1 {
				n = 1 // the first pair has no comma before it
			}
		case '}', ']':
			depth--
		case ',':
			if depth == 1 {
				n++
			}
		}
		i++
	}
	return n
}

// rawRow is the whole line as _msg: the row a caller gets for a line this
// coordinator cannot read as a flat JSON object.
func rawRow(line []byte) query.Row {
	return query.Row{Fields: []query.Field{{Key: "_msg", Value: string(line)}}}
}

func skipWS(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanJSONString reads a quoted string starting at b[i] and returns it as a
// string aliasing *buf, the index just past the closing quote, and whether the
// string was well-formed.
func scanJSONString(b []byte, i int, buf *[]byte) (string, int, bool) {
	if i >= len(b) || b[i] != '"' {
		return "", i, false
	}
	i++
	start := i
	// Fast path: no escape and no byte that needs coercing, so the value is
	// one copy of a contiguous span.
	for i < len(b) {
		c := b[i]
		if c == '"' {
			return appendStr(buf, b[start:i]), i + 1, true
		}
		if c == '\\' {
			break
		}
		if c < 0x20 {
			return "", i, false // control byte, unescaped
		}
		i++
	}
	if i >= len(b) {
		return "", i, false
	}

	// Escaped path: copy what is already scanned, then decode the rest.
	off := len(*buf)
	*buf = append(*buf, b[start:i]...)
	for i < len(b) {
		c := b[i]
		switch {
		case c == '"':
			return bufStr(buf, off), i + 1, true
		case c < 0x20:
			return "", i, false
		case c != '\\':
			*buf = append(*buf, c)
			i++
			continue
		}
		// c == '\\'
		i++
		if i >= len(b) {
			return "", i, false
		}
		switch b[i] {
		case '"', '\\', '/':
			*buf = append(*buf, b[i])
			i++
		case 'b':
			*buf = append(*buf, '\b')
			i++
		case 'f':
			*buf = append(*buf, '\f')
			i++
		case 'n':
			*buf = append(*buf, '\n')
			i++
		case 'r':
			*buf = append(*buf, '\r')
			i++
		case 't':
			*buf = append(*buf, '\t')
			i++
		case 'u':
			r, ni, ok := scanUnicodeEscape(b, i+1)
			if !ok {
				return "", ni, false
			}
			*buf = utf8.AppendRune(*buf, r)
			i = ni
		default:
			return "", i, false
		}
	}
	return "", i, false
}

// scanUnicodeEscape reads the four hex digits at b[i:] and, when they are a
// high surrogate followed by a \u low surrogate, the pair. i points just past
// the 'u'.
func scanUnicodeEscape(b []byte, i int) (rune, int, bool) {
	r, ok := hex4(b, i)
	if !ok {
		return 0, i, false
	}
	i += 4
	if !utf16.IsSurrogate(rune(r)) {
		return rune(r), i, true
	}
	// A lone surrogate is what encoding/json emits U+FFFD for, and matching it
	// keeps the two implementations byte-identical on the same input.
	if i+1 < len(b) && b[i] == '\\' && b[i+1] == 'u' {
		if lo, ok := hex4(b, i+2); ok {
			if dec := utf16.DecodeRune(rune(r), rune(lo)); dec != utf8.RuneError {
				return dec, i + 6, true
			}
		}
	}
	return utf8.RuneError, i, true
}

func hex4(b []byte, i int) (uint32, bool) {
	if i+4 > len(b) {
		return 0, false
	}
	var v uint32
	for j := 0; j < 4; j++ {
		c := b[i+j]
		switch {
		case c >= '0' && c <= '9':
			v = v<<4 | uint32(c-'0')
		case c >= 'a' && c <= 'f':
			v = v<<4 | uint32(c-'a'+10)
		case c >= 'A' && c <= 'F':
			v = v<<4 | uint32(c-'A'+10)
		default:
			return 0, false
		}
	}
	return v, true
}

// scanJSONValue reads one value at b[i] and returns its text.
//
// The text is what the Decoder produced with UseNumber: a string unescaped, a
// number as its literal, a bool as "true"/"false", null as the empty string.
func scanJSONValue(b []byte, i int, buf *[]byte) (string, int, bool) {
	if i >= len(b) {
		return "", i, false
	}
	switch c := b[i]; {
	case c == '"':
		return scanJSONString(b, i, buf)
	case c == 't':
		if i+4 <= len(b) && string(b[i:i+4]) == "true" {
			return "true", i + 4, true
		}
		return "", i, false
	case c == 'f':
		if i+5 <= len(b) && string(b[i:i+5]) == "false" {
			return "false", i + 5, true
		}
		return "", i, false
	case c == 'n':
		if i+4 <= len(b) && string(b[i:i+4]) == "null" {
			return "", i + 4, true
		}
		return "", i, false
	case c == '-' || (c >= '0' && c <= '9'):
		ni, ok := scanNumber(b, i)
		if !ok {
			return "", ni, false
		}
		return appendStr(buf, b[i:ni]), ni, true
	case c == '{' || c == '[':
		ni, ok := scanComposite(b, i)
		if !ok {
			return "", ni, false
		}
		// scanComposite only balances the brackets, so `{0}` gets that far.
		// The span is validated properly rather than approximately, because a
		// nested value that is not JSON means the LINE is not JSON and the row
		// is rawRow. This is the only encoding/json call left on this path and
		// a flat row -- every row a storage node emits -- never reaches it.
		if !json.Valid(b[i:ni]) {
			return "", ni, false
		}
		return appendStr(buf, b[i:ni]), ni, true
	}
	return "", i, false
}

// scanNumber validates a JSON number and returns the index just past it. The
// grammar is checked rather than assumed, because accepting `01` or `1.` here
// where the Decoder rejected them would make the two disagree on which lines
// fall back to rawRow.
func scanNumber(b []byte, i int) (int, bool) {
	start := i
	if i < len(b) && b[i] == '-' {
		i++
	}
	switch {
	case i < len(b) && b[i] == '0':
		i++
	case i < len(b) && b[i] >= '1' && b[i] <= '9':
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return i, false
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return i, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return i, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	return i, i > start
}

// scanComposite returns the index just past a balanced object or array. It is
// string-aware: a brace inside a string value does not close anything.
func scanComposite(b []byte, i int) (int, bool) {
	depth := 0
	for i < len(b) {
		switch b[i] {
		case '"':
			ni, ok := skipStringRaw(b, i)
			if !ok {
				return ni, false
			}
			i = ni
			continue
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1, true
			}
			if depth < 0 {
				return i, false
			}
		}
		i++
	}
	return i, false
}

// skipStringRaw advances past a quoted string without decoding it.
func skipStringRaw(b []byte, i int) (int, bool) {
	i++ // opening quote
	for i < len(b) {
		switch b[i] {
		case '\\':
			i += 2
			continue
		case '"':
			return i + 1, true
		}
		i++
	}
	return i, false
}

// appendStr copies s into *buf and returns a string over the copy.
func appendStr(buf *[]byte, s []byte) string {
	off := len(*buf)
	*buf = append(*buf, s...)
	return bufStr(buf, off)
}

// bufStr returns the bytes appended to *buf since off as a string aliasing the
// buffer.
//
// Call it AFTER the appends that make up the string, never before: the
// soundness rests on the bytes behind the returned string being final, not on
// the buffer never reallocating. If it does reallocate, this string points at
// the old array, which still holds the right bytes and is kept alive by this
// very pointer.
func bufStr(buf *[]byte, off int) string {
	if len(*buf) == off {
		return ""
	}
	return unsafe.String(&(*buf)[off], len(*buf)-off)
}

// looksLikeJSONObject reports whether a line could be a row: the cheap check
// mergeRows makes before treating a shard's line as data.
//
// It is deliberately structural rather than a full parse. mergeRows keeps lines
// as byte slices and only the coordinator-pipes path decodes them, so a full
// parse here would undo that; and the case this exists for -- a proxy's HTML
// error page, a plain-text error, an empty body -- is caught by the first byte.
// A line that starts with '{' and ends with '}' but is not valid JSON still
// reaches jsonLineToRow, which answers rawRow for it.
func looksLikeJSONObject(line []byte) bool {
	i := skipWS(line, 0)
	if i >= len(line) || line[i] != '{' {
		return false
	}
	for j := len(line) - 1; j >= i; j-- {
		switch line[j] {
		case ' ', '\t', '\r':
			continue
		case '}':
			return true
		}
		return false
	}
	return false
}

// truncateLine is a shard's line cut to n bytes for an error message.
func truncateLine(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
