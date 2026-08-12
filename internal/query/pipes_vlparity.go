package query

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// HashPipe is `hash(field) as result` -- a stable 64-bit FNV-1a hash of the
// field value, hex-encoded (for grouping or anonymizing a high-cardinality key).
type HashPipe struct{ Field, As string }

func (p *HashPipe) apply(rows []Row) []Row {
	as := orDefault(p.As, "hash")
	for ri := range rows {
		h := fnv.New64a()
		h.Write([]byte(rowField(rows[ri], p.Field)))
		setRowField(&rows[ri], as, strconv.FormatUint(h.Sum64(), 16))
	}
	return rows
}

// More LogsQL pipes for VictoriaLogs parity: extract_regexp, decolorize,
// pack_json, pack_logfmt, sample. Each is a row transform in the same shape as
// pipes_more.go (apply mutates the row stream in place).

// ExtractRegexpPipe is `extract_regexp "re" [from field]`: RE2 with named
// capture groups (?P<name>...), each group set as a field. Default field _msg.
type ExtractRegexpPipe struct {
	From string
	re   *regexp.Regexp
}

// newExtractRegexp compiles the pattern at parse time so a bad regex is a parse
// error, not an apply-time panic.
func newExtractRegexp(pattern, from string) (*ExtractRegexpPipe, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return &ExtractRegexpPipe{From: from, re: re}, nil
}

func (p *ExtractRegexpPipe) apply(rows []Row) []Row {
	from := orDefault(p.From, "_msg")
	names := p.re.SubexpNames()
	for ri := range rows {
		m := p.re.FindStringSubmatch(rowField(rows[ri], from))
		if m == nil {
			continue
		}
		for gi, name := range names {
			if gi == 0 || name == "" {
				continue
			}
			setRowField(&rows[ri], name, m[gi])
		}
	}
	return rows
}

// ansiRe matches ANSI/VT100 escape sequences (CSI ... final byte).
var ansiRe = regexp.MustCompile("\x1b\\[[0-9;?]*[ -/]*[@-~]")

// DecolorizePipe is `decolorize [field]` -- strip ANSI color codes. Default _msg.
type DecolorizePipe struct{ Field string }

func (p *DecolorizePipe) apply(rows []Row) []Row {
	f := orDefault(p.Field, "_msg")
	for ri := range rows {
		v := rowField(rows[ri], f)
		if strings.IndexByte(v, 0x1b) >= 0 {
			setRowField(&rows[ri], f, ansiRe.ReplaceAllString(v, ""))
		}
	}
	return rows
}

// PackPipe is `pack_json [fields (a,b)] [as dst]` / `pack_logfmt ...`: pack the
// listed fields (or all fields) into dst (default _msg) as JSON or logfmt.
type PackPipe struct {
	Fields []string
	As     string
	Logfmt bool
}

func (p *PackPipe) apply(rows []Row) []Row {
	as := orDefault(p.As, "_msg")
	for ri := range rows {
		var packed string
		if p.Logfmt {
			packed = packLogfmt(rows[ri], p.Fields)
		} else {
			packed = packJSON(rows[ri], p.Fields)
		}
		setRowField(&rows[ri], as, packed)
	}
	return rows
}

// packJSON renders the selected fields (or all, in row order) as a JSON object.
func packJSON(r Row, keys []string) string {
	var b strings.Builder
	b.WriteByte('{')
	first := true
	emit := func(k, v string) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		vb, _ := json.Marshal(v)
		b.Write(vb)
	}
	if len(keys) == 0 {
		for _, f := range r.Fields {
			emit(f.Key, f.Value)
		}
	} else {
		for _, k := range keys {
			emit(k, rowField(r, k))
		}
	}
	b.WriteByte('}')
	return b.String()
}

// packLogfmt renders the selected fields (or all) as logfmt key=value pairs,
// quoting a value that contains a space, '=' or '"'.
func packLogfmt(r Row, keys []string) string {
	var parts []string
	emit := func(k, v string) {
		if strings.ContainsAny(v, " =\"") {
			v = strconv.Quote(v)
		}
		parts = append(parts, k+"="+v)
	}
	if len(keys) == 0 {
		for _, f := range r.Fields {
			emit(f.Key, f.Value)
		}
	} else {
		for _, k := range keys {
			emit(k, rowField(r, k))
		}
	}
	return strings.Join(parts, " ")
}

// UnrollPipe is `unroll (field)` -- explode a field holding a JSON array (or a
// whitespace-separated list) into one row per element, the other fields copied.
type UnrollPipe struct{ Field string }

func (p *UnrollPipe) apply(rows []Row) []Row {
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		elems := unrollValues(rowField(r, p.Field))
		if len(elems) == 0 {
			out = append(out, r) // nothing to unroll: pass the row through unchanged
			continue
		}
		for _, e := range elems {
			nr := cloneRow(r)
			setRowField(&nr, p.Field, e)
			out = append(out, nr)
		}
	}
	return out
}

// unrollValues splits a field value into elements: a JSON array yields its
// elements, otherwise the value is whitespace-split (empty yields none).
func unrollValues(v string) []string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "[") {
		var arr []any
		if json.Unmarshal([]byte(v), &arr) == nil {
			out := make([]string, len(arr))
			for i, e := range arr {
				out[i] = fmt.Sprint(e)
			}
			return out
		}
	}
	if v == "" {
		return nil
	}
	return strings.Fields(v)
}

func cloneRow(r Row) Row {
	f := make([]Field, len(r.Fields))
	copy(f, r.Fields)
	return Row{Time: r.Time, Fields: f}
}

// UnpackSyslogPipe is `unpack_syslog [from field]` -- parse an RFC5424 or
// RFC3164 syslog line (default _msg) into fields: priority, facility, severity,
// timestamp, hostname, app_name, proc_id, msg_id, message.
type UnpackSyslogPipe struct{ From string }

func (p *UnpackSyslogPipe) apply(rows []Row) []Row {
	from := orDefault(p.From, "_msg")
	for ri := range rows {
		parseSyslog(rowField(rows[ri], from), func(k, v string) {
			setRowField(&rows[ri], k, v)
		})
	}
	return rows
}

// parseSyslog decodes a syslog line, emitting each parsed field. It handles the
// RFC5424 header (<PRI>1 TS HOST APP PROCID MSGID SD MSG) and the RFC3164
// header (<PRI>Mon DD HH:MM:SS HOST TAG: MSG); the priority yields facility and
// severity. Best-effort: a line it cannot parse emits only what it recognizes.
func parseSyslog(line string, emit func(k, v string)) {
	s := strings.TrimSpace(line)
	if !strings.HasPrefix(s, "<") {
		return
	}
	end := strings.IndexByte(s, '>')
	if end < 0 {
		return
	}
	pri, err := strconv.Atoi(s[1:end])
	if err != nil {
		return
	}
	emit("priority", strconv.Itoa(pri))
	emit("facility", strconv.Itoa(pri/8))
	emit("severity", strconv.Itoa(pri%8))
	s = s[end+1:]

	if strings.HasPrefix(s, "1 ") { // RFC5424
		f := strings.SplitN(s, " ", 7)
		if len(f) < 7 {
			return
		}
		emitNonNil(emit, "timestamp", f[1])
		emitNonNil(emit, "hostname", f[2])
		emitNonNil(emit, "app_name", f[3])
		emitNonNil(emit, "proc_id", f[4])
		emitNonNil(emit, "msg_id", f[5])
		rest := f[6]
		if strings.HasPrefix(rest, "- ") { // no structured data
			emit("message", rest[2:])
		} else if rest == "-" {
			emit("message", "")
		} else if strings.HasPrefix(rest, "[") { // skip the structured-data block
			if i := strings.Index(rest, "] "); i >= 0 {
				emit("message", rest[i+2:])
			} else {
				emit("message", rest)
			}
		} else {
			emit("message", rest)
		}
		return
	}
	// RFC3164: Mon DD HH:MM:SS HOST TAG: MSG
	f := strings.Fields(s)
	if len(f) < 5 {
		return
	}
	emit("timestamp", f[0]+" "+f[1]+" "+f[2])
	emit("hostname", f[3])
	tag := f[4]
	if b := strings.IndexByte(tag, '['); b >= 0 { // strip [pid]
		tag = tag[:b]
	}
	emit("app_name", strings.TrimSuffix(tag, ":"))
	if len(f) > 5 { // the message is everything after the TAG token
		emit("message", strings.Join(f[5:], " "))
	}
}

func emitNonNil(emit func(k, v string), k, v string) {
	if v != "-" && v != "" {
		emit(k, v)
	}
}

// JSONArrayLenPipe is `json_array_len(field) as result` -- the element count of
// a JSON-array field (0 if the field is not a JSON array).
type JSONArrayLenPipe struct{ Field, As string }

func (p *JSONArrayLenPipe) apply(rows []Row) []Row {
	as := orDefault(p.As, "json_array_len")
	for ri := range rows {
		n := 0
		var arr []json.RawMessage
		if json.Unmarshal([]byte(rowField(rows[ri], p.Field)), &arr) == nil {
			n = len(arr)
		}
		setRowField(&rows[ri], as, strconv.Itoa(n))
	}
	return rows
}

// UnpackWordsPipe is `unpack_words [from field] [as result]` -- the sorted set
// of distinct words in a field, as a JSON array. Default field _msg, result words.
type UnpackWordsPipe struct{ From, As string }

func (p *UnpackWordsPipe) apply(rows []Row) []Row {
	from := orDefault(p.From, "_msg")
	as := orDefault(p.As, "words")
	for ri := range rows {
		setRowField(&rows[ri], as, jsonStrArray(distinctWords(rowField(rows[ri], from))))
	}
	return rows
}

// distinctWords splits on non-alphanumeric runs and returns the sorted distinct
// words.
func distinctWords(s string) []string {
	seen := map[string]struct{}{}
	var words []string
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_')
	})
	for _, w := range fields {
		if _, ok := seen[w]; !ok {
			seen[w] = struct{}{}
			words = append(words, w)
		}
	}
	sort.Strings(words)
	return words
}

// SamplePipe is `sample N` -- keep every Nth row (deterministic 1/N sampling).
type SamplePipe struct{ N int }

func (p *SamplePipe) apply(rows []Row) []Row {
	if p.N <= 1 {
		return rows
	}
	out := rows[:0]
	for i, r := range rows {
		if i%p.N == 0 {
			out = append(out, r)
		}
	}
	return out
}
