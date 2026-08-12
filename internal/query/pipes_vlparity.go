package query

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

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
