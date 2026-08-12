package query

import (
	"regexp"
	"strconv"
	"strings"
)

// Additional LogsQL pipes closing the coverage gap with VictoriaLogs:
// unpack_logfmt, replace, replace_regexp, copy, len, drop_empty_fields.

// UnpackLogfmtPipe is `unpack_logfmt [from field] [prefix p]`: parse a logfmt
// (key=value) field (default _msg) and add its keys as fields.
type UnpackLogfmtPipe struct {
	From   string
	Prefix string
}

func (p *UnpackLogfmtPipe) apply(rows []Row) []Row {
	from := orDefault(p.From, "_msg")
	for ri := range rows {
		parseLogfmtPairs(rowField(rows[ri], from), func(k, v string) {
			setRowField(&rows[ri], p.Prefix+k, v)
		})
	}
	return rows
}

// parseLogfmtPairs tokenizes a logfmt line, calling emit per key=value.
func parseLogfmtPairs(line string, emit func(k, v string)) {
	i, n := 0, len(line)
	for i < n {
		for i < n && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		ks := i
		for i < n && line[i] != '=' && line[i] != ' ' {
			i++
		}
		key := line[ks:i]
		if key == "" {
			i++
			continue
		}
		val := ""
		if i < n && line[i] == '=' {
			i++
			if i < n && line[i] == '"' {
				i++
				vs := i
				for i < n && line[i] != '"' {
					i++
				}
				val = line[vs:i]
				if i < n {
					i++
				}
			} else {
				vs := i
				for i < n && line[i] != ' ' {
					i++
				}
				val = line[vs:i]
			}
		}
		emit(key, val)
	}
}

// ReplacePipe is `replace ("old","new") [at field]` (Regexp: replace_regexp,
// New is an RE2 replacement template).
type ReplacePipe struct {
	Field  string
	Old    string
	New    string
	Regexp bool
	re     *regexp.Regexp
}

func (p *ReplacePipe) apply(rows []Row) []Row {
	f := orDefault(p.Field, "_msg")
	if p.Regexp && p.re == nil {
		p.re = regexp.MustCompile(p.Old)
	}
	for ri := range rows {
		v := rowField(rows[ri], f)
		if p.Regexp {
			v = p.re.ReplaceAllString(v, p.New)
		} else {
			v = strings.ReplaceAll(v, p.Old, p.New)
		}
		setRowField(&rows[ri], f, v)
	}
	return rows
}

// CopyPipe is `copy src as dst, ...` -- duplicate fields under new names.
type CopyPipe struct{ From, To []string }

func (p *CopyPipe) apply(rows []Row) []Row {
	for ri := range rows {
		for i := range p.From {
			setRowField(&rows[ri], p.To[i], rowField(rows[ri], p.From[i]))
		}
	}
	return rows
}

// LenPipe is `len(field) as result` -- the byte length of a field.
type LenPipe struct{ Field, As string }

func (p *LenPipe) apply(rows []Row) []Row {
	as := orDefault(p.As, "len")
	for ri := range rows {
		setRowField(&rows[ri], as, strconv.Itoa(len(rowField(rows[ri], p.Field))))
	}
	return rows
}

// DropEmptyPipe is `drop_empty_fields` -- remove fields whose value is empty.
type DropEmptyPipe struct{}

func (p *DropEmptyPipe) apply(rows []Row) []Row {
	for ri := range rows {
		nf := rows[ri].Fields[:0]
		for _, f := range rows[ri].Fields {
			if f.Value != "" {
				nf = append(nf, f)
			}
		}
		rows[ri].Fields = nf
	}
	return rows
}
