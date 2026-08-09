package query

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSQL parses a small SQL SELECT subset by translating it to LogsQL and
// reusing the whole query engine -- so SQL-familiar users (and BI tools) can
// query logs, which VictoriaLogs cannot do. Supported:
//
//	SELECT * | col[, col...] | agg(...)[, ...]   (agg: count(*), sum/avg/min/max/count_uniq(field), quantile(p, field))
//	FROM <table>                                 (table name ignored -- one log stream)
//	WHERE cond [AND|OR cond ...]                 (=, !=, <>, <, >, <=, >=, LIKE '%x%')
//	GROUP BY field
//	ORDER BY field|alias [ASC|DESC]
//	LIMIT n
//
// Anything outside the subset is a clear error rather than a silent wrong
// answer.
func ParseSQL(sql string) (*Query, error) {
	lq, err := TranslateSQL(sql)
	if err != nil {
		return nil, err
	}
	return ParseLogsQL(lq)
}

// TranslateSQL turns the SQL subset into an equivalent LogsQL string.
func TranslateSQL(sql string) (string, error) {
	toks := sqlTokens(sql)
	p := &sqlParser{toks: toks}
	return p.translate()
}

type sqlParser struct {
	toks []string
	pos  int
}

func (p *sqlParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return ""
}
func (p *sqlParser) next() string {
	t := p.peek()
	if p.pos < len(p.toks) {
		p.pos++
	}
	return t
}
func (p *sqlParser) eatKW(kw string) bool {
	if strings.EqualFold(p.peek(), kw) {
		p.pos++
		return true
	}
	return false
}

func (p *sqlParser) translate() (string, error) {
	if !p.eatKW("SELECT") {
		return "", fmt.Errorf("simdlogs: SQL must start with SELECT")
	}
	sel, aggs, err := p.parseSelect()
	if err != nil {
		return "", err
	}
	if !p.eatKW("FROM") {
		return "", fmt.Errorf("simdlogs: SQL: expected FROM")
	}
	p.next() // table name, ignored

	var where string
	if p.eatKW("WHERE") {
		where, err = p.parseWhere()
		if err != nil {
			return "", err
		}
	}
	var groupBy []string
	if p.eatKW("GROUP") {
		if !p.eatKW("BY") {
			return "", fmt.Errorf("simdlogs: SQL: expected BY after GROUP")
		}
		groupBy = p.parseIdentList()
	}
	var orderBy string
	var orderDesc bool
	if p.eatKW("ORDER") {
		if !p.eatKW("BY") {
			return "", fmt.Errorf("simdlogs: SQL: expected BY after ORDER")
		}
		orderBy = p.orderTerm()
		if p.eatKW("DESC") {
			orderDesc = true
		} else {
			p.eatKW("ASC")
		}
	}
	limit := ""
	if p.eatKW("LIMIT") {
		n := p.next()
		if _, err := strconv.Atoi(n); err != nil {
			return "", fmt.Errorf("simdlogs: SQL: LIMIT wants a number, got %q", n)
		}
		limit = n
	}

	// Build the LogsQL string.
	head := where
	if head == "" {
		head = "*"
	}
	var b strings.Builder
	b.WriteString(head)
	if len(aggs) > 0 {
		b.WriteString(" | stats")
		if len(groupBy) > 0 {
			b.WriteString(" by (" + strings.Join(groupBy, ", ") + ")")
		}
		b.WriteString(" " + strings.Join(aggs, ", "))
	}
	if orderBy != "" {
		b.WriteString(" | sort by (" + orderBy + ")")
		if orderDesc {
			b.WriteString(" desc")
		}
	}
	if limit != "" {
		b.WriteString(" | limit " + limit)
	}
	if len(aggs) == 0 && len(sel) > 0 && sel[0] != "*" {
		b.WriteString(" | fields " + strings.Join(sel, ", "))
	}
	return b.String(), nil
}

// parseSelect reads the projection: plain columns and/or aggregates (which
// become LogsQL stats terms with a deterministic alias).
func (p *sqlParser) parseSelect() (cols, aggs []string, err error) {
	for {
		tok := p.next()
		if isAggFn(tok) && p.peek() == "(" {
			agg, e := p.parseAgg(tok)
			if e != nil {
				return nil, nil, e
			}
			aggs = append(aggs, agg)
		} else {
			cols = append(cols, tok)
		}
		if p.peek() == "," {
			p.next()
			continue
		}
		break
	}
	return cols, aggs, nil
}

// parseAgg turns count(*) / sum(x) / quantile(0.9, x) into a LogsQL stats term
// with a stable alias (so ORDER BY can reference it).
func (p *sqlParser) parseAgg(fn string) (string, error) {
	p.next() // (
	var args []string
	for p.peek() != ")" && p.peek() != "" {
		a := p.next()
		if a != "," {
			args = append(args, a)
		}
	}
	if p.peek() != ")" {
		return "", fmt.Errorf("simdlogs: SQL: unclosed %s(", fn)
	}
	p.next() // )
	lf := strings.ToLower(fn)
	alias := aggAlias(fn, args)
	switch lf {
	case "count":
		return "count() as " + alias, nil
	case "quantile":
		if len(args) != 2 {
			return "", fmt.Errorf("simdlogs: SQL: quantile(p, field)")
		}
		return fmt.Sprintf("quantile(%s, %s) as %s", args[0], args[1], alias), nil
	default:
		if len(args) != 1 {
			return "", fmt.Errorf("simdlogs: SQL: %s(field)", fn)
		}
		return fmt.Sprintf("%s(%s) as %s", lf, args[0], alias), nil
	}
}

// orderTerm reads an ORDER BY term, consuming an aggregate call and returning
// the alias parseAgg produced for it (so ORDER BY count(*) sorts the stats
// column).
func (p *sqlParser) orderTerm() string {
	tok := p.next()
	if isAggFn(tok) && p.peek() == "(" {
		p.next() // (
		var args []string
		for p.peek() != ")" && p.peek() != "" {
			a := p.next()
			if a != "," {
				args = append(args, a)
			}
		}
		if p.peek() == ")" {
			p.next()
		}
		return aggAlias(tok, args)
	}
	return tok
}

// aggAlias is the deterministic stats-column name for an aggregate, shared by
// SELECT and ORDER BY so they agree.
func aggAlias(fn string, args []string) string {
	lf := strings.ToLower(fn)
	switch lf {
	case "count":
		return "count"
	case "quantile":
		if len(args) == 2 {
			return "quantile_" + args[1]
		}
		return "quantile"
	default:
		if len(args) == 1 {
			return lf + "_" + args[0]
		}
		return lf
	}
}

// parseWhere turns SQL conditions into a LogsQL filter expression.
func (p *sqlParser) parseWhere() (string, error) {
	var b strings.Builder
	for {
		if isClauseKW(p.peek()) || p.peek() == "" {
			break
		}
		field := p.next()
		op := p.next()
		val := unquote(p.next())
		term, err := sqlTerm(field, op, val)
		if err != nil {
			return "", err
		}
		b.WriteString(term)
		kw := p.peek()
		if strings.EqualFold(kw, "AND") {
			p.next()
			b.WriteString(" and ")
		} else if strings.EqualFold(kw, "OR") {
			p.next()
			b.WriteString(" or ")
		} else {
			break
		}
	}
	return b.String(), nil
}

func sqlTerm(field, op, val string) (string, error) {
	switch op {
	case "=":
		return field + ":=" + quoteVal(val), nil
	case "!=", "<>":
		return "not " + field + ":=" + quoteVal(val), nil
	case ">":
		return field + ":>" + val, nil
	case ">=":
		return field + ":>=" + val, nil
	case "<":
		return field + ":<" + val, nil
	case "<=":
		return field + ":<=" + val, nil
	default:
		if strings.EqualFold(op, "LIKE") {
			return field + ":~" + quoteVal(strings.Trim(val, "%")), nil
		}
		return "", fmt.Errorf("simdlogs: SQL: unsupported operator %q", op)
	}
}

func (p *sqlParser) parseIdentList() []string {
	var out []string
	for {
		out = append(out, p.next())
		if p.peek() == "," {
			p.next()
			continue
		}
		break
	}
	return out
}

func isAggFn(t string) bool {
	switch strings.ToLower(t) {
	case "count", "sum", "avg", "min", "max", "count_uniq", "quantile":
		return true
	}
	return false
}

func isClauseKW(t string) bool {
	switch strings.ToUpper(t) {
	case "GROUP", "ORDER", "LIMIT", "":
		return true
	}
	return false
}

func quoteVal(v string) string {
	if strings.ContainsAny(v, ` "|(){}!~=<>:,`) {
		return `"` + strings.ReplaceAll(v, `"`, `\"`) + `"`
	}
	return v
}

func unquote(v string) string {
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return v[1 : len(v)-1]
	}
	return v
}

// sqlTokens splits SQL into tokens, keeping quoted strings and multi-char
// operators intact.
func sqlTokens(s string) []string {
	var toks []string
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'':
			j := i + 1
			for j < len(s) && s[j] != '\'' {
				j++
			}
			toks = append(toks, s[i:min(j+1, len(s))])
			i = j + 1
		case c == '(' || c == ')' || c == ',' || c == '*':
			toks = append(toks, string(c))
			i++
		case c == '=' || c == '<' || c == '>' || c == '!':
			j := i + 1
			if j < len(s) && (s[j] == '=' || s[j] == '>') {
				toks = append(toks, s[i:j+1])
				i = j + 1
			} else {
				toks = append(toks, string(c))
				i++
			}
		default:
			j := i
			for j < len(s) && !sqlDelim(s[j]) {
				j++
			}
			toks = append(toks, s[i:j])
			i = j
		}
	}
	return toks
}

func sqlDelim(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '(', ')', ',', '=', '<', '>', '!', '\'', '*':
		return true
	}
	return false
}
