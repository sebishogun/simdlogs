package query

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseLogsQL parses the LogsQL filter grammar into a Query:
//
//	AND (juxtaposition or the AND keyword), OR, NOT, and parentheses;
//	field:value            equality
//	field:="exact"         equality (explicit)
//	field:~"re"            regexp (metachars) or substring
//	field:>N >=N <N <=N     numeric comparison
//	field:in(a,b,c)        set membership
//	field:val*             prefix ; field:*sub* / *sub  substring
//	a bare word            substring on _msg
//
// A flat conjunction lowers to Query.Preds (the lean path the engine already
// optimizes); anything with OR/NOT/parentheses becomes a Query.Filter tree.
// Time comes from the request's start/end, not the query text. Empty or "*"
// matches everything.
func ParseLogsQL(s string) (*Query, error) {
	q := &Query{}
	s = strings.TrimSpace(s)
	if s == "" || s == "*" {
		return q, nil
	}
	toks, err := lexLogsQL(s)
	if err != nil {
		return nil, err
	}
	p := &lqlParser{toks: toks}
	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	pipes, err := p.parsePipes()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("simdlogs: unexpected %q in query", p.peek().val)
	}
	q.Pipes = pipes
	// A match-all filter (empty AND) leaves both Preds and Filter empty.
	if e.Op == OpAnd && len(e.Kids) == 0 {
		return q, nil
	}
	if preds, ok := flatConjunction(e); ok {
		q.Preds = preds
	} else {
		q.Filter = e
	}
	return q, nil
}

// flatConjunction returns the leaf predicates of an AND-only tree, or false
// if the tree contains OR/NOT (which needs the Filter representation).
func flatConjunction(e *Expr) ([]Pred, bool) {
	switch e.Op {
	case OpLeaf:
		return []Pred{e.Pred}, true
	case OpAnd:
		var out []Pred
		for _, k := range e.Kids {
			ps, ok := flatConjunction(k)
			if !ok {
				return nil, false
			}
			out = append(out, ps...)
		}
		return out, true
	default:
		return nil, false
	}
}

// ---- lexer ----

type tkind uint8

const (
	tEOF tkind = iota
	tIdent
	tString
	tColon
	tOp // = ~ > >= < <=
	tLParen
	tRParen
	tComma
	tPipe
	tAnd
	tOr
	tNot
)

type token struct {
	val  string
	kind tkind
}

func lexSpecial(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '(', ')', ',', ':', '=', '~', '>', '<', '"', '|':
		return true
	}
	return false
}

func lexLogsQL(s string) ([]token, error) {
	var out []token
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			out = append(out, token{"(", tLParen})
			i++
		case c == ')':
			out = append(out, token{")", tRParen})
			i++
		case c == ',':
			out = append(out, token{",", tComma})
			i++
		case c == '|':
			out = append(out, token{"|", tPipe})
			i++
		case c == ':':
			out = append(out, token{":", tColon})
			i++
		case c == '=' || c == '~':
			out = append(out, token{string(c), tOp})
			i++
		case c == '>' || c == '<':
			op := string(c)
			i++
			if i < len(s) && s[i] == '=' {
				op += "="
				i++
			}
			out = append(out, token{op, tOp})
		case c == '"':
			i++
			var sb strings.Builder
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
					sb.WriteByte(s[i])
					i++
					continue
				}
				sb.WriteByte(s[i])
				i++
			}
			if i >= len(s) {
				return nil, fmt.Errorf("simdlogs: unterminated string in query")
			}
			i++ // closing quote
			out = append(out, token{sb.String(), tString})
		default:
			start := i
			for i < len(s) && !lexSpecial(s[i]) {
				i++
			}
			w := s[start:i]
			switch strings.ToLower(w) {
			case "and":
				out = append(out, token{w, tAnd})
			case "or":
				out = append(out, token{w, tOr})
			case "not":
				out = append(out, token{w, tNot})
			default:
				out = append(out, token{w, tIdent})
			}
		}
	}
	return append(out, token{"", tEOF}), nil
}

// ---- parser ----

type lqlParser struct {
	toks []token
	pos  int
}

func (p *lqlParser) peek() token { return p.toks[p.pos] }
func (p *lqlParser) peekAt(n int) token {
	if p.pos+n >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[p.pos+n]
}
func (p *lqlParser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func startsTerm(k tkind) bool {
	return k == tIdent || k == tString || k == tNot || k == tLParen
}

func (p *lqlParser) parseOr() (*Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	kids := []*Expr{left}
	for p.peek().kind == tOr {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		kids = append(kids, r)
	}
	if len(kids) == 1 {
		return kids[0], nil
	}
	return &Expr{Op: OpOr, Kids: kids}, nil
}

func (p *lqlParser) parseAnd() (*Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	kids := []*Expr{left}
	for {
		if p.peek().kind == tAnd {
			p.next()
		}
		if !startsTerm(p.peek().kind) {
			break
		}
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		kids = append(kids, r)
	}
	if len(kids) == 1 {
		return kids[0], nil
	}
	return &Expr{Op: OpAnd, Kids: kids}, nil
}

func (p *lqlParser) parseUnary() (*Expr, error) {
	switch p.peek().kind {
	case tNot:
		p.next()
		c, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &Expr{Op: OpNot, Child: c}, nil
	case tLParen:
		p.next()
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tRParen {
			return nil, fmt.Errorf("simdlogs: expected )")
		}
		p.next()
		return e, nil
	default:
		return p.parseTerm()
	}
}

func (p *lqlParser) parseTerm() (*Expr, error) {
	t := p.next()
	if t.kind != tIdent && t.kind != tString {
		return nil, fmt.Errorf("simdlogs: unexpected %q", t.val)
	}
	if p.peek().kind != tColon {
		if t.val == "*" {
			return &Expr{Op: OpAnd}, nil // match all (empty conjunction)
		}
		// bare word -> substring on _msg
		return &Expr{Op: OpLeaf, Pred: Pred{Field: "_msg", Kind: Contains, Value: t.val}}, nil
	}
	p.next() // consume ':'
	pred, err := p.parseMatcher(t.val)
	if err != nil {
		return nil, err
	}
	return &Expr{Op: OpLeaf, Pred: pred}, nil
}

func (p *lqlParser) parseMatcher(field string) (Pred, error) {
	t := p.peek()
	switch {
	case t.kind == tOp && t.val == "=":
		p.next()
		v, err := p.value()
		return Pred{Field: field, Kind: Eq, Value: v}, err
	case t.kind == tOp && t.val == "~":
		p.next()
		v, err := p.value()
		if err != nil {
			return Pred{}, err
		}
		if strings.ContainsAny(v, `.*+?()[]{}|^$\`) {
			return Pred{Field: field, Kind: Regexp, Value: v}, nil
		}
		return Pred{Field: field, Kind: Contains, Value: v}, nil
	case t.kind == tOp:
		p.next()
		nv := p.next()
		f, err := strconv.ParseFloat(nv.val, 64)
		if err != nil {
			return Pred{}, fmt.Errorf("simdlogs: %s%s: want number, got %q", field, t.val, nv.val)
		}
		var k PredKind
		switch t.val {
		case ">":
			k = Gt
		case ">=":
			k = Ge
		case "<":
			k = Lt
		case "<=":
			k = Le
		}
		return Pred{Field: field, Kind: k, Num: f}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "in") && p.peekAt(1).kind == tLParen:
		p.next() // in
		p.next() // (
		var vals []string
		for p.peek().kind != tRParen && p.peek().kind != tEOF {
			vt := p.next()
			if vt.kind != tIdent && vt.kind != tString {
				return Pred{}, fmt.Errorf("simdlogs: bad in() value %q", vt.val)
			}
			vals = append(vals, vt.val)
			if p.peek().kind == tComma {
				p.next()
			}
		}
		if p.peek().kind != tRParen {
			return Pred{}, fmt.Errorf("simdlogs: expected ) in in()")
		}
		p.next() // )
		return Pred{Field: field, Kind: In, Values: vals}, nil
	default:
		v, err := p.value()
		if err != nil {
			return Pred{}, err
		}
		switch {
		case strings.HasPrefix(v, "*"):
			return Pred{Field: field, Kind: Contains, Value: strings.Trim(v, "*")}, nil
		case strings.HasSuffix(v, "*"):
			return Pred{Field: field, Kind: Prefix, Value: strings.TrimSuffix(v, "*")}, nil
		default:
			return Pred{Field: field, Kind: Eq, Value: v}, nil
		}
	}
}

func (p *lqlParser) value() (string, error) {
	t := p.next()
	if t.kind != tIdent && t.kind != tString {
		return "", fmt.Errorf("simdlogs: expected a value, got %q", t.val)
	}
	return t.val, nil
}

// ---- pipes ----

func (p *lqlParser) parsePipes() ([]Pipe, error) {
	var pipes []Pipe
	for p.peek().kind == tPipe {
		p.next()
		name := p.next()
		if name.kind != tIdent {
			return nil, fmt.Errorf("simdlogs: expected a pipe name, got %q", name.val)
		}
		switch strings.ToLower(name.val) {
		case "stats":
			sp, err := p.parseStats()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, sp)
		case "sort":
			sp, err := p.parseSort()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, sp)
		case "limit", "head":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &LimitPipe{N: n})
		case "fields":
			fs, err := p.parseBareFieldList()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &FieldsPipe{Keep: fs})
		case "uniq":
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "by") {
				p.next()
			}
			fs, err := p.parseFieldGroup()
			if err != nil {
				return nil, err
			}
			up := &UniqPipe{By: fs}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "limit") {
				p.next()
				n, err := p.intArg()
				if err != nil {
					return nil, err
				}
				up.Limit = n
			}
			pipes = append(pipes, up)
		case "top":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			fs, err := p.parseFieldGroup()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &TopPipe{N: n, By: fs})
		case "tail":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &TailPipe{N: n})
		case "offset":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &OffsetPipe{N: n})
		case "rename":
			rp, err := p.parseRename()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, rp)
		case "delete", "drop":
			fs, err := p.parseBareFieldList()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &DeletePipe{Drop: fs})
		default:
			return nil, fmt.Errorf("simdlogs: unknown pipe %q", name.val)
		}
	}
	return pipes, nil
}

func (p *lqlParser) parseStats() (*StatsPipe, error) {
	sp := &StatsPipe{}
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "by") {
		p.next()
		fs, err := p.parseFieldGroup()
		if err != nil {
			return nil, err
		}
		sp.By = fs
	}
	for {
		a, err := p.parseAgg()
		if err != nil {
			return nil, err
		}
		sp.Aggs = append(sp.Aggs, a)
		if p.peek().kind == tComma {
			p.next()
			continue
		}
		break
	}
	if len(sp.Aggs) == 0 {
		return nil, fmt.Errorf("simdlogs: stats needs at least one aggregation")
	}
	return sp, nil
}

func (p *lqlParser) parseAgg() (Agg, error) {
	fn := p.next()
	if fn.kind != tIdent {
		return Agg{}, fmt.Errorf("simdlogs: expected an aggregation, got %q", fn.val)
	}
	if p.peek().kind != tLParen {
		return Agg{}, fmt.Errorf("simdlogs: expected ( after %s", fn.val)
	}
	p.next() // (
	field := ""
	if k := p.peek().kind; k == tIdent || k == tString {
		field = p.next().val
	}
	if p.peek().kind != tRParen {
		return Agg{}, fmt.Errorf("simdlogs: expected ) in %s()", fn.val)
	}
	p.next() // )
	kind, ok := aggKind(fn.val)
	if !ok {
		return Agg{}, fmt.Errorf("simdlogs: unknown aggregation %q", fn.val)
	}
	a := Agg{Field: field, Kind: kind}
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
		p.next()
		al := p.next()
		if al.kind != tIdent && al.kind != tString {
			return Agg{}, fmt.Errorf("simdlogs: expected an alias after 'as'")
		}
		a.Alias = al.val
	}
	if a.Alias == "" {
		if field == "" {
			a.Alias = strings.ToLower(fn.val)
		} else {
			a.Alias = strings.ToLower(fn.val) + "(" + field + ")"
		}
	}
	return a, nil
}

func (p *lqlParser) parseSort() (*SortPipe, error) {
	sp := &SortPipe{}
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "by") {
		p.next()
		fs, err := p.parseFieldGroup()
		if err != nil {
			return nil, err
		}
		sp.By = fs
	}
	for p.peek().kind == tIdent {
		switch strings.ToLower(p.peek().val) {
		case "desc":
			p.next()
			sp.Desc = true
		case "asc":
			p.next()
		case "limit":
			p.next()
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			sp.Limit = n
		default:
			return sp, nil
		}
	}
	return sp, nil
}

// parseFieldGroup reads `(a, b, c)` or a single bare field.
func (p *lqlParser) parseFieldGroup() ([]string, error) {
	if p.peek().kind != tLParen {
		if k := p.peek().kind; k == tIdent || k == tString {
			return []string{p.next().val}, nil
		}
		return nil, fmt.Errorf("simdlogs: expected ( or a field")
	}
	p.next() // (
	var fs []string
	for p.peek().kind != tRParen && p.peek().kind != tEOF {
		f := p.next()
		if f.kind != tIdent && f.kind != tString {
			return nil, fmt.Errorf("simdlogs: bad field %q", f.val)
		}
		fs = append(fs, f.val)
		if p.peek().kind == tComma {
			p.next()
		}
	}
	if p.peek().kind != tRParen {
		return nil, fmt.Errorf("simdlogs: expected )")
	}
	p.next()
	return fs, nil
}

// parseBareFieldList reads `a, b, c` with no parentheses (the fields pipe).
func (p *lqlParser) parseBareFieldList() ([]string, error) {
	var fs []string
	for {
		f := p.next()
		if f.kind != tIdent && f.kind != tString {
			return nil, fmt.Errorf("simdlogs: bad field %q", f.val)
		}
		fs = append(fs, f.val)
		if p.peek().kind == tComma {
			p.next()
			continue
		}
		break
	}
	return fs, nil
}

func (p *lqlParser) parseRename() (*RenamePipe, error) {
	rp := &RenamePipe{}
	for {
		from := p.next()
		if from.kind != tIdent && from.kind != tString {
			return nil, fmt.Errorf("simdlogs: rename: expected a field, got %q", from.val)
		}
		as := p.next()
		if !strings.EqualFold(as.val, "as") {
			return nil, fmt.Errorf("simdlogs: rename: expected 'as', got %q", as.val)
		}
		to := p.next()
		if to.kind != tIdent && to.kind != tString {
			return nil, fmt.Errorf("simdlogs: rename: expected a name, got %q", to.val)
		}
		rp.From = append(rp.From, from.val)
		rp.To = append(rp.To, to.val)
		if p.peek().kind == tComma {
			p.next()
			continue
		}
		break
	}
	return rp, nil
}

func (p *lqlParser) intArg() (int, error) {
	t := p.next()
	n, err := strconv.Atoi(t.val)
	if err != nil {
		return 0, fmt.Errorf("simdlogs: expected a number, got %q", t.val)
	}
	return n, nil
}

func aggKind(name string) (AggKind, bool) {
	switch strings.ToLower(name) {
	case "count":
		return AggCount, true
	case "sum":
		return AggSum, true
	case "avg":
		return AggAvg, true
	case "min":
		return AggMin, true
	case "max":
		return AggMax, true
	case "uniq":
		return AggUniq, true
	case "count_uniq":
		return AggCountUniq, true
	}
	return 0, false
}
