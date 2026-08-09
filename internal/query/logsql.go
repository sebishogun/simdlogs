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
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("simdlogs: unexpected %q in query", p.peek().val)
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
	case ' ', '\t', '\n', '\r', '(', ')', ',', ':', '=', '~', '>', '<', '"':
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
