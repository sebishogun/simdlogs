package query

import (
	"fmt"
	"strconv"
	"strings"
)

// The math pipe: `math "EXPR" as field`. EXPR is a small arithmetic language
// (+ - * / and parens over field names and numbers), parsed once here rather
// than through the LogsQL lexer -- so operators like * and - never collide
// with the glob/negation meanings they carry in the filter grammar. A field
// that is not numeric reads as 0; division by zero yields 0.

type mathNode interface{ eval(r Row) float64 }

type mathNum float64

func (n mathNum) eval(Row) float64 { return float64(n) }

type mathVar string

func (v mathVar) eval(r Row) float64 {
	f, _ := strconv.ParseFloat(rowField(r, string(v)), 64)
	return f
}

type mathBin struct {
	op   byte
	l, r mathNode
}

func (b mathBin) eval(r Row) float64 {
	l, rr := b.l.eval(r), b.r.eval(r)
	switch b.op {
	case '+':
		return l + rr
	case '-':
		return l - rr
	case '*':
		return l * rr
	case '/':
		if rr == 0 {
			return 0
		}
		return l / rr
	}
	return 0
}

// MathPipe computes expr per row and writes it to As.
type MathPipe struct {
	As     string
	expr   mathNode
	fields []string // field names the expression reads (for materialization)
}

func (p *MathPipe) apply(rows []Row) []Row {
	for ri := range rows {
		v := p.expr.eval(rows[ri])
		setRowField(&rows[ri], p.As, strconv.FormatFloat(v, 'g', -1, 64))
	}
	return rows
}

// parseMath parses EXPR into an evaluable node and the field names it reads.
func parseMath(s string) (mathNode, []string, error) {
	mp := &mathParser{s: s}
	n, err := mp.expr()
	if err != nil {
		return nil, nil, err
	}
	mp.ws()
	if mp.i < len(mp.s) {
		return nil, nil, fmt.Errorf("simdlogs: math: unexpected %q", mp.s[mp.i:])
	}
	return n, mathVars(n), nil
}

type mathParser struct {
	s string
	i int
}

func (p *mathParser) ws() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

func (p *mathParser) expr() (mathNode, error) {
	n, err := p.term()
	if err != nil {
		return nil, err
	}
	for {
		p.ws()
		if p.i >= len(p.s) || (p.s[p.i] != '+' && p.s[p.i] != '-') {
			break
		}
		op := p.s[p.i]
		p.i++
		r, err := p.term()
		if err != nil {
			return nil, err
		}
		n = mathBin{op, n, r}
	}
	return n, nil
}

func (p *mathParser) term() (mathNode, error) {
	n, err := p.factor()
	if err != nil {
		return nil, err
	}
	for {
		p.ws()
		if p.i >= len(p.s) || (p.s[p.i] != '*' && p.s[p.i] != '/') {
			break
		}
		op := p.s[p.i]
		p.i++
		r, err := p.factor()
		if err != nil {
			return nil, err
		}
		n = mathBin{op, n, r}
	}
	return n, nil
}

func (p *mathParser) factor() (mathNode, error) {
	p.ws()
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("simdlogs: math: unexpected end of expression")
	}
	switch c := p.s[p.i]; {
	case c == '(':
		p.i++
		n, err := p.expr()
		if err != nil {
			return nil, err
		}
		p.ws()
		if p.i >= len(p.s) || p.s[p.i] != ')' {
			return nil, fmt.Errorf("simdlogs: math: expected )")
		}
		p.i++
		return n, nil
	case c == '-': // unary minus
		p.i++
		n, err := p.factor()
		if err != nil {
			return nil, err
		}
		return mathBin{'-', mathNum(0), n}, nil
	default:
		start := p.i
		for p.i < len(p.s) && !strings.ContainsRune(" \t+-*/()", rune(p.s[p.i])) {
			p.i++
		}
		tok := p.s[start:p.i]
		if tok == "" {
			return nil, fmt.Errorf("simdlogs: math: unexpected %q", string(c))
		}
		if f, err := strconv.ParseFloat(tok, 64); err == nil {
			return mathNum(f), nil
		}
		return mathVar(tok), nil
	}
}

func mathVars(n mathNode) []string {
	switch t := n.(type) {
	case mathVar:
		return []string{string(t)}
	case mathBin:
		return append(mathVars(t.l), mathVars(t.r)...)
	}
	return nil
}
