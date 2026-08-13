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
//	_stream:{k="v",k=~re}  stream selector (= != =~ !~ over label fields)
//	!term                  NOT (also the NOT keyword)
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
	tLBrace  // { -- opens a _stream selector
	tRBrace  // }
	tTimeVal // the raw value of a _time:<expr> filter, captured whole
)

type token struct {
	val  string
	kind tkind
}

func lexSpecial(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '(', ')', ',', ':', '=', '~', '>', '<', '"', '|', '{', '}', '!':
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
		case c == '{':
			out = append(out, token{"{", tLBrace})
			i++
		case c == '}':
			out = append(out, token{"}", tRBrace})
			i++
		case c == '=':
			i++
			if i < len(s) && s[i] == '~' { // =~  regexp match
				out = append(out, token{"=~", tOp})
				i++
			} else {
				out = append(out, token{"=", tOp})
			}
		case c == '~':
			out = append(out, token{"~", tOp})
			i++
		case c == '!':
			i++
			switch {
			case i < len(s) && s[i] == '=': // !=  not-equal
				out = append(out, token{"!=", tOp})
				i++
			case i < len(s) && s[i] == '~': // !~  regexp not-match
				out = append(out, token{"!~", tOp})
				i++
			default: // bare ! is a NOT prefix (e.g. !error)
				out = append(out, token{"!", tNot})
			}
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
			// _time:<expr> -- the time value can contain ':' (HH:MM, RFC3339),
			// brackets and commas, which the generic lexer would split. Capture
			// the whole expression as one tTimeVal token: from just past the
			// colon to the next top-level whitespace/pipe (commas inside [] stay).
			if w == "_time" && i < len(s) && s[i] == ':' {
				out = append(out, token{w, tIdent}, token{":", tColon})
				i++ // past ':'
				vs := i
				depth := 0
				for i < len(s) {
					ch := s[i]
					if ch == '[' || ch == '(' {
						depth++
					} else if ch == ']' || ch == ')' {
						depth--
					} else if depth <= 0 && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '|' || ch == ')') {
						break
					}
					i++
				}
				out = append(out, token{s[vs:i], tTimeVal})
				continue
			}
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
	if strings.EqualFold(t.val, "_stream") && p.peek().kind == tLBrace {
		return p.parseStreamSelector()
	}
	pred, err := p.parseMatcher(t.val)
	if err != nil {
		return nil, err
	}
	return &Expr{Op: OpLeaf, Pred: pred}, nil
}

// parseStreamSelector parses `_stream:{label=val, label=~re, label!=val, ...}`
// -- VictoriaLogs' stream selector -- into an AND of predicates over the
// underlying label fields (which are ordinary stored fields here). = and =~ are
// equality/regexp; != and !~ wrap them in NOT. An empty selector matches all.
func (p *lqlParser) parseStreamSelector() (*Expr, error) {
	p.next() // {
	var kids []*Expr
	for p.peek().kind != tRBrace && p.peek().kind != tEOF {
		label := p.next()
		if label.kind != tIdent && label.kind != tString {
			return nil, fmt.Errorf("simdlogs: stream selector: expected a label, got %q", label.val)
		}
		op := p.next()
		if op.kind != tOp {
			return nil, fmt.Errorf("simdlogs: stream selector: expected an operator after %q, got %q", label.val, op.val)
		}
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		leaf := &Expr{Op: OpLeaf, Pred: Pred{Field: label.val, Value: val}}
		switch op.val {
		case "=":
			leaf.Pred.Kind = Eq
		case "=~":
			leaf.Pred.Kind = Regexp
		case "!=":
			leaf.Pred.Kind = Eq
			leaf = &Expr{Op: OpNot, Child: leaf}
		case "!~":
			leaf.Pred.Kind = Regexp
			leaf = &Expr{Op: OpNot, Child: leaf}
		default:
			return nil, fmt.Errorf("simdlogs: stream selector: bad operator %q (use = != =~ !~)", op.val)
		}
		kids = append(kids, leaf)
		if p.peek().kind == tComma {
			p.next()
		}
	}
	if p.peek().kind != tRBrace {
		return nil, fmt.Errorf("simdlogs: stream selector: expected }")
	}
	p.next() // }
	if len(kids) == 1 {
		return kids[0], nil
	}
	return &Expr{Op: OpAnd, Kids: kids}, nil
}

func (p *lqlParser) parseMatcher(field string) (Pred, error) {
	t := p.peek()
	if t.kind == tTimeVal { // _time:<expr>, captured whole by the lexer
		p.next()
		return parseTimeExpr(t.val)
	}
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
	case t.kind == tIdent && strings.EqualFold(t.val, "range") && p.peekAt(1).kind == tLParen:
		lo, hi, err := p.twoNumArgs("range")
		if err != nil {
			return Pred{}, err
		}
		return Pred{Field: field, Kind: RangeNum, Num: lo, Num2: hi}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "len_range") && p.peekAt(1).kind == tLParen:
		lo, hi, err := p.twoNumArgs("len_range")
		if err != nil {
			return Pred{}, err
		}
		return Pred{Field: field, Kind: LenRange, Num: lo, Num2: hi}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "string_range") && p.peekAt(1).kind == tLParen:
		lo, hi, err := p.twoStrArgs("string_range")
		if err != nil {
			return Pred{}, err
		}
		return Pred{Field: field, Kind: StringRange, Value: lo, Value2: hi}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "i") && p.peekAt(1).kind == tLParen:
		p.next() // i
		p.next() // (
		v, err := p.value()
		if err != nil {
			return Pred{}, err
		}
		if p.peek().kind != tRParen {
			return Pred{}, fmt.Errorf("simdlogs: expected ) in i()")
		}
		p.next() // )
		return Pred{Field: field, Kind: IContains, Value: v}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "seq") && p.peekAt(1).kind == tLParen:
		vals, err := p.parenValueList("seq")
		if err != nil {
			return Pred{}, err
		}
		return Pred{Field: field, Kind: Seq, Values: vals}, nil
	case t.kind == tIdent && strings.EqualFold(t.val, "ipv4_range") && p.peekAt(1).kind == tLParen:
		lo, hi, err := p.twoStrArgs("ipv4_range")
		if err != nil {
			return Pred{}, err
		}
		loN, ok1 := ipToU32(lo)
		hiN, ok2 := ipToU32(hi)
		if !ok1 || !ok2 {
			return Pred{}, fmt.Errorf("simdlogs: ipv4_range: bad IPv4 %q or %q", lo, hi)
		}
		return Pred{Field: field, Kind: IPv4Range, Num: float64(loN), Num2: float64(hiN)}, nil
	case t.kind == tIdent && fieldCmpKind(t.val) != 0 && p.peekAt(1).kind == tLParen:
		kind := fieldCmpKind(t.val)
		p.next() // name
		p.next() // (
		f2, err := p.value()
		if err != nil {
			return Pred{}, err
		}
		if p.peek().kind != tRParen {
			return Pred{}, fmt.Errorf("simdlogs: expected ) in %s()", t.val)
		}
		p.next() // )
		return Pred{Field: field, Kind: kind, Field2: f2}, nil
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

// twoNumArgs parses `name(lo, hi)` as two float64s (the peeked ident is name).
func (p *lqlParser) twoNumArgs(name string) (float64, float64, error) {
	p.next() // name
	p.next() // (
	a := p.next()
	lo, err := strconv.ParseFloat(a.val, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("simdlogs: %s: want number, got %q", name, a.val)
	}
	if p.peek().kind == tComma {
		p.next()
	}
	b := p.next()
	hi, err := strconv.ParseFloat(b.val, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("simdlogs: %s: want number, got %q", name, b.val)
	}
	if p.peek().kind != tRParen {
		return 0, 0, fmt.Errorf("simdlogs: expected ) in %s()", name)
	}
	p.next() // )
	return lo, hi, nil
}

// fieldCmpKind maps a *_field function name to its PredKind, or 0 if none.
func fieldCmpKind(name string) PredKind {
	switch strings.ToLower(name) {
	case "eq_field":
		return EqField
	case "ne_field":
		return NeField
	case "lt_field":
		return LtField
	case "le_field":
		return LeField
	case "gt_field":
		return GtField
	case "ge_field":
		return GeField
	}
	return 0
}

// parenValueList parses `name(v1, v2, ...)` as a list of string values (the
// peeked ident is name). Shared by in()/seq().
func (p *lqlParser) parenValueList(name string) ([]string, error) {
	p.next() // name
	p.next() // (
	var vals []string
	for p.peek().kind != tRParen && p.peek().kind != tEOF {
		vt := p.next()
		if vt.kind != tIdent && vt.kind != tString {
			return nil, fmt.Errorf("simdlogs: bad %s() value %q", name, vt.val)
		}
		vals = append(vals, vt.val)
		if p.peek().kind == tComma {
			p.next()
		}
	}
	if p.peek().kind != tRParen {
		return nil, fmt.Errorf("simdlogs: expected ) in %s()", name)
	}
	p.next() // )
	return vals, nil
}

// twoStrArgs parses `name(a, b)` as two string values.
func (p *lqlParser) twoStrArgs(name string) (string, string, error) {
	p.next() // name
	p.next() // (
	lo, err := p.value()
	if err != nil {
		return "", "", err
	}
	if p.peek().kind == tComma {
		p.next()
	}
	hi, err := p.value()
	if err != nil {
		return "", "", err
	}
	if p.peek().kind != tRParen {
		return "", "", fmt.Errorf("simdlogs: expected ) in %s()", name)
	}
	p.next() // )
	return lo, hi, nil
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
		case "sort", "order":
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
		case "fields", "keep":
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
		case "offset", "skip":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &OffsetPipe{N: n})
		case "rename", "mv":
			rp, err := p.parseRename()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, rp)
		case "delete", "drop", "del", "rm":
			fs, err := p.parseBareFieldList()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &DeletePipe{Drop: fs})
		case "filter", "where":
			e, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &FilterPipe{Expr: e})
		case "unpack_json":
			up := &UnpackJSONPipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				up.From = f
			}
			if p.peek().kind == tIdent && (strings.EqualFold(p.peek().val, "prefix") || strings.EqualFold(p.peek().val, "result_prefix")) {
				p.next()
				pf, err := p.value()
				if err != nil {
					return nil, err
				}
				up.Prefix = pf
			}
			pipes = append(pipes, up)
		case "extract":
			ep := &ExtractPipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				ep.From = f
			}
			pat := p.next()
			if pat.kind != tString && pat.kind != tIdent {
				return nil, fmt.Errorf("simdlogs: extract expects a pattern string, got %q", pat.val)
			}
			ep.Pattern = pat.val
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") { // trailing `from field` too
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				ep.From = f
			}
			pipes = append(pipes, ep)
		case "format":
			tpl := p.next()
			if tpl.kind != tString && tpl.kind != tIdent {
				return nil, fmt.Errorf("simdlogs: format expects a template string, got %q", tpl.val)
			}
			fp := &FormatPipe{Template: tpl.val}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				fp.As = a
			}
			pipes = append(pipes, fp)
		case "rank":
			terms := p.next()
			if terms.kind != tString && terms.kind != tIdent {
				return nil, fmt.Errorf("simdlogs: rank expects quoted terms, got %q", terms.val)
			}
			rp := &RankPipe{Terms: strings.Fields(terms.val)}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "at") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				rp.Field = f
			}
			pipes = append(pipes, rp)
		case "unpack_logfmt":
			up := &UnpackLogfmtPipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				up.From = f
			}
			if p.peek().kind == tIdent && (strings.EqualFold(p.peek().val, "prefix") || strings.EqualFold(p.peek().val, "result_prefix")) {
				p.next()
				pf, err := p.value()
				if err != nil {
					return nil, err
				}
				up.Prefix = pf
			}
			pipes = append(pipes, up)
		case "replace", "replace_regexp":
			if p.peek().kind != tLParen {
				return nil, fmt.Errorf("simdlogs: %s expects (\"old\", \"new\")", name.val)
			}
			p.next() // (
			oldv, err := p.value()
			if err != nil {
				return nil, err
			}
			if p.peek().kind == tComma {
				p.next()
			}
			newv, err := p.value()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tRParen {
				return nil, fmt.Errorf("simdlogs: %s: expected )", name.val)
			}
			p.next() // )
			rp := &ReplacePipe{Old: oldv, New: newv, Regexp: strings.EqualFold(name.val, "replace_regexp")}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "at") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				rp.Field = f
			}
			pipes = append(pipes, rp)
		case "copy", "cp":
			cr, err := p.parseRename() // same "a as b, c as d" grammar as rename
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &CopyPipe{From: cr.From, To: cr.To})
		case "len", "hash":
			if p.peek().kind != tLParen {
				return nil, fmt.Errorf("simdlogs: %s expects (field)", name.val)
			}
			p.next() // (
			fld, err := p.value()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tRParen {
				return nil, fmt.Errorf("simdlogs: %s: expected )", name.val)
			}
			p.next() // )
			isHash := strings.EqualFold(name.val, "hash")
			as := "len"
			if isHash {
				as = "hash"
			}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				as = a
			}
			if isHash {
				pipes = append(pipes, &HashPipe{Field: fld, As: as})
			} else {
				pipes = append(pipes, &LenPipe{Field: fld, As: as})
			}
		case "drop_empty_fields":
			pipes = append(pipes, &DropEmptyPipe{})
		case "collapse_nums", "pattern":
			cp := &CollapseNumsPipe{Full: strings.EqualFold(name.val, "pattern")}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "at") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				cp.Field = f
			}
			pipes = append(pipes, cp)
		case "math", "eval":
			ex := p.next()
			if ex.kind != tString {
				return nil, fmt.Errorf("simdlogs: math expects a quoted expression, got %q", ex.val)
			}
			node, flds, err := parseMath(ex.val)
			if err != nil {
				return nil, err
			}
			mp := &MathPipe{expr: node, fields: flds, As: "math"}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				mp.As = a
			}
			pipes = append(pipes, mp)
		case "extract_regexp":
			from := ""
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				from = f
			}
			pat := p.next()
			if pat.kind != tString && pat.kind != tIdent {
				return nil, fmt.Errorf("simdlogs: extract_regexp expects a pattern string, got %q", pat.val)
			}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				from = f
			}
			ep, err := newExtractRegexp(pat.val, from)
			if err != nil {
				return nil, fmt.Errorf("simdlogs: extract_regexp: %w", err)
			}
			pipes = append(pipes, ep)
		case "decolorize":
			dp := &DecolorizePipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "at") {
				p.next()
			}
			if k := p.peek().kind; k == tIdent || k == tString {
				dp.Field = p.next().val
			}
			pipes = append(pipes, dp)
		case "pack_json", "pack_logfmt":
			pp := &PackPipe{Logfmt: strings.EqualFold(name.val, "pack_logfmt")}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "fields") {
				p.next()
				fs, err := p.parseFieldGroup()
				if err != nil {
					return nil, err
				}
				pp.Fields = fs
			}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				pp.As = a
			}
			pipes = append(pipes, pp)
		case "sample":
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &SamplePipe{N: n})
		case "first", "last":
			// `first N [by] (fields)` == sort by those fields, limit N; last is
			// the descending sort. Sugar over SortPipe, which already limits.
			n, err := p.intArg()
			if err != nil {
				return nil, err
			}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "by") {
				p.next()
			}
			fs, err := p.parseFieldGroup()
			if err != nil {
				return nil, err
			}
			pipes = append(pipes, &SortPipe{By: fs, Limit: n, Desc: strings.EqualFold(name.val, "last")})
		case "unroll":
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "by") {
				p.next()
			}
			fs, err := p.parseFieldGroup()
			if err != nil {
				return nil, err
			}
			if len(fs) == 0 {
				return nil, fmt.Errorf("simdlogs: unroll expects a field")
			}
			pipes = append(pipes, &UnrollPipe{Field: fs[0]})
		case "unpack_syslog":
			up := &UnpackSyslogPipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				up.From = f
			}
			pipes = append(pipes, up)
		case "json_array_len":
			if p.peek().kind != tLParen {
				return nil, fmt.Errorf("simdlogs: json_array_len expects (field)")
			}
			p.next() // (
			fld, err := p.value()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tRParen {
				return nil, fmt.Errorf("simdlogs: json_array_len: expected )")
			}
			p.next() // )
			jp := &JSONArrayLenPipe{Field: fld, As: "json_array_len"}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				jp.As = a
			}
			pipes = append(pipes, jp)
		case "unpack_words":
			wp := &UnpackWordsPipe{}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "from") {
				p.next()
				f, err := p.value()
				if err != nil {
					return nil, err
				}
				wp.From = f
			}
			if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
				p.next()
				a, err := p.value()
				if err != nil {
					return nil, err
				}
				wp.As = a
			}
			pipes = append(pipes, wp)
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
	kind, ok := aggKind(fn.val)
	if !ok {
		return Agg{}, fmt.Errorf("simdlogs: unknown aggregation %q", fn.val)
	}
	// quantile(p, field) takes a leading percentile; median(field) is p=0.5.
	pct := 0.5
	if kind == AggQuantile && !strings.EqualFold(fn.val, "median") {
		pt := p.next()
		v, err := strconv.ParseFloat(pt.val, 64)
		if err != nil {
			return Agg{}, fmt.Errorf("simdlogs: quantile expects a percentile 0..1, got %q", pt.val)
		}
		pct = v
		if p.peek().kind == tComma {
			p.next()
		}
	}
	field := ""
	if k := p.peek().kind; k == tIdent || k == tString {
		field = p.next().val
	}
	// row_min/row_max take (sortField, outField).
	field2 := ""
	if kind == AggRowMin || kind == AggRowMax {
		if p.peek().kind == tComma {
			p.next()
			f2 := p.next()
			if f2.kind != tIdent && f2.kind != tString {
				return Agg{}, fmt.Errorf("simdlogs: %s expects (sort, out) fields", fn.val)
			}
			field2 = f2.val
		}
	}
	if p.peek().kind != tRParen {
		return Agg{}, fmt.Errorf("simdlogs: expected ) in %s()", fn.val)
	}
	p.next() // )
	a := Agg{Field: field, Field2: field2, Kind: kind, P: pct}
	// Optional conditional aggregate: `agg(...) if (<filter>)`.
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "if") {
		p.next() // if
		if p.peek().kind != tLParen {
			return Agg{}, fmt.Errorf("simdlogs: if expects (<filter>)")
		}
		p.next() // (
		e, err := p.parseOr()
		if err != nil {
			return Agg{}, err
		}
		if p.peek().kind != tRParen {
			return Agg{}, fmt.Errorf("simdlogs: if: expected )")
		}
		p.next() // )
		a.If = e
	}
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().val, "as") {
		p.next()
		al := p.next()
		if al.kind != tIdent && al.kind != tString {
			return Agg{}, fmt.Errorf("simdlogs: expected an alias after 'as'")
		}
		a.Alias = al.val
	} else if p.peek().kind == tIdent {
		// Bare alias, VL-style: `count() errors` == `count() as errors`. Only an
		// ident here can be an alias (a following agg comes after a comma, and a
		// following pipe after `|`), so this is unambiguous.
		a.Alias = p.next().val
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
	case "quantile", "median":
		return AggQuantile, true
	case "values":
		return AggValues, true
	case "uniq_values":
		return AggUniqValues, true
	case "sum_len":
		return AggSumLen, true
	case "count_empty":
		return AggCountEmpty, true
	case "row_any":
		return AggRowAny, true
	case "count_uniq_hash": // VL's approximate distinct; our count_uniq already spills to HLL
		return AggCountUniq, true
	case "histogram":
		return AggHistogram, true
	case "row_min":
		return AggRowMin, true
	case "row_max":
		return AggRowMax, true
	case "rate":
		return AggRate, true
	case "rate_sum":
		return AggRateSum, true
	}
	return 0, false
}
