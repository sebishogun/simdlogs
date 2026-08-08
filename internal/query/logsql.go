package query

import (
	"fmt"
	"strings"
)

// ParseLogsQL parses the LogsQL subset the first milestone supports into a
// Query: space- or AND-separated field predicates, each `field:value`,
// `field:="exact"`, or `field:~"substr"` (a leading/trailing * makes it a
// substring; a bare `~"re"` is a regexp). Time comes from the request's
// start/end params, not the query text, matching the reference's split of
// query vs time range. The subset is documented as such in the README;
// the full grammar is Phase 7.
//
// An empty query matches everything (a pure time-window scan).
func ParseLogsQL(s string) (*Query, error) {
	q := &Query{}
	s = strings.TrimSpace(s)
	if s == "" || s == "*" {
		return q, nil
	}
	// Split on whitespace and the AND keyword; OR/NOT and parentheses are
	// Phase 7. Quoted values may contain spaces, so scan with quote
	// awareness rather than a naive Fields split.
	toks := splitPredicates(s)
	for _, tok := range toks {
		if strings.EqualFold(tok, "and") || tok == "" {
			continue
		}
		p, err := parsePred(tok)
		if err != nil {
			return nil, err
		}
		q.Preds = append(q.Preds, p)
	}
	return q, nil
}

func parsePred(tok string) (Pred, error) {
	colon := strings.IndexByte(tok, ':')
	if colon <= 0 {
		return Pred{}, fmt.Errorf("simdlogs: predicate %q lacks field:value", tok)
	}
	field := tok[:colon]
	rest := tok[colon+1:]
	switch {
	case strings.HasPrefix(rest, "="):
		return Pred{Field: field, Kind: Eq, Value: unquote(rest[1:])}, nil
	case strings.HasPrefix(rest, "~"):
		v := unquote(rest[1:])
		// A regex metacharacter means RE2; otherwise substring.
		if strings.ContainsAny(v, `.*+?()[]{}|^$\`) {
			return Pred{Field: field, Kind: Regexp, Value: v}, nil
		}
		return Pred{Field: field, Kind: Contains, Value: v}, nil
	default:
		v := unquote(rest)
		if strings.HasPrefix(v, "*") || strings.HasSuffix(v, "*") {
			return Pred{Field: field, Kind: Contains, Value: strings.Trim(v, "*")}, nil
		}
		return Pred{Field: field, Kind: Eq, Value: v}, nil
	}
}

// splitPredicates splits on unquoted whitespace.
func splitPredicates(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			cur.WriteByte(c)
		case (c == ' ' || c == '\t') && !inQuote:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
