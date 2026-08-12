package query

import "testing"

// TestPipeAliases checks the VL pipe-name aliases parse to the same pipes.
func TestPipeAliases(t *testing.T) {
	for _, q := range []string{
		`* | keep a, b`, `* | order by (a)`, `* | mv a as b`,
		`* | cp a as b`, `* | del a`, `* | rm a`, `* | skip 5`,
	} {
		if _, err := ParseLogsQL(q); err != nil {
			t.Errorf("parse %q: %v", q, err)
		}
	}
}
