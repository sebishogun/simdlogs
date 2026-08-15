package query

import (
	"strings"
	"testing"
)

// Fuzzing the query languages.
//
// Both parsers read a string an authenticated client controls, and both feed a
// scan that runs over mapped group files. The properties:
//
//  1. NO PANIC, and no unbounded work. A parser that recursed on nesting would
//     take the process down on a query anyone with read access can send.
//  2. A parse that SUCCEEDS produces a query the engine can run. Accepting
//     something the executor then panics on moves the crash one layer down,
//     where it is harder to find and just as fatal.
//  3. DETERMINISM. The same string twice must parse the same way, or a cursor
//     issued against one parse names rows from another -- and the cursor is
//     bound to a hash of the query text.

func FuzzParseLogsQL(f *testing.F) {
	for _, seed := range []string{
		"*",
		"level:=error",
		`_msg:~"a|b"`,
		"* | stats count() n",
		"* | stats by (level) count() n",
		"* | sort by (_time) desc | limit 10",
		"* | filter level:=error | fields level",
		"* | unpack_json | math a + b as c",
		"* | join by (id) (*)",
		"(((((((((((((((((((((*)))))))))))))))))))))",
		"* |" + strings.Repeat(" limit 1 |", 64) + " limit 1",
		`level:="unterminated`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 1<<16 {
			return // the HTTP layer bounds the query string; this is not that test
		}
		q1, err1 := ParseLogsQL(s)
		q2, err2 := ParseLogsQL(s)

		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("%q parsed then failed: %v / %v", s, err1, err2)
		}
		if err1 != nil {
			if err1.Error() != err2.Error() {
				t.Fatalf("%q gave two different errors:\n  %v\n  %v", s, err1, err2)
			}
			return
		}
		if q1 == nil || q2 == nil {
			t.Fatalf("%q parsed to a nil query with no error", s)
		}
		if len(q1.Pipes) != len(q2.Pipes) {
			t.Fatalf("%q parsed to %d pipes then %d", s, len(q1.Pipes), len(q2.Pipes))
		}
		// A parsed pipe must classify. ClassifyPipe defaults to
		// coordinator-only, so this cannot fail by omission -- what it catches
		// is a pipe whose type assertion panics.
		for _, p := range q1.Pipes {
			if c := ClassifyPipe(p); c.String() == "" {
				t.Fatalf("pipe %T has no class", p)
			}
		}
		// And the planner must not panic on it, whatever it is.
		plan := PlanDistributed(q1.Pipes)
		if len(plan.ShardPipes)+len(plan.CoordinatorPipes) < len(q1.Pipes) && plan.Reject == "" {
			t.Fatalf("%q: planning dropped pipes without refusing: %d + %d < %d",
				s, len(plan.ShardPipes), len(plan.CoordinatorPipes), len(q1.Pipes))
		}
	})
}

func FuzzParseSQL(f *testing.F) {
	for _, seed := range []string{
		"SELECT * FROM logs",
		"SELECT level, _msg FROM logs WHERE level = 'error' LIMIT 10",
		"SELECT count(*) FROM logs GROUP BY level",
		"SELECT * FROM logs ORDER BY _time DESC",
		"SELECT",
		"SELECT * FROM logs WHERE (((((((((((1)))))))))))",
		"SELECT * FROM logs WHERE level = 'unterminated",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 1<<16 {
			return
		}
		q1, err1 := ParseSQL(s)
		_, err2 := ParseSQL(s)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("%q parsed then failed: %v / %v", s, err1, err2)
		}
		if err1 != nil {
			if err1.Error() != err2.Error() {
				t.Fatalf("%q gave two different errors:\n  %v\n  %v", s, err1, err2)
			}
			return
		}
		if q1 == nil {
			t.Fatalf("%q parsed to a nil query with no error", s)
		}
		// The router plans SQL with the same classifier, so it must not panic
		// on anything the SQL parser accepts.
		PlanDistributed(q1.Pipes)
	})
}

// ResolveWindow rewrites a query's time predicates into its From/To window.
// A window that WIDENS is the failure that matters: a paged read would then
// return rows the original query excluded.
func FuzzResolveWindow(f *testing.F) {
	for _, seed := range []string{
		"*",
		"_time:>2026-01-01",
		"_time:>2026-01-01 _time:<2025-01-01",
		"_time:>9223372036854775807",
		"_time:<-9223372036854775808",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 4096 {
			return
		}
		q, err := ParseLogsQL(s)
		if err != nil || q == nil {
			return
		}
		ResolveWindow(q)
		from1, to1 := q.From, q.To

		// From ABOVE To is not a defect: `_time:>2026-01-01 _time:<2025-01-01`
		// asks for a range that cannot contain anything, and the scan answers
		// zero rows -- checked directly, not assumed. What this pins is that
		// resolving is IDEMPOTENT. A window that moved on a second resolve
		// would make a paged read return a different range from the first page,
		// and the cursor is bound to the window it was issued for.
		ResolveWindow(q)
		if q.From != from1 || q.To != to1 {
			t.Fatalf("%q: resolving twice moved the window from [%d,%d] to [%d,%d]",
				s, from1, to1, q.From, q.To)
		}
		// And a resolved window must not have lost its bound entirely: a query
		// that named an upper bound and resolved to "no upper bound" would scan
		// past what it asked for.
		if strings.Contains(s, "_time:<") && to1 == 0 && from1 == 0 {
			t.Fatalf("%q named an upper bound and resolved to an unbounded window", s)
		}
	})
}
