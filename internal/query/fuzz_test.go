package query

import (
	"strings"
	"testing"
	"time"
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
		// The "a named upper bound survives resolution" invariant is NOT here.
		//
		// It was, as `strings.Contains(s, "_time:<")`, and it has been red on a
		// cached corpus entry ever since the fuzzer found `0_time:<0` -- a
		// predicate on a field NAMED `0_time`, which names no time bound and
		// correctly resolves to no window. Anchoring the match so the field has
		// to be exactly `_time` fixed that one and the fuzzer immediately
		// produced `#_time:<0`, a comment. It will keep producing them: quoted
		// strings, regexes, pipe arguments.
		//
		// The invariant needs to know whether the CALLER named a time bound,
		// and the only thing that knows is the parser. Re-deriving it from the
		// input text is what failed twice, so it moved to
		// TestATimeUpperBoundSurvivesResolution below, which names its queries
		// and does not have to guess.
		//
		// What stays here is idempotence, which is fuzzable without inferring
		// intent from the string.
	})
}

// A named _time bound survives ResolveWindow.
//
// A table, not a fuzz invariant: whether a query names a time bound is a fact
// about the PARSE, and the fuzz body only has the input text. Two attempts to
// recover it from the text were both wrong -- `0_time:<0` is a field called
// `0_time`, `#_time:<0` is a comment -- and each left a release gate red on a
// query the engine handles correctly.
//
// Named queries cannot have that problem, and they say exactly which shapes
// are covered rather than implying all of them.
func TestATimeUpperBoundSurvivesResolution(t *testing.T) {
	// A bare date is a whole DAY, so the two comparisons land on opposite
	// edges of it: `<2026-01-01` is everything before that day begins, and
	// `>2025-01-01` is everything after it ends -- 2025-01-02, not 2025-01-01.
	// Asserting the naive instant fails, and the engine is the one that is
	// right; this test learned it by asserting the wrong number first.
	jan2026 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	jan2ndOf2025 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano()
	for _, tc := range []struct {
		q                string
		bounded          bool
		wantFrom, wantTo int64
	}{
		{q: `_time:<2026-01-01`, bounded: true, wantTo: jan2026},
		{q: `_time:>2025-01-01 _time:<2026-01-01`, bounded: true,
			wantFrom: jan2ndOf2025, wantTo: jan2026},
		{q: `_time:>2025-01-01`, bounded: true, wantFrom: jan2ndOf2025},
		{q: `* | stats count() c`, bounded: false},
		// The shapes that used to redden the gate: a field whose name ENDS in
		// _time, and a comment. Neither names a time bound.
		{q: `0_time:<0`, bounded: false},
		{q: `event_time:<7`, bounded: false},
		{q: `#_time:<0`, bounded: false},
	} {
		t.Run(tc.q, func(t *testing.T) {
			q, err := ParseLogsQL(tc.q)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			ResolveWindow(q)
			got := q.From != 0 || q.To != 0
			if got != tc.bounded {
				t.Fatalf("resolved to [%d,%d]; bounded=%v, want %v",
					q.From, q.To, got, tc.bounded)
			}
			// The INSTANT, not merely the presence of one.
			//
			// "bounded" alone passes on a bound that resolved to the wrong
			// time, which is the failure that matters: a window naming
			// 2026-01-01 and resolving to 2025-01-01 is bounded and wrong.
			if tc.wantTo != 0 && q.To != tc.wantTo {
				t.Errorf("To = %d, want %d (%s)", q.To, tc.wantTo,
					time.Unix(0, tc.wantTo).UTC())
			}
			if tc.wantFrom != 0 && q.From != tc.wantFrom {
				t.Errorf("From = %d, want %d (%s)", q.From, tc.wantFrom,
					time.Unix(0, tc.wantFrom).UTC())
			}
		})
	}
}
