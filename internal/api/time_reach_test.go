package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// twoRowNode is a node holding one recent row and one an hour old, so a
// relative window can tell them apart.
//
// RELATIVE timestamps deliberately. The shared `corpus` helper pins its rows to
// 2026-06-01, which no `_time:5m` can ever match, and a test written on it would
// compare two empty answers and pass on every defect in this file.
func twoRowNode(t *testing.T) *httptest.Server {
	t.Helper()
	now := time.Now()
	return realShard(t, []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"recent"}`, now.Add(-1*time.Minute).UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"old"}`, now.Add(-1*time.Hour).UnixNano()),
	})
}

// A RELATIVE `_time` FILTER ON `/select/sql`, WITH NO `end` PARAMETER.
//
// `sqlQuery` calls `SetWindow(timeWindow(r))`, which marks the window resolved
// (`ToSet`) and, with no `end` in the request, sets `To` to the 1<<62 sentinel
// meaning "no upper bound". `resolveTimePreds` then reads `ToSet` as permission
// to use `To` as `now`, so every relative filter here resolved against the year
// 2116 and matched nothing, at HTTP 200. `federatedSQL` forwards the same query
// to every shard, so a cluster answered 0 exactly as a node did.
//
// THE `end=`-GIVEN CASE IS A CONTROL, NOT THE TEST. It passed on the defect,
// because supplying a real end makes `To` an instant rather than a sentinel. A
// test written only that way is green on the bug -- which is the whole reason
// the no-`end` case is the one that has to be here.
func TestARelativeTimeFilterOnSQLWithNoEndParameter(t *testing.T) {
	node := twoRowNode(t)
	end := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)

	for _, tc := range []struct {
		name, query, extra string
		want, notWant      string
	}{
		{
			name:  "greater-than, no end parameter",
			query: `SELECT * FROM logs WHERE _time > '5m'`,
			want:  "recent", notWant: "old",
		},
		{
			name:  "greater-or-equal, no end parameter",
			query: `SELECT * FROM logs WHERE _time >= '5m'`,
			want:  "recent", notWant: "old",
		},
		{
			// The control that passed on the defect.
			name:  "greater-than, with an end parameter",
			query: `SELECT * FROM logs WHERE _time > '5m'`,
			extra: "&end=" + urlEscape(end),
			want:  "recent", notWant: "old",
		},
		{
			// The other control: no time filter, so a case above returning
			// nothing is the filter rather than an empty store.
			name:  "no time filter at all",
			query: `SELECT * FROM logs`,
			want:  "recent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/sql?query="+
				urlEscape(tc.query)+tc.extra)
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			if !strings.Contains(body, `"`+tc.want+`"`) {
				t.Errorf("%q did not return the %q row. With no `end`, "+
					"`timeWindow` gives To = 1<<62 -- a sentinel, not an "+
					"instant -- and a relative filter resolved against it lands "+
					"in the year 2116:\n%.300s", tc.query, tc.want, body)
			}
			if tc.notWant != "" && strings.Contains(body, `"`+tc.notWant+`"`) {
				t.Errorf("%q returned the %q row, which is outside the window "+
					"it asked for:\n%.300s", tc.query, tc.notWant, body)
			}
		})
	}
}

// The same query through the LogsQL route answers the same thing, which is what
// makes the SQL answer above wrong rather than merely different.
func TestSQLAndLogsQLAgreeOnARelativeTimeFilter(t *testing.T) {
	node := twoRowNode(t)

	sqlCode, sqlBody := chaosGet(t, node.URL+"/select/sql?query="+
		urlEscape(`SELECT * FROM logs WHERE _time > '5m'`))
	lqCode, lqBody := chaosGet(t, node.URL+"/select/logsql/query?query="+
		urlEscape(`_time:5m`))
	if sqlCode != 200 || lqCode != 200 {
		t.Fatalf("sql %d, logsql %d", sqlCode, lqCode)
	}
	sqlHas := strings.Contains(sqlBody, `"recent"`)
	lqHas := strings.Contains(lqBody, `"recent"`)
	if sqlHas != lqHas {
		t.Errorf("one binary, one window, two answers: /select/sql has the "+
			"recent row = %v, /select/logsql/query = %v.\nsql:    %.200s\nlogsql: %.200s",
			sqlHas, lqHas, sqlBody, lqBody)
	}
	if !lqHas {
		t.Fatal("neither route returned the recent row, so this test compared " +
			"two empty answers and proved nothing")
	}
}

// tailLive opens a tail, ingests n rows after the stream is open, and returns
// how many came back.
//
// Ingesting AFTER the stream opens is the whole point: it is the case a frozen
// window, a stale clone, or a surviving row cap silently drops.
func tailLive(t *testing.T, query string, extra string, n int, drain time.Duration) int {
	t.Helper()
	node := realShard(t, nil)

	resp := tailOpen(t, node, query, extra)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("tail answered %d", resp.StatusCode)
	}

	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf(`{"_time":%d,"_msg":"live%d","level":"info"}`,
			time.Now().UnixNano(), i)
	}
	time.Sleep(150 * time.Millisecond)
	post, err := node.Client().Post(node.URL+"/insert/jsonline",
		"application/x-ndjson", strings.NewReader(strings.Join(lines, "\n")+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	post.Body.Close()

	// COUNT EVERYTHING that arrives within the window, rather than stopping at
	// the expected number. Stopping early cannot see over-delivery, and a tail
	// that polls is a place duplicates can appear.
	return len(tailDrain(resp, `"live`, drain))
}

// A TAIL'S `limit=` DROPS ROWS, EVERY POLL, FOREVER.
//
// The tail clears `q.Limit` and leaves `q.LastN` -- and `limit=` is the
// parameter that sets LastN, not Limit. A tail has no last row to count back
// from, so the cap has no meaning there; what it did instead was bound EACH
// POLL, so a stream delivered N rows per poll and dropped the rest, silently,
// at 200, for as long as it stayed open.
func TestATailWithALimitStillDeliversEveryRow(t *testing.T) {
	const rows = 5
	for _, tc := range []struct {
		name, extra string
	}{
		{"no limit", ""},
		{"limit=1", "&limit=1"},
		{"limit=2", "&limit=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tailLive(t, `*`, tc.extra, rows, 4*time.Second)
			if got < rows {
				t.Errorf("a tail%s delivered %d of %d rows ingested after the "+
					"stream opened. `limit=` sets LastN, and a tail that clears "+
					"only Limit keeps it -- bounding every poll rather than the "+
					"stream, so the rest are dropped at 200 with the connection "+
					"open", strings.ReplaceAll(tc.extra, "&", " "), got, rows)
			}
		})
	}
}

// A TAIL WHOSE `_time` SITS UNDER AN `or` OR A `not` EXERCISES `cloneExpr`.
//
// `CloneResolvable` deep-copies two things, and which one a relative `_time`
// lands in depends on the query's SHAPE. `ParseLogsQL` puts a bare `_time:5m`
// in `q.Preds` with a nil Filter; an `or` or a `not` puts it in the Filter
// TREE. The existing tail test drives only bare forms, so replacing
// `cloneExpr(q.Filter)` with a plain `q.Filter` left the whole suite green
// while the second column below went to zero:
//
//	_time:5m                              Preds    3 of 3    3 of 3
//	level:error or _time:5m               Filter   3 of 3    0 of 3
//	(_time:5m or level:info) and _msg:...  Filter   1 of 1    0 of 1
func TestATailWhoseTimeFilterSitsInTheExpressionTreeKeepsDelivering(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		// In q.Preds -- the shape already covered, kept as the control.
		{"a bare relative filter", `_time:5m`, 3},
		// In the Filter tree, which is what cloneExpr copies.
		{"under an or", `level:error or _time:5m`, 3},
		{"nested under an and", `(_time:5m or level:error) and _msg:live0`, 1},
		{"under a not", `not _time:1h`, 0},
		// IN A PIPE, which is the third place a relative `_time` can land and
		// the one `clonePipesResolvable` exists for. The tail now KEEPS its
		// row-local pipes, so this expression is re-resolved every poll -- and
		// a clone that shares the FilterPipe resolves the template instead,
		// freezing it at the first poll's instant exactly as a shared Filter
		// tree does.
		{"in a filter pipe", `* | filter _time:5m`, 3},
		{"in a filter pipe, one-sided", `* | filter _time:>=5m`, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tailLive(t, tc.query, "", 3, 4*time.Second)
			if got != tc.want {
				t.Errorf("%q delivered %d of 3 rows ingested after the stream "+
					"opened, want %d. A relative `_time` under an `or` or a "+
					"`not` lives in the Filter TREE rather than in q.Preds, and "+
					"a clone that shares that tree resolves the ORIGINAL -- "+
					"freezing the template at the first poll's instant",
					tc.query, got, tc.want)
			}
		})
	}
}

// A `_time` FILTER IN A PIPE MATCHES THE SAME ROWS AS ONE IN THE QUERY.
//
// Two defects met here. `resolveTimePreds` walked `q.Filter` and `q.Preds` and
// never a `FilterPipe`'s expression, so a relative pred reached the evaluator
// with Rel still true and T1/T2 still OFFSETS. And `matchPredRow` had no case
// for the time kinds at all -- `_time` is `Row.Time`, not a `Row.Fields` entry,
// so `rowField` returned "" and every comparison failed even once resolved.
// Both answered 200 with no rows:
//
//	_time:5m                   1 row
//	* | filter _time:5m        0 rows
//	* | filter _msg:recent     1 row   (the control that always worked)
func TestAFilterPipeOnTimeMatchesTheSameRowsAsAQueryFilter(t *testing.T) {
	node := twoRowNode(t)

	for _, tc := range []struct {
		name, query   string
		want, notWant string
	}{
		{"relative, in a pipe", `* | filter _time:5m`, "recent", "old"},
		{"one-sided relative, in a pipe", `* | filter _time:>=5m`, "recent", "old"},
		{"absolute range, in a pipe",
			`* | filter _time:[2020-01-01T00:00:00Z, 2030-01-01T00:00:00Z]`,
			"recent", ""},
		{"a wide relative window keeps both", `* | filter _time:2h`, "old", ""},

		// The controls. The first is the same filter in the query rather than
		// in a pipe; the second is a pipe filter on an ordinary field, which
		// always worked and shows the pipe itself is fine.
		{"the same filter in the query", `_time:5m`, "recent", "old"},
		{"a non-time filter in a pipe", `* | filter _msg:recent`, "recent", "old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/logsql/query?query="+
				urlEscape(tc.query))
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			if !strings.Contains(body, `"`+tc.want+`"`) {
				t.Errorf("%q did not return the %q row. `_time` is Row.Time and "+
					"not a Row.Fields entry, so a row evaluator with no case for "+
					"the time kinds looks it up as a field, gets \"\", and "+
					"matches nothing:\n%.300s", tc.query, tc.want, body)
			}
			if tc.notWant != "" && strings.Contains(body, `"`+tc.notWant+`"`) {
				t.Errorf("%q returned the %q row, outside the window it asked "+
					"for:\n%.300s", tc.query, tc.notWant, body)
			}
		})
	}
}

// tailPipeRun opens a tail with a query, ingests four rows -- two level=error,
// two level=info -- and returns the status plus the delivered lines.
func tailPipeRun(t *testing.T, q string) (int, []string) {
	t.Helper()
	node := realShard(t, nil)
	resp := tailOpen(t, node, q, "")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil
	}

	lines := make([]string, 4)
	for i := range lines {
		lvl := "info"
		if i%2 == 0 {
			lvl = "error"
		}
		lines[i] = fmt.Sprintf(`{"_time":%d,"_msg":"m%d","level":"%s"}`,
			time.Now().UnixNano(), i, lvl)
	}
	time.Sleep(150 * time.Millisecond)
	post, err := node.Client().Post(node.URL+"/insert/jsonline",
		"application/x-ndjson", strings.NewReader(strings.Join(lines, "\n")+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	post.Body.Close()

	return 200, tailDrain(resp, `"m`, 3*time.Second)
}

// A TAIL'S PIPES ARE RUN OR REFUSED, NEVER SILENTLY DROPPED.
//
// `q.Pipes = nil` threw away every pipe on the grounds that stats and sort
// never terminate on a stream. Measured, four rows of which two are level=info:
//
//	tail?query=*                       4 of 4
//	tail?query=* | filter level:error  4 of 4   (both info rows delivered)
//	tail?query=* | fields _msg         full records, every field present
//
// Someone tailing only errors got everything and had nothing to tell them so.
func TestATailRunsItsRowLocalPipesAndRefusesTheRest(t *testing.T) {
	t.Run("filter actually filters", func(t *testing.T) {
		code, got := tailPipeRun(t, `* | filter level:error`)
		if code != 200 {
			t.Fatalf("a row-local pipe was refused: %d", code)
		}
		if len(got) == 0 {
			t.Fatal("nothing was delivered, so this case proves nothing")
		}
		for _, line := range got {
			if strings.Contains(line, `"info"`) {
				t.Errorf("`| filter level:error` delivered a level=info row. "+
					"The pipe was dropped, and the stream is answering a "+
					"different query than the one asked for:\n%.200s", line)
			}
		}
		if len(got) != 2 {
			t.Errorf("delivered %d rows, want the 2 that are level=error", len(got))
		}
	})

	t.Run("fields actually projects", func(t *testing.T) {
		code, got := tailPipeRun(t, `* | fields _msg`)
		if code != 200 {
			t.Fatalf("a row-local pipe was refused: %d", code)
		}
		if len(got) == 0 {
			t.Fatal("nothing was delivered, so this case proves nothing")
		}
		for _, line := range got {
			if strings.Contains(line, `"level"`) {
				t.Errorf("`| fields _msg` delivered a row still carrying "+
					"`level`. The projection was dropped:\n%.200s", line)
			}
		}
	})

	t.Run("the control: no pipe delivers everything", func(t *testing.T) {
		code, got := tailPipeRun(t, `*`)
		if code != 200 {
			t.Fatalf("%d", code)
		}
		if len(got) != 4 {
			t.Errorf("a bare tail delivered %d of 4 rows; without this the two "+
				"cases above could pass on a stream that delivers nothing",
				len(got))
		}
	})

	// A pipe that cannot stream is REFUSED, not ignored. Ignoring it answers a
	// different question and says the answer is complete.
	//
	// THE PAIRING IS WHAT IS ASSERTED: the exact name AND the exact reason, in
	// the one string the endpoint emits. A substring of one shared reason is
	// what the earlier version checked, and collapsing the reasons back into a
	// single message left `sort`, `limit`, `uniq`, `top` and `stats count()`
	// green -- only `sample` gated anything, because only `sample` had a
	// different substring.
	//
	// Every reason the language can produce appears below, so no two of them
	// can be merged without a failure here. The full set, and which pipes
	// carry it, is measured pipe by pipe in
	// TestEveryPipeInTheLanguageHasATailRefusal.
	for _, tc := range []struct {
		q, name, why string
	}{
		// Computed over the whole result set. Both halves of the stats split:
		// `count()` is mergeable and `avg()` is not, which is a DISTRIBUTION
		// property -- a tail has no coordinator, so both are refused for the
		// same reason and this pair is red if the message follows the class
		// again.
		{`* | stats count() c`, "stats", whyNeverFinal},
		{`* | stats avg(d) a`, "stats", whyNeverFinal},
		{`* | sort by (_time)`, "sort", whyNeverFinal},
		{`* | uniq by (level)`, "uniq", whyNeverFinal},
		{`* | top 1 by (level)`, "top", whyNeverFinal},
		// The introspection pipes, whose names are the ones a lowered Go type
		// name mangles: `fieldvalues`, `blockscount`.
		{`* | field_values level`, "field_values", whyNeverFinal},
		{`* | blocks_count`, "blocks_count", whyNeverFinal},
		// Slices of a result set. `limit` and `offset` are `rows[:N]` and
		// `rows[N:]` -- they do NOT need the whole result set, and saying so
		// was false; what stops them is that each poll is its own result set.
		{`* | limit 2`, "limit", whyPerPoll},
		{`* | offset 2`, "offset", whyPerPoll},
		{`* | tail 2`, "tail", whyPerPoll},
		{`* | sample 2`, "sample", whyPerPoll},
		// Reaches outside the rows it was given. `stream_context` came back as
		// `streamcontext` for as long as the name was a lowered type.
		{`* | union (*)`, "union", whySecondSet},
		{`* | stream_context before 1`, "stream_context", whySecondSet},

		// CHAINS. Every case above and every other `logsql/tail` call site in
		// this package drives `*`, a bare filter, or ONE pipe -- so
		// `nonStreamingPipe` reduced to inspecting `pipes[0]` answered all of
		// them identically and the whole tail gate set stayed green. Measured
		// at the endpoint with that mutation:
		//
		//	* | filter level:error | stats count() c   400 -> 200, rows streamed
		//
		// The pipe that cannot stream must be found wherever it sits, and the
		// message must name IT rather than the first pipe of the query.
		//
		// Last, after a row-local pipe -- the mutation's case.
		{`* | filter level:error | stats count() c`, "stats", whyNeverFinal},
		// In the middle, with a row-local pipe on each side.
		{`* | filter level:error | sample 2 | fields _msg`, "sample", whyPerPoll},
		// First, with a row-local pipe after it: the control the mutation keeps
		// green, so a red row above is the position and not the chain.
		{`* | sort by (_time) | filter level:error`, "sort", whyNeverFinal},
		// TWO refused pipes: the FIRST is the one named. A caller removes pipes
		// one at a time, and naming the last one sends them to the wrong edit.
		{`* | filter level:error | union (*) | stats count() c`, "union", whySecondSet},
		// A `+` INSIDE THE QUERY, which the old hand-rolled `urlEscape` left
		// unescaped: the server received `math "n  1" as m` and answered 400
		// with a parse error, which is a 400 for the wrong reason. This row is
		// red unless the query arrives as written.
		{`* | math "n + 1" as m | stats count() c`, "stats", whyNeverFinal},
	} {
		tc := tc
		t.Run("refused: "+tc.q, func(t *testing.T) {
			code, body := tailPipeRunBody(t, tc.q)
			want := "`" + tc.name + "` cannot run here: " + tc.why
			if code == 400 && !strings.Contains(body, want) {
				t.Errorf("%q is refused with\n got %q\nwant it to contain %q\n"+
					"A refusal whose stated reason is not its actual one tells a "+
					"caller something they cannot act on, and one that names a "+
					"token the language does not have cannot be pasted back into "+
					"a query", tc.q, strings.TrimSpace(body), want)
			}
			if code != 400 {
				t.Errorf("%q answered %d. A pipe that cannot stream has no "+
					"answer on a tail; running the tail without it reports rows "+
					"as though the pipe had been applied", tc.q, code)
			}
		})
	}
}

// THE UPPER BOUND IS EXCLUSIVE, in the query and in a pipe alike.
//
// The group scan is `from <= ts < to` (`storage/column.go`'s
// `decodeTimeRangeInto`) and the row evaluator is `r.Time >= from && r.Time <
// to`. Flipping the row evaluator's `<` to `<=` is a real divergence and left
// the whole suite green: one filter, two spellings, two answers at the
// endpoint.
//
// A row EXACTLY at the exclusive upper bound is the only input that tells the
// two apart, which is why the fixture is two rows one second apart rather than
// anything more realistic.
func TestTheUpperTimeBoundIsExclusiveInBothSpellings(t *testing.T) {
	base := time.Date(2027, 3, 4, 12, 0, 0, 0, time.UTC)
	node := realShard(t, []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"atlow"}`, base.UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"athigh"}`, base.Add(time.Second).UnixNano()),
	})

	lo := base.Format(time.RFC3339)
	hi := base.Add(time.Second).Format(time.RFC3339)
	window := "[" + lo + ", " + hi + ")"

	for _, tc := range []struct {
		name, query string
	}{
		{"in the query", `_time:` + window},
		{"in a filter pipe", `* | filter _time:` + window},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/logsql/query?query="+
				urlEscape(tc.query))
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			if !strings.Contains(body, `"atlow"`) {
				t.Errorf("the row AT the inclusive lower bound is missing:\n%.200s",
					body)
			}
			if strings.Contains(body, `"athigh"`) {
				t.Errorf("the row at the EXCLUSIVE upper bound was returned. "+
					"The group scan is `from <= ts < to`; a row evaluator using "+
					"`<=` makes one filter answer differently depending on "+
					"whether it sits in the query or in a pipe:\n%.200s", body)
			}
		})
	}
}

// THE DAY AND WEEK ARMS OF THE ROW EVALUATOR, on a fixture that can separate a
// right answer from an always-true one AND from a local-zone one.
//
// The first version used `day_range[00:00, 23:59]` and `week_range[Mon, Sun]`
// on the relative-timestamp fixture, and justified the whole-day bounds as "a
// narrow range would make this a clock-dependent test". That reasoning was
// wrong: the clock dependence came from the fixture's RELATIVE timestamps, not
// from the range width. A range that matches everything cannot fail --
// `matchDayWeek` replaced by `return true` passed both cases, and so did a row
// arm that judged every row by the epoch instead of by `r.Time`.
//
// The narrow-range version still could not see the ZONE. Both evaluators
// convert with `.UTC()` (`pipes.go`'s `matchPredRow`, `time_filter.go`'s
// `timePredBitset`), and `_time:day_range` is documented as a UTC minute of
// day -- but replacing `.UTC()` with `.Local()` left all six cases green,
// because this machine is Europe/London and the fixture's March instants sit
// in its offset-zero window. The test's sensitivity to a real defect depended
// on the calendar month it ran in.
//
// So the cases run TWICE: once in this process, and once in a child whose
// `time.Local` is nine hours from UTC, where every instant below lands on a
// different minute of day and two of them on a different weekday. The child is
// a process rather than an assignment here because writing `time.Local` in a
// package whose other tests run servers with live goroutines is a data race,
// and the child has no goroutine but its own when it writes.
//
// Three rows at fixed instants. The right-hand column is the same instant in
// the child's zone, which is what a `.Local()` evaluator would judge them by:
//
//	thu  2027-03-04T10:00:00Z  Thu 10:00   ->  Thu 19:00
//	sat  2027-03-06T22:00:00Z  Sat 22:00   ->  Sun 07:00
//	sun  2027-03-07T00:30:00Z  Sun 00:30   ->  Sun 09:30
func TestTheRowEvaluatorsDayAndWeekArmsSelectByTheRowsOwnTime(t *testing.T) {
	child := os.Getenv(tzChildEnv) != ""
	if child {
		// Before this test starts anything: the only goroutines in this
		// process are the runtime's and the test runner's.
		//
		// A FixedZone rather than a named one: `time.LoadLocation("Asia/Tokyo")`
		// needs a zoneinfo database, and a machine without one would fall back
		// to UTC and quietly take this whole child back to measuring nothing.
		time.Local = time.FixedZone("UTC+9", 9*60*60)
		if _, off := time.Now().Zone(); off == 0 {
			t.Fatal("the child's local zone is still at offset zero, so it " +
				"cannot tell a UTC evaluator from a local one")
		}
	}

	thu := time.Date(2027, 3, 4, 10, 0, 0, 0, time.UTC) // Thursday 10:00
	sat := time.Date(2027, 3, 6, 22, 0, 0, 0, time.UTC) // Saturday 22:00
	sun := time.Date(2027, 3, 7, 0, 30, 0, 0, time.UTC) // Sunday 00:30
	node := realShard(t, []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"thu"}`, thu.UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"sat"}`, sat.UnixNano()),
		fmt.Sprintf(`{"_time":%d,"_msg":"sun"}`, sun.UnixNano()),
	})

	for _, tc := range []struct {
		name, query string
		want        []string
		notWant     []string
	}{
		// MINUTE-OF-DAY. In UTC only `thu` is inside 09:00-11:00; in the
		// child's zone only `sun` (09:30) is, so a `.Local()` evaluator both
		// loses a row it must return and returns one it must not.
		{"day range selects by hour, in a pipe",
			`* | filter _time:day_range[09:00, 11:00]`,
			[]string{"thu"}, []string{"sat", "sun"}},
		{"the complementary day range",
			`* | filter _time:day_range[21:00, 23:00]`,
			[]string{"sat"}, []string{"thu", "sun"}},
		// DAY-OF-WEEK. `sat` is Saturday in UTC and Sunday nine hours east.
		{"week range selects by weekday, in a pipe",
			`* | filter _time:week_range[Sat, Sat]`,
			[]string{"sat"}, []string{"thu", "sun"}},
		{"the neighbouring weekday",
			`* | filter _time:week_range[Sun, Sun]`,
			[]string{"sun"}, []string{"thu", "sat"}},
		// The control: this one answers the same in both zones, so a case
		// above going red is the zone and not the fixture.
		{"the complementary week range",
			`* | filter _time:week_range[Mon, Thu]`,
			[]string{"thu"}, []string{"sat", "sun"}},
		// The same filters in the QUERY, which take the group scan rather than
		// the row evaluator. Both spellings must agree, and the group scan has
		// its own `.UTC()`.
		{"day range in the query", `_time:day_range[09:00, 11:00]`,
			[]string{"thu"}, []string{"sat", "sun"}},
		{"week range in the query", `_time:week_range[Sat, Sat]`,
			[]string{"sat"}, []string{"thu", "sun"}},
		{"the neighbouring weekday in the query", `_time:week_range[Sun, Sun]`,
			[]string{"sun"}, []string{"thu", "sat"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/logsql/query?query="+
				urlEscape(tc.query))
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			for _, w := range tc.want {
				if !strings.Contains(body, `"`+w+`"`) {
					t.Errorf("%q did not return %q. The day and week arms are "+
						"UTC in both evaluators; one that reads the row's time "+
						"in the local zone answers this differently wherever "+
						"the offset is not zero (here: %s):\n%.300s",
						tc.query, w, time.Now().Location(), body)
				}
			}
			for _, n := range tc.notWant {
				if strings.Contains(body, `"`+n+`"`) {
					t.Errorf("%q returned %q, which is outside the range. A row "+
						"evaluator that answers true regardless, one that reads "+
						"a fixed instant instead of the row's own time, or one "+
						"that converts to the local zone (here: %s) fails "+
						"this:\n%.300s", tc.query, n, time.Now().Location(), body)
				}
			}
		})
	}

	if child || t.Failed() {
		return
	}
	runTheseCasesInANonUTCZone(t, "TestTheRowEvaluatorsDayAndWeekArmsSelectByTheRowsOwnTime")
}

// tzChildEnv marks the re-executed child that runs a test's cases with a local
// zone nine hours from UTC.
const tzChildEnv = "SIMDLOGS_TZ_CHILD"

// runTheseCasesInANonUTCZone re-runs one test in a child process whose
// `time.Local` is not UTC.
//
// The machine decides whether a `.UTC()` -> `.Local()` mutation is observable
// at all: under TZ=UTC the two are the same function, and under Europe/London
// they are the same function for any instant outside British Summer Time. A
// test that is only sensitive in some zones in some months is not a gate, and
// the fixtures cannot fix it -- the zone has to be part of the run.
func runTheseCasesInANonUTCZone(t *testing.T, name string) {
	t.Helper()
	// Bounded: a child that hangs would otherwise sit there until the package
	// timeout, and a test binary that never exits is how a sibling repository
	// accumulated ~2038 of them and OOM-killed the machine on 2026-08-14.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.v")
	cmd.Env = append(os.Environ(), tzChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the child running %s in a UTC+9 process did not finish inside "+
			"120s; it was killed rather than left running:\n%s", name, out)
	}
	if err != nil {
		t.Errorf("%s fails in a process whose local zone is UTC+9, and passes in "+
			"this one. The answers below are UTC answers, so a local-zone "+
			"evaluator is the difference:\n%s", name, out)
	}
	// The child must have RUN the cases. `-test.run` that matches nothing exits
	// 0, so a renamed test would leave this helper reporting success forever.
	if !strings.Contains(string(out), "--- PASS: "+name) {
		t.Errorf("the child did not run %s at all (no PASS line), so nothing was "+
			"measured in the non-UTC zone:\n%s", name, out)
	}
}

// THE DAY RANGE'S ENDPOINTS ARE INCLUSIVE, in both spellings.
//
// `matchDayWeek` is `min >= p.T1 && min <= p.T2`, and `bracketInner` accepts
// only `[` and `]` -- there is no exclusive spelling of a day_range, so the
// inclusive endpoints ARE the whole contract. Nothing measured them: the
// day/week test picks 10:00 strictly inside `[09:00, 11:00]`, so
// `min > p.T1 && min < p.T2` left all of its cases green.
//
// A row exactly at each endpoint is the only input that tells the two apart.
func TestADayRangeIncludesBothOfItsEndpoints(t *testing.T) {
	day := time.Date(2027, 3, 4, 0, 0, 0, 0, time.UTC)
	at := func(h, m int) int64 {
		return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute).UnixNano()
	}
	node := realShard(t, []string{
		fmt.Sprintf(`{"_time":%d,"_msg":"before"}`, at(8, 59)),
		fmt.Sprintf(`{"_time":%d,"_msg":"atlow"}`, at(9, 0)),
		fmt.Sprintf(`{"_time":%d,"_msg":"inside"}`, at(10, 0)),
		fmt.Sprintf(`{"_time":%d,"_msg":"athigh"}`, at(11, 0)),
		fmt.Sprintf(`{"_time":%d,"_msg":"after"}`, at(11, 1)),
	})

	for _, tc := range []struct {
		name, query string
	}{
		{"in a filter pipe", `* | filter _time:day_range[09:00, 11:00]`},
		{"in the query", `_time:day_range[09:00, 11:00]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t, node.URL+"/select/logsql/query?query="+
				urlEscape(tc.query))
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			for _, w := range []string{"atlow", "inside", "athigh"} {
				if !strings.Contains(body, `"`+w+`"`) {
					t.Errorf("%q dropped the %q row. day_range has no exclusive "+
						"spelling -- `bracketInner` accepts only [ and ] -- so a "+
						"row exactly at 09:00 or exactly at 11:00 is inside the "+
						"range by the only definition the language has:\n%.300s",
						tc.query, w, body)
				}
			}
			for _, n := range []string{"before", "after"} {
				if strings.Contains(body, `"`+n+`"`) {
					t.Errorf("%q returned the %q row, one minute outside the "+
						"range. Without this, a day_range that matched "+
						"everything would pass the three cases above:\n%.300s",
						tc.query, n, body)
				}
			}
		})
	}
}

// tailOpen issues a tail request and returns once the HEADERS are in. The
// caller closes the body.
//
// Bounded by a context rather than by a client timeout, and separate from the
// body read, because a tail that answers 200 NEVER CLOSES: `chaosGet` reads to
// EOF, so every 200 costs it the whole 8 s `chaosTimeout`. Measured under a
// "nothing is ever refused" mutation, where all 18 refusal cases of
// TestATailRunsItsRowLocalPipesAndRefusesTheRest answer 200:
//
//	tailPipeRunBody via chaosGet     153.5 s   red
//	tailPipeRunBody via tailOpen       9.5 s   red
//
// Red both ways, and with the same message: `chaosGet` discards the read error
// (`b, _ := io.ReadAll(...)`) and returns the status the headers already
// carried, so the 8 s per case bought nothing at all.
func tailOpen(t *testing.T, node *httptest.Server, q, extra string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), chaosTimeout)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, "GET",
		node.URL+"/select/logsql/tail?query="+urlEscape(q)+extra, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := node.Client().Do(req)
	if err != nil {
		t.Fatalf("GET tail?query=%s%s: %v", q, extra, err)
	}
	return resp
}

// tailPipeRunBody opens a tail and returns the status with the RESPONSE BODY,
// for the refusals, whose message is part of what is being asserted.
//
// A refusal whose stated reason is not its actual one tells a caller something
// they cannot act on, and this repository has recorded that shape three times.
//
// The body is read only when the answer is FINITE. A 200 here is the failure
// this helper's callers assert on, and it is a stream that never closes: there
// is no body to wait for and nothing in one they assert, so the status returns
// at the headers.
func tailPipeRunBody(t *testing.T, q string) (int, string) {
	t.Helper()
	node := realShard(t, nil)
	resp := tailOpen(t, node, q, "")
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		// A refusal that did not happen. The stream is open and will stay
		// open; there is no body to wait for and nothing in it the caller
		// asserts on.
		return resp.StatusCode, ""
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, chaosReadCap))
	return resp.StatusCode, string(b)
}

// tailDrain counts the lines a stream delivers within `drain`, keeping the ones
// containing `match`.
//
// EVERYTHING that arrives inside the window, rather than stopping at the
// expected number: stopping early cannot see over-delivery, and both defects
// this file is about deliver MORE rows than the query asked for.
func tailDrain(resp *http.Response, match string, drain time.Duration) []string {
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if strings.Contains(sc.Text(), match) {
				got = append(got, sc.Text())
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(drain):
	}
	resp.Body.Close()
	<-done
	return got
}

// tailReplay ingests four rows -- two level=error, two level=info -- BEFORE the
// tail opens, then opens it with a `start_offset` wide enough to cover them and
// returns what the REPLAY delivered.
//
// The order matters and it is the whole point. `tailLive` and `tailPipeRun`
// both ingest AFTER the stream is open, so every pipe assertion in this file
// lands on the poll loop; the backlog replay is a SECOND execution of the
// pipeline (`backlog := q.CloneResolvable()`, then `RunPipeline(store,
// backlog)`) and nothing reached it.
//
// The replayed rows cannot come back a second time from the poll loop: the
// cursor is taken at `store.TailCursor()` before the replay runs, so the groups
// these rows are in are already behind it. A count is therefore exact rather
// than a lower bound.
func tailReplay(t *testing.T, q string) []string {
	t.Helper()
	now := time.Now()
	lines := make([]string, 4)
	for i := range lines {
		lvl := "info"
		if i%2 == 0 {
			lvl = "error"
		}
		lines[i] = fmt.Sprintf(`{"_time":%d,"_msg":"m%d","level":"%s"}`,
			now.Add(-time.Duration(i)*time.Second).UnixNano(), i, lvl)
	}
	node := realShard(t, lines)

	resp := tailOpen(t, node, q, "&start_offset=60s")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("the tail answered %d for %q", resp.StatusCode, q)
	}
	return tailDrain(resp, `"m`, 2*time.Second)
}

// THE BACKLOG REPLAY RUNS THE QUERY'S PIPES TOO.
//
// A tail replays the last `start_offset` of the stream before it starts
// polling, so a client that sets one gets TWO executions of the pipeline: the
// backlog's, and one per poll. Every other pipe assertion in this package
// drives the poll path -- both helpers ingest after the stream is open -- so
// `backlog.Pipes = nil` after the clone left the whole `internal/api` package
// green while the first window came back unfiltered, at 200, silently.
//
// Measured on the pristine tree, four rows ingested BEFORE the open, of which
// two are level=error, with `start_offset=60s`:
//
//	tail?query=* | filter level:error   2 of 4, no info row
//	tail?query=* | fields _msg          4 rows, none carrying `level`
//	tail?query=*                        4 of 4
func TestATailsBacklogReplayRunsThePipes(t *testing.T) {
	t.Run("filter actually filters the replay", func(t *testing.T) {
		got := tailReplay(t, `* | filter level:error`)
		if len(got) == 0 {
			t.Fatal("the replay delivered nothing, so this case proves nothing")
		}
		for _, line := range got {
			if strings.Contains(line, `"info"`) {
				t.Errorf("`| filter level:error` replayed a level=info row. The "+
					"backlog is a second run of the pipeline, and a clone whose "+
					"pipes were dropped replays the whole window as though the "+
					"filter had been applied:\n%.200s", line)
			}
		}
		if len(got) != 2 {
			t.Errorf("the replay delivered %d rows, want the 2 that are "+
				"level=error", len(got))
		}
	})

	t.Run("fields actually projects the replay", func(t *testing.T) {
		got := tailReplay(t, `* | fields _msg`)
		if len(got) != 4 {
			t.Fatalf("the replay delivered %d rows, want 4 -- a projection case "+
				"over an empty replay measures nothing", len(got))
		}
		for _, line := range got {
			if strings.Contains(line, `"level"`) {
				t.Errorf("`| fields _msg` replayed a row still carrying `level`. "+
					"The projection was dropped from the backlog query:\n%.200s", line)
			}
		}
	})

	t.Run("the control: no pipe replays the whole window", func(t *testing.T) {
		got := tailReplay(t, `*`)
		if len(got) != 4 {
			t.Errorf("a bare tail replayed %d of 4 rows ingested before it opened. "+
				"Without this the two cases above could pass on a replay that "+
				"delivers nothing", len(got))
		}
	})
}

// A far-future epoch `start=` matches nothing, in every unit.
//
// `unixToNanos` multiplied each unit up to nanoseconds and the multiply
// WRAPPED: seconds admits values to 1e11 against a cap of 9.2e9, millis 1e14
// against 9.2e12, micros 1e17 against 9.2e15. A wrapped product is a negative
// `From`, and a negative From matches the whole store -- measured,
// `?start=13000000000` (epoch seconds, year 2381) answered all 30 rows at
// HTTP 200 where the correct answer is none. The multiplications saturate
// now; the past-instant controls pin that saturation did not become refusal.
func TestAFutureEpochStartMatchesNothing(t *testing.T) {
	single := realShard(t, corpus(1)[0])
	for _, tc := range []struct {
		name, start string
		rows        int
	}{
		{"epoch seconds year 2381", "13000000000", 0},
		{"epoch millis year 2381", "13000000000000", 0},
		{"epoch micros year 2408", "13830000000000000", 0},
		{"epoch seconds year 2000 (control)", "946684800", 30},
		{"epoch millis year 2000 (control)", "946684800000", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := chaosGet(t,
				single.URL+"/select/logsql/query?query=%2A&start="+tc.start)
			if code != 200 {
				t.Fatalf("%d: %.200s", code, body)
			}
			got := 0
			for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
				if strings.TrimSpace(l) != "" {
					got++
				}
			}
			if got != tc.rows {
				t.Fatalf("start=%s returned %d rows, want %d -- a future bound "+
					"that wraps negative matches everything: %.200s",
					tc.start, got, tc.rows, body)
			}
		})
	}
}
