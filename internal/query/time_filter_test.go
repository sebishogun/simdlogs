package query

import (
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestTimeFilters(t *testing.T) {
	mk := func(y, mo, d, h int) int64 {
		return time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC).UnixNano()
	}
	t0 := mk(2024, 1, 15, 10) // Mon
	t1 := mk(2024, 1, 15, 14)
	t2 := mk(2024, 1, 16, 10) // Tue
	t3 := mk(2024, 1, 20, 10) // Sat
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tag := storage.BuildDict([]string{"a", "b", "c", "d"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{t0, t1, t2, t3}},
		{Name: "tag", Type: storage.ColDict, Dict: &tag},
	}}); err != nil {
		t.Fatal(err)
	}
	count := func(q string, now int64) int {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		// SetNow, not `pq.Now = now`. A bare assignment leaves NowSet false,
		// resolveTimePreds then takes the To fallback -- which here is the
		// 1<<62 sentinel -- and `_time:5m` resolves into the year 2116 and
		// matches nothing. This test caught exactly that when the flag landed,
		// which is the whole argument for the flag.
		//
		// A case passing now=0 means "no request time", which is what the old
		// `now == 0` fallback expressed. It is spelled out here rather than
		// implied, and only the absolute cases use it.
		if now != 0 {
			pq.SetNow(now)
		} else {
			pq.SetNow(pq.To)
		}
		return len(RunPipeline(s, pq))
	}
	cases := []struct {
		q    string
		now  int64
		want int
	}{
		{`_time:2024-01-15`, 0, 2},               // that day: t0,t1
		{`_time:[2024-01-15, 2024-01-16]`, 0, 3}, // inclusive Jan16 day: +t2
		{`_time:>=2024-01-16`, 0, 2},             // t2,t3
		{`_time:<2024-01-16`, 0, 2},              // t0,t1
		{`_time:day_range[09:00, 11:00]`, 0, 3},  // 10:00 rows: t0,t2,t3
		{`_time:week_range[Mon, Fri]`, 0, 3},     // t0,t1,t2 weekdays; t3 Sat out
		{`_time:5m`, t3 + int64(time.Minute), 1}, // last 5m ending now: t3
		{`_time:2024-01-15 tag:a`, 0, 1},         // combines with a field filter
	}
	for _, c := range cases {
		if n := count(c.q, c.now); n != c.want {
			t.Errorf("%s = %d rows, want %d", c.q, n, c.want)
		}
	}
}

// A relative `_time:` filter on a Query with NEITHER Now nor a window set
// resolves against the clock, not against a sentinel.
//
// `Now` unset fell back to `To`, and `To` unset is the 1<<62 sentinel on the
// paths that use one -- so `Now=0, To=1<<62` resolved `_time:5m` to
// [4611685718427387904, 4611686018427387904]: a five-minute window in the year
// 2116, at HTTP 200, matching nothing. No handler reaches it (every one sets
// Now), which is what made it survive; the same was true of the `ToSet`
// collision until a caller was written that did not.
func TestARelativeTimeFilterWithNoRequestTimeUsesTheClock(t *testing.T) {
	q, err := ParseLogsQL(`_time:5m`)
	if err != nil {
		t.Fatal(err)
	}
	// Now unset, and To at the 1<<62 sentinel -- the exact configuration the
	// real paths carry and the one that produced the 2116 window. An all-zero
	// Query would also pass a fix that only special-cased zero.
	q.To = int64(1) << 62
	resolveTimePreds(q)

	var lo, hi int64
	for i := range q.Preds {
		if q.Preds[i].Kind == TimeRange {
			lo, hi = q.Preds[i].T1, q.Preds[i].T2
		}
	}
	if lo == 0 && hi == 0 {
		t.Fatal("no TimeRange predicate was resolved, so this asserts nothing")
	}
	now := time.Now().UnixNano()
	// Within a day of now in either direction is generous and still refuses
	// 2116 by a factor of ten thousand.
	const day = int64(24 * time.Hour)
	if hi < now-day || hi > now+day {
		t.Errorf("_time:5m resolved to [%d, %d]; its end is %v, not near now "+
			"(%v). An unset Now must not fall through to an unset To",
			lo, hi, time.Unix(0, hi).UTC(), time.Unix(0, now).UTC())
	}
	if want := hi - int64(5*time.Minute); lo != want {
		t.Errorf("the window is [%d, %d], which is %v wide, not five minutes",
			lo, hi, time.Duration(hi-lo))
	}
}

// A `stats ... if (<filter>)` GUARD IS A SECOND *Expr, AND IT MUST RESOLVE.
//
// `resolveTimePipes` said in its own comment that FilterPipe was "the only pipe
// holding a free-standing *Expr today", and its switch matched the comment.
// `Agg.If` is the second one: a relative `_time:` predicate under it kept its
// unresolved OFFSET (T1 = 5m as a duration, Rel = true) and would have compared
// a row's nanosecond timestamp against 300000000000 -- matching nothing, at
// HTTP 200. That is the same defect the comment's last sentence records for
// `| filter _time:5m`, reached through the pipe it did not name.
//
// GATED HERE AND NOT OVER HTTP, deliberately: the lexer swallows the closing
// bracket of `if (...)` into the time value, so
// `* | stats count() if (_time:5m) as c` is a 400 reading `bad _time value
// "5m)"` and no request can reach this today. The guard IS reachable for every
// other predicate kind, so this is one lexer fix away from being a wrong
// answer, and a gate that only tested what the lexer allows would go green on
// the day it stopped being latent.
func TestAStatsIfGuardResolves(t *testing.T) {
	const now = int64(1_700_000_000_000_000_000)
	const fiveMin = int64(5 * 60 * 1e9)

	relPred := func() *Expr {
		return &Expr{Op: OpLeaf, Pred: Pred{
			Field: "_time", Kind: TimeRange, Rel: true, T1: fiveMin,
		}}
	}

	// resolveTimePipes writes the instant onto the guard.
	sp := &StatsPipe{Aggs: []Agg{{Kind: AggCount, Alias: "c", If: relPred()}}}
	resolveTimePipes([]Pipe{sp}, now)
	got := sp.Aggs[0].If.Pred
	if got.Rel {
		t.Fatalf("the `if (...)` guard is still relative: %+v\n"+
			"An unresolved offset is compared against a row's nanosecond "+
			"timestamp, so the guard matches nothing.", got)
	}
	if got.T1 != now-fiveMin || got.T2 != now {
		t.Errorf("the guard resolved to [%d, %d), want [%d, %d)",
			got.T1, got.T2, now-fiveMin, now)
	}

	// And clonePipesResolvable must copy it, or the second run of a repeated
	// template resolves an instant the first run already produced. This is the
	// tail's shape: one parsed query, run on every poll.
	tmpl := &StatsPipe{Aggs: []Agg{{Kind: AggCount, Alias: "c", If: relPred()}}}
	pipes := []Pipe{tmpl}
	cloned := clonePipesResolvable(pipes)
	if cloned[0] == pipes[0] {
		t.Fatal("clonePipesResolvable shared the StatsPipe, so resolving the " +
			"clone mutates the template")
	}
	resolveTimePipes(cloned, now)
	if !tmpl.Aggs[0].If.Pred.Rel {
		t.Error("resolving the clone resolved the TEMPLATE's guard: a repeated " +
			"query resolves offsets the previous run already turned into instants")
	}

	// A stats pipe with no guard is SHARED, so the common query pays nothing.
	plain := &StatsPipe{Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}
	if got := clonePipesResolvable([]Pipe{plain}); got[0] != plain {
		t.Error("a stats pipe with no `if (...)` guard was copied; nothing " +
			"resolveTimePipes writes to lives in it")
	}
}
