package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// In-query _time filters (LogsQL `_time:<expr>`). The lexer captures the whole
// expression as one tTimeVal token; parseTimeExpr turns it into a Pred:
//
//	_time:5m                    last 5 minutes (relative to Now)
//	_time:2024-01-15            that whole day
//	_time:2024-01-15T10:30:00Z  that second
//	_time:[a, b] (a, b] ...     range, bracket controls inclusivity, empty side = open
//	_time:>=a  _time:<b         one-sided
//	_time:day_range[08:00, 18:00]   minute-of-day window (UTC)
//	_time:week_range[Mon, Fri]      weekday window (UTC)
//
// Relative forms carry offsets in T1/T2 with Rel=true; resolveTimePreds turns
// them absolute using q.Now once, before evaluation.

func isTimePred(k PredKind) bool {
	return k == TimeRange || k == TimeDayRange || k == TimeWeekRange
}

// parseTimeExpr parses a _time:<expr> value into a predicate.
func parseTimeExpr(raw string) (Pred, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Pred{}, fmt.Errorf("simdlogs: empty _time filter")
	}
	switch {
	case strings.HasPrefix(s, "day_range"):
		return parseDayRange(s)
	case strings.HasPrefix(s, "week_range"):
		return parseWeekRange(s)
	}
	for _, op := range []string{">=", "<=", ">", "<"} {
		if strings.HasPrefix(s, op) {
			return oneSidedTime(op, strings.TrimSpace(s[len(op):]))
		}
	}
	if s[0] == '[' || s[0] == '(' {
		return parseTimeRangeBracket(s)
	}
	// A bare point: a relative duration (window ending now) or an absolute
	// timestamp (the interval of its precision).
	if dur, ok := parseDurationNs(s); ok {
		return Pred{Field: "_time", Kind: TimeRange, Rel: true, T1: dur, T2: 0}, nil
	}
	lo, hi, ok := parseAbsTime(s)
	if !ok {
		return Pred{}, fmt.Errorf("simdlogs: bad _time value %q", s)
	}
	return Pred{Field: "_time", Kind: TimeRange, T1: lo, T2: hi}, nil
}

// oneSidedTime handles _time:>=a etc. Durations are relative to Now.
func oneSidedTime(op, arg string) (Pred, error) {
	p := Pred{Field: "_time", Kind: TimeRange, T1: math.MinInt64, T2: math.MaxInt64}
	if dur, ok := parseDurationNs(arg); ok {
		// a duration bound is a point Now-dur; Rel encodes it as offsets.
		p.Rel = true
		switch op {
		case ">=", ">":
			p.T1, p.T2 = dur, 0 // [Now-dur, Now]
		case "<=", "<":
			p.T1, p.T2 = math.MaxInt64, dur // (-inf, Now-dur]  (T1 sentinel kept absolute)
		}
		return p, nil
	}
	lo, hi, ok := parseAbsTime(arg)
	if !ok {
		return Pred{}, fmt.Errorf("simdlogs: bad _time bound %q", arg)
	}
	switch op {
	case ">=":
		p.T1 = lo
	case ">":
		p.T1 = hi
	case "<=":
		p.T2 = hi
	case "<":
		p.T2 = lo
	}
	return p, nil
}

// parseTimeRangeBracket handles [a, b], (a, b), [a, b), (a, b], with either side
// possibly empty (open). Durations inside are relative to Now (kept as offsets).
func parseTimeRangeBracket(s string) (Pred, error) {
	if len(s) < 2 {
		return Pred{}, fmt.Errorf("simdlogs: bad _time range %q", s)
	}
	loInc := s[0] == '['
	hiInc := s[len(s)-1] == ']'
	inner := s[1 : len(s)-1]
	comma := strings.IndexByte(inner, ',')
	if comma < 0 {
		return Pred{}, fmt.Errorf("simdlogs: _time range needs a comma: %q", s)
	}
	aStr := strings.TrimSpace(inner[:comma])
	bStr := strings.TrimSpace(inner[comma+1:])
	p := Pred{Field: "_time", Kind: TimeRange, T1: math.MinInt64, T2: math.MaxInt64}

	// Ranges with relative durations are rare and mixing them with absolute
	// bounds is ambiguous; support absolute endpoints (the common case).
	if aStr != "" {
		lo, next, ok := parseAbsTime(aStr)
		if !ok {
			return Pred{}, fmt.Errorf("simdlogs: bad _time range start %q", aStr)
		}
		if loInc {
			p.T1 = lo
		} else {
			p.T1 = next // exclusive: start after the whole interval named
		}
	}
	if bStr != "" {
		hi, next, ok := parseAbsTime(bStr)
		if !ok {
			return Pred{}, fmt.Errorf("simdlogs: bad _time range end %q", bStr)
		}
		if hiInc {
			p.T2 = next // inclusive: cover the end interval
		} else {
			p.T2 = hi
		}
	}
	return p, nil
}

// parseDurationNs parses a compound duration (1h30m, 5m, 90s, 3d, 1w) to ns.
//
// The scale and the accumulation both SATURATE. `9999999999s` is 317 years,
// whose product with a nanosecond is 1e19 against int64's 9.2e18, and the
// wrapped total made `_time:>9999999999s` -- "since 317 years ago", i.e. every
// row -- resolve to a bound in the future that matched none.
func parseDurationNs(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	units := map[byte]int64{
		's': int64(time.Second), 'm': int64(time.Minute), 'h': int64(time.Hour),
		'd': int64(24 * time.Hour), 'w': int64(7 * 24 * time.Hour),
	}
	var total int64
	i := 0
	for i < len(s) {
		j := i
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == i || j >= len(s) {
			return 0, false
		}
		num, err := strconv.Atoi(s[i:j])
		if err != nil {
			return 0, false
		}
		mult, ok := units[s[j]]
		if !ok {
			return 0, false
		}
		total = SatAdd(total, SatScale(int64(num), mult))
		i = j + 1
	}
	return total, true
}

var timeLayouts = []struct {
	layout   string
	interval int64
}{
	{"2006-01-02T15:04:05Z07:00", int64(time.Second)},
	{"2006-01-02T15:04:05", int64(time.Second)},
	{"2006-01-02T15:04", int64(time.Minute)},
	{"2006-01-02T15", int64(time.Hour)},
	{"2006-01-02 15:04:05", int64(time.Second)},
	{"2006-01-02", int64(24 * time.Hour)},
	{"2006-01", int64(0)}, // month; interval computed below
	{"2006", int64(0)},    // year
}

// MinNanoTime and MaxNanoTime are the only instants int64 nanoseconds since
// the epoch can hold: 1677-09-21T00:12:43.145224192Z through
// 2262-04-11T23:47:16.854775807Z. `time.Parse` accepts any year, so every
// conversion into this domain has to say what it does outside it.
var (
	MinNanoTime = time.Unix(0, math.MinInt64)
	MaxNanoTime = time.Unix(0, math.MaxInt64)
)

// SatNanos is t.UnixNano() SATURATED rather than wrapped.
//
// UnixNano outside the representable range is documented as undefined and in
// practice wraps, which is not a near miss: `_time:>3000-01-01` came back as an
// instant in 1918, so a lower bound meaning "the far future" became one meaning
// "before everything" and a filter that should match nothing matched every row,
// at HTTP 200 in a structurally valid answer. Below the range it wraps the
// other way, into the future.
//
// A saturated BOUND is the right answer, not merely a non-wrapping one: an
// instant past 2262 behaves as the +infinity it stands for, so `>` it matches
// nothing and `<` it matches everything, and both directions are gated.
// Saturation is wrong for an instant that is a FACT rather than a comparison --
// a row's own timestamp -- which is why the ingest path refuses those instead
// (internal/ingest/time.go).
func SatNanos(t time.Time) int64 {
	if t.After(MaxNanoTime) {
		return math.MaxInt64
	}
	if t.Before(MinNanoTime) {
		return math.MinInt64
	}
	return t.UnixNano()
}

// SatAdd is a+b clamped to the int64 range instead of wrapped. Every absolute
// `_time` bound is a start plus the width of its own precision -- a bare day
// covers 24h, a bare year covers a year -- and near the ends of the domain that
// addition overflows even when both operands are representable.
func SatAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}

// SatScale is n*unit clamped instead of wrapped: the magnitude rule that turns
// a bare epoch number into nanoseconds, and the duration parser, both multiply
// a value the caller chose by a unit this file chose.
func SatScale(n, unit int64) int64 {
	if unit == 0 {
		return 0
	}
	if n > math.MaxInt64/unit {
		return math.MaxInt64
	}
	if n < math.MinInt64/unit {
		return math.MinInt64
	}
	return n * unit
}

// parseAbsTime parses an absolute timestamp, returning the first nanosecond of
// the interval its precision represents and the first nanosecond after it (so a
// bare day covers 24h).
//
// BOTH ends saturate. This used to return the start and the interval's WIDTH
// and let each of the four call sites add them, which overflowed twice over:
// once in UnixNano for a year outside the domain, and again in the addition for
// a year inside it whose end is not (`2262-01-01` is representable and
// `2263-01-01` is not).
func parseAbsTime(s string) (start, end int64, ok bool) {
	for _, l := range timeLayouts {
		t, err := time.Parse(l.layout, s)
		if err != nil {
			continue
		}
		t = t.UTC()
		var next time.Time
		switch l.layout {
		case "2006-01": // month and year: the interval runs to the next unit
			next = t.AddDate(0, 1, 0)
		case "2006":
			next = t.AddDate(1, 0, 0)
		default:
			next = t.Add(time.Duration(l.interval))
		}
		return SatNanos(t), SatNanos(next), true
	}
	return 0, 0, false
}

// parseDayRange parses day_range[HH:MM, HH:MM] into a minute-of-day window.
func parseDayRange(s string) (Pred, error) {
	inner, err := bracketInner(s, "day_range")
	if err != nil {
		return Pred{}, err
	}
	comma := strings.IndexByte(inner, ',')
	if comma < 0 {
		return Pred{}, fmt.Errorf("simdlogs: day_range needs two times")
	}
	lo, err := parseHHMM(strings.TrimSpace(inner[:comma]))
	if err != nil {
		return Pred{}, err
	}
	hi, err := parseHHMM(strings.TrimSpace(inner[comma+1:]))
	if err != nil {
		return Pred{}, err
	}
	return Pred{Field: "_time", Kind: TimeDayRange, T1: int64(lo), T2: int64(hi)}, nil
}

// parseWeekRange parses week_range[Mon, Fri] into a weekday bitmask.
func parseWeekRange(s string) (Pred, error) {
	inner, err := bracketInner(s, "week_range")
	if err != nil {
		return Pred{}, err
	}
	comma := strings.IndexByte(inner, ',')
	if comma < 0 {
		return Pred{}, fmt.Errorf("simdlogs: week_range needs two days")
	}
	lo, err := parseWeekday(strings.TrimSpace(inner[:comma]))
	if err != nil {
		return Pred{}, err
	}
	hi, err := parseWeekday(strings.TrimSpace(inner[comma+1:]))
	if err != nil {
		return Pred{}, err
	}
	var mask int64
	for d := lo; ; d = (d + 1) % 7 {
		mask |= 1 << uint(d)
		if d == hi {
			break
		}
	}
	return Pred{Field: "_time", Kind: TimeWeekRange, T1: mask}, nil
}

func bracketInner(s, prefix string) (string, error) {
	s = strings.TrimSpace(strings.TrimPrefix(s, prefix))
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return "", fmt.Errorf("simdlogs: %s expects [a, b]", prefix)
	}
	return s[1 : len(s)-1], nil
}

func parseHHMM(s string) (int, error) {
	c := strings.IndexByte(s, ':')
	if c < 0 {
		return 0, fmt.Errorf("simdlogs: bad time-of-day %q", s)
	}
	h, err1 := strconv.Atoi(s[:c])
	m, err2 := strconv.Atoi(s[c+1:])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("simdlogs: bad time-of-day %q", s)
	}
	return h*60 + m, nil
}

func parseWeekday(s string) (int, error) {
	switch strings.ToLower(s) {
	case "sun", "sunday":
		return 0, nil
	case "mon", "monday":
		return 1, nil
	case "tue", "tuesday":
		return 2, nil
	case "wed", "wednesday":
		return 3, nil
	case "thu", "thursday":
		return 4, nil
	case "fri", "friday":
		return 5, nil
	case "sat", "saturday":
		return 6, nil
	}
	return 0, fmt.Errorf("simdlogs: bad weekday %q", s)
}

// ResolvedWindow returns the window a query text will actually be scanned
// over: [from, to] narrowed by the absolute `_time:` bounds in the query
// itself, with relative ones resolved against now.
//
// IT PARSES ITS OWN QUERY, and that is the whole point of the signature.
//
// The first version took a `*Query` and copied it -- `c := *q` -- with a
// comment claiming the copy was safe because `resolveTimePreds` is idempotent
// and its narrowing only reads the predicates. The narrowing does. The
// RESOLUTION ahead of it does not: `resolveTimePred` writes
// `p.T1, p.T2, p.Rel = from, to, false` in place, and a shallow copy shares
// the `Preds` backing array and the `Filter` pointer. So calling it froze a
// relative filter at whatever `now` happened to be first, and a later
// evaluation at a different `now` silently reused the frozen bounds:
//
//	rows at 12:00:00-12:00:29, query `_time:10s`
//	  without this call, now = base+40s                 0 rows
//	  with it at now = base+15s, then now = base+40s   10 rows
//
// Two goroutines calling it on one Query is a data race on `Pred`. Taking the
// text instead of the Query removes the aliasing rather than documenting it:
// there is no caller's state to share, because the Query this resolves is one
// nobody else has a reference to.
//
// A parse costs 37 ns for `*` and about 1.6 us for a four-pipe query with two
// aggregates -- against a fan-out to every shard, on the one call per request
// that has a coordinator half.
func ResolvedWindow(raw string, from, to, now int64) (int64, int64) {
	q, err := ParseLogsQL(raw)
	if err != nil {
		return from, to
	}
	q.SetWindow(from, to)
	q.SetNow(now)
	resolveTimePreds(q)
	return q.From, q.To
}

// resolveTimePreds turns relative _time predicates absolute using q.Now (or q.To
// when Now is unset), once, and narrows q.From/q.To by any top-level absolute
// _time window so whole groups can still be skipped. Idempotent.
func resolveTimePreds(q *Query) {
	now := q.Now
	if !q.NowSet {
		// `!q.NowSet`, not `now == 0`. The old test could not tell a caller
		// that set Now to zero from one that never set it, and the fallback
		// below is To -- whose own unset form on some paths is the 1<<62
		// sentinel. `Now=0, To=1<<62` resolved `_time:5m` into a five-minute
		// window in the year 2116, at 200.
		now = q.To
		if !q.ToSet {
			// Neither was given. The clock is the only defensible reading of
			// "relative to now" when nothing says what now is, and it is what
			// a caller who wrote `_time:5m` and set no window meant. Falling
			// through to an unset To is what produced 2116.
			now = time.Now().UnixNano()
		}
	}
	resolveTimeExpr(q.Filter, now)
	for i := range q.Preds {
		resolveTimePred(&q.Preds[i], now)
	}
	// AND THE FILTERS CARRIED BY PIPES.
	//
	// `| filter _time:5m` puts its expression in a FilterPipe, not in q.Filter,
	// and this walked only the latter -- so the pred reached the row evaluator
	// with Rel still true and T1/T2 still OFFSETS rather than instants. It
	// matched nothing, at 200:
	//
	//	_time:5m                  1 row
	//	* | filter _time:5m       0 rows
	//	* | filter _msg:recent    1 row   (the control)
	//
	// Two spellings of one filter, two answers. The same walk serves both,
	// because a FilterPipe's expression is an *Expr like any other.
	//
	// NOT included in the window narrowing below. A mid-pipe filter runs after
	// whatever precedes it, so its bounds are not bounds on the SCAN -- an
	// aggregate before it can emit rows whose timestamps are outside them.
	resolveTimePipes(q.Pipes, now)
	// Window narrowing from top-level AND absolute ranges (perf only; the
	// per-row predicate still enforces correctness).
	from, to := int64(math.MinInt64), int64(math.MaxInt64)
	collectTimeBounds(q.Filter, &from, &to)
	for i := range q.Preds {
		if q.Preds[i].Kind == TimeRange {
			tightenBounds(&q.Preds[i], &from, &to)
		}
	}
	if from > q.From {
		q.From = from
	}
	// NARROWING ONLY, unless the caller never resolved an end.
	//
	// `q.To == 0` used to be the second half of this condition, and it read a
	// RESOLVED epoch end as "unset" -- so a `_time:` filter widened the window
	// past the end the request asked for. ToSet distinguishes the two; a
	// programmatic caller that leaves To at zero still gets the filter's end.
	if to < q.To || !q.ToSet {
		if to != math.MaxInt64 {
			q.To = to
		}
	}
}

func resolveTimeExpr(e *Expr, now int64) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		resolveTimePred(&e.Pred, now)
	case OpNot:
		resolveTimeExpr(e.Child, now)
	default:
		for _, k := range e.Kids {
			resolveTimeExpr(k, now)
		}
	}
}

func resolveTimePred(p *Pred, now int64) {
	if p.Kind != TimeRange || !p.Rel {
		return
	}
	// T1/T2 hold offsets (see oneSidedTime/parseTimeExpr): from = now-T1 unless
	// T1 is the +inf sentinel; to = now unless T2 is a >0 offset.
	// SatAdd, not `now - offset`: the offsets are durations the caller wrote
	// and a saturated one (317 years, from parseDurationNs) subtracted from
	// `now` wraps into the FUTURE, turning "since a long time ago" into a bound
	// nothing matches.
	from, to := int64(math.MinInt64), now
	if p.T1 != math.MaxInt64 {
		from = SatAdd(now, -p.T1)
	} else {
		from = math.MinInt64
	}
	if p.T2 != 0 {
		to = SatAdd(now, -p.T2)
	}
	p.T1, p.T2, p.Rel = from, to, false
}

// collectTimeBounds tightens [from,to] from top-level AND absolute time ranges.
func collectTimeBounds(e *Expr, from, to *int64) {
	if e == nil {
		return
	}
	switch e.Op {
	case OpLeaf:
		if e.Pred.Kind == TimeRange {
			tightenBounds(&e.Pred, from, to)
		}
	case OpAnd:
		for _, k := range e.Kids {
			collectTimeBounds(k, from, to)
		}
	}
}

func tightenBounds(p *Pred, from, to *int64) {
	if p.Rel {
		return
	}
	if p.T1 > *from {
		*from = p.T1
	}
	if p.T2 < *to {
		*to = p.T2
	}
}

// timePredBitset marks rows whose _time satisfies the predicate. TimeRange uses
// the block-aware mask (skips blocks outside the window); day/week decode the
// column and test each timestamp in UTC.
func timePredBitset(g *storage.Reader, p *Pred, n int) *Bitset {
	b := NewBitset(n)
	switch p.Kind {
	case TimeRange:
		// MinInt64 IS A BOUND. See predMatchesRow for the measurement -- this
		// is the columnar half of the same clamp, and the two had to be
		// removed together or the row scan and the block scan would answer
		// differently for the same filter. decodeTimeRangeInto compares
		// `mx < from` and `mn >= from`, both of which are correct at MinInt64;
		// there was nothing here for the clamp to protect.
		mask := g.TimeRangeMaskInto("_time", p.T1, p.T2, nil)
		packBools(b, mask)
	case TimeDayRange, TimeWeekRange:
		ts := g.Timestamps("_time", nil, nil)
		for i := 0; i < n && i < len(ts); i++ {
			if matchDayWeek(time.Unix(0, ts[i]).UTC(), p) {
				b.Set(i)
			}
		}
	}
	return b
}

// matchDayWeek is the day-of-week and minute-of-day test, in UTC.
//
// One function because there are two evaluators -- the group scan above and
// `matchPredRow` for rows already in memory -- and two copies of a rule is two
// places for it to drift. The row evaluator had NO copy, which is why
// `| filter _time:day_range[...]` matched nothing.
func matchDayWeek(tm time.Time, p *Pred) bool {
	switch p.Kind {
	case TimeDayRange:
		min := int64(tm.Hour()*60 + tm.Minute())
		return min >= p.T1 && min <= p.T2
	case TimeWeekRange:
		return p.T1&(1<<uint(tm.Weekday())) != 0
	}
	return false
}

// CloneResolvable returns a copy of q whose time predicates can be resolved
// without touching the original's.
//
// `resolveTimePreds` turns a RELATIVE `_time:` predicate absolute IN PLACE --
// it writes T1/T2 and clears Rel through `&q.Preds[i]` and through the Filter
// tree's leaves -- and it is idempotent, so a second resolve against a later
// `now` does nothing. Both of those are right for a batch query, which resolves
// once and answers once.
//
// They are wrong for anything that re-runs the same query over time. A shallow
// `*q` shares the Preds backing array and the Filter's nodes, so resolving the
// copy freezes the ORIGINAL at that instant: `/select/logsql/tail` with
// `_time:5m` delivered its backlog and then nothing, forever, at HTTP 200 with
// the connection open -- 0 of 3 rows ingested afterwards, for every relative
// form, while absolute filters and `*` delivered all 3.
//
// EVERYTHING `resolveTimePreds` MUTATES IS DEEP-COPIED, and nothing else is.
// That is three things, not the two an earlier version of this comment claimed:
// the Preds slice, the Filter tree, and the expression inside every FilterPipe.
// The last was added with the walk that resolves it; a clone that misses what
// the resolver touches is the same defect in a different place.
//
// `Pred.re` is a compiled *regexp.Regexp, which is immutable and safe for
// concurrent use, so sharing it is correct and recompiling per poll would not
// be.
//
// NOT copied, and worth naming because it is a real limit rather than an
// oversight: `stampPipes` writes `rangeSec` and `q` into *StatsPipe,
// *UniqPipe and *TopPipe, which this clone SHARES.
//
// Inert on both callers today, and the REASON changed under this comment: it
// used to say "the tail sets `q.Pipes = nil`", and the tail no longer does --
// it keeps row-local pipes and refuses the rest. `StatsPipe`, `UniqPipe` and
// `TopPipe` are all in the refused set, so `stampPipes` still never runs on a
// tail; the conclusion survives and the reason it gave did not. Every subquery
// path re-parses.
//
// A future caller that clones a pipeline with an aggregate and runs it twice
// would write through to the template.
func (q *Query) CloneResolvable() *Query {
	c := *q
	if len(q.Preds) > 0 {
		c.Preds = make([]Pred, len(q.Preds))
		copy(c.Preds, q.Preds)
	}
	c.Filter = cloneExpr(q.Filter)
	c.Pipes = clonePipesResolvable(q.Pipes)
	return &c
}

// cloneExpr deep-copies a filter tree. The Pred values come with it; the
// compiled regexp inside each is shared deliberately.
// resolveTimePipes resolves the filter expressions pipes carry.
//
// TWO PIPES HOLD AN *Expr, not one. FilterPipe holds the obvious one, and
// StatsPipe holds one per aggregate in `Agg.If` -- the `if (<filter>)` guard.
// The comment here said FilterPipe was "the only pipe holding a free-standing
// *Expr today" and the switch matched what the comment said, so an
// `if (_time:5m)` guard would have kept its unresolved OFFSET and matched
// nothing: exactly the defect the last sentence of this comment is about,
// reached through the pipe the sentence did not name.
//
// It is LATENT rather than measurable today, and saying so is the point: the
// lexer swallows the closing bracket of `if (...)` into the time value, so
// `* | stats count() if (_time:5m) as c` is a 400 reading `bad _time value
// "5m)"`. The `if` guard is reachable for every OTHER predicate kind, and this
// is one lexer fix away from being a wrong answer. `TestAStatsIfGuardResolves`
// covers it at the package level, where the lexer is not in the way.
//
// Written as a switch rather than a single cast so that a pipe which gains one
// later is a compile-time addition here rather than a silent omission -- silent
// omission is precisely how `| filter _time:5m` came to match nothing.
func resolveTimePipes(pipes []Pipe, now int64) {
	for _, p := range pipes {
		switch t := p.(type) {
		case *FilterPipe:
			resolveTimeExpr(t.Expr, now)
		case *StatsPipe:
			for i := range t.Aggs {
				resolveTimeExpr(t.Aggs[i].If, now)
			}
		}
	}
}

// clonePipesResolvable copies the pipes whose contents resolveTimePipes
// mutates, leaving the rest shared.
//
// The same reasoning as CloneResolvable's: a template that is run repeatedly --
// a tail polling every interval -- must not be resolved in place, or the second
// run resolves offsets that the first already turned into instants. The pipes
// resolveTimePipes writes to are copied; every other pipe is shared, because
// nothing in this file writes to one.
//
// StatsPipe is copied for the same reason FilterPipe is, and it was missing for
// the same reason: `Agg.If` is an *Expr and resolveTimePipes now resolves it.
// The unexported fields (rangeSec, q) are stamped per run by RunPipeline and
// carry nothing across one, so the copy takes By and Aggs and leaves them zero
// -- which is what a freshly parsed StatsPipe looks like.
func clonePipesResolvable(pipes []Pipe) []Pipe {
	var out []Pipe
	grow := func() {
		if out == nil {
			out = make([]Pipe, len(pipes))
			copy(out, pipes)
		}
	}
	for i, p := range pipes {
		switch t := p.(type) {
		case *FilterPipe:
			grow()
			out[i] = &FilterPipe{Expr: cloneExpr(t.Expr)}
		case *StatsPipe:
			if !aggsCarryExpr(t.Aggs) {
				continue // nothing here for resolveTimePipes to write to
			}
			grow()
			aggs := make([]Agg, len(t.Aggs))
			copy(aggs, t.Aggs)
			for j := range aggs {
				aggs[j].If = cloneExpr(aggs[j].If)
			}
			out[i] = &StatsPipe{By: t.By, Aggs: aggs}
		}
	}
	if out == nil {
		return pipes
	}
	return out
}

// aggsCarryExpr reports whether any aggregate has an `if (...)` guard. A stats
// pipe without one is shared rather than copied, so the common query pays
// nothing for this.
func aggsCarryExpr(aggs []Agg) bool {
	for i := range aggs {
		if aggs[i].If != nil {
			return true
		}
	}
	return false
}

func cloneExpr(e *Expr) *Expr {
	if e == nil {
		return nil
	}
	c := *e
	if len(e.Kids) > 0 {
		c.Kids = make([]*Expr, len(e.Kids))
		for i, k := range e.Kids {
			c.Kids[i] = cloneExpr(k)
		}
	}
	c.Child = cloneExpr(e.Child)
	return &c
}
