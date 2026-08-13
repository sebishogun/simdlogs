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
	lo, iv, ok := parseAbsTime(s)
	if !ok {
		return Pred{}, fmt.Errorf("simdlogs: bad _time value %q", s)
	}
	return Pred{Field: "_time", Kind: TimeRange, T1: lo, T2: lo + iv}, nil
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
	lo, iv, ok := parseAbsTime(arg)
	if !ok {
		return Pred{}, fmt.Errorf("simdlogs: bad _time bound %q", arg)
	}
	switch op {
	case ">=":
		p.T1 = lo
	case ">":
		p.T1 = lo + iv
	case "<=":
		p.T2 = lo + iv
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
		lo, iv, ok := parseAbsTime(aStr)
		if !ok {
			return Pred{}, fmt.Errorf("simdlogs: bad _time range start %q", aStr)
		}
		if loInc {
			p.T1 = lo
		} else {
			p.T1 = lo + iv
		}
	}
	if bStr != "" {
		hi, iv, ok := parseAbsTime(bStr)
		if !ok {
			return Pred{}, fmt.Errorf("simdlogs: bad _time range end %q", bStr)
		}
		if hiInc {
			p.T2 = hi + iv // inclusive: cover the end interval
		} else {
			p.T2 = hi
		}
	}
	return p, nil
}

// parseDurationNs parses a compound duration (1h30m, 5m, 90s, 3d, 1w) to ns.
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
		total += int64(num) * mult
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

// parseAbsTime parses an absolute timestamp, returning its start nanos and the
// width of the interval its precision represents (so a bare day covers 24h).
func parseAbsTime(s string) (int64, int64, bool) {
	for _, l := range timeLayouts {
		t, err := time.Parse(l.layout, s)
		if err != nil {
			continue
		}
		t = t.UTC()
		iv := l.interval
		if iv == 0 { // month or year: interval to the next unit
			switch l.layout {
			case "2006-01":
				iv = t.AddDate(0, 1, 0).UnixNano() - t.UnixNano()
			case "2006":
				iv = t.AddDate(1, 0, 0).UnixNano() - t.UnixNano()
			}
		}
		return t.UnixNano(), iv, true
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

// resolveTimePreds turns relative _time predicates absolute using q.Now (or q.To
// when Now is unset), once, and narrows q.From/q.To by any top-level absolute
// _time window so whole groups can still be skipped. Idempotent.
func resolveTimePreds(q *Query) {
	now := q.Now
	if now == 0 {
		now = q.To
	}
	resolveTimeExpr(q.Filter, now)
	for i := range q.Preds {
		resolveTimePred(&q.Preds[i], now)
	}
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
	if to < q.To || q.To == 0 {
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
	from, to := int64(math.MinInt64), now
	if p.T1 != math.MaxInt64 {
		from = now - p.T1
	} else {
		from = math.MinInt64
	}
	if p.T2 != 0 {
		to = now - p.T2
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
		from, to := p.T1, p.T2
		if from == math.MinInt64 {
			from = 0
		}
		mask := g.TimeRangeMaskInto("_time", from, to, nil)
		packBools(b, mask)
	case TimeDayRange, TimeWeekRange:
		ts := g.Timestamps("_time", nil, nil)
		for i := 0; i < n && i < len(ts); i++ {
			tm := time.Unix(0, ts[i]).UTC()
			switch p.Kind {
			case TimeDayRange:
				min := int64(tm.Hour()*60 + tm.Minute())
				if min >= p.T1 && min <= p.T2 {
					b.Set(i)
				}
			case TimeWeekRange:
				if p.T1&(1<<uint(tm.Weekday())) != 0 {
					b.Set(i)
				}
			}
		}
	}
	return b
}
