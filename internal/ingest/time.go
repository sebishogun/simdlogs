package ingest

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02 15:04:05",
}

// minStorable and maxStorable are the only instants a stored timestamp can
// hold: int64 nanoseconds since the epoch reach 1677-09-21T00:12:43.145224192Z
// through 2262-04-11T23:47:16.854775807Z. `time.Parse` accepts any year.
var (
	minStorable = time.Unix(0, math.MinInt64)
	maxStorable = time.Unix(0, math.MaxInt64)
)

// ErrTimeOutOfRange is a `_time` that PARSES and cannot be stored.
//
// The row is REFUSED and counted, not stored. The alternative -- saturating,
// which is what every query BOUND in this tree does with the same instants --
// is right for a comparison and wrong for a fact. A bound past 2262 means the
// +infinity it saturates to, so `gte` it matches nothing and `lte` it matches
// everything, which is what the client asked for. A ROW's timestamp is not a
// comparison: clamping 9999-01-01 to 2262-04-11 files the row under an instant
// the client never sent, where a query for 2262 returns it and a retention pass
// for 2262 deletes it. There is no clamped value that is not a fabrication.
//
// What the tree did instead was neither. `t.UnixNano()` wrapped, so the row was
// accepted at HTTP 200, counted as ingested, written to the store -- and landed
// outside every window a query can name, unreachable and unmentioned. Measured
// on four rows through /insert/jsonline (one normal, year 9999, year 3000, year
// 1000): `{"ingested":4,"skipped":0}` and `query=*` answering ONE row.
//
// internal/api/cluster.go makes the same call on the same shape, for a bucket
// timestamp arriving from a shard: out of range is refused, because "converting
// it wraps to an unrelated date".
var ErrTimeOutOfRange = fmt.Errorf("simdlogs: timestamp outside the storable range " +
	"(1677-09-21T00:12:43Z to 2262-04-11T23:47:16Z)")

// tsRangeError is ErrTimeOutOfRange carrying the value that was refused, with
// the message built in Error() rather than at construction.
//
// ONE TYPE, TWO PRODUCERS, because both are per-record reject paths.
//
// AND SIX SITES OF THE SAME SHAPE ARE NOT CONVERTED. The list has been wrong
// twice: round 19 named three, round 20 named six and put one of them on the
// wrong side of the class, and both versions cited LINE NUMBERS that drifted
// within two rounds -- `time.go:72` came to point at a sentence and
// `time.go:122` at a closing brace, each 21 lines above the real site, which
// is the length of the list that named them. Functions, not lines.
//
// THE CLASS, MEASURED RATHER THAN REASONED ABOUT. `testing.AllocsPerRun` over
// 200,000 calls past the bound, on this tree:
//
//	discarded WarnAt / Warn, any argument shape        0.000 allocs
//	  ("%v", err) / ("%s", err) / no arguments /
//	  ("%v: %d s + %d ns", err, int64, int64) /
//	  ("%q: %d bytes ...", string, int)
//	fmt.Errorf on a reject arm                         2.000 allocs
//
// So warnFull covers EVERYTHING handed to WarnAt -- the variadic slice and the
// int-to-any boxing both stay on the caller's stack when the call returns
// early -- and covers NOTHING built before WarnAt is reached. `lokipb.go`'s
// three-argument WarnAt was listed as a member and is not one: it costs
// nothing. The members are the `fmt.Errorf` sites, and there are five
// functions:
//
//	nanosOf              (this file, below) -- per record through parseLayout,
//	                     so every date-LAYOUT spelling of an unstorable instant
//	floatNanos           (this file, below) -- the float arm of numberTime
//	ddTime               (datadog.go) -- "%w: %v ms since the epoch", per entry
//	IngestOTLPLogsProto  (otelproto.go) -- twice, time_unix_nano and
//	                     observed_time_unix_nano past MaxInt64, per record
//	IngestJSONLinesOpts  (jsonline.go) -- the vector arm, "%s has an unusable
//	                     value for a %d-dimension field", per record
//
// AND THE LAST ONE IS ON THE `_bulk` PATH, which the previous version of this
// list explicitly denied of every member ("None is on the `_bulk` path, which
// is why none was converted"). `/_bulk` and `/insert/jsonline` both call
// IngestJSONLinesOpts, and a bulk body of documents whose embedding is the
// wrong length pays two allocations per document with the message thrown away
// past the 32nd. That is the reason the exemption was granted, and it did not
// hold.
// journald's byte scan got this form in round 19 (see tsRangeErr: three
// allocations for 200 bytes became two for 48) and parseTime -- the arm every
// `_bulk` document with an out-of-range decimal `_time` reaches -- kept
// `fmt.Errorf("%w: %s ns since the epoch", ...)` one file over. A _bulk at the
// action cap pays it up to 1,048,575 times, and the string is then handed to
// WarnAt, which drops it past the 32nd warning.
type tsRangeError struct {
	value string
	unit  string // "us" or "ns": the two per-record parsers that refuse a value
}

func (e *tsRangeError) Error() string {
	return ErrTimeOutOfRange.Error() + ": " + e.value + " " + e.unit + " since the epoch"
}

func (e *tsRangeError) Unwrap() error { return ErrTimeOutOfRange }

// nanosOf is t.UnixNano() with the wrap made visible instead of silent.
func nanosOf(t time.Time) (int64, error) {
	if t.Before(minStorable) || t.After(maxStorable) {
		return 0, fmt.Errorf("%w: %s", ErrTimeOutOfRange, t.UTC().Format(time.RFC3339))
	}
	return t.UnixNano(), nil
}

func parseLayout(layout, s string) (int64, error) {
	t, err := time.Parse(layout, s)
	if err != nil {
		return 0, err
	}
	return nanosOf(t)
}

// numberTime reads a `_time` that arrived as a JSON NUMBER, in nanoseconds.
//
// One function for the two spellings of the same instant. `{"_time":"17e17"}`
// and `{"_time":17e17}` are the same timestamp to every client that writes
// them, and the tree had a range-checked path for the first and a bare
// `Value.Int()` for the second.
//
// An integer literal keeps its digits and goes through parseTime, so the
// nanosecond count above 2^53 is exact and ParseInt's range refusal applies.
// Anything else is a float64 -- `9.3e18`, `2534023008e11`, `1.7e18` -- and
// float64 -> int64 is IMPLEMENTATION-DEFINED outside the int64 range, which on
// amd64 is MinInt64 in BOTH directions. That is why the two probes above
// landed on the same instant going opposite ways.
func numberTime(raw []byte) (int64, bool, error) {
	if isIntegerLiteral(raw) {
		return parseTime(string(raw))
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, false, nil // not a number this can read: ordinary data
	}
	return floatNanos(f)
}

// floatNanos converts a float64 nanosecond count, REFUSING what int64 cannot
// hold rather than saturating it. See ErrTimeOutOfRange for why a row's
// timestamp is refused where a query's bound is saturated.
//
// The bound is 2^63 exactly, not MaxInt64: float64 cannot represent MaxInt64,
// and `float64(math.MaxInt64)` IS 2^63 -- so a `f > float64(math.MaxInt64)`
// test admits 2^63 itself and converts it to MinInt64. MinInt64 is -2^63
// exactly and is representable, so the low end is inclusive and the high end
// is not. NaN fails both comparisons and needs its own arm; the infinities do
// not, and are refused by the same bound.
func floatNanos(f float64) (int64, bool, error) {
	const twoPow63 = 9223372036854775808.0 // 2^63: one past MaxInt64
	if math.IsNaN(f) || f >= twoPow63 || f < -twoPow63 {
		return 0, false, fmt.Errorf("%w: %v ns since the epoch", ErrTimeOutOfRange, f)
	}
	return int64(f), true, nil
}
