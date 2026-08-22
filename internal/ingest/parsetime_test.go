package ingest

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestParseTimeShapes guards the allDigits fast-path guard: every shape that
// parsed before must still parse to the same nanos, and non-numeric input must
// not reach ParseInt (which was allocating a discarded error per row).
func TestParseTimeShapes(t *testing.T) {
	rfc := "2024-05-17T03:21:09.987654321Z"
	want, err := time.Parse(time.RFC3339Nano, rfc)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		in string
		ns int64
		ok bool
		// outOfRange marks the third outcome: a value that PARSES as a time
		// and cannot be stored. It is not `ok`, and it is not ordinary data
		// either -- the caller refuses and counts the record rather than
		// stamping it with the fallback clock. See ErrTimeOutOfRange.
		outOfRange bool
	}{
		{in: "1700000000000000000", ns: 1_700_000_000_000_000_000, ok: true}, // unix nanos
		{in: "-1000", ns: -1000, ok: true},                                   // signed
		{in: rfc, ns: want.UnixNano(), ok: true},                             // RFC3339Nano
		{in: "2024-05-17T03:21:09Z", ns: time.Date(2024, 5, 17, 3, 21, 9, 0, time.UTC).UnixNano(), ok: true},
		{in: ""},
		{in: "not-a-time"},
		{in: "-"},
		{in: "12x34"},
		// The ends of the storable range, and one nanosecond past each.
		{in: "2262-04-11T23:47:16.854775807Z", ns: math.MaxInt64, ok: true},
		{in: "1677-09-21T00:12:43.145224192Z", ns: math.MinInt64, ok: true},
		{in: "2262-04-11T23:47:16.854775808Z", outOfRange: true},
		{in: "1677-09-21T00:12:43.145224191Z", outOfRange: true},
		{in: "9999-01-01T00:00:00Z", outOfRange: true},
		{in: "3000-01-01T00:00:00Z", outOfRange: true},
		{in: "1000-01-01T00:00:00Z", outOfRange: true},
		{in: "3000-01-01 00:00:00", outOfRange: true},
		// THE ALL-DIGITS SPELLING, which is the only one Loki and OTLP send.
		//
		// ParseInt fails these with ErrRange and the value then FELL THROUGH
		// to the layouts -- none of which matches a run of digits -- so
		// parseTime reported "not a timestamp at all" and the caller stamped
		// the row with the receiver's clock. Every row of this table above was
		// a date LAYOUT, so nothing here could see it, and `TestAllDigits`
		// twenty lines down already listed "9999999999999999999" while
		// asserting only that allDigits RECOGNISES it.
		{in: "253402300800000000000", outOfRange: true}, // year 9999 in ns
		{in: "9999999999999999999", outOfRange: true},   // 19 nines: one digit's worth past MaxInt64
		{in: "-9999999999999999999", outOfRange: true},  // and the same the other way
		{in: "99999999999999999999999", outOfRange: true},
		// The exact ends of the int64 domain, spelled as digits. These are
		// storable and must stay accepted: a refusal one digit wide the wrong
		// way would satisfy every row above.
		{in: "9223372036854775807", ns: math.MaxInt64, ok: true},
		{in: "-9223372036854775808", ns: math.MinInt64, ok: true},
		{in: "9223372036854775808", outOfRange: true},  // one past MaxInt64
		{in: "-9223372036854775809", outOfRange: true}, // one past MinInt64
	}
	for _, c := range cases {
		got, ok, err := parseTime(c.in)
		if ok != c.ok {
			t.Errorf("parseTime(%q) ok=%v want %v (err=%v)", c.in, ok, c.ok, err)
			continue
		}
		if errors.Is(err, ErrTimeOutOfRange) != c.outOfRange {
			t.Errorf("parseTime(%q) out-of-range=%v want %v (err=%v)",
				c.in, errors.Is(err, ErrTimeOutOfRange), c.outOfRange, err)
		}
		if ok && got != c.ns {
			t.Errorf("parseTime(%q) = %d want %d", c.in, got, c.ns)
		}
	}
}

func TestAllDigits(t *testing.T) {
	for _, s := range []string{"0", "123", "-5", "+7", "9999999999999999999"} {
		if !allDigits(s) {
			t.Errorf("allDigits(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "-", "+", "1.5", "2024-05-17T03:21:09Z", "abc", "1a"} {
		if allDigits(s) {
			t.Errorf("allDigits(%q) = true, want false", s)
		}
	}
}

// A `_time` THAT ARRIVED AS A JSON NUMBER GETS THE SAME RANGE CHECK.
//
// jsonline's Number arm was `ts, haveTS = val.Int(), true`: no parseTime, no
// nanosOf, no range check. Measured on /insert/jsonline, every one answering
// 200 {"ingested":4,"skipped":0}:
//
//	{"_time":9.3e18}        stored at 1677-09-21T00:12:43.145224192Z
//	{"_time":2534023008e11} stored at 1677-09-21T00:12:43.145224192Z
//	{"_time":-9.3e18}       stored at 1677-09-21T00:12:43.145224192Z
//
// Three instants, two directions, one fabricated result -- int64() of an
// out-of-range float64 is implementation-defined and is MinInt64 on amd64.
func TestANumericTimeIsRangeCheckedLikeAQuotedOne(t *testing.T) {
	for _, c := range []struct {
		in         string
		ns         int64
		ok         bool
		outOfRange bool
	}{
		// Integer literals keep their digits: parseTime, and its range refusal.
		{in: "1700000000000000000", ns: 1_700_000_000_000_000_000, ok: true},
		{in: "9223372036854775807", ns: math.MaxInt64, ok: true},
		{in: "-9223372036854775808", ns: math.MinInt64, ok: true},
		{in: "9223372036854775808", outOfRange: true},
		{in: "253402300800000000000", outOfRange: true},
		// Floats and exponent forms, which never reached a check at all.
		{in: "1.7e18", ns: 1_700_000_000_000_000_000, ok: true},
		{in: "9.3e18", outOfRange: true},
		{in: "-9.3e18", outOfRange: true},
		{in: "2534023008e11", outOfRange: true},
		// The float bound is 2^63, not float64(MaxInt64) -- they are the SAME
		// number, and a `> float64(MaxInt64)` test admits it and converts it
		// to MinInt64.
		{in: "9223372036854775808.0", outOfRange: true},
		{in: "-9223372036854775808.0", ns: math.MinInt64, ok: true},
		{in: "9.2e18", ns: 9_200_000_000_000_000_000, ok: true},
	} {
		got, ok, err := numberTime([]byte(c.in))
		if ok != c.ok {
			t.Errorf("numberTime(%s) ok=%v want %v (err=%v)", c.in, ok, c.ok, err)
			continue
		}
		if errors.Is(err, ErrTimeOutOfRange) != c.outOfRange {
			t.Errorf("numberTime(%s) out-of-range=%v want %v (err=%v)",
				c.in, errors.Is(err, ErrTimeOutOfRange), c.outOfRange, err)
		}
		if ok && got != c.ns {
			t.Errorf("numberTime(%s) = %d want %d", c.in, got, c.ns)
		}
	}
	// NaN and the infinities: neither a storable instant nor ordinary data.
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, ok, err := floatNanos(f); ok || !errors.Is(err, ErrTimeOutOfRange) {
			t.Errorf("floatNanos(%v) ok=%v err=%v, want a range refusal", f, ok, err)
		}
	}
}

// A WARNING'S POSITION IS THE RECORD'S, AND IT IS IN THE RECORD FIELD.
//
// `Warning.Offset` is documented as a BYTE offset into the body. Eight call
// sites wrote `res.Warn(int64(ordinal), ...)` into it -- jsonline (three),
// logfmt, loki, lokipb, otel and options -- so record 3 of a batch was recorded
// as byte 3 of the body. It compiled, it read plausibly, and nothing was red,
// because NOTHING READS THE FIELD: the API layer renders `w.Msg` and drops the
// position entirely. datadog.go is the one that got it right, and it got it
// right by passing 0 and writing down why.
//
// The number is now in `Ordinal`, and `UnknownPos` is what a parser says when
// it cannot give one -- zero is not usable for that, because byte 0 and record
// 0 are both real positions and "0" is what six sites meant by "no idea".
//
// THE THIRD RECORD IS THE ONE THAT FAILS in every body below, so an
// implementation that reported 0, or the byte offset, or the count of
// rejections, is red rather than accidentally right.
func TestAWarningNamesTheRecordItCameFrom(t *testing.T) {
	// A far-future timestamp in the spelling each protocol actually sends.
	const ns9999 = "253402300800000000000"
	ok1 := `{"_time":"2026-06-01T12:00:00Z","_msg":"a"}`
	ok2 := `{"_time":"2026-06-01T12:00:01Z","_msg":"b"}`

	for _, tc := range []struct {
		name string
		run  func(w *Writer) (Result, error)
	}{
		{"jsonline", func(w *Writer) (Result, error) {
			body := ok1 + "\n" + ok2 + "\n" +
				`{"_time":"` + ns9999 + `","_msg":"c"}` + "\n"
			return IngestJSONLines(w, []byte(body), func() int64 { return 42 })
		}},
		{"logfmt", func(w *Writer) (Result, error) {
			body := "_time=2026-06-01T12:00:00Z _msg=a\n" +
				"_time=2026-06-01T12:00:01Z _msg=b\n" +
				"_time=" + ns9999 + " _msg=c\n"
			return IngestLogfmt(w, []byte(body), func() int64 { return 42 })
		}},
		{"loki", func(w *Writer) (Result, error) {
			body := `{"streams":[{"stream":{"app":"a"},"values":[` +
				`["1780315200000000000","a"],["1780315200000000001","b"],` +
				`["` + ns9999 + `","c"]]}]}`
			return IngestLoki(w, []byte(body), func() int64 { return 42 })
		}},
		{"otlp json", func(w *Writer) (Result, error) {
			body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[` +
				`{"timeUnixNano":"1780315200000000000","body":{"stringValue":"a"}},` +
				`{"timeUnixNano":"1780315200000000001","body":{"stringValue":"b"}},` +
				`{"timeUnixNano":"` + ns9999 + `","body":{"stringValue":"c"}}]}]}]}`
			return IngestOTLPLogs(w, []byte(body), func() int64 { return 42 })
		}},
		{"datadog", func(w *Writer) (Result, error) {
			body := `[{"message":"a","timestamp":1780315200000},` +
				`{"message":"b","timestamp":1780315200001},` +
				`{"message":"c","timestamp":253402300800000}]`
			return IngestDatadog(w, []byte(body), func() int64 { return 42 })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := storage.OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			w := NewWriter(st)
			res, err := tc.run(w)
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			if res.Rejected != 1 || res.Accepted != 2 {
				t.Fatalf("accepted=%d rejected=%d, want 2/1: %+v",
					res.Accepted, res.Rejected, res.Warnings)
			}
			if len(res.Warnings) != 1 {
				t.Fatalf("%d warnings, want 1: %+v", len(res.Warnings), res.Warnings)
			}
			got := res.Warnings[0]
			if got.Ordinal != 2 {
				t.Errorf("the warning names record %d; the third record (ordinal 2) "+
					"is the one that failed.\nA client matches warnings to its own "+
					"batch by this number: %+v", got.Ordinal, got)
			}
			if got.Offset != UnknownPos {
				t.Errorf("the warning claims byte offset %d. None of these parsers "+
					"knows a byte offset -- they decoded the body into a struct or "+
					"scanned it by line -- and an ordinal in the byte field is the "+
					"defect this gate is named for: %+v", got.Offset, got)
			}
		})
	}
}
