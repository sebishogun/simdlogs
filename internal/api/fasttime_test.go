package api

import (
	"testing"
	"time"
)

// TestFastRFC3339NanoMatchesStdlib guards the federated-merge fast path: it must
// agree with time.Parse on every shape simdlogs emits, and refuse anything else
// (so the caller falls back rather than producing a wrong ordering).
func TestFastRFC3339NanoMatchesStdlib(t *testing.T) {
	cases := []string{
		"2024-01-01T00:00:00Z",
		"2024-12-31T23:59:59Z",
		"2024-02-29T12:34:56.789Z", // leap day
		"2023-03-01T00:00:00.000000001Z",
		"2025-07-15T08:09:10.123456789Z",
		"1970-01-01T00:00:00Z",
		"1999-12-31T23:59:59.999999999Z",
		"2100-06-15T13:45:30.5Z",
	}
	for _, c := range cases {
		want, err := time.Parse(time.RFC3339Nano, c)
		if err != nil {
			t.Fatalf("stdlib rejected %q: %v", c, err)
		}
		got, ok := fastRFC3339Nano(c)
		if !ok {
			t.Errorf("%s: fast parser bailed (falls back, but should handle this)", c)
			continue
		}
		if got != want.UnixNano() {
			t.Errorf("%s: fast %d != stdlib %d", c, got, want.UnixNano())
		}
	}
	// Shapes it must refuse rather than guess at.
	for _, c := range []string{
		"2024-01-01T00:00:00+02:00", // offset, not Z
		"2024-01-01 00:00:00Z",      // space instead of T
		"not-a-time",
		"2024-01-01T00:00:0Z",
		"",
	} {
		if _, ok := fastRFC3339Nano(c); ok {
			t.Errorf("%q: fast parser accepted a shape it should refuse", c)
		}
	}
	// Every row of a realistic day must round-trip.
	base := time.Date(2024, 5, 17, 3, 21, 9, 987654321, time.UTC)
	for i := 0; i < 5000; i++ {
		ts := base.Add(time.Duration(i) * 1234567 * time.Nanosecond)
		s := ts.UTC().Format(time.RFC3339Nano)
		got, ok := fastRFC3339Nano(s)
		if !ok || got != ts.UnixNano() {
			t.Fatalf("%s: got %d ok=%v want %d", s, got, ok, ts.UnixNano())
		}
	}
}
