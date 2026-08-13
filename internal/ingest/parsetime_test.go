package ingest

import (
	"testing"
	"time"
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
		in   string
		ns   int64
		ok   bool
	}{
		{"1700000000000000000", 1_700_000_000_000_000_000, true}, // unix nanos
		{"-1000", -1000, true},                                   // signed
		{rfc, want.UnixNano(), true},                             // RFC3339Nano
		{"2024-05-17T03:21:09Z", time.Date(2024, 5, 17, 3, 21, 9, 0, time.UTC).UnixNano(), true},
		{"", 0, false},
		{"not-a-time", 0, false},
		{"-", 0, false},
		{"12x34", 0, false},
	}
	for _, c := range cases {
		got, ok := parseTime(c.in)
		if ok != c.ok {
			t.Errorf("parseTime(%q) ok=%v want %v", c.in, ok, c.ok)
			continue
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
