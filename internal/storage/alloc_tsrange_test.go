package storage

import (
	"testing"
)

// TestDecodeTsRangeIntoShortStream is the poisoning test for the reused
// timestamp buffer. A truncated stream stops the decode partway and leaves the
// tail unwritten; with a fresh allocation that tail read as zero, and with a
// pooled buffer it would read as the PREVIOUS group's timestamps -- plausible
// times on the wrong rows. Every truncation must answer exactly what the
// allocating form answers.
func TestDecodeTsRangeIntoShortStream(t *testing.T) {
	const rows = 2048
	ts := make([]int64, rows)
	base := int64(1_700_000_000_000_000_000)
	for i := range ts {
		ts[i] = base + int64(i)*1000
	}
	full := encodeTimestamps(ts)
	hdr := tsHeaderLen(full)

	poison := func(n int) []int64 {
		s := make([]int64, n)
		for i := range s {
			s[i] = -0x0DEADBEEF
		}
		return s
	}

	// Truncations that land inside the stream, plus the whole column.
	cuts := []int{0, 1, 7, 64, 500, len(full) - hdr - 1}
	for _, cut := range cuts {
		if cut < 0 || cut > len(full)-hdr {
			continue
		}
		b := full[:len(full)-cut]
		for _, span := range [][2]int{{0, rows}, {0, 1}, {512, rows}, {1024, 1030}} {
			lo, hi := span[0], span[1]
			want := decodeTsRange(b, lo, hi)
			got := decodeTsRangeInto(poison(hi - lo)[:0], b, lo, hi)
			if len(got) != len(want) {
				t.Fatalf("cut=%d [%d,%d): into len %d, fresh len %d", cut, lo, hi, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("cut=%d [%d,%d): element %d: into %d, fresh %d", cut, lo, hi, i, got[i], want[i])
				}
			}
		}
	}

	// The truncations above must actually reach the unwritten tail, or this
	// test proves nothing: a full decode writes every element and any buffer
	// would pass.
	short := decodeTsRange(full[:len(full)-500], 0, rows)
	if short[rows-1] != 0 {
		t.Fatalf("a 500-byte truncation still decoded the last row (%d) -- the tail this test exists for was never reached", short[rows-1])
	}
}
