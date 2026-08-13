package storage

import (
	"fmt"
	"testing"
)

// TestHexCodecRoundTrip verifies the hex nibble codec decodes identically to
// what was encoded, across block boundaries, odd lengths, and a non-hex block.
func TestHexCodecRoundTrip(t *testing.T) {
	var vals []string
	seed := uint32(12345)
	for i := 0; i < 200; i++ { // spans >3 blocks of 64
		seed = seed*1664525 + 1013904223
		n := 8 + int(seed%25) // varied, odd and even lengths
		b := make([]byte, n)
		for j := range b {
			b[j] = hexChar(byte((seed >> uint(j)) & 0xf))
		}
		vals = append(vals, string(b))
	}
	vals = append(vals, "not-hex-value!", "another_one")
	dedupSorted := BuildDict(vals).Dict
	sec := marshalDictSection(dedupSorted, false)
	for i, want := range dedupSorted {
		if got := dictSectionAt(sec, len(dedupSorted), i); got != want {
			t.Fatalf("at %d: got %q want %q", i, got, want)
		}
		if id := dictSectionSearch(sec, len(dedupSorted), want); id != i {
			t.Fatalf("search %q: got %d want %d", want, id, i)
		}
	}
	all := dictSectionAll(sec, len(dedupSorted))
	for i := range dedupSorted {
		if all[i] != dedupSorted[i] {
			t.Fatalf("all[%d]=%q want %q", i, all[i], dedupSorted[i])
		}
	}
	fmt.Fprintln(&sink, len(all))
}

var sink testWriter

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }
