package storage

import (
	"math/rand"
	"testing"
)

func TestDictRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	vocab := []string{"info", "warn", "error", "debug", "trace", "fatal"}
	for _, n := range []int{0, 1, 31, 32, 33, 1000, 128 * 1024} {
		vals := make([]string, n)
		for i := range vals {
			vals[i] = vocab[rng.Intn(len(vocab))]
		}
		col := BuildDict(vals)
		w := bitWidth(len(col.Dict))
		enc := encodeIndices(col.Indices, w)
		got := decodeIndices(enc, n, w)
		if len(got) != n {
			t.Fatalf("n=%d: decoded %d", n, len(got))
		}
		for i := range col.Indices {
			if got[i] != col.Indices[i] {
				t.Fatalf("n=%d [%d]: idx %d want %d", n, i, got[i], col.Indices[i])
			}
			if col.Dict[got[i]] != vals[i] {
				t.Fatalf("n=%d [%d]: value %q want %q", n, i, col.Dict[got[i]], vals[i])
			}
		}
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for _, n := range []int{0, 1, 100, 1_000_000} {
		ts := make([]int64, n)
		var t0 int64 = 1_700_000_000_000_000_000
		for i := range ts {
			t0 += int64(rng.Intn(2000))
			ts[i] = t0
		}
		enc := encodeTimestamps(ts)
		got := decodeTimestamps(enc, n)
		if len(got) != n {
			t.Fatalf("n=%d decoded %d", n, len(got))
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("n=%d [%d]: %d want %d", n, i, got[i], ts[i])
			}
		}
	}
}

func TestTimestampDecodeZeroAlloc(t *testing.T) {
	ts := make([]int64, 100_000)
	var t0 int64 = 1_700_000_000_000_000_000
	for i := range ts {
		t0 += 1000
		ts[i] = t0
	}
	enc := encodeTimestamps(ts)
	out := make([]int64, len(ts))
	raw := make([]uint64, len(ts))
	// The steady-state decode into caller buffers: no allocation.
	allocs := testing.AllocsPerRun(10, func() {
		got, _ := decodeInto(enc, raw, out)
		_ = got
	})
	if allocs != 0 {
		t.Fatalf("decode allocated %v per run", allocs)
	}
}
