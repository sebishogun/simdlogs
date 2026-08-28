package storage

import (
	"math/rand"
	"testing"
)

// The A/B for pooling decodeForRun's scratch buffers. Both arms are in this
// one binary and run interleaved in one session, because a two-build
// comparison would put the 8.3% code-layout noise floor between them, and the
// difference being measured is partly smaller than that. allocs/op and B/op
// are exact and load-independent; the ns/op is the weaker half and is only
// worth reading on a quiet machine.
//
// The shapes are the three BenchmarkPostingsDecodeOne already uses, since the
// low-cardinality one is where decodeForRun's bulk path does 260KB of work.
func benchDecodeShapes() []struct {
	name       string
	blk        []byte
	w, lo, cnt int
} {
	rng := rand.New(rand.NewSource(11))
	mk := func(nVals, w int) []byte {
		b := make([]byte, ((nVals*w+31)/32)*4+64)
		for i := range b {
			b[i] = byte(rng.Intn(256))
		}
		return b
	}
	return []struct {
		name       string
		blk        []byte
		w, lo, cnt int
	}{
		{"short/w8", mk(256, 8), 8, 0, forBulkThreshold},
		{"medium/w8", mk(4096, 8), 8, 0, 2048},
		{"low-card/w4", mk(131072, 4), 4, 0, 65536},
		{"low-card/w16", mk(131072, 16), 16, 0, 65536},
	}
}

func BenchmarkDecodeForRunScratch(b *testing.B) {
	shapes := benchDecodeShapes()
	// Arms interleaved per shape, so any drift over the run hits both.
	for _, s := range shapes {
		for _, arm := range []struct {
			name string
			pool bool
		}{{"pooled", true}, {"make", false}} {
			b.Run(s.name+"/"+arm.name, func(b *testing.B) {
				poolScratch = arm.pool
				defer func() { poolScratch = true }()
				b.ReportAllocs()
				b.ResetTimer()
				var sink int
				for b.Loop() {
					sink += len(decodeForRun(s.blk, s.w, s.lo, s.cnt, 1<<30))
				}
				_ = sink
			})
		}
	}
}
