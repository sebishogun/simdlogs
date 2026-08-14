package storage

import (
	"math/rand"
	"testing"
)

// Pooling decodeForRun's two scratch buffers is only correct because both are
// fully overwritten before anything reads them. If that ever stops being true
// -- a width whose unpack leaves a tail untouched, a guard that returns early
// after a partial write -- a pooled buffer hands the caller a PREVIOUS run's
// row IDs, and the result is wrong rows rather than a crash. Nothing else in
// the suite would notice, because the unpooled path zeroes and so hides it.
//
// So the pool is deliberately dirtied with a value that cannot occur, and the
// two paths are compared element for element.
func TestPooledDecodeMatchesUnpooled(t *testing.T) {
	const poison = 0xDEADBEEF

	rng := rand.New(rand.NewSource(9))
	for _, w := range []int{1, 2, 3, 4, 5, 7, 8, 11, 16, 23, 32} {
		for _, cnt := range []int{forBulkThreshold, forBulkThreshold + 1, 100, 257, 1024} {
			for _, lo := range []int{0, 1, 31, 32, 33} {
				// A block wide enough for lo+cnt values at this width, padded
				// to the 32-value multiple the writer guarantees.
				nVals := ((lo + cnt + 31) / 32) * 32
				blk := make([]byte, ((nVals*w+31)/32)*4+64)
				for i := range blk {
					blk[i] = byte(rng.Intn(256))
				}

				poolScratch = false
				want := decodeForRun(blk, w, lo, cnt, 1<<30)

				// Dirty every buffer the pooled path could draw, so a read of
				// an unwritten element returns poison rather than a zero that
				// might coincidentally match.
				for _, n := range []int{len(blk) / 4, nVals, nVals * 2} {
					p, s := getU32(n)
					for i := range s {
						s[i] = poison
					}
					putU32(p)
				}

				poolScratch = true
				got := decodeForRun(blk, w, lo, cnt, 1<<30)

				if len(got) != len(want) {
					t.Fatalf("w=%d cnt=%d lo=%d: pooled returned %d values, unpooled %d",
						w, cnt, lo, len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("w=%d cnt=%d lo=%d: value %d is %#x pooled, %#x unpooled",
							w, cnt, lo, i, got[i], want[i])
					}
				}
			}
		}
	}
	poolScratch = true
}

// getU32 must return exactly the requested length, and a buffer big enough to
// be reused must actually be reused rather than reallocated -- the reuse is
// the whole point.
func TestGetU32LengthAndReuse(t *testing.T) {
	// One round trip proves nothing under the race detector: sync.Pool.Put
	// drops the value at random one time in four when it is enabled
	// (go/src/sync/pool.go, "Randomly drop x on floor"), which failed this
	// test in a quarter of all `go test -race` runs. Reuse has to happen
	// within a few attempts, not on a particular one.
	reused := false
	for i := 0; i < 24 && !reused; i++ {
		p, s := getU32(600)
		if len(s) != 600 {
			t.Fatalf("getU32(600) returned len %d", len(s))
		}
		s[599] = 7
		putU32(p)

		p2, s2 := getU32(600)
		if len(s2) != 600 {
			t.Fatalf("getU32(600) returned len %d on reuse", len(s2))
		}
		reused = &s2[0] == &s[0]
		putU32(p2)
	}
	if !reused {
		t.Error("a 600-element request did not reuse the 600-element buffer just returned")
	}

	// Growing past capacity must still return the right length.
	p3, s3 := getU32(500_000)
	if len(s3) != 500_000 {
		t.Fatalf("getU32(500000) returned len %d", len(s3))
	}
	putU32(p3)
}

// The pooled path must not hold a reference the caller can observe: out is
// returned, the scratch buffers are not, and returning a slice that aliases a
// pooled buffer would be a use-after-free in all but name.
func TestDecodeResultDoesNotAliasThePool(t *testing.T) {
	blk := make([]byte, 4096)
	for i := range blk {
		blk[i] = byte(i)
	}
	got := decodeForRun(blk, 8, 0, 512, 1<<30)
	if len(got) == 0 {
		t.Skip("this block decoded no values; nothing to alias")
	}
	before := append([]uint32(nil), got...)

	// Draw and scribble on every buffer the decode just returned to the pool.
	for _, n := range []int{len(blk) / 4, 512, 1024, 2048} {
		p, s := getU32(n)
		for i := range s {
			s[i] = 0xFFFFFFFF
		}
		putU32(p)
	}
	for i := range before {
		if got[i] != before[i] {
			t.Fatalf("result value %d changed from %#x to %#x after the pool was reused: "+
				"the returned slice aliases a pooled buffer", i, before[i], got[i])
		}
	}
}
