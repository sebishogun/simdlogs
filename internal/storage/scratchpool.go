package storage

import "sync"

// Scratch buffers for the FOR-block posting decoder.
//
// decodeForRun's bulk path allocates two []uint32 that are dead the moment it
// returns: `words`, the block's bytes widened to uint32, and `scratch`, the
// unpack target. Both are fully overwritten before they are read -- `words` by
// its own loop, `scratch` by simd.BitUnpackInto, which writes every element of
// dst (whole 32-blocks through the width-specialized kernel, the remainder
// through the general path). So the zeroing `make` performs is work whose
// result is never observed.
//
// At the low-cardinality shape that is 260,802 B/op in three allocations per
// decoded run, and a posting list touches this once per run per group.
//
// Pooling is only safe because of that "fully overwritten" property. A buffer
// handed back by the pool holds a previous run's values, so any path that read
// an element before writing it would silently return another run's row IDs.
// TestPooledDecodeMatchesUnpooled dirties the pool deliberately and compares,
// which is the assertion that property is really held.
var u32Scratch = sync.Pool{
	New: func() any {
		// A pointer, not a slice: a []uint32 in an interface allocates on
		// every Put, which is the cost the pool exists to remove.
		s := make([]uint32, 0, 1024)
		return &s
	},
}

// getU32 returns a slice of exactly n elements whose contents are unspecified.
// The caller must write every element it reads.
func getU32(n int) (*[]uint32, []uint32) {
	p := u32Scratch.Get().(*[]uint32)
	if cap(*p) < n {
		// Grown, not appended to: the old array is garbage either way, and
		// rounding up keeps a workload with slowly-growing runs from
		// reallocating on nearly every call.
		*p = make([]uint32, n, n+n/4)
	}
	return p, (*p)[:n]
}

func putU32(p *[]uint32) {
	*p = (*p)[:0]
	u32Scratch.Put(p)
}

// poolScratch selects the allocation strategy. It exists so both arms compile
// into ONE test binary and can be benchmarked interleaved in a single session:
// comparing two builds would put the 8.3% code-layout noise floor between them
// and hide a difference this size. Production always takes the pooled path.
var poolScratch = true
