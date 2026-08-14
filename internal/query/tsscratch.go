package query

import (
	"sync"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Scratch buffers for the per-group timestamp decode.
//
// appendMatches and histoGroup each decode the window's timestamps into a
// []int64 that is dead the moment they return: the times are copied into
// Row.Time and into histogram buckets, and nothing keeps the slice. At a
// 100K-row group that is 800KB allocated and thrown away per group per query,
// which was a third of every byte a full scan allocated.
//
// The scans run one group per worker, so the buffer cannot be a single shared
// slice; a pool gives each worker its own without a per-call allocation.
//
// Pooling is safe only because decodeTsRangeInto writes every element it
// returns -- including the tail after a short stream, which it now zeroes
// explicitly. A buffer handed back by the pool holds a PREVIOUS group's
// timestamps, so any element left unwritten would date another group's rows
// with times that look entirely plausible. TestPooledTimestampsMatchUnpooled
// dirties the pool deliberately and compares.
//
// Reader.TimestampsRange (the allocating form) stays the default everywhere
// else: rebuild() keeps the slice it is given as a Group's column.
var tsScratch = sync.Pool{
	New: func() any {
		// A pointer, not a slice: a []int64 in an interface allocates on
		// every Put, which is the cost the pool exists to remove.
		s := make([]int64, 0, 4096)
		return &s
	},
}

func getTs() *[]int64 { return tsScratch.Get().(*[]int64) }

func putTs(p *[]int64) {
	*p = (*p)[:0]
	tsScratch.Put(p)
}

// poolTs selects the allocation strategy. It exists so both arms compile into
// ONE test binary and can be benchmarked interleaved in a single session:
// comparing two builds would put the 8.3% code-layout noise floor between
// them. Production always pools.
var poolTs = true

// groupTimestamps decodes rows [lo,hi) of a group's _time column for a caller
// that will not keep them. The returned slice belongs to p, which must be
// handed back with putTs once the caller is done reading.
func groupTimestamps(g *storage.Reader, lo, hi int) (*[]int64, []int64) {
	if !poolTs {
		return nil, g.TimestampsRange("_time", lo, hi)
	}
	p := getTs()
	ts := g.TimestampsRangeInto((*p)[:0], "_time", lo, hi)
	if cap(ts) > cap(*p) {
		// decodeTsRangeInto outgrew the pooled array; keep the bigger one.
		*p = ts
	}
	return p, ts
}

// releaseTs hands a buffer from groupTimestamps back. It tolerates the nil the
// unpooled arm returns, so the call sites read the same in both arms.
func releaseTs(p *[]int64) {
	if p != nil {
		putTs(p)
	}
}
