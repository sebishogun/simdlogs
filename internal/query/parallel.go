package query

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// parallelMinGroups is the group count above which Run fans out. Below it
// the goroutine and merge overhead outweighs the win; a selective window
// touches one or two groups and stays serial.
const parallelMinGroups = 4

// runParallel executes q across candidate groups on a worker pool and
// merges the rows in group (time) order -- identical output to the serial
// path, which the differential test asserts. The plan's group-parallel
// execution: each group is independent work, one SIMD-scanned group per
// worker.
func runParallel(groups []*storage.Reader, q *Query) []Row {
	// From the server's budget, not GOMAXPROCS. Ten concurrent queries on a
	// 32-core box used to spawn 320 workers for 32 cores, all doing
	// memory-bound column decode and evicting each other's cache lines. See
	// budget.go.
	want := len(groups)
	workers, releaseWorkers := scanWorkers(want)
	defer releaseWorkers()
	parts := make([][]Row, len(groups))
	var wg sync.WaitGroup
	ch := make(chan int, len(groups))
	for i := range groups {
		ch <- i
	}
	close(ch)
	var produced atomic.Int64 // only used when q.MaxRows is set
	var scanned atomic.Int64  // materialized bytes, for the query budget
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gi := range ch {
				// Over the row cap: drain without scanning. The result already
				// exceeds MaxRows, so the caller errors either way -- this just
				// stops the remaining groups from materializing.
				if q.MaxRows > 0 && produced.Load() > int64(q.MaxRows) {
					q.stop(fmt.Errorf("%w: more than %d rows matched",
						ErrRowLimit, q.MaxRows))
					continue
				}
				// The deadline and the byte budget, checked between groups.
				// Checking only in the pre-filter meant neither bounded the
				// scan, which is the part that costs.
				if q.exceeded(scanned.Load()) {
					continue
				}
				// groups are already footer-pruned by the caller.
				rows := appendMatches(nil, groups[gi], q)
				parts[gi] = rows
				var n int64
				if q.countsBytes() {
					for _, r := range rows {
						n += rowBytes(r)
					}
				}
				// Record the trip AFTER adding, not only before. Every
				// worker starts with scanned == 0, so with four groups and
				// thirty-two workers the pre-check passed for all of them
				// and nothing ever observed the budget being spent.
				q.exceeded(scanned.Add(n))
				if q.MaxRows > 0 {
					produced.Add(int64(len(rows)))
				}
			}
		}()
	}
	wg.Wait()
	// Merge in group order (groups already time-sorted by the store).
	//
	// The total is known here -- every worker has finished and parts is
	// complete -- so the merge takes one exactly-sized allocation. Growing by
	// append instead reallocated the whole result log2(groups) times and
	// copied every row it had already copied, which was 47% of all bytes a
	// full scan allocated.
	var out []Row
	if mergePresize {
		total := 0
		for _, p := range parts {
			total += len(p)
		}
		if total == 0 {
			// A nil result, not an empty non-nil one: the serial path returns
			// nil for no matches and the two are compared for equality.
			return nil
		}
		out = make([]Row, 0, total)
	}
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// mergePresize selects whether runParallel sizes the merge up front. Both arms
// compile into ONE binary so they can be benchmarked interleaved in a single
// session -- a two-build comparison would put the 8.3% code-layout noise floor
// between them. Production always presizes.
var mergePresize = true

// histogramParallel is Histogram fanned across groups: each worker buckets
// its groups into a local map, merged at the end. The window at scale spans
// hundreds of groups, so this is the aggregation's parallelism.
func histogramParallel(groups []*storage.Reader, q *Query, step int64) map[int64]int {
	// From the server's budget, not GOMAXPROCS. Ten concurrent queries on a
	// 32-core box used to spawn 320 workers for 32 cores, all doing
	// memory-bound column decode and evicting each other's cache lines. See
	// budget.go.
	want := len(groups)
	workers, releaseWorkers := scanWorkers(want)
	defer releaseWorkers()
	parts := make([]map[int64]int, len(groups))
	var wg sync.WaitGroup
	ch := make(chan int, len(groups))
	for i := range groups {
		ch <- i
	}
	close(ch)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gi := range ch {
				// The deadline, per group. runParallel had this; these two
				// did not, so the budget bound the small queries that never
				// needed it and not the fan-out it exists for: 16 groups ran
				// 5.3ms past a 1ms deadline with Stopped never set.
				if q.exceeded(0) {
					continue
				}
				m := map[int64]int{}
				histoGroup(groups[gi], q, step, m)
				parts[gi] = m
			}
		}()
	}
	wg.Wait()
	out := map[int64]int{}
	for _, m := range parts {
		for k, v := range m {
			out[k] += v
		}
	}
	return out
}

// countParallel is Count fanned across groups; partials sum, no ordering.
func countParallel(groups []*storage.Reader, q *Query) int {
	// From the server's budget, not GOMAXPROCS. Ten concurrent queries on a
	// 32-core box used to spawn 320 workers for 32 cores, all doing
	// memory-bound column decode and evicting each other's cache lines. See
	// budget.go.
	want := len(groups)
	workers, releaseWorkers := scanWorkers(want)
	defer releaseWorkers()
	var total int64
	var wg sync.WaitGroup
	ch := make(chan int, len(groups))
	for i := range groups {
		ch <- i
	}
	close(ch)
	var mu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := 0
			for gi := range ch {
				if q.exceeded(0) {
					continue
				}
				// groups are already footer-pruned by the caller.
				local += matchBitset(groups[gi], q).Count()
			}
			mu.Lock()
			total += int64(local)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return int(total)
}

var _ = sort.Ints
