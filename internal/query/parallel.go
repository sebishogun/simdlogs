package query

import (
	"runtime"
	"sort"
	"sync"

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
	workers := runtime.GOMAXPROCS(0)
	if workers > len(groups) {
		workers = len(groups)
	}
	parts := make([][]Row, len(groups))
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
				// groups are already footer-pruned by the caller.
				parts[gi] = appendMatches(nil, groups[gi], q)
			}
		}()
	}
	wg.Wait()
	// Merge in group order (groups already time-sorted by the store).
	var out []Row
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// countParallel is Count fanned across groups; partials sum, no ordering.
func countParallel(groups []*storage.Reader, q *Query) int {
	workers := runtime.GOMAXPROCS(0)
	if workers > len(groups) {
		workers = len(groups)
	}
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
