package query

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Governing what a query is allowed to consume, and how many of them run.
//
// # The worker pool was per query, not per server
//
// Three scan paths sized their fan-out with `runtime.GOMAXPROCS(0)`. That is
// the right number of workers for ONE query on an idle machine and the wrong
// one for every other case: ten concurrent queries on a 32-core box spawned
// 320 workers, all competing for 32 cores. The cost is not the goroutines --
// those are cheap -- it is that every one of them is doing memory-bound
// column-decode work, so the cache lines each of them touches evict the ones
// the others need, and the scheduler thrashes between them. The machine does
// less total work than it would with 32.
//
// So workers come from a budget the server owns, not from a constant each
// query reads. A query takes what is free, never waits for a slot, and never
// takes zero: waiting would make a big query stall behind another big one for
// no gain, and taking zero would deadlock a scan that has groups to walk.
//
// # Admission is separate from budgeting
//
// A worker budget shares the machine between queries that are already running.
// Admission decides whether one starts at all -- globally and per tenant --
// and that is a different question with a different answer: a server at its
// worker budget is busy and should still accept a cheap query, while a server
// at its admission limit has decided it is not going to try.
//
// Rejection is 429 and never a queue that grows without bound: a queue deeper
// than the time a client will wait is a queue that does work nobody collects,
// which is the same waste as the cancelled scans task 6.1 was about.

// ErrQueueTimeout is admission that waited and gave up. It wraps ErrRejected,
// so a caller matching the general case gets both and the HTTP layer needs one
// branch.
var ErrQueueTimeout = fmt.Errorf("%w: timed out waiting for a slot", ErrRejected)

// WorkerBudget is the shared pool of scan workers.
//
// A counter rather than a channel of tokens: a query asks for n and takes what
// is available in one atomic operation, where a channel would make it take
// them one at a time and interleave with other queries doing the same -- two
// queries each asking for 16 of 32 would routinely get 9 and 7.
type WorkerBudget struct {
	total int64
	inUse atomic.Int64
}

// NewWorkerBudget returns a budget of n workers. n <= 0 means GOMAXPROCS,
// which is the number the scan paths used to take each.
func NewWorkerBudget(n int) *WorkerBudget {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	return &WorkerBudget{total: int64(n)}
}

// Acquire takes up to want workers and returns how many were granted with the
// function that returns them.
//
// It never blocks and never grants zero. Not blocking, because a query that
// waited for its full share would stall behind another query's scan for no
// benefit -- it can make progress with fewer. Never zero, because a scan with
// no workers has no way to finish, and a budget that can deadlock a query is
// worse than one that oversubscribes by one.
func (b *WorkerBudget) Acquire(want int) (granted int, release func()) {
	if b == nil || b.total <= 0 {
		return max1(want), func() {}
	}
	if want < 1 {
		want = 1
	}
	for {
		used := b.inUse.Load()
		free := b.total - used
		n := int64(want)
		if n > free {
			n = free
		}
		if n < 1 {
			// Oversubscribed by exactly one, deliberately: see above.
			n = 1
		}
		if b.inUse.CompareAndSwap(used, used+n) {
			var once sync.Once
			return int(n), func() {
				once.Do(func() { b.inUse.Add(-n) })
			}
		}
	}
}

// InUse reports how many workers are currently out, for /metrics.
func (b *WorkerBudget) InUse() int {
	if b == nil {
		return 0
	}
	return int(b.inUse.Load())
}

// Total reports the budget's size.
func (b *WorkerBudget) Total() int {
	if b == nil {
		return 0
	}
	return int(b.total)
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// defaultWorkers is the budget the scan paths use when a server has not
// installed one.
//
// A package-level default rather than a parameter threaded through every scan
// function: the three fan-out sites are deep inside the engine, and the
// alternative was a field on Query that every internal caller and every test
// would have had to set to get the behaviour they already had.
var defaultWorkers atomic.Pointer[WorkerBudget]

// SetWorkerBudget installs the process-wide scan worker budget. A nil budget
// restores the per-query GOMAXPROCS behaviour.
func SetWorkerBudget(b *WorkerBudget) { defaultWorkers.Store(b) }

// scanWorkers is what the fan-out sites call instead of GOMAXPROCS.
func scanWorkers(want int) (int, func()) {
	if b := defaultWorkers.Load(); b != nil {
		return b.Acquire(want)
	}
	return max1(want), func() {}
}

// Admission bounds how many queries run at once, globally and per tenant.
//
// Per tenant as well as globally, because a global limit alone lets one tenant
// fill it: on a shared server the tenant with the most aggressive dashboard
// takes every slot and every other tenant sees 429 for work the server had
// room to do.
type Admission struct {
	global  chan struct{}
	perKey  int
	wait    time.Duration
	mu      sync.Mutex
	byKey   map[string]int
	rejects atomic.Int64
	queued  atomic.Int64
}

// AdmissionConfig configures Admission. The zero value admits everything,
// which is what a single-tenant deployment with its own front end wants.
type AdmissionConfig struct {
	// MaxConcurrent bounds queries in flight across the server. 0 is
	// unbounded.
	MaxConcurrent int
	// MaxPerTenant bounds queries in flight for one tenant. 0 is unbounded.
	MaxPerTenant int
	// Wait is how long a query may queue for a slot before being rejected. 0
	// rejects immediately, which is the right default for an interactive
	// endpoint: a client that waited would rather be told now.
	Wait time.Duration
}

// NewAdmission returns an admission controller.
func NewAdmission(c AdmissionConfig) *Admission {
	a := &Admission{perKey: c.MaxPerTenant, wait: c.Wait, byKey: map[string]int{}}
	if c.MaxConcurrent > 0 {
		a.global = make(chan struct{}, c.MaxConcurrent)
	}
	return a
}

// Acquire admits a query for the given tenant key, or reports why not.
//
// The per-tenant slot is taken FIRST and the global one second. The other
// order deadlocks under load: a query holding a global slot while waiting for
// a tenant slot blocks a query that would have released the tenant slot it is
// waiting for, and with the global limit reached nothing ever moves.
func (a *Admission) Acquire(ctx context.Context, key string) (release func(), err error) {
	if a == nil {
		return func() {}, nil
	}
	relKey, err := a.acquireKey(key)
	if err != nil {
		a.rejects.Add(1)
		return nil, err
	}
	relGlobal, err := a.acquireGlobal(ctx)
	if err != nil {
		relKey()
		a.rejects.Add(1)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			relGlobal()
			relKey()
		})
	}, nil
}

func (a *Admission) acquireKey(key string) (func(), error) {
	if a.perKey <= 0 {
		return func() {}, nil
	}
	a.mu.Lock()
	if a.byKey[key] >= a.perKey {
		n := a.byKey[key]
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: tenant %s already has %d queries running (limit %d)",
			ErrRejected, key, n, a.perKey)
	}
	a.byKey[key]++
	a.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			if a.byKey[key] > 0 {
				a.byKey[key]--
			}
			if a.byKey[key] == 0 {
				// Deleted rather than left at zero: the map is keyed by tenant
				// and a server that has seen a million tenants would otherwise
				// hold a million entries forever.
				delete(a.byKey, key)
			}
			a.mu.Unlock()
		})
	}, nil
}

func (a *Admission) acquireGlobal(ctx context.Context) (func(), error) {
	if a.global == nil {
		return func() {}, nil
	}
	select {
	case a.global <- struct{}{}:
		return a.releaseGlobal, nil
	default:
	}
	if a.wait <= 0 {
		return nil, fmt.Errorf("%w: %d queries already running", ErrRejected, cap(a.global))
	}
	a.queued.Add(1)
	defer a.queued.Add(-1)
	t := time.NewTimer(a.wait)
	defer t.Stop()
	select {
	case a.global <- struct{}{}:
		return a.releaseGlobal, nil
	case <-t.C:
		return nil, ErrQueueTimeout
	case <-ctx.Done():
		// The caller went away while queued. Reported as their cancellation
		// rather than as a rejection: the server did not decline it.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrDeadlineExceeded
		}
		return nil, ErrCanceled
	}
}

func (a *Admission) releaseGlobal() { <-a.global }

// Stats reports admission counters for /metrics.
func (a *Admission) Stats() (inFlight, queued, rejected int64) {
	if a == nil {
		return 0, 0, 0
	}
	return int64(len(a.global)), a.queued.Load(), a.rejects.Load()
}
