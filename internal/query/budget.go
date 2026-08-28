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
			// The floor. Never zero, because a scan with no workers cannot
			// finish and a budget that deadlocks a query is worse than one
			// that oversubscribes.
			//
			// It is one PER CALLER, not one in total: with the budget empty,
			// every concurrent acquirer takes its floor, so 500 callers
			// against a budget of 2 report 500 in use. The comment here used
			// to say "oversubscribed by exactly one", which is true of one
			// caller and of no realistic load. What bounds it in the server is
			// the class semaphore -- at most MaxConcurrentQuery +
			// MaxConcurrentWrite scans exist at once -- not this budget, so
			// simdlogs_scan_workers_in_use can legitimately exceed
			// simdlogs_scan_workers_total and an operator reading it should
			// treat the excess as the count of scans that found the budget
			// empty.
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

// Admission bounds how many queries one tenant runs at once.
//
// Per tenant, and ONLY per tenant. A process-wide gate already exists -- the
// class semaphore in the HTTP middleware, sized by -max-concurrent-query -- and
// the first version of this type had a second one that nothing ever configured:
// NewAdmission was called with MaxPerTenant and Wait, never MaxConcurrent, so
// `global` was always nil. Everything hanging off it was dead. `Stats` reported
// `len(nil chan)` = 0 in-flight however many queries were admitted;
// -query-queue-wait was read only on the path that returned at `global == nil`,
// so a refusal came back in 7 microseconds with the wait set to an hour; and
// ErrQueueTimeout could not be produced by the server at all. That is this
// repo's own named failure -- "a limit that is configuration nothing reads" --
// shipped in the commit whose subject was governing limits.
//
// So the global half is gone rather than wired up: a second process-wide gate
// in front of the one that already works is two numbers an operator has to keep
// consistent for no gain. The queue wait moved to where the waiting actually
// happens.
type Admission struct {
	perKey int
	wait   time.Duration

	mu    sync.Mutex
	byKey map[string]int
	// waiters is a signal channel per key, closed when a slot frees. A
	// condition variable would be simpler and cannot be selected on alongside
	// a context, and a waiter that cannot notice its client hanging up is a
	// slot held for the length of the wait by a query nobody is reading.
	waiters map[string]chan struct{}

	inFlight atomic.Int64
	queued   atomic.Int64
	rejects  atomic.Int64
}

// AdmissionConfig configures Admission. The zero value admits everything,
// which is what a single-tenant deployment with its own front end wants.
type AdmissionConfig struct {
	// MaxPerTenant bounds queries in flight for one tenant. 0 is unbounded.
	MaxPerTenant int
	// Wait is how long a query may wait for a slot before being rejected. 0
	// rejects immediately, which is the right default for an interactive
	// endpoint: a client that waited would rather be told now.
	//
	// It bounds time in the QUEUE, and a queued query holds no slot -- the
	// first version took the tenant slot and then waited, so MaxPerTenant
	// bounded "in flight or queued" while its own documentation said "in
	// flight", and a tenant could hold every slot while running nothing.
	Wait time.Duration
}

// NewAdmission returns an admission controller.
func NewAdmission(c AdmissionConfig) *Admission {
	return &Admission{
		perKey:  c.MaxPerTenant,
		wait:    c.Wait,
		byKey:   map[string]int{},
		waiters: map[string]chan struct{}{},
	}
}

// Acquire admits a query for the given tenant key, or reports why not.
func (a *Admission) Acquire(ctx context.Context, key string) (release func(), err error) {
	if a == nil || a.perKey <= 0 {
		return func() {}, nil
	}
	deadline := time.Time{}
	if a.wait > 0 {
		deadline = time.Now().Add(a.wait)
	}
	for {
		a.mu.Lock()
		if a.byKey[key] < a.perKey {
			a.byKey[key]++
			a.mu.Unlock()
			a.inFlight.Add(1)
			return a.releaseFor(key), nil
		}
		n := a.byKey[key]
		if a.wait <= 0 {
			a.mu.Unlock()
			a.rejects.Add(1)
			return nil, fmt.Errorf("%w: tenant %s already has %d queries running (limit %d)",
				ErrRejected, key, n, a.perKey)
		}
		// Register for the next free slot BEFORE releasing the lock, or a
		// release between the check and the wait is a wakeup that never
		// arrives and a query that waits the whole timeout for a slot that
		// freed immediately.
		w := a.waiters[key]
		if w == nil {
			w = make(chan struct{})
			a.waiters[key] = w
		}
		a.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			a.rejects.Add(1)
			return nil, ErrQueueTimeout
		}
		a.queued.Add(1)
		t := time.NewTimer(remaining)
		select {
		case <-w:
			// A slot freed; loop and try to take it. Not handed over
			// directly, because the waiter that wakes is not necessarily the
			// one that gets the slot and pretending otherwise would be a
			// fairness claim this does not implement.
		case <-t.C:
			a.queued.Add(-1)
			t.Stop()
			a.rejects.Add(1)
			return nil, ErrQueueTimeout
		case <-ctx.Done():
			a.queued.Add(-1)
			t.Stop()
			// NOT counted as a rejection: the server did not decline it, the
			// client went away. The first version counted every error from
			// this function, so a hung-up client incremented
			// simdlogs_query_admission_rejected_total and an operator reading
			// it saw refusals the server never made.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, ErrDeadlineExceeded
			}
			return nil, ErrCanceled
		}
		a.queued.Add(-1)
		t.Stop()
	}
}

// releaseFor returns the slot and wakes anything waiting for this key.
func (a *Admission) releaseFor(key string) func() {
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
			if w := a.waiters[key]; w != nil {
				// Close wakes every waiter for this key; they race for the
				// slot and the losers re-register. Broadcast rather than a
				// single send because a send on an unbuffered channel with no
				// receiver ready would block under the lock.
				close(w)
				delete(a.waiters, key)
			}
			a.mu.Unlock()
			a.inFlight.Add(-1)
		})
	}
}

// Stats reports admission counters for /metrics.
//
// inFlight is counted, not derived from a channel's length. It used to be
// len(a.global), and a.global was always nil, so it reported 0 however many
// queries were admitted.
func (a *Admission) Stats() (inFlight, queued, rejected int64) {
	if a == nil {
		return 0, 0, 0
	}
	return a.inFlight.Load(), a.queued.Load(), a.rejects.Load()
}
