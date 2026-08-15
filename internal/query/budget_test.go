package query

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The budget hands out at most its total, and never zero.
//
// Never zero is the load-bearing half: a scan granted no workers has no way to
// finish, so a budget that can return 0 can deadlock a query. Oversubscribing
// by exactly one is the deliberate alternative.
func TestWorkerBudgetNeverGrantsZero(t *testing.T) {
	b := NewWorkerBudget(4)
	n1, r1 := b.Acquire(4)
	if n1 != 4 {
		t.Fatalf("granted %d of 4", n1)
	}
	if b.InUse() != 4 {
		t.Fatalf("InUse = %d, want 4", b.InUse())
	}
	// Exhausted: the next query still gets one.
	n2, r2 := b.Acquire(8)
	if n2 != 1 {
		t.Fatalf("granted %d against an exhausted budget, want exactly 1", n2)
	}
	r2()
	r1()
	if b.InUse() != 0 {
		t.Fatalf("InUse = %d after releasing everything", b.InUse())
	}
}

// A release is idempotent: a scan path that returns early and also defers must
// not credit the budget twice.
func TestWorkerBudgetReleaseIsIdempotent(t *testing.T) {
	b := NewWorkerBudget(4)
	_, rel := b.Acquire(2)
	rel()
	rel()
	rel()
	if got := b.InUse(); got != 2-2 {
		t.Fatalf("InUse = %d after three releases of one acquisition, want 0", got)
	}
	// And the budget still hands out its full total.
	if n, r := b.Acquire(4); n != 4 {
		t.Fatalf("granted %d of 4 after a double release", n)
	} else {
		r()
	}
}

// Concurrent acquisitions never exceed the total, which is what a channel of
// tokens would get wrong under contention: two queries each asking for half
// would interleave and get uneven, arbitrary shares.
func TestWorkerBudgetIsNeverOversubscribedByMoreThanOne(t *testing.T) {
	const total = 8
	b := NewWorkerBudget(total)
	var peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				n, rel := b.Acquire(4)
				cur := int64(b.InUse())
				for {
					p := peak.Load()
					if cur <= p || peak.CompareAndSwap(p, cur) {
						break
					}
				}
				if n < 1 {
					t.Errorf("granted %d", n)
				}
				rel()
			}
		}()
	}
	wg.Wait()
	// At most total, plus the one-per-caller floor for callers that found it
	// empty. 64 goroutines could in principle each take the floor, so the bar
	// is that it does not run away: the counter returns to zero and the peak
	// stays within a small multiple.
	if b.InUse() != 0 {
		t.Fatalf("InUse = %d after every release", b.InUse())
	}
	if peak.Load() > total+64 {
		t.Fatalf("peak %d against a total of %d", peak.Load(), total)
	}
}

// A nil budget behaves as the code did before: the caller's own number.
func TestANilWorkerBudgetIsTheOldBehaviour(t *testing.T) {
	var b *WorkerBudget
	n, rel := b.Acquire(runtime.GOMAXPROCS(0))
	rel()
	if n != runtime.GOMAXPROCS(0) {
		t.Fatalf("granted %d, want GOMAXPROCS", n)
	}
	if b.InUse() != 0 || b.Total() != 0 {
		t.Fatal("a nil budget reported usage")
	}
}

// The scan really draws from the installed budget, rather than the budget
// being a type nothing calls.
func TestTheScanUsesTheInstalledBudget(t *testing.T) {
	s := execStore(t, parallelMinGroups*4, 100)
	b := NewWorkerBudget(2)
	SetWorkerBudget(b)
	defer SetWorkerBudget(nil)

	e := &Executor{Store: s}
	if err := e.Execute(context.Background(), allRows(&Query{MatAll: true}),
		func([]Row) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Everything is given back when the scan ends.
	if b.InUse() != 0 {
		t.Fatalf("InUse = %d after the scan, want 0: the budget leaks workers", b.InUse())
	}
}

// Global concurrency: the (n+1)th query is refused, and refusals are counted.
func TestAdmissionGlobalConcurrency(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 2})
	r1, err := a.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := a.Acquire(context.Background(), "t2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Acquire(context.Background(), "t3"); !errors.Is(err, ErrRejected) {
		t.Fatalf("%v, want ErrRejected", err)
	}
	if _, _, rejected := a.Stats(); rejected != 1 {
		t.Fatalf("rejected = %d, want 1", rejected)
	}
	r1()
	// A slot came back, so the next one is admitted.
	r3, err := a.Acquire(context.Background(), "t3")
	if err != nil {
		t.Fatalf("%v after a release; the slot was not returned", err)
	}
	r2()
	r3()
	if n, _, _ := a.Stats(); n != 0 {
		t.Fatalf("%d in flight after every release", n)
	}
}

// Per-tenant concurrency, which a global limit alone cannot express: one
// tenant's dashboard must not take every slot on a shared server.
func TestAdmissionPerTenantConcurrency(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 10, MaxPerTenant: 2})
	var rels []func()
	for i := 0; i < 2; i++ {
		r, err := a.Acquire(context.Background(), "noisy")
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	if _, err := a.Acquire(context.Background(), "noisy"); !errors.Is(err, ErrRejected) {
		t.Fatalf("%v, want the noisy tenant refused at its own limit", err)
	}
	// Another tenant is unaffected: eight global slots are still free.
	r, err := a.Acquire(context.Background(), "quiet")
	if err != nil {
		t.Fatalf("a quiet tenant was refused because another one was busy: %v", err)
	}
	r()
	for _, rel := range rels {
		rel()
	}
}

// Queueing: with a wait configured a query blocks for a slot and is admitted
// when one frees, and is rejected when none does.
func TestAdmissionQueueTimeout(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 1, Wait: 50 * time.Millisecond})
	r1, err := a.Acquire(context.Background(), "t")
	if err != nil {
		t.Fatal(err)
	}
	// Nobody releases: the queued query times out.
	start := time.Now()
	if _, err := a.Acquire(context.Background(), "t2"); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("%v, want ErrQueueTimeout", err)
	}
	if waited := time.Since(start); waited < 40*time.Millisecond {
		t.Errorf("gave up after %v; it did not wait", waited)
	}
	// A queue timeout is a rejection, so one branch handles both.
	if !errors.Is(ErrQueueTimeout, ErrRejected) {
		t.Error("ErrQueueTimeout does not match ErrRejected")
	}

	// And a slot that frees admits the waiter.
	go func() {
		time.Sleep(10 * time.Millisecond)
		r1()
	}()
	r2, err := a.Acquire(context.Background(), "t3")
	if err != nil {
		t.Fatalf("a freed slot did not admit the waiter: %v", err)
	}
	r2()
}

// A caller that goes away while queued is reported as ITS cancellation, not as
// the server rejecting it: the server did not decline the query.
func TestAdmissionReportsCallerCancellation(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 1, Wait: time.Second})
	r, err := a.Acquire(context.Background(), "t")
	if err != nil {
		t.Fatal(err)
	}
	defer r()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if _, err := a.Acquire(ctx, "t2"); !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled", err)
	}
}

// A rejected acquisition holds nothing: the per-tenant slot taken on the way
// in is given back when the global one is refused. Without that, a tenant that
// is refused globally leaks its own slot and can never run again.
func TestARejectedAcquisitionLeaksNothing(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 1, MaxPerTenant: 4})
	r, err := a.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := a.Acquire(context.Background(), "b"); !errors.Is(err, ErrRejected) {
			t.Fatalf("%v", err)
		}
	}
	r()
	// Tenant b was refused ten times and must still have all four of its own
	// slots: if the global refusal had leaked them it would be permanently
	// locked out.
	var rels []func()
	for i := 0; i < 1; i++ {
		rb, err := a.Acquire(context.Background(), "b")
		if err != nil {
			t.Fatalf("tenant b is locked out after %d global refusals: %v", 10, err)
		}
		rels = append(rels, rb)
	}
	for _, rel := range rels {
		rel()
	}
}

// Release is idempotent here too, and the per-tenant map does not grow: a
// server that has seen a million tenants must not hold a million entries.
func TestAdmissionReleaseIsIdempotentAndDoesNotLeakKeys(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxConcurrent: 4, MaxPerTenant: 2})
	for i := 0; i < 100; i++ {
		r, err := a.Acquire(context.Background(), fmt.Sprintf("tenant-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		r()
		r()
	}
	a.mu.Lock()
	n := len(a.byKey)
	a.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d tenant entries remain after every release", n)
	}
	if inFlight, _, _ := a.Stats(); inFlight != 0 {
		t.Fatalf("%d in flight", inFlight)
	}
}

// A nil controller admits everything, which is what a deployment with its own
// front end wants and what every existing caller had.
func TestANilAdmissionAdmits(t *testing.T) {
	var a *Admission
	r, err := a.Acquire(context.Background(), "t")
	if err != nil {
		t.Fatal(err)
	}
	r()
	if in, q, rej := a.Stats(); in != 0 || q != 0 || rej != 0 {
		t.Fatal("a nil controller reported counters")
	}
}

// Admission under concurrency never admits more than its limit.
func TestAdmissionRespectsItsLimitUnderContention(t *testing.T) {
	const limit = 4
	a := NewAdmission(AdmissionConfig{MaxConcurrent: limit})
	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r, err := a.Acquire(context.Background(), fmt.Sprintf("t%d", i%3))
				if err != nil {
					continue
				}
				n := cur.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				cur.Add(-1)
				r()
			}
		}(i)
	}
	wg.Wait()
	if peak.Load() > limit {
		t.Fatalf("peak %d concurrent against a limit of %d", peak.Load(), limit)
	}
	if in, _, _ := a.Stats(); in != 0 {
		t.Fatalf("%d in flight at the end", in)
	}
}
