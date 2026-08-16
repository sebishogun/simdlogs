package storage

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func aeStore(t *testing.T) *Store {
	t.Helper()
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func aeGroup(msg string, rows int, base int64) *Group {
	ts := make([]int64, rows)
	vals := make([]string, rows)
	for i := range ts {
		ts[i] = base + int64(i)
		vals[i] = msg
	}
	d := BuildDict(vals)
	return &Group{Rows: rows, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "_msg", Type: ColDict, Dict: &d},
	}}
}

// The digest names the content, so two stores that wrote the same group agree
// even though their local ids need not.
func TestTheDigestIsContentNotIdentity(t *testing.T) {
	a, b := aeStore(t), aeStore(t)
	// b writes a group a does not have FIRST, so the same logical group gets a
	// different local id on each store -- the diverged case repair exists for.
	if _, err := b.AppendGroup(aeGroup("only-on-b", 3, 1000)); err != nil {
		t.Fatal(err)
	}
	for _, st := range []*Store{a, b} {
		if _, err := st.AppendGroup(aeGroup("shared", 4, 2000)); err != nil {
			t.Fatal(err)
		}
	}
	da, err := a.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	db, err := b.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	var shareA, shareB GroupDigest
	for _, d := range da {
		if d.Rows == 4 {
			shareA = d
		}
	}
	for _, d := range db {
		if d.Rows == 4 {
			shareB = d
		}
	}
	if shareA.Digest == "" || shareB.Digest == "" {
		t.Fatalf("did not find the shared group: %v / %v", da, db)
	}
	if shareA.Digest != shareB.Digest {
		t.Fatalf("the same group hashes differently on two stores:\n  %s\n  %s",
			shareA.Digest, shareB.Digest)
	}
	if shareA.ID == shareB.ID {
		t.Fatalf("both stores gave the shared group id %d, so this test is not "+
			"exercising the diverged case it exists for", shareA.ID)
	}
}

// Repair by id would have copied the wrong group. This is that scenario end to
// end: B missed a write, so B's id 1 holds what A calls id 2.
func TestRepairingADivergedReplicaAddsTheMissingGroupOnly(t *testing.T) {
	a, b := aeStore(t), aeStore(t)
	if _, err := a.AppendGroup(aeGroup("W1", 2, 1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendGroup(aeGroup("W1", 2, 1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AppendGroup(aeGroup("W2", 2, 2000)); err != nil { // b misses this
		t.Fatal(err)
	}
	if _, err := a.AppendGroup(aeGroup("W3", 2, 3000)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AppendGroup(aeGroup("W3", 2, 3000)); err != nil {
		t.Fatal(err)
	}

	// B holds W1 and W3 under ids 1 and 2; A holds W1, W2, W3 under 1, 2, 3.
	// Repair by id would give B a second copy of W3.
	before := b.TotalRows()
	da, err := a.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	db, err := b.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range db {
		have[d.Digest] = true
	}
	copied := 0
	for _, d := range da {
		if have[d.Digest] {
			continue
		}
		blob, err := a.GroupBytes(d.Digest)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := b.AdoptGroup(d.Digest, blob)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			copied++
		}
	}
	if copied != 1 {
		t.Fatalf("copied %d groups, want exactly the one B was missing", copied)
	}
	if got := b.TotalRows(); got != before+2 {
		t.Fatalf("B has %d rows after repair, want %d: a second copy of W3 is the "+
			"defect repair-by-id produces", got, before+2)
	}
	// And now the two agree on content.
	da2, _ := a.GroupDigests()
	db2, _ := b.GroupDigests()
	setOf := func(ds []GroupDigest) map[string]bool {
		m := map[string]bool{}
		for _, d := range ds {
			m[d.Digest] = true
		}
		return m
	}
	sa, sb := setOf(da2), setOf(db2)
	if len(sa) != len(sb) {
		t.Fatalf("%d groups on A, %d on B after repair", len(sa), len(sb))
	}
	for d := range sa {
		if !sb[d] {
			t.Fatalf("B is still missing %s", d)
		}
	}
}

// Adoption is idempotent, so a repair pass can be retried.
func TestAdoptingTheSameGroupTwiceAddsItOnce(t *testing.T) {
	a, b := aeStore(t), aeStore(t)
	if _, err := a.AppendGroup(aeGroup("W", 5, 1000)); err != nil {
		t.Fatal(err)
	}
	d, err := a.GroupDigests()
	if err != nil {
		t.Fatal(err)
	}
	blob, err := a.GroupBytes(d[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	first, err := b.AdoptGroup(d[0].Digest, blob)
	if err != nil || !first {
		t.Fatalf("first adopt: %v %v", first, err)
	}
	second, err := b.AdoptGroup(d[0].Digest, blob)
	if err != nil {
		t.Fatal(err)
	}
	if second {
		t.Fatal("the second adopt reported a copy; repair would duplicate on retry")
	}
	if got := b.TotalRows(); got != 5 {
		t.Fatalf("%d rows after adopting the same group twice, want 5", got)
	}
}

// A peer cannot write arbitrary bytes into this store.
func TestAdoptRefusesWhatItCannotVerify(t *testing.T) {
	a, b := aeStore(t), aeStore(t)
	if _, err := a.AppendGroup(aeGroup("W", 3, 1000)); err != nil {
		t.Fatal(err)
	}
	d, _ := a.GroupDigests()
	blob, err := a.GroupBytes(d[0].Digest)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("bytes that do not match the digest", func(t *testing.T) {
		tampered := append([]byte(nil), blob...)
		tampered[len(tampered)/2] ^= 0xff
		ok, err := b.AdoptGroup(d[0].Digest, tampered)
		if err == nil || ok {
			t.Fatalf("adopted tampered bytes: %v %v", ok, err)
		}
		if !strings.Contains(err.Error(), "hash") {
			t.Errorf("the refusal does not say why: %v", err)
		}
	})

	t.Run("bytes that are not a group at all", func(t *testing.T) {
		junk := []byte("this is not a group")
		ok, err := b.AdoptGroup(digestBytes(junk), junk)
		if err == nil || ok {
			t.Fatalf("adopted junk: %v %v", ok, err)
		}
	})

	t.Run("a digest this store cannot serve", func(t *testing.T) {
		if _, err := b.GroupBytes("0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
			t.Fatal("served a group it does not have")
		}
	})

	if got := b.TotalRows(); got != 0 {
		t.Fatalf("%d rows landed in the store from refused adoptions", got)
	}
}

// Repair never deletes: the last good replica cannot be destroyed by it.
//
// A property of the API rather than of one call sequence -- AdoptGroup is the
// only entry point repair has into a store, and it has no path that removes
// anything. This pins that: after adopting, everything that was there is still
// there.
func TestAdoptNeverRemovesWhatWasAlreadyHere(t *testing.T) {
	a, b := aeStore(t), aeStore(t)
	if _, err := b.AppendGroup(aeGroup("only-on-b", 7, 500)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AppendGroup(aeGroup("only-on-a", 3, 1000)); err != nil {
		t.Fatal(err)
	}
	beforeB, _ := b.GroupDigests()
	da, _ := a.GroupDigests()
	blob, err := a.GroupBytes(da[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AdoptGroup(da[0].Digest, blob); err != nil {
		t.Fatal(err)
	}
	afterB, _ := b.GroupDigests()
	still := map[string]bool{}
	for _, d := range afterB {
		still[d.Digest] = true
	}
	for _, d := range beforeB {
		if !still[d.Digest] {
			t.Fatalf("repair removed group %s, which B was the only holder of", d.Digest)
		}
	}
	if got := b.TotalRows(); got != 10 {
		t.Fatalf("%d rows, want 7 kept plus 3 adopted", got)
	}
}

// The size cache must not be believed across writes.
//
// DiskBytes caches for ten seconds so the quota check stays cheap on the write
// path. Cached alone, it made the check wrong there: a store's size was sampled
// once and every write in the window was measured against a size from before
// any of them. A store with a 1-byte budget took four writes in a row -- the
// first because it really was empty, the next three because the answer was
// stale.
func TestTheSizeCacheIsUpdatedByEveryAppend(t *testing.T) {
	st := aeStore(t)
	if err := st.SetQuota(QuotaConfig{MaxTenantBytes: 1}); err != nil {
		t.Fatal(err)
	}
	// The first check on an empty store: nothing is over budget yet, and this
	// is what primes the cache.
	if err := st.CheckWrite(); err != nil {
		t.Fatalf("an empty store refused a write: %v", err)
	}
	if _, err := st.AppendGroup(aeGroup("x", 4, 1000)); err != nil {
		t.Fatal(err)
	}
	// Immediately after, well inside the cache interval.
	err := st.CheckWrite()
	if err == nil {
		t.Fatal("a store over its byte budget accepted the next write; the size " +
			"cache is being believed across appends")
	}
	if !strings.Contains(err.Error(), "quota") {
		t.Errorf("the refusal does not name the budget: %v", err)
	}
	// And the reported size is the real one, not a guess.
	if got := st.DiskBytes(); got <= 0 {
		t.Fatalf("DiskBytes reports %d after an append", got)
	}
}

// Growth tracking must not invent a size before anything has sampled one.
func TestGrowthTrackingDoesNotInventASizeBeforeTheFirstSample(t *testing.T) {
	st := aeStore(t)
	// No CheckWrite, so nothing has primed the cache.
	if _, err := st.AppendGroup(aeGroup("x", 4, 1000)); err != nil {
		t.Fatal(err)
	}
	// The first read must WALK, not report only what was appended since open.
	// They agree here because the store is new; what this pins is that the
	// value comes from the walk rather than from an accumulator starting at 0,
	// which would under-report every group a store already had on disk.
	first := st.DiskBytes()
	if first <= 0 {
		t.Fatalf("DiskBytes reports %d for a store holding a group", first)
	}
	if _, err := st.AppendGroup(aeGroup("y", 4, 2000)); err != nil {
		t.Fatal(err)
	}
	if second := st.DiskBytes(); second <= first {
		t.Fatalf("DiskBytes went %d -> %d across an append", first, second)
	}
}

// Concurrent adopts of ONE group leave one copy.
//
// AdoptGroup is check-then-act: it asks whether this store already holds the
// digest, then appends. The two steps each took the store lock and released
// it, so every concurrent adopt of the same group saw it absent and every one
// appended. Measured before the fix, one digest of four rows:
//
//	2 concurrent   1 said adopted, the store held 1 group,  4 rows
//	4 concurrent   2 said adopted, the store held 2 groups, 8 rows
//	8 concurrent   3 said adopted, the store held 3 groups, 12 rows
//
// The losers all returned adopted=false, so a caller counting successes saw
// exactly one while the store held three -- the duplication is invisible from
// the outside, which is why repair could report complete:true, blocked:0 over
// a shard it had just doubled.
//
// This is what two routers repairing the same shard produce. The router's own
// latch is per PROCESS and cannot close it; the destination is the only
// participant that can see it already holds the group, which is task #428.
//
// THE COUNTS ARE THE ASSERTION, not the return values: a fix that made every
// caller say false while still appending would satisfy a check on the booleans.
func TestConcurrentAdoptsOfOneGroupLeaveOneCopy(t *testing.T) {
	for _, n := range []int{2, 4, 8, 16} {
		t.Run(strconv.Itoa(n)+" at once", func(t *testing.T) {
			src := aeStore(t)
			if _, err := src.AppendGroup(aeGroup("W1", 4, 1000)); err != nil {
				t.Fatal(err)
			}
			ds, err := src.GroupDigests()
			if err != nil || len(ds) != 1 {
				t.Fatalf("the source holds %d groups: %v", len(ds), err)
			}
			blob, err := src.GroupBytes(ds[0].Digest)
			if err != nil {
				t.Fatal(err)
			}

			dst := aeStore(t)
			var wg sync.WaitGroup
			var adopted atomic.Int64
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					ok, err := dst.AdoptGroup(ds[0].Digest, blob)
					if err != nil {
						t.Errorf("adopt: %v", err)
					}
					if ok {
						adopted.Add(1)
					}
				}()
			}
			close(start) // released together, so they contend rather than queue
			wg.Wait()

			got, err := dst.GroupDigests()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Errorf("%d concurrent adopts of one group left %d groups and %d "+
					"rows; the group holds 4 rows and was adopted once by every "+
					"measure the caller can see (%d said adopted)",
					n, len(got), dst.TotalRows(), adopted.Load())
			}
			if dst.TotalRows() != 4 {
				t.Errorf("the store holds %d rows, want the group's 4", dst.TotalRows())
			}
			if adopted.Load() != 1 {
				t.Errorf("%d callers were told they adopted it, want exactly 1", adopted.Load())
			}
		})
	}
}
