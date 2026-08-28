package storage

import (
	"errors"
	"fmt"
	"testing"
)

// The tenant cap does not depend on statfs, so a platform without statfs still
// enforces it.
//
// This is the one that shipped broken. QuotaState returned at the cachedUsage
// error before it ever looked at MaxTenantBytes, so a filesystem that could
// not be measured disabled BOTH budgets -- while quota_other.go's comment, the
// LLD and QuotaState's own comment all said the tenant quota still applied.
// Every platform that file covers enforced nothing at all.
func TestTheTenantCapSurvivesAnUnmeasurableFilesystem(t *testing.T) {
	defer SetDiskUsageForTest(func(string) (DiskUsage, error) {
		return DiskUsage{}, errors.New("statfs failed")
	})()
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100, MaxTenantBytes: 1})
	for i := 0; i < 3; i++ {
		d := BuildDict([]string{fmt.Sprintf("padding padding %d", i)})
		if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}},
			{Name: "_msg", Type: ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	s.sizeAt.Store(0)

	st := s.QuotaState()
	if !st.OverQuota {
		t.Errorf("OverQuota = false with %d bytes stored against a cap of 1", st.StoreBytes)
	}
	if err := s.CheckWrite(); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("CheckWrite = %v, want ErrQuotaExceeded", err)
	}
	// And the reserve -- the half that does need statfs -- is still not
	// enforced, which is the whole point of separating them.
	if st.Warn || st.Reject {
		t.Errorf("a reserve was enforced against a filesystem that could not be measured: %+v", st)
	}
}

// An unconfigured store does not walk its own groups. QuotaState used to call
// DiskBytes before deciding it had nothing to check, so a deployment that had
// configured no budget paid for a locked size walk on every write to answer a
// question no threshold was going to read.
func TestTheZeroBudgetDoesNotSampleAnything(t *testing.T) {
	calls := 0
	defer SetDiskUsageForTest(func(string) (DiskUsage, error) {
		calls++
		return DiskUsage{Total: 1 << 30, Free: 1 << 20}, nil
	})()
	s := quotaStore(t, QuotaConfig{})
	// A size sample would stamp sizeAt; a free-space sample would call the
	// hook. Neither should happen.
	s.sizeAt.Store(0)
	for i := 0; i < 5; i++ {
		if err := s.CheckWrite(); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 0 {
		t.Errorf("an unconfigured store called statfs %d times", calls)
	}
	if at := s.sizeAt.Load(); at != 0 {
		t.Errorf("an unconfigured store walked its own groups")
	}
}

// The disk-usage hook is read on the write path and written by tests. It was a
// plain global, and -race reported it three ways against the shipped tests --
// whose `defer restore()` runs with an httptest server still serving.
func TestTheDiskUsageHookIsRaceFree(t *testing.T) {
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.usageAt.Store(0)
			s.CheckWrite()
		}
	}()
	for i := 0; i < 50; i++ {
		restore := SetDiskUsageForTest(func(string) (DiskUsage, error) {
			return DiskUsage{Total: 1 << 30, Free: int64(i)}, nil
		})
		restore()
	}
	close(stop)
	<-done
}
