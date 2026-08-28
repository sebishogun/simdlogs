package storage

import (
	"errors"
	"fmt"
	"testing"
)

// The thresholds, against a filesystem this test controls.
//
// A test that really filled a disk would either be skipped everywhere or fill
// the developer's, so the thresholds would be exercised by nothing at all.
// SetDiskUsageForTest is the only way any of this gets covered, and it panics
// outside a test binary so it cannot become a switch for turning the
// protection off in production.
func fakeFree(t *testing.T, free int64) func() {
	t.Helper()
	return SetDiskUsageForTest(func(string) (DiskUsage, error) {
		return DiskUsage{Total: 1 << 30, Free: free}, nil
	})
}

func quotaStore(t *testing.T, q QuotaConfig) *Store {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SetQuota(q); err != nil {
		t.Fatal(err)
	}
	return s
}

// Warn degrades; reject refuses; and there is a band between them where the
// store is degraded and still accepting, which is the whole reason there are
// two thresholds rather than one.
func TestReserveThresholds(t *testing.T) {
	q := QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100}
	for _, tc := range []struct {
		name          string
		free          int64
		warn, reject  bool
		acceptsWrites bool
	}{
		{"plenty of room", 5000, false, false, true},
		{"just above the warning", 1001, false, false, true},
		{"at the warning", 1000, true, false, true},
		{"between the two", 500, true, false, true},
		{"at the reject level", 100, true, true, false},
		{"below it", 0, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore := fakeFree(t, tc.free)
			defer restore()
			s := quotaStore(t, q)
			st := s.QuotaState()
			if st.Warn != tc.warn {
				t.Errorf("Warn = %v at %d bytes free, want %v", st.Warn, tc.free, tc.warn)
			}
			if st.Reject != tc.reject {
				t.Errorf("Reject = %v at %d bytes free, want %v", st.Reject, tc.free, tc.reject)
			}
			if got := st.Accepting(); got != tc.acceptsWrites {
				t.Errorf("Accepting = %v, want %v (err %v)", got, tc.acceptsWrites, st.Err)
			}
			err := s.CheckWrite()
			if tc.acceptsWrites {
				if err != nil {
					t.Errorf("CheckWrite = %v, want nil", err)
				}
			} else if !errors.Is(err, ErrDiskFull) {
				t.Errorf("CheckWrite = %v, want ErrDiskFull", err)
			}
		})
	}
}

// The store recovers the moment space comes back: the rejection is a function
// of the current sample, not a latch an operator has to clear.
func TestRejectionRecoversWhenSpaceReturns(t *testing.T) {
	restore := fakeFree(t, 10)
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	if err := s.CheckWrite(); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("%v, want ErrDiskFull", err)
	}
	restore()

	// The sample is cached, so the recovery is visible after the interval
	// rather than instantly. Reaching past the cache is what a test should not
	// do -- it would be testing a different function -- so this clears it the
	// way time does.
	s.usageAt.Store(0)
	defer fakeFree(t, 100000)()
	if err := s.CheckWrite(); err != nil {
		t.Fatalf("still refusing after space came back: %v", err)
	}
	if st := s.QuotaState(); st.Warn || st.Reject {
		t.Errorf("state still shows pressure: %+v", st)
	}
}

// A tenant at its byte quota is refused, and the error says which limit it is
// -- an operator's response to "this machine is full" and "this tenant is over
// its share" are different actions.
func TestTenantQuota(t *testing.T) {
	defer fakeFree(t, 1<<30)()
	s := quotaStore(t, QuotaConfig{MaxTenantBytes: 1})
	// A store with no groups is under any quota above zero.
	if err := s.CheckWrite(); err != nil {
		t.Fatalf("an empty store: %v", err)
	}
	for i := 0; i < 4; i++ {
		d := BuildDict([]string{fmt.Sprintf("v%d", i)})
		if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}},
			{Name: "_msg", Type: ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	s.sizeAt.Store(0) // the size sample is cached for the same reason free space is
	err := s.CheckWrite()
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("%v, want ErrQuotaExceeded", err)
	}
	if errors.Is(err, ErrDiskFull) {
		t.Error("a quota refusal reported itself as a full disk")
	}
	st := s.QuotaState()
	if !st.OverQuota || st.StoreBytes == 0 {
		t.Errorf("%+v", st)
	}
}

// Disk pressure takes priority over the tenant quota in the error, because it
// is the condition that threatens the whole machine.
func TestDiskFullOutranksTheTenantQuota(t *testing.T) {
	defer fakeFree(t, 0)()
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100, MaxTenantBytes: 1})
	d := BuildDict([]string{"x"})
	if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{1}},
		{Name: "_msg", Type: ColDict, Dict: &d},
	}}); err != nil {
		t.Fatal(err)
	}
	s.sizeAt.Store(0)
	if err := s.CheckWrite(); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("%v, want ErrDiskFull to outrank the quota", err)
	}
}

// The zero value enforces nothing, so a deployment that has not configured a
// budget behaves as it did before this existed -- including on a filesystem
// reporting no free space at all.
func TestTheZeroBudgetEnforcesNothing(t *testing.T) {
	defer fakeFree(t, 0)()
	s := quotaStore(t, QuotaConfig{})
	if err := s.CheckWrite(); err != nil {
		t.Fatalf("an unconfigured store refused a write: %v", err)
	}
	st := s.QuotaState()
	if st.Warn || st.Reject || st.OverQuota {
		t.Errorf("%+v", st)
	}
}

// A filesystem that cannot be measured does not stop writes. The check exists
// to protect the store, and turning a statfs failure into a write outage is
// the protection causing the harm it was added to prevent.
func TestAnUnmeasurableFilesystemDoesNotRefuseWrites(t *testing.T) {
	defer SetDiskUsageForTest(func(string) (DiskUsage, error) {
		return DiskUsage{}, errors.New("statfs failed")
	})()
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	if err := s.CheckWrite(); err != nil {
		t.Fatalf("%v, want writes to continue when free space cannot be read", err)
	}
}

// A budget whose reject level is not below its warn level cannot do what
// either field is for, and is refused rather than silently reordered.
func TestQuotaConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		q    QuotaConfig
		bad  bool
	}{
		{"empty", QuotaConfig{}, false},
		{"ordered", QuotaConfig{ReserveWarnBytes: 100, ReserveRejectBytes: 10}, false},
		{"warn only", QuotaConfig{ReserveWarnBytes: 100}, false},
		{"reject only", QuotaConfig{ReserveRejectBytes: 10}, false},
		{"equal", QuotaConfig{ReserveWarnBytes: 100, ReserveRejectBytes: 100}, true},
		{"inverted", QuotaConfig{ReserveWarnBytes: 10, ReserveRejectBytes: 100}, true},
		{"negative warn", QuotaConfig{ReserveWarnBytes: -1}, true},
		{"negative tenant", QuotaConfig{MaxTenantBytes: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.q.Normalize()
			if tc.bad && err == nil {
				t.Fatalf("%+v was accepted", tc.q)
			}
			if !tc.bad && err != nil {
				t.Fatalf("%+v: %v", tc.q, err)
			}
			// And SetQuota refuses it too, rather than storing something
			// Normalize would have rejected.
			s, cerr := OpenStore(t.TempDir())
			if cerr != nil {
				t.Fatal(cerr)
			}
			defer s.Close()
			if serr := s.SetQuota(tc.q); (serr != nil) != tc.bad {
				t.Fatalf("SetQuota: %v, Normalize: %v", serr, err)
			}
		})
	}
}

// Refusals are counted by cause, because the two are different incidents.
func TestRejectionsAreCountedByCause(t *testing.T) {
	disk0, quota0 := RejectedWrites()

	noteRejection(fmt.Errorf("wrapped: %w", ErrDiskFull))
	noteRejection(ErrQuotaExceeded)
	noteRejection(errors.New("something else"))

	disk1, quota1 := RejectedWrites()
	if disk1-disk0 != 1 {
		t.Errorf("disk rejections went %d -> %d", disk0, disk1)
	}
	if quota1-quota0 != 1 {
		t.Errorf("quota rejections went %d -> %d", quota0, quota1)
	}
}

// Reads keep working past both thresholds. The answer to a full disk is to
// look at what is there and delete some of it, and a store that refuses reads
// has taken away the only tool the operator has.
func TestReadsSurviveAFullDisk(t *testing.T) {
	restore := fakeFree(t, 1<<30)
	s := quotaStore(t, QuotaConfig{ReserveWarnBytes: 1000, ReserveRejectBytes: 100})
	for i := 0; i < 5; i++ {
		d := BuildDict([]string{fmt.Sprintf("v%d", i)})
		if _, err := s.AppendGroup(&Group{Rows: 1, Columns: []Column{
			{Name: "_time", Type: ColTimestamp, Ts: []int64{int64(i + 1)}},
			{Name: "_msg", Type: ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	want := len(readAllRows(t, s))
	restore()
	defer fakeFree(t, 0)()
	s.usageAt.Store(0)

	if err := s.CheckWrite(); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("writes should be refused: %v", err)
	}
	if got := len(readAllRows(t, s)); got != want {
		t.Fatalf("%d rows readable on a full disk, want %d", got, want)
	}
	// And retention -- the operation that frees space -- still runs.
	if n := s.DropGroupsBefore(3); n == 0 {
		t.Error("retention dropped nothing on a full disk; the recovery path is blocked")
	}
}
