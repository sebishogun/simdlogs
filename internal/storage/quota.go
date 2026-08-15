package storage

import (
	"errors"
	"flag"
	"fmt"
	"sync/atomic"
	"time"
)

// Disk pressure and storage budgets.
//
// # Why a reserve rather than a percentage
//
// A log store that fills its filesystem does not stop at the last byte: it
// fails mid-write, and the failure lands wherever the write happened to be.
// Every durable path here is temp-file-plus-rename, so a full disk is
// survivable in the sense that nothing is torn -- but a store with no free
// space cannot compact, cannot recompact, cannot write a manifest record, and
// cannot even record a RETENTION removal, which is the one operation that
// would free space. That is the shape to avoid: the recovery needs room.
//
// So the budget is expressed as bytes to keep free, not as a percentage used.
// A percentage is the wrong unit for the thing being protected -- 5% of a
// 40 TB array is 2 TB of slack nobody needs, and 5% of a 20 GB volume is 1 GB,
// which is less than one large group plus the manifest rewrite that follows
// it. Reserve bytes say what the recovery actually costs.
//
// # Two thresholds, and what each does
//
// WARN is where readiness degrades: the store still accepts everything, and an
// operator watching /-/ready sees it before a single write fails. REJECT is
// where new writes stop. Queries, /metrics, retention and the admin surface
// keep working past both, because the answer to a full disk is to read what is
// there and delete some of it, and a store that refuses reads has removed the
// only tool the operator has.
//
// The gap between them is deliberate and has to be wide enough to notice: a
// store that degrades and rejects at the same byte gives nobody a chance to
// act, which is the same as having only one threshold.

// ErrDiskFull is a write refused because the filesystem is at or past the
// reject threshold. Distinct from a quota error so an operator can tell "this
// tenant is over its share" from "this machine is out of room".
var ErrDiskFull = errors.New("storage: disk space below the reserve")

// ErrQuotaExceeded is a write refused because the tenant is at its byte quota.
var ErrQuotaExceeded = errors.New("storage: tenant storage quota exceeded")

// DiskUsage is what a filesystem reports. Bytes, because that is what the
// syscall returns and what a reserve is expressed in.
type DiskUsage struct {
	// Total and Free are of the filesystem holding the store's directory.
	// Free is what is available to THIS user, not the raw free count: a
	// filesystem with a root reserve reports more free than a non-root
	// process can use, and writing against the larger number fails at the
	// smaller one.
	Total, Free int64
}

// QuotaConfig is the budget. The zero value enforces nothing, so a deployment
// that has not decided gets the behaviour it had before this existed.
type QuotaConfig struct {
	// ReserveWarnBytes is the free-space level at which the store reports
	// itself degraded but keeps accepting writes. 0 disables the warning.
	ReserveWarnBytes int64

	// ReserveRejectBytes is the free-space level at which new writes are
	// refused. 0 disables rejection.
	//
	// Below the warning level, necessarily: a reject threshold above the warn
	// threshold would reject before it warned, which is a configuration that
	// cannot do what either field is for. Normalize refuses it rather than
	// silently reordering them.
	ReserveRejectBytes int64

	// MaxTenantBytes bounds one store's own bytes on disk. 0 is unbounded.
	// Checked against what the store has written, not against the filesystem,
	// so one noisy tenant cannot consume a shared volume.
	MaxTenantBytes int64
}

// Normalize checks the configuration and reports what is wrong with it.
func (q QuotaConfig) Normalize() error {
	if q.ReserveWarnBytes < 0 || q.ReserveRejectBytes < 0 || q.MaxTenantBytes < 0 {
		return fmt.Errorf("storage: a negative quota (warn %d, reject %d, tenant %d)",
			q.ReserveWarnBytes, q.ReserveRejectBytes, q.MaxTenantBytes)
	}
	if q.ReserveWarnBytes > 0 && q.ReserveRejectBytes > 0 &&
		q.ReserveRejectBytes >= q.ReserveWarnBytes {
		return fmt.Errorf(
			"storage: the reject reserve (%d) is not below the warn reserve (%d), so writes "+
				"would stop before anything reported degraded",
			q.ReserveRejectBytes, q.ReserveWarnBytes)
	}
	return nil
}

// QuotaState is what the store currently thinks of its space.
type QuotaState struct {
	Usage      DiskUsage
	StoreBytes int64 // this store's own bytes
	Warn       bool  // free space at or below the warn reserve
	Reject     bool  // free space at or below the reject reserve
	OverQuota  bool  // this store is at or above MaxTenantBytes
	// Err is nil when a write would be accepted, and the reason otherwise.
	Err error
}

// Accepting reports whether a write would be taken.
func (q QuotaState) Accepting() bool { return q.Err == nil }

// diskUsageFn is the syscall, indirected so a test can supply a filesystem
// that is full without needing one.
//
// A test that had to really fill a disk would either be skipped everywhere or
// would fill the developer's disk, so the thresholds would be exercised by
// nothing. The indirection is the only way this gets tested at all.
var diskUsageFn = statfsUsage

// SetDiskUsageForTest replaces the free-space source and returns a restore
// function. It panics outside a test binary, like the fault injector: a
// production build that could be told the disk is empty has a switch for
// disabling the protection this file exists to provide.
func SetDiskUsageForTest(fn func(dir string) (DiskUsage, error)) func() {
	if !testBinary() {
		panic("storage: SetDiskUsageForTest called outside a test binary")
	}
	prev := diskUsageFn
	diskUsageFn = fn
	return func() { diskUsageFn = prev }
}

// SetQuota installs the budget. Safe to call while the store is serving; the
// next check sees it.
func (s *Store) SetQuota(q QuotaConfig) error {
	if err := q.Normalize(); err != nil {
		return err
	}
	s.quota.Store(&q)
	return nil
}

// Quota returns the configured budget.
func (s *Store) Quota() QuotaConfig {
	if q := s.quota.Load(); q != nil {
		return *q
	}
	return QuotaConfig{}
}

// QuotaState samples the filesystem and the store's own size.
//
// The sample is cached briefly, because this is on the write path and statfs
// is a syscall per call otherwise: a burst of a thousand small writes would
// make a thousand of them. The staleness that buys is bounded by the interval
// and is the reason the reserve is a RESERVE -- there is room to be wrong by
// one interval's worth of writes.
func (s *Store) QuotaState() QuotaState {
	q := s.Quota()
	st := QuotaState{StoreBytes: s.DiskBytes()}
	if q == (QuotaConfig{}) {
		return st
	}
	u, err := s.cachedUsage()
	if err != nil {
		// A filesystem that cannot be measured is not a filesystem to refuse
		// writes on: the check exists to protect the store, and turning a
		// statfs failure into a write outage is the protection causing the
		// harm. Reported through the state so a metric can show it.
		st.Usage = DiskUsage{}
		return st
	}
	st.Usage = u
	if q.ReserveWarnBytes > 0 && u.Free <= q.ReserveWarnBytes {
		st.Warn = true
	}
	if q.ReserveRejectBytes > 0 && u.Free <= q.ReserveRejectBytes {
		st.Reject = true
		st.Err = fmt.Errorf("%w: %d bytes free, reserve is %d",
			ErrDiskFull, u.Free, q.ReserveRejectBytes)
	}
	if q.MaxTenantBytes > 0 && st.StoreBytes >= q.MaxTenantBytes {
		st.OverQuota = true
		if st.Err == nil {
			st.Err = fmt.Errorf("%w: %d bytes used of %d",
				ErrQuotaExceeded, st.StoreBytes, q.MaxTenantBytes)
		}
	}
	return st
}

// CheckWrite is what a writer calls before accepting rows. It returns nil when
// the write may proceed.
func (s *Store) CheckWrite() error { return s.QuotaState().Err }

// cachedUsage samples the filesystem at most once per quotaSampleInterval.
func (s *Store) cachedUsage() (DiskUsage, error) {
	now := nowNanos()
	if at := s.usageAt.Load(); at != 0 && now-at < int64(quotaSampleInterval) {
		if u := s.usage.Load(); u != nil {
			return *u, nil
		}
	}
	u, err := diskUsageFn(s.dir)
	if err != nil {
		return DiskUsage{}, err
	}
	s.usage.Store(&u)
	s.usageAt.Store(now)
	return u, nil
}

// DiskBytes is the store's own size, cached the same way and for the same
// reason. It is a walk of the directory, which is far more expensive than
// statfs, so its interval is longer.
func (s *Store) DiskBytes() int64 {
	now := nowNanos()
	if at := s.sizeAt.Load(); at != 0 && now-at < int64(quotaSizeInterval) {
		return s.sizeBytes.Load()
	}
	var total int64
	s.mu.RLock()
	for _, g := range s.groups {
		if g.reader != nil {
			total += int64(len(g.reader.blob))
		}
	}
	s.mu.RUnlock()
	// The manifest and the lock file are small and constant; the groups are
	// the size. Counted from the mapped blobs rather than by walking the
	// directory, because the walk is what /metrics already does once every
	// fifteen seconds and doing it again per write is the cost this cache
	// exists to avoid.
	s.sizeBytes.Store(total)
	s.sizeAt.Store(now)
	return total
}

// quotaCounters are process-wide, for /metrics.
var (
	writesRejectedDisk  atomic.Int64
	writesRejectedQuota atomic.Int64
)

// RejectedWrites reports how many writes each threshold has refused.
func RejectedWrites() (disk, quota int64) {
	return writesRejectedDisk.Load(), writesRejectedQuota.Load()
}

// NoteRejectedWrite counts a refusal by its cause, for /metrics. Exported
// because the refusal happens at the HTTP layer, where the request is, and the
// counter belongs with the thresholds that produced it.
func NoteRejectedWrite(err error) { noteRejection(err) }

// noteRejection counts a refusal by its cause.
func noteRejection(err error) {
	switch {
	case errors.Is(err, ErrDiskFull):
		writesRejectedDisk.Add(1)
	case errors.Is(err, ErrQuotaExceeded):
		writesRejectedQuota.Add(1)
	}
}

// The sampling intervals.
//
// statfs is one syscall and the write path may call it per request, so a short
// cache is enough to stop a burst turning into a syscall storm. The size walk
// reads every mapped group's length under a read lock, which is cheap but not
// free, and its answer moves only when a group is written or removed.
const (
	quotaSampleInterval = 2 * time.Second
	quotaSizeInterval   = 10 * time.Second
)

func nowNanos() int64 { return time.Now().UnixNano() }

// testBinary reports whether this is a `go test` binary, the same check the
// fault injector uses.
func testBinary() bool { return flag.Lookup("test.v") != nil }
