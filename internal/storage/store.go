package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// Store is the immutable group set: one file per group (mmap-append is
// racy, so a group is written whole and never mutated), with an index
// listing them in time order. Readers are safe for concurrent queries.
type Store struct {
	dir      string
	mu       sync.RWMutex
	groups   []*groupEntry
	nextID   uint64
	closed   bool
	openHook func(uint64)
	lock     *dirLock
	man      *manifest
	// tombstones are files whose group is committed as removed but whose
	// unlink failed; retried on every later retention pass.
	tombstones []string
	// structMu serializes structural operations -- recompaction, cold
	// demotion, promotion -- against each other. They each read a candidate
	// set, do IO, then swap; overlapping two of them lets a stale candidate
	// recreate a group another one removed.
	structMu sync.Mutex

	// health carries the corruption policy and what it has seen.
	health healthState
}

type groupEntry struct {
	id      uint64
	path    string
	reader  *Reader
	timeMin int64
	timeMax int64
	unmap   func() error // releases the mmap backing reader.blob

	// Ownership. A snapshot holds a reference for as long as a caller can
	// read the mapping; retirement marks a version replaced or deleted, and
	// the mapping is released when the two meet at zero. See snapshot.go.
	refs     atomic.Int64
	retired  atomic.Bool
	unmapped atomic.Bool
}

// OpenStore opens or creates a store rooted at dir, loading any groups
// already present in time order.
// OpenOptions configures how a store opens. The zero value is the safe
// default: refuse to open on any unreadable group.
type OpenOptions struct {
	// Policy is what to do with a committed group that cannot be read.
	Policy CorruptionPolicy
}

// OpenStore opens dir with the default options, which refuse to open when any
// committed group is unreadable.
func OpenStore(dir string) (*Store, error) {
	return OpenStoreWith(dir, OpenOptions{})
}

// OpenStoreWith opens dir under an explicit policy.
//
// Under CorruptionQuarantine an unreadable group is moved into the store's
// quarantine directory with a record of where it was, why it moved, and its
// checksum, and the store opens with what remains -- DEGRADED, and not ready
// until [Store.AcknowledgeDegraded] is called. Under CorruptionFail, the
// default, the first unreadable group is an error and nothing is moved.
func OpenStoreWith(dir string, opts OpenOptions) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Exclusive for the life of the store. Two processes on one directory
	// each allocate ids from their own nextID, so both write group-7.bin and
	// one destroys the other's data with nothing to detect it.
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	man, err := openManifest(dir)
	if err != nil {
		lock.unlock()
		return nil, err
	}
	s := &Store{dir: dir, lock: lock, man: man}
	s.health.policy = opts.Policy

	files, _ := filepath.Glob(filepath.Join(dir, "group-*.bin"))
	onDisk := make(map[uint64]string, len(files))
	for _, f := range files {
		if id, ok := groupIDFromName(f); ok {
			onDisk[id] = f
		}
	}
	// A directory with groups and NO MANIFEST FILE predates the manifest.
	// Validate what is there and commit one snapshot naming it, so from here
	// on visibility is a committed fact rather than whatever the glob
	// returned.
	//
	// The gate is "the file was not there", not "the visible set is empty".
	// The visible set is empty in two states that must be treated as
	// opposites: a legacy directory, where every group on disk is real data
	// to adopt, and a directory whose groups were written and never committed,
	// where every one of them must stay invisible. Gating on the empty set
	// adopted both, so:
	//
	//   - a crash between AppendGroup's rename and its commit record made the
	//     uncommitted batch READABLE at the next open, whenever it was the
	//     only batch;
	//   - a crash between retention's commit-remove and its unlink RESURRECTED
	//     the removed group, whenever it was the last live one -- the exact
	//     failure the manifest was introduced to prevent.
	if !man.preexisted && len(onDisk) > 0 {
		ids := make([]uint64, 0, len(onDisk))
		for id, path := range onDisk {
			b, unmap, err := mmapFile(path)
			if err != nil {
				// A group that cannot be MAPPED is as unreadable as one that
				// cannot be parsed, and this was a bare `continue` one line
				// above the branch that applies the policy -- so a legacy
				// directory with one unopenable group opened clean under
				// `fail` and reported "healthy: 2 groups".
				if opts.Policy != CorruptionQuarantine {
					man.close()
					lock.unlock()
					return nil, fmt.Errorf("storage: group %d cannot be opened: %w", id, err)
				}
				if qerr := quarantineGroup(dir, path, id, err.Error()); qerr != nil {
					man.close()
					lock.unlock()
					return nil, fmt.Errorf("storage: group %d cannot be opened (%v) and could not be quarantined: %w",
						id, err, qerr)
				}
				s.health.recordCorrupt(fmt.Sprintf("group %d: %s", id, err))
				continue
			}
			_, rerr := ReadGroup(b)
			unmap()
			if rerr == nil {
				ids = append(ids, id)
				continue
			}
			// An unreadable group in a legacy directory is the same event as
			// one in a committed store and gets the same policy. It used to be
			// dropped silently: the group never reached the visibleIDs loop,
			// so `fail` did not fail and Health reported a clean store missing
			// data. Measured on a three-group directory with the MANIFEST
			// removed and one group corrupted: OpenStore succeeded, Health
			// said "healthy: 2 groups", Corrupt was 0.
			if opts.Policy != CorruptionQuarantine {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d is unreadable: %w", id, rerr)
			}
			if qerr := quarantineGroup(dir, path, id, rerr.Error()); qerr != nil {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d is unreadable (%v) and could not be quarantined: %w",
					id, rerr, qerr)
			}
			s.health.recordCorrupt(fmt.Sprintf("group %d: %s", id, rerr.Error()))
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if err := man.bootstrap(ids); err != nil {
			man.close()
			lock.unlock()
			return nil, err
		}
	}
	// The MANIFEST file must exist from here on, and creating it is the last
	// step of opening rather than the first.
	//
	// Last, because the bootstrap decision above reads whether it was there,
	// and creating it first would answer "yes" for a legacy directory whose
	// previous open crashed during validation -- every legacy group silently
	// invisible.
	//
	// But it must exist BEFORE any group file can be written, or the two
	// states become identical on disk again: a directory holding groups and no
	// manifest is a legacy directory to adopt, and a store that wrote its
	// first group and crashed before committing it must adopt nothing. With
	// the file created here, the second case always has an empty manifest to
	// distinguish it.
	if err := man.ensureOpen(); err != nil {
		man.close()
		lock.unlock()
		return nil, err
	}

	// nextID starts above EVERY id the store has ever committed, which is not
	// the same as every id the manifest still names.
	//
	// The quarantining open removes the id from the manifest, so the next
	// open's visibleIDs() no longer holds it and nextID regressed past it.
	// The store then reissued that id to real data -- and if that file later
	// went missing, the stale quarantine record made a genuine loss read as
	// "quarantined by an earlier open", under the fail policy. A committed id
	// is never reused, and "committed" has to include what was committed and
	// then quarantined.
	//
	// The maximum over the group files on disk (the glob above, which includes
	// uncommitted ones) and the quarantine directory.
	//
	// That is not "every id ever issued" -- retention UNLINKS, so a store
	// whose groups have all been dropped restarts its ids from 0. The property
	// that matters is narrower and does hold: a QUARANTINED id is never
	// reissued, because a quarantined file is always in quarantine/. And a
	// retention-removed id cannot be laundered by the recovery gate either,
	// since it leaves no record and the gate requires one.
	for id := range onDisk {
		if id >= s.nextID {
			s.nextID = id + 1
		}
	}
	for _, id := range quarantinedIDs(dir) {
		if id >= s.nextID {
			s.nextID = id + 1
		}
	}

	// Removals are collected and committed ONCE, after the loop. One commit
	// per corrupt group is one fsync per corrupt group, and a directory-wide
	// loss makes that N synchronous commits at open -- with N crash windows
	// instead of one.
	var removals []uint64
	for _, id := range man.visibleIDs() {
		// nextID must advance past EVERY committed id, including one that is
		// about to be quarantined. Skipping it left nextID behind the id and
		// the store reissued it, so a second quarantine of "group 2" renamed
		// over the first one's evidence.
		if id >= s.nextID {
			s.nextID = id + 1
		}

		path, ok := onDisk[id]
		if !ok {
			// Committed but absent. Two very different situations:
			//
			// A quarantine that was interrupted between the rename and the
			// manifest commit leaves exactly this state, and the record in
			// quarantine/ says so. Refusing it made quarantine unable to
			// recover from its own crash window -- the store never opened
			// again, under EITHER policy, which is the opposite of what the
			// policy was chosen for.
			//
			// Anything else is a file removed behind the manifest's back, and
			// a silently short store answers queries with missing data.
			if quarantineRecordExists(dir, id) {
				removals = append(removals, id)
				s.health.recordCorrupt(fmt.Sprintf("group %d: quarantined by an earlier open", id))
				continue
			}
			man.close()
			lock.unlock()
			return nil, fmt.Errorf("storage: group %d is committed but its file is missing", id)
		}
		b, unmap, err := mmapFile(path)
		if err != nil {
			// The mirror of the legacy-path bug: here a group that could not
			// be mapped was a HARD error under both policies, so quarantine
			// could not quarantine the one kind of damage most likely to need
			// it. Both branches take the policy now.
			if opts.Policy != CorruptionQuarantine {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d cannot be opened: %w", id, err)
			}
			// Quarantined like any other unreadable group, so it leaves a
			// record. Removing it from the manifest without moving it would
			// drop a committed group with no evidence, which is the shape
			// that let a stray file launder a missing group.
			if qerr := quarantineGroup(dir, path, id, err.Error()); qerr != nil {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d cannot be opened (%v) and could not be quarantined: %w",
					id, err, qerr)
			}
			s.health.recordCorrupt(fmt.Sprintf("group %d: %s", id, err))
			removals = append(removals, id)
			continue
		}
		r, err := ReadGroup(b)
		if err != nil {
			unmap()
			if opts.Policy != CorruptionQuarantine {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d is unreadable: %w", id, err)
			}
			// Quarantine: move it aside, and record the removal for the one
			// commit after the loop.
			//
			// The record is written before the rename because the record is
			// the point: a quarantined file nobody can identify is evidence
			// destroyed, where a record naming a file still in the store is
			// something an operator can act on and the next open re-does.
			//
			// The manifest commit comes after the move, and the window that
			// leaves -- file moved, manifest still naming it -- is recovered
			// by the branch above rather than argued away.
			reason := err.Error()
			if qerr := quarantineGroup(dir, path, id, reason); qerr != nil {
				man.close()
				lock.unlock()
				return nil, fmt.Errorf("storage: group %d is unreadable (%v) and could not be quarantined: %w",
					id, err, qerr)
			}
			removals = append(removals, id)
			s.health.recordCorrupt(fmt.Sprintf("group %d: %s", id, reason))
			continue
		}
		s.groups = append(s.groups, &groupEntry{id: id, path: path, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	}
	if len(removals) > 0 {
		if cerr := man.commit(nil, removals, nil); cerr != nil {
			man.close()
			lock.unlock()
			return nil, fmt.Errorf("storage: %d groups were quarantined but the manifest could not record them: %w",
				len(removals), cerr)
		}
	}
	// Any group file the manifest does not name is reclaimed here.
	//
	// It got there one of two ways and neither leaves data behind. A crash
	// between a rename and its commit leaves an uncommitted file, which no
	// reader will ever open because visibility is the manifest's. A crash
	// between a compaction's commit and its unlink leaves the INPUTS it
	// already merged, which the manifest has already stopped naming.
	//
	// The version that left them said "an operator can inspect it", and for a
	// single failed append that is one file. Compaction changed the scale: a
	// batch is up to a whole run of groups, so one kill leaked most of a
	// store's bytes with nothing that would ever reclaim them -- the tombstone
	// list is in memory and a restart drops it. Measured, a kill before the
	// unlink followed by two reopens: 1 live group and 8 orphan files.
	//
	// Safe here and nowhere else: this process holds the directory's exclusive
	// lock and has not written a group yet, so a file the manifest does not
	// name cannot be one another writer is mid-way through. nextID was taken
	// from the pre-removal glob above, so removing them now cannot make this
	// open reissue an id; a LATER open computing a smaller nextID is the same
	// state retention already produces by unlinking, and the quarantine
	// directory -- which nextID also spans -- is untouched.
	reclaimOrphanGroups(dir, onDisk, man)
	s.sortGroups()
	qn, readable := countQuarantined(dir)
	s.health.setQuarantined(qn, readable)
	s.health.setAcknowledged(readAcknowledgement(dir, qn))
	return s, nil
}

// reclaimOrphanGroups unlinks group files the manifest does not name.
//
// Errors are ignored on purpose: a file that cannot be removed is disk, not
// data, and refusing to open a store over it would turn a reclaimable leak
// into an outage.
func reclaimOrphanGroups(dir string, onDisk map[uint64]string, man *manifest) {
	for id, path := range onDisk {
		// RETIRED, not "not visible". A record has to have said this id was
		// removed. An id nothing ever mentioned -- an append that crashed
		// before its commit, or anything past a torn record replay stopped at
		// -- is left alone, because "the manifest does not name it" and "the
		// manifest recorded it gone" are different facts and only the second
		// licenses an unlink. The first version conflated them and deleted
		// committed groups out of a store that then called itself healthy.
		if !man.wasRetired(id) || man.isVisible(id) {
			continue
		}
		if quarantineRecordExists(dir, id) {
			// Quarantined by an earlier open: the record is evidence and the
			// id must stay accounted for.
			continue
		}
		os.Remove(path)
	}
}

// Health is the store's current health. A value copy, so a readiness probe
// reading it holds no lock while it writes a response.
func (s *Store) Health() Health {
	s.mu.RLock()
	groups := len(s.groups)
	s.mu.RUnlock()
	return s.health.snapshot(groups)
}

// AcknowledgeDegraded records that an operator has accepted the store's
// degraded state, which makes it ready.
//
// It is deliberately not automatic: a store that acknowledged itself would be
// a store that quarantined a group and carried on, which is the failure this
// surface exists to make visible.
//
// It IS persisted, in the quarantine directory, together with the count it
// accepted. A restart with the same count stays acknowledged; one more
// quarantined group makes the counts differ and the store is unacknowledged
// again. The alternative -- not persisting, so a restart re-asks -- sounded
// safer and was not: the restart also cleared Corrupt, so instead of being
// re-asked the operator was told the store was healthy.
func (s *Store) AcknowledgeDegraded() error {
	h := s.Health()
	if err := writeAcknowledgement(s.dir, h.Quarantined); err != nil {
		return err
	}
	s.health.setAcknowledged(true)
	return nil
}

// AppendGroup writes a group crash-safely: the bytes go to a temp file,
// fsync, rename into place (atomic), then the index picks it up. A crash
// between temp and rename leaves the temp file, which OpenStore ignores.
func (s *Store) AppendGroup(g *Group) (uint64, error) {
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()

	blob := g.Marshal()
	final := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
	if err := writeFileAtomic(final, blob, DataFileMode); err != nil {
		// The last two steps of writeFileAtomic -- opening the directory and
		// fsyncing it -- run AFTER the rename has already landed, so a failure
		// there returns an error with group-N.bin sitting at its final name.
		// The first version of this cleanup said the window was "every step
		// after writeFileAtomic returns", which is where the reasoning went
		// wrong: two steps inside it are past the point of no return. EMFILE
		// on the open and EIO on the sync both classify retryable, so the
		// retry loop left one full-size orphan per attempt -- the exact
		// pathology this exists to stop. Before the rename there is no final
		// file and the remove is a no-op.
		s.discardUncommitted(final, id, err)
		return 0, err
	}
	// Map the freshly written file rather than keeping the marshaled blob on
	// the heap, so a large store holds only its working set in RAM (the OS
	// pages the mapping in and out). The Marshal output is now free to GC.
	mb, unmap, err := mmapFile(final)
	if err != nil {
		s.discardUncommitted(final, id, err)
		return 0, err
	}
	r, err := ReadGroup(mb)
	if err != nil {
		unmap()
		s.discardUncommitted(final, id, err)
		return 0, err
	}
	s.mu.Lock()
	// Commit before the group is visible. A crash between the rename and this
	// point leaves an uncommitted file that the next open ignores, which is
	// the difference between "not written" and "half written but queryable".
	if err := s.man.commit([]uint64{id}, nil, nil); err != nil {
		s.mu.Unlock()
		unmap()
		s.discardUncommitted(final, id, err)
		return 0, err
	}
	s.groups = append(s.groups, &groupEntry{id: id, path: final, reader: r, timeMin: r.TimeMin, timeMax: r.TimeMax, unmap: unmap})
	s.sortGroups()
	s.mu.Unlock()
	return id, nil
}

// discardUncommitted removes a group file that was made durable but never
// committed, so a failed append leaves nothing behind.
//
// It is still the only thing that reclaims one. reclaimOrphanGroups at open
// does NOT: a file no record ever named cannot be told from one whose commit
// is still coming, or from a committed group whose record replay could not
// reach, so it is left alone. That is why this cleanup matters and why the
// paragraph below, which used to end "only a human deleting files by hand can
// reclaim", is still the truth for this case.
//
// The window is every step of AppendGroup from the rename onward: the last two
// steps of writeFileAtomic -- opening the parent directory and fsyncing it --
// then the mmap, the group re-read, and the manifest commit. The manifest does
// not name the file, so no reader will ever open it and no retention pass will
// ever remove it -- it is disk that only a human deleting files by hand can
// reclaim.
//
// "After writeFileAtomic returns" is where an earlier version of this sentence
// drew the line, and it is wrong: os.Rename has already landed by then, so a
// dir-open or dir-sync failure returns an error with the file at its final
// name. That was corrected at the call site, in docs/lld/ingest.md and in
// docs/wrong.md, and left standing here -- on the function the correction is
// about, which is the one a reader of it sees. That matters most for the failure it is most likely to follow: on
// a full disk the group's own bytes can fit while the manifest record does
// not, and every retry of the append then leaves another full-size file. The
// recovery loop consumes the disk faster than the operator frees it.
//
// The visibility check is not belt-and-braces. m.commit truncates its record
// away on every failure before the sync, so the id is invisible and the file
// is genuinely an orphan -- but the fault point AFTER the sync returns an
// error with the record durable and the id visible. Removing the file there
// would leave a committed group with no bytes, which is worse than the leak.
// That point is crash-only in production, and the check is what keeps it that
// way. The crash matrix does not reach here -- it drives manifest.commit
// directly -- but TestPostSyncCommitFailureKeepsTheGroupFile does, through
// SetFaultHookForTest and faultManifestSync, and goes red without this check.
// A previous version of this comment said reverting it left the suite green;
// that was true when it was written and stopped being true when the test
// arrived, and the sentence stayed.
//
// The directory sync makes the removal durable. Without it a crash can
// resurrect the orphan, which is the same leak one power loss later.
func (s *Store) discardUncommitted(path string, id uint64, commitErr error) {
	// A commit that could not be rolled back leaves the record's durability
	// unknown: it may be sitting in the page cache and reach the disk with no
	// crash involved, in which case the manifest names a group whose file this
	// would delete, and the next open fails with "committed but its file is
	// missing" rather than starting. A leaked file is recoverable; a store
	// that refuses to open is not.
	if errors.Is(commitErr, ErrRollbackFailed) {
		return
	}
	s.mu.Lock()
	visible := s.man.isVisible(id)
	s.mu.Unlock()
	if visible {
		return
	}
	if err := os.Remove(path); err != nil {
		return // already gone, or a directory this process cannot write
	}
	_ = syncDirNamed(path)
}

// Close releases every group's mmap. The store must not be used afterward.
// Close stops new snapshots and retires every group. A mapping still held by
// an open snapshot is released when that snapshot closes, not here: unmapping
// under a live reader is a segfault, and shutdown is exactly when in-flight
// queries are most likely to still be running.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var firstErr error
	for _, g := range s.groups {
		if err := g.retire(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.groups = nil
	if err := s.man.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.lock.unlock(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// CommitRemoval records that groups are no longer part of the store. The
// manifest is the commit point, so a removal that is committed stays removed
// across a restart even if the unlink afterwards fails -- which is what let
// retention resurrect a group it had already dropped.
func (s *Store) CommitRemoval(ids ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.man.commit(nil, ids, nil)
}

func (s *Store) sortGroups() {
	sort.Slice(s.groups, func(i, j int) bool { return s.groups[i].timeMin < s.groups[j].timeMin })
}

// Groups returns the readers whose time span overlaps [from, to), in time
// order -- the first skip, before any column is touched. openHook, if set,
// is called per returned group, so a test can prove skipped groups are
// never opened.
func (s *Store) Groups(from, to int64) []*Reader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Reader
	for _, g := range s.groups {
		if g.reader.TimeRangeMatches(from, to) {
			if s.openHook != nil {
				s.openHook(g.id)
			}
			out = append(out, g.reader)
		}
	}
	return out
}

// openHook counts group opens for the skip test; nil in production.
var _ = 0

func (s *Store) SetOpenHook(fn func(uint64)) { s.openHook = fn }

// Len is the group count.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.groups)
}

// TotalRows sums the row counts of every group -- the stored record total,
// for the /metrics gauge.
func (s *Store) TotalRows() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, g := range s.groups {
		n += g.reader.Rows
	}
	return n
}

// TailCursor is the live-tail watermark: the delivery boundary a tailer
// subscribes at, so it streams only groups appended afterward. It is one past
// the highest current id (0 on an empty store), which -- because ids start at
// 0 -- is what makes even the first-ever group tailable.
func (s *Store) TailCursor() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var next uint64 // 0 => empty store; first group (id 0) is >= 0 and thus delivered
	for _, g := range s.groups {
		if g.id+1 > next {
			next = g.id + 1
		}
	}
	return next
}

// GroupsAfterID returns the readers of groups at or beyond the cursor, and the
// advanced watermark -- the live-tail poll: each tick processes only what
// arrived since the last cursor. Pair with TailCursor for the initial value.
func (s *Store) GroupsAfterID(cursor uint64) (readers []*Reader, next uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	next = cursor
	for _, g := range s.groups {
		if g.id >= cursor {
			readers = append(readers, g.reader)
			if g.id+1 > next {
				next = g.id + 1
			}
		}
	}
	return readers, next
}
