package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// ErrLocked reports a store directory already held by another process.
var ErrLocked = errors.New("storage: directory is locked")

// ManifestFileName is the log of committed group-set changes.
const ManifestFileName = "MANIFEST"

// manifestRecordLimit bounds one record's payload. A corrupt length otherwise
// asks for an arbitrary allocation before anything is validated.
const manifestRecordLimit = 1 << 24 // 16 MB

// A manifestRecord is one atomic change to the visible group set.
//
// Group visibility used to be "whatever group-*.bin the directory glob
// returns". That is not a commit point, and two failures came straight out
// of it: retention that unlinked after dropping its in-memory entry
// resurrected the group when the unlink failed, and a half-written group was
// visible the instant its rename landed, before anything recorded that it
// should be. A record here is the commit, and the file on disk is only the
// payload it refers to.
type manifestRecord struct {
	Seq     uint64
	Add     []uint64
	Remove  []uint64
	Receipt []byte // optional idempotency token; used by cluster writes (task 8.2)
}

func (r *manifestRecord) encode() []byte {
	payload := make([]byte, 0, 32+8*(len(r.Add)+len(r.Remove))+len(r.Receipt))
	payload = binary.LittleEndian.AppendUint64(payload, r.Seq)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(r.Add)))
	for _, id := range r.Add {
		payload = binary.LittleEndian.AppendUint64(payload, id)
	}
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(r.Remove)))
	for _, id := range r.Remove {
		payload = binary.LittleEndian.AppendUint64(payload, id)
	}
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(r.Receipt)))
	payload = append(payload, r.Receipt...)

	out := make([]byte, 0, 8+len(payload))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(payload)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(payload, crc32c))
	return append(out, payload...)
}

// decodeManifestRecord reads one record from b, returning it and the bytes
// consumed. ok is false when b holds no complete, valid record -- which is
// where replay stops, because a torn tail is exactly what a crash mid-append
// leaves behind.
func decodeManifestRecord(b []byte) (rec manifestRecord, n int, ok bool) {
	if len(b) < 8 {
		return rec, 0, false
	}
	size := int(binary.LittleEndian.Uint32(b[0:]))
	want := binary.LittleEndian.Uint32(b[4:])
	if size < 0 || size > manifestRecordLimit || len(b) < 8+size {
		return rec, 0, false
	}
	payload := b[8 : 8+size]
	if crc32.Checksum(payload, crc32c) != want {
		return rec, 0, false
	}

	c := &cursor{b: payload}
	rec.Seq = c.u64()
	nAdd := int(c.u32())
	if c.err != nil || nAdd < 0 || nAdd > manifestRecordLimit/8 {
		return manifestRecord{}, 0, false
	}
	for i := 0; i < nAdd; i++ {
		rec.Add = append(rec.Add, c.u64())
	}
	nRem := int(c.u32())
	if c.err != nil || nRem < 0 || nRem > manifestRecordLimit/8 {
		return manifestRecord{}, 0, false
	}
	for i := 0; i < nRem; i++ {
		rec.Remove = append(rec.Remove, c.u64())
	}
	rlen := int(c.u32())
	if c.err != nil || rlen < 0 || rlen > len(payload) {
		return manifestRecord{}, 0, false
	}
	if rlen > 0 {
		if !c.need(rlen) {
			return manifestRecord{}, 0, false
		}
		rec.Receipt = append([]byte(nil), payload[c.at:c.at+rlen]...)
		c.at += rlen
	}
	if c.err != nil {
		return manifestRecord{}, 0, false
	}
	return rec, 8 + size, true
}

// manifest is the append-only commit log for one store directory.
type manifest struct {
	path    string
	f       *os.File
	seq     uint64
	records int // records appended since the last compaction
	visible map[uint64]bool

	// retired is every id a record explicitly REMOVED and no later record
	// added back. It is not the complement of visible, and the difference is
	// the whole point: an id that appears in no record at all -- because its
	// commit never landed, or because replay stopped at a torn record before
	// reaching it -- is absent from BOTH sets, and only this one licenses
	// deleting its file.
	//
	// Reclaiming "everything not visible" instead deleted committed data. One
	// flipped byte in the fifth of ten records left five group files, with the
	// store reporting `healthy: 5 groups`; a MANIFEST truncated to zero left
	// none of twenty. It also destroyed the documented recovery -- remove the
	// MANIFEST and let the legacy path adopt the directory -- because there
	// was nothing left to adopt.
	retired map[uint64]bool

	// preexisted is whether the MANIFEST file was on disk when this manifest
	// was opened. It is the bootstrap discriminator; see openManifest.
	preexisted bool
}

// openManifest replays dir's manifest. Records are
// applied in order and replay stops at the first record that is incomplete or
// fails its checksum: everything after a torn record is unreachable, because
// the sequence it belongs to never committed.
func openManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, ManifestFileName)
	m := &manifest{path: path, visible: map[uint64]bool{}, retired: map[uint64]bool{}}

	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	// Whether the file was ALREADY THERE, which is the only thing that
	// distinguishes a directory predating the manifest from one whose groups
	// were written and never committed. OpenStore adopts every group on disk
	// in the first case and must adopt none in the second, and it used to
	// decide with "the visible set is empty" -- true of both.
	m.preexisted = err == nil
	truncateAt := 0
	for off := 0; off < len(b); {
		rec, n, ok := decodeManifestRecord(b[off:])
		if !ok {
			break
		}
		m.apply(rec)
		off += n
		truncateAt = off
	}
	// Drop a torn tail so the next append starts from a record boundary.
	// Leaving it would put the new record after garbage, and the next replay
	// would stop at the garbage and lose everything written afterwards.
	if truncateAt < len(b) {
		if err := os.Truncate(path, int64(truncateAt)); err != nil {
			return nil, err
		}
	}

	// The append handle is NOT opened here, and that is the point: opening it
	// with O_CREATE creates the file, which destroys the very fact the
	// bootstrap decision reads. Creating it here left a window -- the whole
	// mmap-and-validate pass over a legacy directory -- in which a crash made
	// the next open see a manifest that existed and was empty, and every
	// legacy group silently invisible. ensureOpen creates it at the first
	// write, by which time the decision has been made.
	return m, nil
}

// ensureOpen opens the append handle, creating the file if it is not there.
// Every path that writes a record calls it, so no caller has to know that
// openManifest leaves the handle nil.
func (m *manifest) ensureOpen() error {
	if m.f != nil {
		return nil
	}
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, DataFileMode)
	if err != nil {
		return err
	}
	m.f = f
	return nil
}

func (m *manifest) apply(rec manifestRecord) {
	for _, id := range rec.Add {
		m.visible[id] = true
		// An id that is added has not been retired, whatever an earlier record
		// said. Ids are never reissued while a store is open, so this is
		// defensive rather than reachable -- and a set that only grows is the
		// kind that eventually licenses deleting a live file.
		delete(m.retired, id)
	}
	for _, id := range rec.Remove {
		delete(m.visible, id)
		m.retired[id] = true
	}
	if rec.Seq > m.seq {
		m.seq = rec.Seq
	}
}

// commit appends one record and fsyncs it. It returns only after the change
// is durable, so a caller may treat a successful commit as the point the
// group set changed.
func (m *manifest) commit(add, remove []uint64, receipt []byte) error {
	// Where the record starts, so a short write can be undone. Without this,
	// a partial record stayed on the tail and every later commit was appended
	// after it -- replay then stopped at the garbage and silently dropped
	// every acknowledged, fsynced commit that followed.
	//
	// The size, NOT Seek(0, io.SeekCurrent). The file is opened O_APPEND,
	// which repositions before each write and leaves the descriptor's offset
	// at 0 until the first one -- so the current offset was 0 for the first
	// commit of every process. A failed first commit then truncated the whole
	// manifest, and OpenStore's legacy path re-adopted every group file on
	// disk, resurrecting exactly the removals this log exists to make
	// durable. ENOSPC during a retention pass is the ordinary way to hit it.
	if err := m.ensureOpen(); err != nil {
		return err
	}
	fi, err := m.f.Stat()
	if err != nil {
		return err
	}
	off := fi.Size()
	seq := m.seq + 1
	rec := manifestRecord{Seq: seq, Add: add, Remove: remove, Receipt: receipt}
	if _, werr := m.f.Write(rec.encode()); werr != nil {
		return joinRollback(werr, m.truncateTo(off))
	}
	// Between the write and the sync: the record is in the page cache and is
	// NOT a commit. A crash here must leave the group invisible.
	if ferr := fault(faultManifestWrite); ferr != nil {
		return joinRollback(ferr, m.truncateTo(off))
	}
	if serr := m.f.Sync(); serr != nil {
		return joinRollback(serr, m.truncateTo(off))
	}
	// Only now is the record durable, so only now does the sequence advance.
	m.seq = seq
	m.apply(rec)
	m.records++
	// After the sync: the record IS durable, so a crash here must leave the
	// group visible even though nothing has returned to the caller yet. These
	// two points are the commit boundary, and the crash matrix has to be able
	// to stop on either side of it.
	//
	// The fault fires AFTER the in-memory state advances, not before. A
	// synced record cannot be truncated away, so a fault before the update
	// left memory permanently behind the disk: the injected error reported
	// batch N rejected, the store showed it absent, the NEXT commit reused
	// its sequence number, and a reopen made the rejected batch appear. Two
	// records carrying one Seq, from one injected error. A crash reaches this
	// point either way -- the process dies before the return -- so the matrix
	// is unaffected and only the injected-error path changes.
	//
	// This point is CRASH-ONLY in production: nothing installs a hook, and
	// there is no real error between Sync succeeding and the return. An
	// injected error here still leaves the store's two in-memory structures
	// disagreeing -- m.visible holds the id, s.groups does not, because
	// AppendGroup unmaps and returns -- which is a trap for anyone extending
	// TestInjectedManifestFaultKeepsMemoryAndDiskAgreeing up to the Store.
	if ferr := fault(faultManifestSync); ferr != nil {
		return ferr
	}
	// Fold the log down once it is long. Without a caller, compact() was
	// dead code and replay stayed proportional to every change ever made.
	if m.records >= compactThreshold {
		if err := m.compact(); err != nil {
			// The log is still valid, just long; report nothing and try again
			// at the next commit.
			m.reopen()
			return nil
		}
		m.records = 0
	}
	return nil
}

// truncateTo rolls a partially-written record back off the tail, and reports
// whether the rollback itself is durable.
//
// The return value is not decoration. A caller above this decides whether a
// group file whose commit failed is an orphan to delete, and it decides by
// asking whether the manifest names the id. That question is answered from
// memory; the invariant it stands in for is a durability one. If the record
// was fully written and the SYNC failed -- a dying disk returning EIO -- and
// then the rollback's own Truncate or Sync fails too, the record stays in the
// page cache and the kernel writes it back with no crash involved. Memory says
// invisible, disk ends up saying committed, and deleting the file on the
// strength of memory leaves a committed group with no bytes: the next open
// fails with "committed but its file is missing" and the store never starts.
//
// So a failed rollback is reported, and the caller keeps the file. A leaked
// group file is recoverable; a store that refuses to open is not.
func (m *manifest) truncateTo(off int64) error {
	// A write path, so it opens the handle the same way commit does: a caller
	// may roll back a manifest it has not written to in this process.
	if err := m.ensureOpen(); err != nil {
		return err
	}
	if err := m.f.Truncate(off); err != nil {
		return err
	}
	if _, err := m.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	return m.f.Sync()
}

// ErrRollbackFailed reports a commit that failed AND could not be rolled back,
// so whether the record is durable is unknown. A caller holding a file that
// commit was naming must keep it.
var ErrRollbackFailed = errors.New("storage: a failed manifest commit could not be rolled back")

// isVisible reports whether the manifest says a group id is part of the
// store.
func (m *manifest) isVisible(id uint64) bool { return m.visible[id] }

// wasRetired reports whether a record removed this id and none added it back.
//
// A manifest fold (compact) rewrites the log as one Add record naming the
// visible set, which drops this. That is a leak and not a loss: a file whose
// removal was folded away stops being reclaimable and stays on disk. Stated
// rather than fixed, because the fix is carrying a retired list through the
// fold, and a list that grows forever in a file is its own problem.
func (m *manifest) wasRetired(id uint64) bool { return m.retired[id] }

// visibleIDs returns the committed ids in ascending order.
func (m *manifest) visibleIDs() []uint64 {
	out := make([]uint64, 0, len(m.visible))
	for id := range m.visible {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// bootstrap writes one snapshot record naming ids, for a legacy directory
// that has group files and no manifest. It is written through the atomic
// helper rather than appended, so a crash leaves either no manifest or a
// complete one.
func (m *manifest) bootstrap(ids []uint64) error {
	// The handle may be nil: openManifest no longer creates the file, and
	// bootstrap is exactly the caller that runs before the first write. In
	// this tree it is ALWAYS nil, so this branch is dead -- kept because a
	// future caller with an open handle would need it, and correct for that
	// caller: a non-nil handle means the file exists, so reopen's O_CREATE
	// cannot manufacture the fact OpenStore's gate reads. That is why this one
	// reopen survives while the one below was deleted.
	if m.f != nil {
		if err := m.f.Close(); err != nil {
			m.f = nil
			m.reopen()
			return err
		}
	}
	m.f = nil
	m.seq = 1
	rec := manifestRecord{Seq: 1, Add: ids}
	if err := writeFileAtomic(m.path, rec.encode(), DataFileMode); err != nil {
		// NO reopen() here. reopen opens with O_CREATE, and on a legacy
		// directory the MANIFEST does not exist yet -- so a FAILED bootstrap
		// created a 0-byte one, which is exactly the fact OpenStore's gate
		// reads to decide whether the directory is legacy. Every later open
		// then saw a manifest that existed, skipped the bootstrap, replayed
		// nothing, and returned a store with zero groups AND NO ERROR, on a
		// directory holding the only copy of the data.
		//
		// A transient ENOSPC turned into permanent silent loss, which is the
		// shape reopen's own doc comment exists for, one branch over. Leaving
		// the handle nil is correct and sufficient: every writer goes through
		// ensureOpen, and the next open re-decides and re-bootstraps.
		return err
	}
	m.visible = map[uint64]bool{}
	m.apply(rec)
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, DataFileMode)
	if err != nil {
		return err
	}
	m.f = f
	return nil
}

// compact rewrites the manifest as a single record naming everything
// currently visible, so replay cost stays proportional to the live group set
// rather than to every change ever made. It goes through the atomic helper:
// a crash leaves the old manifest or the new one.
// compactThreshold is the record count past which a commit compacts the log.
// Replay cost is proportional to the file, so an append-only manifest on a
// busy store grows without bound.
const compactThreshold = 4096

func (m *manifest) compact() error {
	ids := m.visibleIDs()
	// The same guard commit and truncateTo take. Not reachable through Store
	// today -- OpenStore always ensureOpens before returning -- but this is a
	// write path, and the next caller added here would inherit a nil-handle
	// Close returning ErrInvalid and falling into the reopen below.
	if err := m.ensureOpen(); err != nil {
		return err
	}
	if err := m.f.Close(); err != nil {
		// Clear the handle first. Returning with m.f non-nil but closed made
		// reopen()'s nil check a no-op, so every later commit failed with
		// "file already closed" forever -- the same permanent-unwritability
		// defect reopen exists for, one branch over. close(2) reports EIO or
		// ENOSPC on writeback failure, so this is reachable.
		m.f = nil
		m.reopen()
		return err
	}
	m.f = nil
	m.seq++
	rec := manifestRecord{Seq: m.seq, Add: ids}
	if err := writeFileAtomic(m.path, rec.encode(), DataFileMode); err != nil {
		m.reopen()
		return err
	}
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, DataFileMode)
	if err != nil {
		return err
	}
	m.f = f
	return nil
}

// reopen restores the append handle after a failed compaction or bootstrap.
// Both close the file before rewriting it, so a failure left m.f nil and
// every later commit failed with os.ErrInvalid forever -- a transient disk
// error turned into a permanently unwritable store.
func (m *manifest) reopen() {
	if m.f != nil {
		return
	}
	if f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, DataFileMode); err == nil {
		m.f = f
	}
}

func (m *manifest) close() error {
	if m == nil || m.f == nil {
		return nil
	}
	err := m.f.Close()
	m.f = nil
	return err
}

// groupIDFromName parses group-<id>.bin. It replaces a Sscanf that ignored
// its error, so a file named group-.bin or group-99999999999999999999.bin
// silently became id 0 and collided with a real group.
func groupIDFromName(name string) (uint64, bool) {
	var id uint64
	n, err := fmt.Sscanf(filepath.Base(name), "group-%d.bin", &id)
	if err != nil || n != 1 {
		return 0, false
	}
	if filepath.Base(name) != fmt.Sprintf("group-%d.bin", id) {
		return 0, false
	}
	return id, true
}

// joinRollback pairs a commit failure with the outcome of undoing it. When the
// rollback failed the result matches ErrRollbackFailed, which is what tells a
// caller that the record's durability is unknown and its file must be kept.
func joinRollback(commitErr, rollbackErr error) error {
	if rollbackErr == nil {
		return commitErr
	}
	// Both wrapped, not formatted. %v flattened the commit error to text, so
	// errors.Is could no longer reach its errno: a rollback-failed ENOSPC
	// classified as an unrecognised failure and was answered "retry in a
	// second" instead of "someone has to free space".
	return fmt.Errorf("%w: %w (rollback: %w)", ErrRollbackFailed, commitErr, rollbackErr)
}
