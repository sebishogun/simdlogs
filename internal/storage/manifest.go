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
}

// openManifest replays dir's manifest and opens it for appending. Records are
// applied in order and replay stops at the first record that is incomplete or
// fails its checksum: everything after a torn record is unreachable, because
// the sequence it belongs to never committed.
func openManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, ManifestFileName)
	m := &manifest{path: path, visible: map[uint64]bool{}}

	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
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

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, DataFileMode)
	if err != nil {
		return nil, err
	}
	m.f = f
	return m, nil
}

func (m *manifest) apply(rec manifestRecord) {
	for _, id := range rec.Add {
		m.visible[id] = true
	}
	for _, id := range rec.Remove {
		delete(m.visible, id)
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
	fi, err := m.f.Stat()
	if err != nil {
		return err
	}
	off := fi.Size()
	seq := m.seq + 1
	rec := manifestRecord{Seq: seq, Add: add, Remove: remove, Receipt: receipt}
	if _, werr := m.f.Write(rec.encode()); werr != nil {
		m.truncateTo(off)
		return werr
	}
	if serr := m.f.Sync(); serr != nil {
		m.truncateTo(off)
		return serr
	}
	// Only now is the record durable, so only now does the sequence advance.
	m.seq = seq
	m.apply(rec)
	m.records++
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

// truncateTo rolls the file back to a record boundary after a failed append.
// A failure here is reported by leaving the manifest as it is: the next open
// truncates the torn tail, which is the same repair one restart later.
func (m *manifest) truncateTo(off int64) {
	if err := m.f.Truncate(off); err != nil {
		return
	}
	m.f.Seek(off, io.SeekStart)
	m.f.Sync()
}

// isVisible reports whether the manifest says a group id is part of the
// store.
func (m *manifest) isVisible(id uint64) bool { return m.visible[id] }

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
	if err := m.f.Close(); err != nil {
		m.f = nil
		m.reopen()
		return err
	}
	m.f = nil
	m.seq = 1
	rec := manifestRecord{Seq: 1, Add: ids}
	if err := writeFileAtomic(m.path, rec.encode(), DataFileMode); err != nil {
		m.reopen()
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
