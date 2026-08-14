package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Corruption policy and storage health.
//
// # The problem this solves
//
// One unreadable group made OpenStore return an error, which made the whole
// tenant unopenable. That is the right default -- a store that silently drops
// a group answers queries with missing data and nothing says so -- but it is
// the wrong and only behaviour: an operator holding a store with one bad group
// out of ten thousand has no way to read the other 9,999, and no way to see
// what is wrong without a hex editor.
//
// So: two policies, and a health surface that makes the state visible either
// way.
//
//	CorruptionFail        refuse to open. The default.
//	CorruptionQuarantine  move the bad groups aside, open the rest, and report
//	                      the store DEGRADED until an operator acknowledges it.
//
// # Why a degraded store is not ready
//
// Quarantine keeps the store serving, which is what an operator wants at 3am
// and exactly what makes it dangerous: a store missing a group returns fewer
// rows for every query touching that range, with no error. So a quarantined
// store opens, serves, and reports NOT READY until [Store.AcknowledgeDegraded]
// is called. Readiness is what a load balancer reads, so the default is that
// a degraded replica is taken out of rotation and a human decides to put it
// back.

// CorruptionPolicy is what OpenStore does with a committed group it cannot
// read.
type CorruptionPolicy uint8

const (
	// CorruptionFail refuses to open the store. The zero value, so a caller
	// that passes no options gets the safe behaviour.
	CorruptionFail CorruptionPolicy = iota

	// CorruptionQuarantine moves each unreadable group out of the store
	// directory, records why, and opens with the groups that remain. The
	// store is degraded until acknowledged.
	CorruptionQuarantine
)

func (p CorruptionPolicy) String() string {
	switch p {
	case CorruptionFail:
		return "fail"
	case CorruptionQuarantine:
		return "quarantine"
	}
	return fmt.Sprintf("CorruptionPolicy(%d)", uint8(p))
}

// ParseCorruptionPolicy maps a configuration string to a policy. It is strict:
// an unrecognised value is an error rather than a silent fall back to the
// default, because falling back to "fail" for an operator who typed
// "quarintine" is a store that will not open and a configuration that looks
// right.
func ParseCorruptionPolicy(s string) (CorruptionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "fail":
		return CorruptionFail, nil
	case "quarantine":
		return CorruptionQuarantine, nil
	}
	return CorruptionFail, fmt.Errorf("storage: unknown corruption policy %q; want fail or quarantine", s)
}

// QuarantineDirName is the subdirectory quarantined groups move into. It is
// inside the store directory so a quarantined group travels with the store --
// an operator copying the directory to another machine to investigate takes
// the evidence with it -- and it does not match the group glob, so nothing
// reads it back.
const QuarantineDirName = "quarantine"

// DirFileMode is the mode new store subdirectories are created with. It is the
// literal every MkdirAll in this package already used; named once so the
// quarantine directory cannot drift from the store directory.
const DirFileMode = 0o755

// QuarantineRecord is the sidecar written beside every quarantined group.
//
// It carries what an operator needs and cannot recover afterwards: where the
// file was, why it was moved, and a checksum of the bytes as they were at the
// moment of the move. The checksum is the one that matters -- it distinguishes
// "the file was already corrupt on disk" from "something changed it after we
// moved it", which is the first question asked when a second copy of the same
// group reads fine.
type QuarantineRecord struct {
	GroupID       uint64 `json:"group_id"`
	OriginalPath  string `json:"original_path"`
	Reason        string `json:"reason"`
	CRC32C        uint32 `json:"crc32c"`
	Bytes         int64  `json:"bytes"`
	QuarantinedAt string `json:"quarantined_at"`

	// QuarantinedName is the file's name inside the quarantine directory. It
	// is not the original base name: ids are reused, so the name carries the
	// checksum too.
	QuarantinedName string `json:"quarantined_name"`
}

// quarantineName is the name a quarantined group takes. id first so an
// operator can find every copy of one group by prefix, checksum second so two
// different bodies under one id cannot collide.
func quarantineName(id uint64, sum uint32) string {
	return fmt.Sprintf("group-%d-%08x.bin", id, sum)
}

// quarantineRecordExists reports whether the quarantine directory holds a
// record for a group id. It is how a store recognises its own interrupted
// quarantine: the file is gone from the store and the manifest still names it,
// which is otherwise indistinguishable from a file someone deleted.
func quarantineRecordExists(dir string, id uint64) bool {
	qdir := filepath.Join(dir, QuarantineDirName)
	ents, err := os.ReadDir(qdir)
	if err != nil {
		return false
	}
	prefix := fmt.Sprintf("group-%d-", id)
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), prefix) || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		// The NAME is not the evidence. This used to return true on the
		// filename alone, so one empty `group-1-00000000.bin.json` dropped
		// into the directory authorized removing group 1 from the manifest --
		// under the DEFAULT policy, reported as "quarantined by an earlier
		// open", with QuarantinedGroups listing nothing and the quarantined
		// count at zero. A store that says a group was quarantined and has
		// nothing quarantined is laundering a missing group into a clean
		// state.
		//
		// So the record is read, it must name this id, and the file it says
		// it moved must be there. That also makes this agree with
		// QuarantinedGroups and countQuarantined, which read the same
		// directory and disagreed with it.
		b, rerr := os.ReadFile(filepath.Join(qdir, e.Name()))
		if rerr != nil {
			continue
		}
		var rec QuarantineRecord
		if json.Unmarshal(b, &rec) != nil || rec.GroupID != id || rec.QuarantinedName == "" {
			continue
		}
		if _, serr := os.Stat(filepath.Join(qdir, rec.QuarantinedName)); serr != nil {
			continue
		}
		return true
	}
	return false
}

// quarantinedIDs is every group id the quarantine directory holds a file for.
//
// It exists for nextID. The quarantining open removes the id from the
// manifest, so the NEXT open's visibleIDs() no longer contains it and nextID
// regressed past it -- the store then reissued the id to real data, and if
// that file later went missing the stale record made it look like an old
// quarantine. A committed id is never reused, and "committed" has to include
// what was committed and then quarantined.
func quarantinedIDs(dir string) []uint64 {
	ents, err := os.ReadDir(filepath.Join(dir, QuarantineDirName))
	if err != nil {
		return nil
	}
	var out []uint64
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".bin") {
			continue
		}
		// "group-<id>-<crc>.bin"
		rest, ok := strings.CutPrefix(e.Name(), "group-")
		if !ok {
			continue
		}
		idStr, _, ok := strings.Cut(rest, "-")
		if !ok {
			continue
		}
		if id, perr := strconv.ParseUint(idStr, 10, 64); perr == nil {
			out = append(out, id)
		}
	}
	return out
}

// Health is a store's health snapshot. It is a value, copied out under the
// lock, so a readiness probe reading it cannot hold the store's mutex while it
// writes a response.
type Health struct {
	// Groups is how many groups the store is serving.
	Groups int

	// Corrupt is how many committed groups could not be read at the last
	// open. Under CorruptionFail the store does not open at all, so this is
	// non-zero only under CorruptionQuarantine.
	Corrupt int

	// Quarantined is how many groups were moved aside, over the store's whole
	// history -- it counts what is in the quarantine directory, not what this
	// open moved.
	Quarantined int

	// LastError is the most recent corruption message, empty when there has
	// been none.
	LastError string

	// Acknowledged is whether an operator has accepted the degraded state.
	Acknowledged bool

	// Policy is the policy this store was opened with.
	Policy CorruptionPolicy

	// FromDirectory is set when this Health was read from a store directory
	// rather than from an open store. Groups, Corrupt and Policy are then
	// UNKNOWN rather than zero, and String() says so instead of printing
	// "0 groups serving, 0 corrupt, policy fail" for a store it never opened.
	FromDirectory bool

	// Unreadable is set when the quarantine directory itself could not be
	// read. It is a marker, not a count: it used to be written into Corrupt,
	// which /metrics sums into simdlogs_storage_corrupt_groups -- a gauge
	// whose help text says "committed groups that could not be read at open".
	// A permissions problem on one directory is not one corrupt group.
	Unreadable bool
}

// Degraded reports whether the store is serving less than it was given.
//
// Quarantined counts, not just Corrupt. Corrupt is what THIS open found and is
// zero on the next one -- the quarantining open removed the group from the
// manifest, so the restart sees a consistent store and nothing to complain
// about. The data is still gone. A degradation signal that clears on restart
// reads 0 one restart after permanent loss, which is exactly when an operator
// most needs it to read 1.
//
// It clears when the quarantine directory is emptied, which is an operator
// deciding the evidence has been dealt with.
func (h Health) Degraded() bool { return h.Corrupt > 0 || h.Quarantined > 0 || h.Unreadable }

// Ready reports whether the store should take traffic: healthy, or degraded
// and explicitly acknowledged.
//
// The two are separate on purpose. A degraded store WORKS -- it opens, it
// serves, its queries return -- and that is what makes it dangerous, because
// every query touching a quarantined group returns fewer rows and nothing in
// the response says so. Readiness is what a load balancer reads, so the
// default is out of rotation until a human decides otherwise.
func (h Health) Ready() bool { return !h.Degraded() || h.Acknowledged }

// String is the one-line form for a log or a readiness body.
func (h Health) String() string {
	if !h.Degraded() {
		return fmt.Sprintf("healthy: %d groups", h.Groups)
	}
	ack := "unacknowledged"
	if h.Acknowledged {
		ack = "acknowledged"
	}
	if h.FromDirectory {
		// Read from the directory: only the quarantine count, the
		// acknowledgement and readability are known.
		return fmt.Sprintf("degraded (%s, from the store directory): %d quarantined%s",
			ack, h.Quarantined, lastErrorSuffix(h.LastError))
	}
	return fmt.Sprintf("degraded (%s, policy %s): %d groups serving, %d corrupt, %d quarantined: %s",
		ack, h.Policy, h.Groups, h.Corrupt, h.Quarantined, h.LastError)
}

func lastErrorSuffix(msg string) string {
	if msg == "" {
		return ""
	}
	return ": " + msg
}

// health is the mutable state behind Health. Separate from Store so the
// quarantine path can build it before a Store exists.
type healthState struct {
	mu           sync.RWMutex
	corrupt      int
	quarantined  int
	unreadable   bool
	lastError    string
	acknowledged bool
	policy       CorruptionPolicy
}

func (h *healthState) snapshot(groups int) Health {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return Health{
		Groups:       groups,
		Corrupt:      h.corrupt,
		Quarantined:  h.quarantined,
		Unreadable:   h.unreadable,
		LastError:    h.lastError,
		Acknowledged: h.acknowledged,
		Policy:       h.policy,
	}
}

func (h *healthState) recordCorrupt(msg string) {
	h.mu.Lock()
	h.corrupt++
	h.lastError = msg
	h.mu.Unlock()
}

func (h *healthState) setQuarantined(n int, readable bool) {
	h.mu.Lock()
	h.quarantined = n
	if !readable {
		// A quarantine directory that will not open is not a healthy store.
		// Reporting 0 for it -- which is what ignoring the error does --
		// prints a clean health surface for a permissions or IO problem
		// sitting on top of the evidence.
		//
		// A marker, not a corrupt-group count: this used to do corrupt++,
		// and /metrics sums Corrupt into a gauge documented as "committed
		// groups that could not be read at open".
		h.lastError = "quarantine directory could not be read"
		h.unreadable = true
	}
	h.mu.Unlock()
}

func (h *healthState) setAcknowledged(v bool) {
	h.mu.Lock()
	h.acknowledged = v
	h.mu.Unlock()
}

// quarantineGroup moves one unreadable group out of the store directory and
// writes its record beside it.
//
// The move is a rename inside the same directory tree, so it is atomic: the
// file is either in the store or in quarantine, never both and never neither.
// The record is written BEFORE the rename, under the quarantine name, so a
// crash between the two leaves a record naming a file that is still in the
// store -- which an operator can act on -- rather than a quarantined file
// nobody can identify.
func quarantineGroup(dir, path string, id uint64, reason string) (err error) {
	qdir := filepath.Join(dir, QuarantineDirName)
	if err := os.MkdirAll(qdir, DirFileMode); err != nil {
		return err
	}
	// Sync the STORE directory, not the new one: MkdirAll added an entry to
	// it, and this package's own writeFileAtomic treats the parent-directory
	// sync as load-bearing for exactly this reason. Without it a power loss
	// can leave the renamed file pointing into a directory that is not there.
	if err := syncDirNamed(qdir); err != nil {
		return err
	}

	// The checksum is of the bytes AS THEY ARE, read straight off disk. It is
	// what distinguishes "already corrupt when we found it" from "changed
	// after we moved it", which is the first question asked when another copy
	// of the same group reads fine.
	sum, size, cerr := checksumFile(path)
	if cerr != nil {
		// BEST EFFORT, not fatal. The group that most needs quarantining is
		// often one that cannot be read at all -- EIO on a bad sector, EACCES
		// after a permissions accident -- and refusing to move it because its
		// checksum could not be taken leaves the store unopenable under the
		// policy chosen to keep it open. The record says the checksum is
		// absent and why, which is more than a group dropped with no record.
		// Only append the cause when it is not already the reason. A group
		// that cannot be mapped usually cannot be read either, so the same
		// text appeared twice: "permission denied (checksum unavailable:
		// permission denied)".
		if cs := cerr.Error(); cs != reason {
			reason = fmt.Sprintf("%s (checksum unavailable: %s)", reason, cs)
		} else {
			reason += " (checksum unavailable for the same reason)"
		}
		sum, size = 0, -1
	}
	rec := QuarantineRecord{
		GroupID:       id,
		OriginalPath:  path,
		Reason:        reason,
		CRC32C:        sum,
		Bytes:         size,
		QuarantinedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	blob, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	// The name carries the CHECKSUM as well as the id, because group ids are
	// reused: quarantining the highest-id group leaves nextID behind it and
	// the store reissues that id, so a second quarantine of "group-2" renamed
	// over the first one's file AND wrote over the first one's record. The
	// record is the entire point of quarantine, and it was the thing being
	// destroyed. Same id with the same bytes lands on the same name, which is
	// idempotent and right; different bytes get a different name.
	base := quarantineName(id, sum)
	rec.QuarantinedName = base
	if blob, err = json.MarshalIndent(rec, "", "  "); err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(qdir, base+".json"), blob, DataFileMode); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(qdir, base)); err != nil {
		return err
	}
	// The rename changed two directory entries; sync both so the move
	// survives a power loss. Without this a crash can leave the file readable
	// under its old name, and the next open finds a group the manifest no
	// longer names.
	if err := syncDirNamed(path); err != nil {
		return err
	}
	return syncDirNamed(filepath.Join(qdir, base))
}

func checksumFile(path string) (uint32, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	h := crc32.New(crc32c)
	n, err := f.WriteTo(h)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum32(), n, nil
}

// countQuarantined is how many groups are in the quarantine directory. It
// counts group files, not records, so a record left by an interrupted
// quarantine does not inflate the number.
// It returns the count and whether the directory could be read. A caller that
// ignores the second result reports a clean store for a quarantine directory
// it cannot open, which is the opposite of what a permissions problem means.
func countQuarantined(dir string) (int, bool) {
	ents, err := os.ReadDir(filepath.Join(dir, QuarantineDirName))
	if err != nil {
		// Not existing is the normal case and is not a failure to read.
		return 0, os.IsNotExist(err)
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "group-") && strings.HasSuffix(e.Name(), ".bin") {
			n++
		}
	}
	return n, true
}

// QuarantinedGroups lists what is in a store directory's quarantine, newest
// record first. It reads the directory rather than any in-memory state, so it
// answers for groups quarantined by an earlier process.
func QuarantinedGroups(dir string) ([]QuarantineRecord, error) {
	qdir := filepath.Join(dir, QuarantineDirName)
	ents, err := os.ReadDir(qdir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]QuarantineRecord, 0, len(ents))
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(qdir, e.Name()))
		if err != nil {
			continue
		}
		var rec QuarantineRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QuarantinedAt > out[j].QuarantinedAt })
	return out, nil
}

// AcknowledgedFileName is the marker an operator's acknowledgement writes into
// the quarantine directory.
//
// Acknowledgement is PERSISTED, and the marker records how many groups were
// quarantined when it was made. A restart with the same count reads as
// acknowledged; one more quarantined group makes the counts differ and the
// store is unacknowledged again, which is what an operator means by "I have
// seen this" -- they saw that state, not every state after it.
//
// The first version did not persist it, on the reasoning that a restart should
// re-ask. That was wrong for the reason the whole surface exists: the restart
// also cleared the degradation, so the operator was never re-asked, they were
// told everything was fine.
const AcknowledgedFileName = "ACKNOWLEDGED"

// ackMu serializes acknowledgement writes within this process.
//
// writeFileAtomic uses a fixed `path + ".tmp"`, so two concurrent writers race
// on one temp name and the loser's rename finds nothing: measured, 9 to 25 of
// 100 concurrent POSTs to /admin/acknowledge-degraded returned 500. The
// contents are never wrong -- every writer writes the same bytes -- so this is
// availability, on the one endpoint whose job is clearing a readiness failure.
//
// A process-wide mutex rather than a per-directory one: acknowledgement is an
// operator action measured in seconds, and a map of mutexes keyed by path is a
// leak with no upper bound.
//
// Two PROCESSES writing one store directory still race, and the store lock
// does NOT cover it: AcknowledgeDegradedDir never takes the lock, and it
// exists for directories whose store is not open -- where no lock is held by
// anyone. The consequence is the same spurious "rename ... .tmp: no such
// file" and not corruption, since every writer writes the same bytes.
var ackMu sync.Mutex

// writeAcknowledgement records that n quarantined groups have been accepted.
func writeAcknowledgement(dir string, n int) error {
	ackMu.Lock()
	defer ackMu.Unlock()
	qdir := filepath.Join(dir, QuarantineDirName)
	if err := os.MkdirAll(qdir, DirFileMode); err != nil {
		return err
	}
	body := fmt.Appendf(nil, "%d\n", n)
	return writeFileAtomic(filepath.Join(qdir, AcknowledgedFileName), body, DataFileMode)
}

// readAcknowledgement reports whether the marker accepts exactly the current
// quarantined count.
func readAcknowledgement(dir string, quarantined int) bool {
	b, err := os.ReadFile(filepath.Join(dir, QuarantineDirName, AcknowledgedFileName))
	if err != nil {
		return false
	}
	// Atoi, not Sscanf: Sscanf("%d") stops at the first non-digit and reports
	// success, so "1 and whatever else" read as an acknowledgement of 1.
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return false
	}
	return n == quarantined
}

// errNothingToAcknowledge is returned when a directory holds no quarantined
// group. Not a failure: a caller counting acknowledgements skips it.
var errNothingToAcknowledge = errors.New("storage: nothing quarantined to acknowledge")

// ErrNothingToAcknowledge reports whether err is that case.
func ErrNothingToAcknowledge(err error) bool { return errors.Is(err, errNothingToAcknowledge) }

// AcknowledgeDegradedDir records an operator's acceptance for a store
// directory that is not open.
//
// A server evicts idle tenants, and an evicted tenant is still degraded -- its
// data is still missing. Acknowledging it by reopening the store would evict
// something else to make room, so the marker is written where the store reads
// it at its next open instead. The count comes from the directory, which is
// the same thing an open store would have counted.
func AcknowledgeDegradedDir(dir string) error {
	n, readable := countQuarantined(dir)
	if !readable {
		return fmt.Errorf("storage: cannot read %s to acknowledge it",
			filepath.Join(dir, QuarantineDirName))
	}
	if n == 0 {
		// Nothing quarantined means nothing to accept, and the caller must not
		// count it: a store degraded by Corrupt alone would have gone ready
		// with no marker on disk, and been unacknowledged again at the next
		// open.
		return errNothingToAcknowledge
	}
	return writeAcknowledgement(dir, n)
}

// HealthOfDir reports what a store directory's quarantine says, without
// opening the store.
//
// It exists for a server deciding readiness at startup: opening every tenant
// to find out whether any is degraded would mmap and replay all of them, and
// the answer is a directory listing. Groups is 0 and Corrupt is 0 because
// neither is knowable without opening; Quarantined and Acknowledged are exact,
// and Degraded() reads from Quarantined, which is the durable half.
//
// The second result is false when the directory is not a store. FromDirectory
// is set on the result, so String() does not print Groups, Corrupt and Policy
// -- which are zero here because they are unknown, not because they are zero.
func HealthOfDir(dir string) (Health, bool) {
	if _, err := os.Stat(filepath.Join(dir, ManifestFileName)); err != nil {
		return Health{}, false
	}
	n, readable := countQuarantined(dir)
	h := Health{Quarantined: n, Acknowledged: readAcknowledgement(dir, n), FromDirectory: true}
	if !readable {
		h.Unreadable = true
		h.LastError = "quarantine directory could not be read"
	}
	return h, true
}

// There was a HealthOfDirCached here, keyed on the quarantine directory's
// modification time. It is gone, and the reason is worth keeping: part of the
// answer it cached is the CONTENTS of quarantine/ACKNOWLEDGED, so an in-place
// rewrite of that file changed the answer without changing the directory --
// and an equal mtime is not proof of an unchanged directory anyway, since one
// second is the natural timestamp granularity on several filesystems this can
// run on. The caller throttles on a time window instead, which depends on no
// filesystem property.
