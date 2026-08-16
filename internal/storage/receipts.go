package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// Write receipts: making a replicated write safe to retry.
//
// # The problem a retry creates
//
// A router replicates one ingest request to every member of a shard. Any of
// those can fail after the data landed -- the connection drops while the
// response is coming back, the router times out, the process is killed between
// the fsync and the reply. The router cannot tell "did not commit" from
// "committed and the answer was lost", and those need opposite responses:
// retry the first, do not retry the second.
//
// Without a way to tell, both options are wrong. Retrying duplicates every row
// on the replicas that did commit -- silently, because a log store has no
// primary key and a duplicated line looks exactly like a line that happened
// twice. Not retrying loses the rows on the replicas that did not.
//
// # The receipt
//
// The router stamps each write with an id and the storage node records it in
// the manifest. A retry carrying an id already there is answered "already
// committed", nothing is written, and the router can retry as often as it
// likes.
//
// There are two ways the id gets there, and they differ in exactly one
// respect. AppendGroupIdempotent commits it in the SAME record as the group,
// so the rows and the receipt become durable together and no crash can leave
// one without the other. CommitReceipt commits it in its own record after the
// rows are durable, which is what the batching writer needs -- a flush holds
// rows from many requests, so no single group is "this request's rows" -- and
// that leaves a window: a crash between the two loses the receipt while
// keeping the rows, so a retry stores them again.
//
// Given a choice between a duplicate and a loss, that window takes the
// duplicate. Recording the receipt first would close it in the other
// direction: the rows lost, the receipt claiming they are stored, and the
// retry that would have saved them refused. An earlier version of this comment
// claimed there was no window at all, which was true of one path and not the
// one the server actually uses.
//
// # Why the retention is bounded and what that costs
//
// The receipt set grows with every write and lives in a file that is replayed
// at startup, so it cannot grow forever. It is bounded by count, and the
// bound is the honest limit of the guarantee: a retry that arrives after
// maxReceipts further writes have gone through is no longer recognised and
// WILL duplicate. That is stated rather than hidden, because the alternative
// -- an unbounded set -- fails later and worse, as a startup that takes
// minutes and a manifest that never compacts.

// ErrDuplicateWrite reports that this write id is already committed. It is not
// a failure: the caller's data is stored, and the correct response is success.
var ErrDuplicateWrite = errors.New("storage: this write id is already committed")

// maxReceipts bounds the remembered ids.
//
// 65536 covers a long retry window at any realistic ingest rate -- an agent
// retries within seconds and a router within its request deadline -- while
// keeping the replay cost and the in-memory set small. It is a count rather
// than a duration because the manifest has no clock: records carry a sequence,
// not a timestamp, and a time-based bound would need one written into every
// record for a guarantee measured in seconds.
const maxReceipts = 1 << 16

// WriteID is a write's idempotency token.
//
// Cryptographically random rather than a counter or a hash of the body. A
// counter collides across routers, which is precisely the multi-writer case
// this exists for; a content hash makes two genuinely distinct but identical
// batches -- the same log line twice, which happens constantly -- into one
// write, silently dropping data the client sent on purpose.
type WriteID string

// NewWriteID mints one.
func NewWriteID() (WriteID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("storage: no entropy for a write id: %w", err)
	}
	return WriteID(hex.EncodeToString(b[:])), nil
}

// ValidWriteID reports whether a client-supplied id is usable.
//
// Client-supplied ids are accepted so a retry can carry the same one, which
// means the value is attacker-controlled: it is bounded in length and
// restricted to hex so it cannot be used to write arbitrary bytes into the
// manifest, and so two ids cannot differ only by something the encoding
// normalizes away.
func ValidWriteID(id string) bool {
	if len(id) < 8 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// receiptSet is the bounded set of committed write ids, in commit order.
type receiptSet struct {
	mu    sync.Mutex
	seen  map[WriteID]bool
	order []WriteID
}

func newReceiptSet() *receiptSet {
	return &receiptSet{seen: map[WriteID]bool{}}
}

// has reports whether this id is already committed.
func (rs *receiptSet) has(id WriteID) bool {
	if id == "" {
		return false
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.seen[id]
}

// add records an id, evicting the oldest past the bound.
func (rs *receiptSet) add(id WriteID) {
	if id == "" {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.seen[id] {
		return
	}
	rs.seen[id] = true
	rs.order = append(rs.order, id)
	for len(rs.order) > maxReceipts {
		delete(rs.seen, rs.order[0])
		rs.order = rs.order[1:]
	}
}

// len reports how many ids are remembered, for tests and metrics.
func (rs *receiptSet) count() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.seen)
}

// AppendGroupIdempotent is AppendGroup with a write id.
//
// It returns ErrDuplicateWrite -- and writes NOTHING -- when the id is already
// committed. The check and the commit are both under the store's write lock,
// so two concurrent retries of the same id cannot both pass it: one commits,
// the other sees the receipt.
//
// The id is committed in the SAME manifest record as the group, because one
// record is one transaction. Written separately there would be a window in
// which the rows are visible and the receipt is not -- and a retry landing in
// that window duplicates every row, which is the exact failure this exists to
// prevent, made rarer and therefore harder to find.
func (s *Store) AppendGroupIdempotent(g *Group, id WriteID) (uint64, error) {
	if id == "" {
		return s.AppendGroup(g)
	}
	if s.receipts.has(id) {
		return 0, ErrDuplicateWrite
	}
	return s.appendGroupWithReceipt(g, id)
}

// CommittedWrite reports whether a write id is already stored.
//
// A router asks before retrying: an id that is committed needs no retry, and
// one that is not does. It is also what a replica answers with when it is
// asked to repeat a write it has already taken.
func (s *Store) CommittedWrite(id WriteID) bool { return s.receipts.has(id) }

// ReceiptCount is how many write ids this store remembers.
func (s *Store) ReceiptCount() int { return s.receipts.count() }

// CommitReceipt records a write id as committed, durably, in its own manifest
// record.
//
// This is the FALLBACK. A batching writer's groups do not map one-to-one onto
// requests -- a flush contains rows from many, and one request's rows may span
// groups -- so a receipt cannot always ride the group that makes it true. When
// it cannot, it is committed here, after those rows are durable.
//
// That leaves a window: a crash between the group commit and this one loses the
// receipt while keeping the rows, so a retry stores them again. The alternative
// -- recording the receipt first -- loses the rows while claiming they are
// stored, and refuses the retry that would have saved them. Given a choice
// between a duplicate and a loss, this takes the duplicate.
//
// Writer.flushCarrying avoids the choice where it can: when the flush enqueues
// exactly one group and nothing else is in flight, that group's commit implies
// the whole write is durable, so the id goes into ITS record through
// AppendGroupIdempotent and there is no window at all. The conditions fail
// under concurrency, which is what this function is for.
func (s *Store) CommitReceipt(id WriteID) error {
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipts.has(id) {
		return nil // already recorded; committing twice is not an error
	}
	if err := s.man.commit(nil, nil, []byte(id)); err != nil {
		return err
	}
	s.receipts.add(id)
	return nil
}
