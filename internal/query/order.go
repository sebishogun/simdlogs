package query

import (
	"fmt"
	"sort"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A total order over rows, and the scan that walks it.
//
// # Why a timestamp is not an identity
//
// Pagination needs to resume: "give me the rows after the last one you gave
// me". A timestamp cannot express that. Log timestamps collide constantly --
// a burst of records written in the same millisecond, an agent that stamps
// whole seconds, a batch that inherits one ingest time -- and "after time T"
// either repeats every row at T or drops every row at T but one. Both are
// wrong, and which one you get depends on data the caller cannot see.
//
// So the cursor is a TUPLE: (timestamp, group id, row index). Time first
// because that is the order a caller asked for; the group id second because
// it is assigned once by AppendGroup and never reused, so it survives
// compaction, retention and a restart in a way a group's POSITION in a
// snapshot does not; the row index last because it is a group's own stable
// order. Every row in the store has a distinct tuple, and the tuple is enough
// to say "strictly after this one" without knowing anything else.
//
// # Why the order has to be pinned rather than described
//
// Equal timestamps, groups whose time ranges overlap, and out-of-order ingest
// each make "time order" ambiguous, and the engine's answer to each is an
// accident of how the scan happens to walk. That accident is fine until a
// caller pages through it, at which point an unstable order silently skips
// rows and repeats others. order_test.go pins the answer for all three
// against what the engine does today, so a future change to the walk is a
// test failure rather than a pagination bug in production.

// A RowKey is a row's position in the total order. It is the cursor's payload.
type RowKey struct {
	// Time is the row's timestamp.
	Time int64
	// Group is the manifest id of the group holding the row. Never reused, so
	// it survives compaction and restart; a group's index within a snapshot
	// does not.
	Group uint64
	// Row is the row's index within its group.
	Row uint32
}

// Before reports whether a sorts strictly before b in ascending (oldest-first)
// order. The comparison is lexicographic over the whole tuple, so it is a
// total order and never reports two distinct rows as equal.
func (a RowKey) Before(b RowKey) bool {
	if a.Time != b.Time {
		return a.Time < b.Time
	}
	if a.Group != b.Group {
		return a.Group < b.Group
	}
	return a.Row < b.Row
}

// after reports whether a sorts strictly after b.
func (a RowKey) after(b RowKey) bool { return b.Before(a) }

// Direction is which way a page walks.
type Direction uint8

const (
	// Oldest walks ascending: the oldest matching row first. This is the order
	// an export or a replay wants.
	Oldest Direction = iota
	// Newest walks descending: the newest matching row first, which is what a
	// log viewer shows.
	Newest
)

func (d Direction) String() string {
	if d == Newest {
		return "newest"
	}
	return "oldest"
}

// A Page is one step of a paginated walk.
type Page struct {
	// Rows are the matching rows, in Direction order.
	Rows []Row
	// Keys are their tuples, index for index with Rows.
	Keys []RowKey
	// Next is the tuple to resume after, valid only when More is true.
	Next RowKey
	// More reports whether the scan stopped because the page filled rather
	// than because the rows ran out. A caller that pages until More is false
	// has seen every row exactly once.
	More bool
}

// keyedRow pairs a row with its tuple during the sort. Rows are sorted by key
// rather than the keys being derived after sorting, because two rows with the
// same timestamp are indistinguishable by the row alone -- which is the whole
// reason the tuple exists.
type keyedRow struct {
	row Row
	key RowKey
}

// ScanPage returns the rows strictly after `after` in Direction order, up to
// limit.
//
// `after` is nil for the first page. The scan is over ONE snapshot, taken at
// entry: rows appended while a caller pages are not in it, which is what makes
// "every row exactly once" true. A caller who wants the new rows asks again
// from the start, and the tuple tells it which ones it has already seen.
func ScanPage(s Store, q *Query, after *RowKey, dir Direction, limit int) (*Page, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: a page limit of %d", ErrRejected, limit)
	}
	if !Streamable(q) {
		// Same rule as the streaming select, for the same reason: a pipe that
		// aggregates or globally reorders defines its own output order, and a
		// cursor into it would name a row that the next page's aggregate does
		// not produce.
		return nil, fmt.Errorf("%w: a query with pipes cannot be paginated by row", ErrRejected)
	}
	resolveTimePreds(q)
	sn := snapshotOf(s, q.From, q.To)
	defer sn.Close()

	// Collected, then sorted, then cut. Bounded by MaxRows and the byte
	// budget like any other scan -- this is not the streaming path and does
	// not claim to be: a page's position depends on the total order, so the
	// candidate set has to exist before the page can be taken from it.
	var found []keyedRow
	for gi, g := range sn.Groups {
		if q.exceeded(0) {
			break
		}
		if !groupCanMatch(g, q) {
			continue
		}
		gid := uint64(gi)
		if gi < len(sn.GroupIDs) {
			gid = sn.GroupIDs[gi]
		}
		rows := appendMatches(nil, g, q)
		idx := matchedRowIndices(g, q, len(rows))
		for i, r := range rows {
			k := RowKey{Time: r.Time, Group: gid, Row: uint32(i)}
			if i < len(idx) {
				k.Row = idx[i]
			}
			if after != nil {
				if dir == Newest && !k.Before(*after) {
					continue
				}
				if dir == Oldest && !k.after(*after) {
					continue
				}
			}
			found = append(found, keyedRow{row: r, key: k})
		}
		if q.MaxRows > 0 && len(found) > q.MaxRows {
			q.stop(fmt.Errorf("%w: more than %d rows matched", ErrRowLimit, q.MaxRows))
			return nil, q.stopErr()
		}
	}
	if err := q.stopErr(); err != nil {
		return nil, err
	}

	sort.Slice(found, func(a, b int) bool {
		if dir == Newest {
			return found[b].key.Before(found[a].key)
		}
		return found[a].key.Before(found[b].key)
	})

	p := &Page{}
	if len(found) > limit {
		found = found[:limit]
		p.More = true
	}
	p.Rows = make([]Row, 0, len(found))
	p.Keys = make([]RowKey, 0, len(found))
	for _, kr := range found {
		p.Rows = append(p.Rows, kr.row)
		p.Keys = append(p.Keys, kr.key)
	}
	if p.More && len(p.Keys) > 0 {
		p.Next = p.Keys[len(p.Keys)-1]
	}
	if !p.More {
		// A page that did not fill is the last one, so there is no cursor to
		// hand back. Left zero rather than set to the last key: a caller that
		// used it would ask for the rows after the final row and get an empty
		// page forever, which reads as "still more, come back" rather than
		// "done".
		p.Next = RowKey{}
	}
	return p, nil
}

// matchedRowIndices returns the in-group row index of each match, in the order
// appendMatches produced them.
//
// The indices are what make the tuple stable: the position of a row within
// appendMatches' output changes with the FILTER, so a cursor built from it
// would name a different row for a different query -- and the cursor carries a
// query hash precisely so that cannot happen, which only works if the tuple
// itself is filter-independent.
func matchedRowIndices(g *storage.Reader, q *Query, want int) []uint32 {
	sel := matchBitset(g, q)
	if sel == nil {
		return nil
	}
	out := make([]uint32, 0, want)
	sel.ForEach(func(i int) { out = append(out, uint32(i)) })
	return out
}

// ResolveWindow turns a relative time window into the absolute one the scan
// will use, before anything else reads From/To.
//
// Exported for the pagination layer, which has to hash the window a cursor is
// bound to. Hashing the unresolved query text would bind the cursor to
// `_time:5m` -- a different absolute window on every request -- so paging
// would slide forward as the clock moved, repeating rows the caller had seen
// and skipping ones it had not.
func ResolveWindow(q *Query) { resolveTimePreds(q) }
