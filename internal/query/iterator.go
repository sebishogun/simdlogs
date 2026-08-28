package query

import (
	"fmt"
	"sync"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Streaming a bare select instead of materializing it.
//
// # What was wrong with materializing
//
// `Run` returns `[]Row`. Every row that matches is in memory at once before
// the first byte reaches the client, so peak memory is the size of the ANSWER
// rather than the size of the working set -- and a select whose answer does
// not fit is not slow, it is impossible. The server's response to that was
// `-search.maxRows`: refuse a bare select whose result exceeds a cap. That is
// the right refusal to have and the wrong thing to need, because the query it
// refuses is one the machine could have answered a group at a time.
//
// ScanEach answers it a group at a time. Peak rows in memory is one group's
// matches (serial) or a bounded window of them (pipelined), regardless of how
// many rows match in total.
//
// # Why not just make Run lazy
//
// Because most of what the engine does is not row-wise. `stats`, `sort`,
// `uniq` and `top` are defined over the whole result: a sort cannot emit its
// first row until it has seen its last, and no amount of iterator plumbing
// changes that. Those stay materialized, and the memory bound that protects
// them stays too. What streams is exactly what CAN stream -- a select with no
// pipes -- and `Streamable` is the one place that decides.
//
// # Why the pipelined path exists
//
// A serial walk would have made big unpiped selects SLOWER than they were:
// `Run` fans out across groups above `parallelMinGroups`, and streaming by
// walking groups one at a time would have thrown that away to save memory.
// The pipelined path keeps the fan-out and bounds what it holds -- at most one
// in-flight group per worker, delivered in group order -- so streaming costs
// throughput nowhere.

// Streamable reports whether q's answer can be produced group by group.
//
// Three things disqualify a query, each for its own reason:
//
//   - Pipes. A stats or sort pipe is defined over the whole result. Even the
//     row-wise ones are applied by RunPipeline to a materialized slice, so
//     streaming past them would mean re-implementing each pipe as an operator
//     -- a much larger change than this one, and one that has to be right for
//     every pipe rather than for none.
//   - LastN. `runNewest` walks the groups BACKWARDS and keeps each one's last
//     n matches. Its output order is not group order and its early stop
//     depends on rows it has already kept, so it is not a forward walk at all.
//   - MaxRows. The cap must be decided before the first byte is written; a
//     stream that discovered the overflow at row n+1 would have already sent
//     n rows and could not take them back. Those queries stay materialized --
//     the cap is what makes materializing them safe.
//
// Limit does not disqualify: the serial path already returns the first n rows
// in group order, and a stream stops at the same place.
func Streamable(q *Query) bool {
	return q != nil && len(q.Pipes) == 0 && q.LastN == 0 && q.MaxRows == 0
}

// ScanEach walks q's matching rows in group (time) order and hands each
// group's rows to fn.
//
// The slice fn receives is only valid for the duration of the call: the serial
// path reuses one backing array across groups, which is what makes a
// million-row select allocate like a one-group one. A caller that keeps rows
// copies them.
//
// fn's error stops the scan and is returned unchanged, so a sink writing to a
// hung-up connection ends the query rather than filling a buffer nobody reads.
// A budget or cancellation stop is returned as its typed error from
// executor.go -- ScanEach never reports a truncated walk as a complete one.
func ScanEach(s Store, q *Query, fn func(rows []Row) error) error {
	if !Streamable(q) {
		return fmt.Errorf("%w: this query is not streamable", ErrRejected)
	}
	resolveTimePreds(q)
	sn := snapshotOf(s, q.From, q.To)
	defer sn.Close()

	survivors := sn.Groups[:0]
	for _, g := range sn.Groups {
		if q.exceeded(0) {
			break
		}
		if groupCanMatch(g, q) {
			survivors = append(survivors, g)
		}
	}
	if q.maxGroups > 0 && len(survivors) > q.maxGroups {
		q.stop(fmt.Errorf("%w: %d groups survived the prune, ceiling is %d",
			ErrTooManyGroups, len(survivors), q.maxGroups))
		return q.stopErr()
	}
	if err := q.stopErr(); err != nil {
		return err
	}

	if len(survivors) < parallelMinGroups {
		return scanSerial(survivors, q, fn)
	}
	return scanPipelined(survivors, q, fn)
}

// emitter carries the state that both walks share: the batch's byte figure the
// budget is expressed in, and how many rows have been delivered against Limit.
type emitter struct {
	q  *Query
	fn func([]Row) error
	// bytes is the CURRENT batch's size, not a running total. See emit.
	bytes int64
	sent  int
}

// emit delivers one group's rows and reports whether the walk should stop.
//
// The Limit truncation happens HERE rather than in the caller so that both
// walks cut at the same row: the serial path used to return out[:q.Limit]
// after appending a whole group, and a stream that delivered the whole group
// and then stopped would send more rows than the caller asked for.
func (e *emitter) emit(rows []Row) (stop bool, err error) {
	if e.q.Limit > 0 && e.sent+len(rows) > e.q.Limit {
		rows = rows[:e.q.Limit-e.sent]
		stop = true
	}
	// The WORKING SET, not a running total.
	//
	// MaxBytes and MaxMemory both bound "how much this query holds", and on the
	// materialized path those coincide: every row produced is still in the
	// answer. On a stream they do not -- the rows are handed to the sink and
	// released, so a walk that holds one group at a time holds one group's
	// bytes however many it has delivered.
	//
	// Accumulating was measured doing exactly the harm streaming exists to
	// prevent: a scan holding 8,156 B at a time was refused by a 32,624 B
	// ceiling, and over HTTP the same counter is -search.maxQueryBytes, so a
	// large answer got 200, 4.5 MB and `unexpected EOF` where the materialized
	// path had returned a clean 413 with nothing on the wire. Streaming made
	// the failure worse than the thing it replaced.
	//
	// engine.go's own comment predicted this: "when a streaming sink lands the
	// two stop coinciding and this has to become the live figure rather than
	// the running one". It landed and this was not changed.
	//
	// A stream is therefore unbounded in TOTAL bytes, which is the point: the
	// answer can exceed memory. The wall-clock deadline still bounds it.
	e.bytes = 0
	if e.q.countsBytes() {
		for _, r := range rows {
			e.bytes += rowBytes(r)
		}
	}
	if len(rows) > 0 {
		if err := e.fn(rows); err != nil {
			return true, err
		}
		e.sent += len(rows)
	}
	// After the delivery, not only before the first group: a budget checked
	// only on entry is a budget nothing ever observes being spent.
	if e.q.exceeded(e.bytes) {
		return true, nil
	}
	return stop, nil
}

// scanSerial is the low-group-count walk. One buffer, reused: after the first
// group the []Row backing array is already big enough and the walk stops
// allocating for rows entirely.
func scanSerial(survivors []*storage.Reader, q *Query, fn func([]Row) error) error {
	e := &emitter{q: q, fn: fn}
	var buf []Row
	for _, g := range survivors {
		buf = appendMatches(buf[:0], g, q)
		stop, err := e.emit(buf)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return q.stopErr()
}

// scanPipelined keeps Run's fan-out and bounds what it holds.
//
// One channel per group, handed to the consumer in group order through a
// window of `workers` slots. The window is the memory bound: a producer cannot
// start group n+workers until the consumer has taken group n, so at most that
// many groups' matches exist at once -- as opposed to all of them, which is
// what materializing meant.
//
// Order comes from the slot queue rather than from sorting at the end, so the
// output is byte-identical to the serial walk and to Run without any of them
// having to agree on a comparison.
func scanPipelined(survivors []*storage.Reader, q *Query, fn func([]Row) error) error {
	workers, releaseWorkers := scanWorkers(len(survivors))
	defer releaseWorkers()

	slots := make(chan chan []Row, workers)
	done := make(chan struct{})
	var closeDone sync.Once
	var wg sync.WaitGroup

	go func() {
		defer close(slots)
		for _, g := range survivors {
			ch := make(chan []Row, 1)
			select {
			case slots <- ch:
			case <-done:
				// The consumer stopped -- hit its limit, lost its client, or
				// blew a budget. Nothing further is scanned.
				return
			}
			wg.Add(1)
			go func(g *storage.Reader) {
				defer wg.Done()
				// The budget is re-checked per group here as well as in the
				// consumer: a producer that started before the stop would
				// otherwise decode a whole group whose rows are discarded.
				if q.exceeded(0) {
					ch <- nil
					return
				}
				ch <- appendMatches(nil, g, q)
			}(g)
		}
	}()

	// DEFERRED, not a statement at the end.
	//
	// The snapshot is unmapped when ScanEach returns, so a producer still
	// reading a group's blob after that is a use-after-unmap -- and as a
	// plain statement this was skipped on the one return path that does not
	// reach it: a panic or runtime.Goexit out of the sink. `defer
	// sn.Close()` still runs on that path, so the mapping went away with
	// producers inside it. Reproduced: a sink that panics over 64 groups
	// segfaulted 5/5 runs in lz4BlockDecodeAVX512, and t.Fatalf inside a sink
	// (which is runtime.Goexit) hit it 2/3.
	//
	// Deleting this line left the whole suite green, so the invariant its
	// comment describes had no coverage at all.
	defer wg.Wait()

	// Every producer writes to a buffered channel and so never blocks, which
	// is what lets the consumer walk away.
	var err error
	stopped := false
	e := &emitter{q: q, fn: fn}
	for ch := range slots {
		rows := <-ch
		if stopped {
			continue // drain; the dispatcher is already unwinding
		}
		var stop bool
		stop, err = e.emit(rows)
		if stop || err != nil {
			stopped = true
			closeDone.Do(func() { close(done) })
		}
	}
	if err != nil {
		return err
	}
	return q.stopErr()
}
