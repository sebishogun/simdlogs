// Package ingest turns log lines into stored groups. The writer buffers
// rows column-first and flushes a group at the size/byte/time trigger;
// the jsonline pipeline parses on simdjson with the values interned into
// the group's per-column dictionaries.
//
// Flushing is asynchronous: a full group's buffers are handed to a pool of
// flush workers (dictionary build, encode, fsync) while the parser fills
// the next group. Parse and flush are each ~half the ingest cost and were
// serial; overlapping them is the throughput lever, and it is why the
// reference ingests asynchronously too. Crash-safety (write, fsync, atomic
// rename) is unchanged -- it lives in the store, per group.
package ingest

import (
	"errors"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Flush triggers. A group closes at whichever comes first; crash-safety
// (write, fsync, atomic rename) lives in the store regardless.
const (
	FlushRows  = storage.MaxRows
	FlushBytes = 64 << 20
	FlushEvery = 2 * time.Second
)

// flushWorkers bounds the concurrent group flushes. Flush is the ingest
// bottleneck (a group flushes slower than the parser fills the next), so a
// small pool lets several groups build dictionaries at once and keeps the
// parser from blocking; more than a few buys nothing and costs memory (each
// in-flight group holds its buffers).
var flushWorkers = min(4, runtime.NumCPU())

// Writer accumulates rows and flushes groups to a store. Add is for one
// goroutine; the flush pool runs concurrently behind it.
type Writer struct {
	store     *storage.Store
	ts        []int64
	cols      map[string]*colBuf
	colOrder  []string
	strmFlds  []string // fields that identify a log stream; synthesize _stream from them
	limits    RecordLimits
	maxLine   int
	maxDecomp int
	compact   bool // compact mode: flate the dict (smaller groups, slower decode)
	bytes     int
	lastFlsh  time.Time
	mu        sync.Mutex

	jobs    chan flushJob
	workers sync.WaitGroup // worker goroutines, joined by Close
	// batch takes new jobs; live is every batch with jobs still outstanding.
	// A Flush waits on all of live, because the row buffer is shared and a
	// caller's rows are routinely carried away by another goroutine's
	// Flush. Both guarded by mu.
	batch *flushBatch
	live  []*flushBatch
	// hist is the last batchHistory batches, newest last, so a caller that
	// took a Mark before adding rows can ask about exactly the batches its
	// rows could have joined. Retiring by anything coarser loses writes:
	// with a shared buffer another goroutine's Flush carries this caller's
	// rows away, and if that batch is dropped before this caller asks, the
	// caller is told success for rows that failed.
	hist    []*flushBatch
	nextSeq uint64

	closed    atomic.Bool
	closeOnce sync.Once
}

// flushBatch is one Flush's worth of jobs. One per Flush, not one per
// Writer, and that is the whole point.
//
// A single shared sync.WaitGroup was reused across concurrent Flush calls:
// one goroutine's Add ran while another's Wait was in progress, which is
// the documented misuse and panics with "WaitGroup is reused before
// previous Wait has returned". A tenant's Writer is shared by every request
// and by every syslog connection, and handleSyslogConn flushes after each
// line with no recover above it -- so two syslog senders, needing no
// credential at all, killed the process. Over HTTP the same shape produced
// 46 panics in 640 concurrent posts to one tenant, each downgraded to a 500
// whose rows may or may not be durable.
type flushBatch struct {
	seq  uint64 // monotonic; identifies which batches a caller's rows could be in
	wg   sync.WaitGroup
	err  atomic.Pointer[error]
	done atomic.Bool
}

// colBuf is one column's row values awaiting a flush.
type colBuf struct {
	name string
	vals []string
}

// flushJob is a group's buffers handed to a worker. The worker owns them;
// the writer allocates fresh buffers on handoff, so there is no sharing.
type flushJob struct {
	ts       []int64
	colOrder []string
	vals     map[string][]string
	compact  bool
	batch    *flushBatch // the Flush waiting for this job, never nil
}

// NewWriter makes a writer over the store and starts its flush pool.
func NewWriter(s *storage.Store) *Writer { return NewWriterWorkers(s, flushWorkers) }

// NewWriterWorkers is NewWriter with an explicit flush-pool size, for
// sharded parallel ingest where many writers share the cores and each wants
// a smaller pool (see IngestJSONLinesParallel).
func NewWriterWorkers(s *storage.Store, workers int) *Writer {
	if workers < 1 {
		workers = 1
	}
	w := &Writer{
		store:    s,
		cols:     map[string]*colBuf{},
		lastFlsh: nowFn(),
		jobs:     make(chan flushJob, workers),
		batch:    &flushBatch{},
	}
	w.hist = append(w.hist, w.batch)
	for i := 0; i < workers; i++ {
		w.workers.Add(1)
		go w.worker()
	}
	return w
}

var nowFn = time.Now // overridable in tests

// worker builds and stores one group per job: dictionary build and marshal
// (the expensive, parallelizable half) run here, off the parse goroutine.
func (w *Writer) worker() {
	defer w.workers.Done()
	for j := range w.jobs {
		cols := make([]storage.Column, 0, len(j.colOrder)+1)
		cols = append(cols, storage.Column{Name: "_time", Type: storage.ColTimestamp, Ts: j.ts})
		for _, k := range j.colOrder {
			d := storage.BuildDict(j.vals[k])
			cols = append(cols, storage.Column{Name: k, Type: storage.ColDict, Dict: &d})
		}
		g := &storage.Group{Rows: len(j.ts), Columns: cols, Compact: j.compact}
		if _, err := w.store.AppendGroup(g); err != nil {
			e := err
			j.batch.err.CompareAndSwap(nil, &e)
		}
		j.batch.wg.Done()
	}
}

// Add appends one record: a timestamp and a set of string fields. Unknown
// fields create a column; a row missing a known field gets an empty value
// in it, which the dict encodes once (schema-free, like the reference).
func (w *Writer) Add(ts int64, fields map[string]string) {
	w.add(ts, fields, false)
}

// AddStreamOverridden is Add for a record whose _stream was already built
// from the request's own _stream_fields. The deployment default is skipped
// for it.
//
// The flag is a parameter rather than a sniff of fields["_stream"], and that
// distinction is the fix: deciding per row by looking for the key made the
// override per row instead of per request. A row whose override label came
// out empty fell back to the deployment fields, so one request could produce
// a column mixing {host="h1"} and {service="api"} -- and any payload field
// literally named _stream suppressed deployment labelling for that row. Since
// _stream is what stream-scoped retention groups on, that let a client choose
// its own retention bucket.
func (w *Writer) AddStreamOverridden(ts int64, fields map[string]string) {
	w.add(ts, fields, true)
}

// SetMaxDecompressedBytes bounds what one compressed body may expand to.
//
// The Loki protobuf path needs this because snappy's ratio on log text is
// routinely 4-6x and far higher on repetitive input, so a body that passed the
// WIRE limit can still declare gigabytes. It used to be a hardcoded 256 MiB
// that ignored the operator's -max-decompressed-bytes entirely -- lowering the
// configured limit did nothing on that path, and with MaxConcurrentWrite at 32
// a set of 20-byte requests could each claim 256 MiB.
func (w *Writer) SetMaxDecompressedBytes(n int) {
	w.mu.Lock()
	w.maxDecomp = n
	w.mu.Unlock()
}

// MaxDecompressedBytes reports the configured bound, or 0 when unset.
func (w *Writer) MaxDecompressedBytes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxDecomp
}

// SetMaxLineBytes rejects an input line longer than n bytes. Zero disables
// the bound.
func (w *Writer) SetMaxLineBytes(n int) {
	w.mu.Lock()
	w.maxLine = n
	w.mu.Unlock()
}

// LineTooLong reports whether a line exceeds the configured bound.
func (w *Writer) LineTooLong(n int) bool {
	w.mu.Lock()
	max := w.maxLine
	w.mu.Unlock()
	return max > 0 && n > max
}

// SetRecordLimits bounds what one record may carry. Zero fields mean no
// bound for that dimension.
//
// The limits are applied here rather than in each parser, because a
// per-protocol check is a check that gets forgotten on the next protocol --
// these were declared in the configuration and read by nothing.
func (w *Writer) SetRecordLimits(l RecordLimits) {
	w.mu.Lock()
	w.limits = l
	w.mu.Unlock()
}

// reservedField reports whether a name carries meaning the store depends on:
// the message, the timestamp, and the stream label retention groups by.
func reservedField(k string) bool {
	return k == "_msg" || k == "_time" || k == "_stream"
}

// controlField reports whether a name is reserved for the server's own
// out-of-band signalling rather than for log data. A leading dot is not a
// name any log producer emits, and the live tail ends a cut-short stream
// with a {".error": ...} line -- which only distinguishes a marker from a
// log line if a log line can never carry that key. Enforced on ingest, not
// merely documented: the first version of the tail sentinel asserted this
// property in a comment while the store accepted the name.
func controlField(k string) bool { return len(k) > 0 && k[0] == '.' }

// truncateForLimits drops fields and clips values that exceed the record
// limits. It clips rather than rejecting the record: a log line with one
// oversized field is still worth storing, and dropping the whole record
// loses more than it protects.
func (w *Writer) truncateForLimits(fields map[string]string) {
	l := w.limits
	if l.MaxNameBytes > 0 || l.MaxValueBytes > 0 {
		for k, v := range fields {
			if reservedField(k) {
				continue
			}
			if l.MaxNameBytes > 0 && len(k) > l.MaxNameBytes {
				delete(fields, k)
				continue
			}
			// A stream field's value is clipped like any other -- but that
			// changes the label retention groups on, so the fields that build
			// _stream are left alone.
			if w.isStreamField(k) {
				continue
			}
			if l.MaxValueBytes > 0 && len(v) > l.MaxValueBytes {
				fields[k] = v[:l.MaxValueBytes]
			}
		}
	}
	if l.MaxFields > 0 && len(fields) > l.MaxFields {
		// Deterministic: drop the highest-sorting names, so the same record
		// always stores the same fields.
		//
		// The reserved names are exempt. '_' is 0x5F, which sorts after
		// digits and uppercase, so a plain sort dropped _msg first: a wide
		// record kept AAA/BBB/CCC and lost the log line itself. _stream also
		// drives stream-scoped retention, so dropping it moves the record to
		// a different retention bucket.
		keys := make([]string, 0, len(fields))
		for k := range fields {
			if reservedField(k) {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		budget := l.MaxFields - (len(fields) - len(keys))
		if budget < 0 {
			budget = 0
		}
		if budget < len(keys) {
			for _, k := range keys[budget:] {
				delete(fields, k)
			}
		}
	}
}

// isStreamField reports whether k contributes to the synthesized _stream
// label. Caller holds w.mu.
func (w *Writer) isStreamField(k string) bool {
	for _, f := range w.strmFlds {
		if f == k {
			return true
		}
	}
	return false
}

func (w *Writer) add(ts int64, fields map[string]string, streamOverridden bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return // the batch is going nowhere; Flush reports ErrWriterClosed
	}
	// Control names go first, whatever the limits say. Gating this on
	// w.limits meant a Writer built through the exported NewWriter, or the
	// four-argument IngestJSONLinesParallel, stored a forged ".error" -- and
	// the tail's end-of-stream marker is only distinguishable from a log
	// line because that name cannot be stored.
	for k := range fields {
		if controlField(k) {
			delete(fields, k)
		}
	}
	if w.limits != (RecordLimits{}) {
		w.truncateForLimits(fields)
	}
	// When this writer owns stream labelling and the request did not override
	// it, a _stream carried in the payload is not authoritative and is
	// dropped: the synthesized label replaces it below.
	dropPayloadStream := len(w.strmFlds) > 0 && !streamOverridden
	row := len(w.ts)
	w.ts = append(w.ts, ts)
	for k, v := range fields {
		if k == "_stream" && dropPayloadStream {
			continue
		}
		cb := w.cols[k]
		if cb == nil {
			cb = &colBuf{name: k, vals: make([]string, row)} // backfill prior rows
			w.cols[k] = cb
			w.colOrder = append(w.colOrder, k)
		}
		// backfill any gap (rows added before this column existed)
		for len(cb.vals) < row {
			cb.vals = append(cb.vals, "")
		}
		cb.vals = append(cb.vals, v)
		w.bytes += len(k) + len(v)
	}
	// Synthesize the _stream label from the configured stream fields, so
	// `stats by (_stream)` and stream-scoped retention have a value to group
	// on. The selector `_stream:{...}` queries the underlying label fields
	// directly and needs no column, so this is opt-in (empty strmFlds = off).
	//
	// The request's own _stream_fields override the deployment default, and
	// the caller says so explicitly. Synthesizing on top of an overridden
	// record appended a second value to the same column for one row; the
	// padding loop below only lengthens columns that are short, so nothing
	// corrected it, and every later row read one row late in that column.
	if len(w.strmFlds) > 0 && !streamOverridden {
		if sv := buildStreamLabel(w.strmFlds, fields); sv != "" {
			cb := w.cols["_stream"]
			if cb == nil {
				cb = &colBuf{name: "_stream", vals: make([]string, row)}
				w.cols["_stream"] = cb
				w.colOrder = append(w.colOrder, "_stream")
			}
			for len(cb.vals) < row {
				cb.vals = append(cb.vals, "")
			}
			cb.vals = append(cb.vals, sv)
			w.bytes += len("_stream") + len(sv)
		}
	}
	// pad columns this row did not set
	for _, k := range w.colOrder {
		cb := w.cols[k]
		for len(cb.vals) <= row {
			cb.vals = append(cb.vals, "")
		}
	}
	if len(w.ts) >= FlushRows || w.bytes >= FlushBytes || nowFn().Sub(w.lastFlsh) >= FlushEvery {
		w.flushLocked()
	}
}

// SetStreamFields declares which fields identify a log stream. When set, Add
// synthesizes a canonical _stream label ({k="v",...}, keys sorted) from those
// of them present on each record. Set once before ingest; safe under the same
// lock Add takes.
func (w *Writer) SetStreamFields(fs []string) {
	w.mu.Lock()
	w.strmFlds = append(w.strmFlds[:0], fs...)
	w.mu.Unlock()
}

// SetCompact enables compact mode: flushed groups flate their dictionaries for
// a smaller on-disk footprint, at the cost of slower dict decode. Opt-in; the
// default (LZ4) is unchanged. Set once before ingest.
func (w *Writer) SetCompact(on bool) {
	w.mu.Lock()
	w.compact = on
	w.mu.Unlock()
}

// buildStreamLabel renders the present stream fields as a canonical VL-style
// stream label; keys are sorted so the same label set always yields the same
// string (and thus the same dict id). Empty when no stream field is present.
func buildStreamLabel(streamFields []string, fields map[string]string) string {
	type kv struct{ k, v string }
	present := make([]kv, 0, len(streamFields))
	for _, k := range streamFields {
		if v, ok := fields[k]; ok && v != "" {
			present = append(present, kv{k, v})
		}
	}
	if len(present) == 0 {
		return ""
	}
	sort.Slice(present, func(i, j int) bool { return present[i].k < present[j].k })
	var sb strings.Builder
	sb.WriteByte('{')
	for i, p := range present {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(p.k)
		sb.WriteString(`="`)
		sb.WriteString(p.v)
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

// Flush enqueues the buffered rows and waits for every in-flight group to
// land -- the batch boundary, where durability is promised.
// ErrWriterClosed reports a write to a writer that has been closed. It is a
// typed error rather than a panic because it is reachable from an ordinary
// request: http.Server.Shutdown lets in-flight requests finish, so a handler
// can call Flush after Close has already run.
var ErrWriterClosed = errors.New("ingest: writer is closed")

func (w *Writer) Flush() error {
	// defer, not a bare Unlock. flushLocked used to send on a channel that
	// Close had already closed; the panic unwound past the Unlock, so the
	// mutex was never released and the next Close blocked on it forever --
	// shutdown deadlocked permanently after one racing request.
	w.mu.Lock()
	closed := w.closed.Load()
	var wait []*flushBatch
	if !closed {
		w.flushLocked()
		// Wait on EVERY batch with work outstanding, not only this call's.
		// The buffer is shared by every request and every syslog connection
		// on the tenant, so a row added here is routinely carried away by
		// another goroutine's Flush; waiting only on this call's batch told
		// 9 of 32 concurrent callers "stored" for rows that had failed in
		// someone else's batch.
		wait = append(wait, w.live...)
		// New jobs go to a fresh batch, so nothing in `wait` can gain one
		// after this point -- which is what makes each WaitGroup waited
		// exactly once and never re-Added after. A single shared WaitGroup
		// was reused across callers, the documented misuse: two syslog
		// senders, needing no credential, killed the process, and 46 of 640
		// concurrent HTTP posts to one tenant panicked into 500s.
		w.batch = w.newBatchLocked()
	}
	w.mu.Unlock()
	if closed {
		return ErrWriterClosed
	}

	var first error
	for _, b := range wait {
		b.wg.Wait()
		b.done.Store(true)
		if e := b.err.Load(); e != nil && first == nil {
			first = *e
		}
	}

	// Retire what is finished. Errors are no longer retained here: a caller
	// that needs to know whether ITS rows landed uses Mark/FlushMark, which
	// asks about exactly the batches those rows could have joined. Holding
	// errored batches for a plain Flush was the previous attempt, and it
	// made every later empty Flush -- an empty body, a rejected line, Close
	// at shutdown -- return a failure that was already reported.
	w.mu.Lock()
	kept := w.live[:0]
	for _, b := range w.live {
		if !b.done.Load() {
			kept = append(kept, b)
		}
	}
	w.live = kept
	w.mu.Unlock()
	return first
}

// Close flushes, stops the pool, and joins the workers. After Close the
// writer is done; the store remains usable.
func (w *Writer) Close() error {
	err := w.Flush()
	w.closeOnce.Do(func() {
		// Mark closed under the same lock Add and Flush take, so no send can
		// be in flight when the channel closes.
		w.mu.Lock()
		w.closed.Store(true)
		w.mu.Unlock()
		close(w.jobs)
		w.workers.Wait()
	})
	return err
}

// flushLocked hands the current buffers to the pool and swaps in fresh
// ones, so the parser continues immediately. Must hold w.mu.
func (w *Writer) flushLocked() {
	if len(w.ts) == 0 {
		return
	}
	vals := make(map[string][]string, len(w.colOrder))
	for _, k := range w.colOrder {
		vals[k] = w.cols[k].vals
	}
	if len(w.live) == 0 || w.live[len(w.live)-1] != w.batch {
		w.live = append(w.live, w.batch)
	}
	w.batch.wg.Add(1)
	w.jobs <- flushJob{ts: w.ts, colOrder: w.colOrder, vals: vals, compact: w.compact, batch: w.batch}

	// Fresh buffers; the job owns the handed-off ones.
	w.ts = make([]int64, 0, FlushRows)
	w.cols = map[string]*colBuf{}
	w.colOrder = nil
	w.bytes = 0
	w.lastFlsh = nowFn()
}

// batchHistory bounds the batch ring. A caller marks, adds rows, then
// flushes, so it is never more than a handful of batches behind; 64 is far
// past any real interleaving and costs three words each.
const batchHistory = 64

// newBatchLocked installs the next batch and remembers it. w.mu must be held.
func (w *Writer) newBatchLocked() *flushBatch {
	w.nextSeq++
	b := &flushBatch{seq: w.nextSeq}
	w.hist = append(w.hist, b)
	if len(w.hist) > batchHistory {
		w.hist = append(w.hist[:0], w.hist[len(w.hist)-batchHistory:]...)
	}
	return b
}

// Mark names the point a caller is about to add rows from. Pass it to
// FlushMark to learn whether THOSE rows reached the store.
//
// Flush cannot answer that question. The row buffer is shared by every
// request and every syslog connection on a tenant, so another goroutine's
// Flush routinely carries a caller's rows away, and a plain Flush reports on
// whatever it happened to wait for. That is how a caller got 200 for rows
// that died in someone else's batch.
func (w *Writer) Mark() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.batch.seq
}

// FlushMark flushes and reports the first error from any batch a row added
// since mark could have joined -- that is, every batch from mark onward.
func (w *Writer) FlushMark(mark uint64) error {
	w.mu.Lock()
	closed := w.closed.Load()
	var wait []*flushBatch
	if !closed {
		w.flushLocked()
		for _, b := range w.hist {
			if b.seq >= mark {
				wait = append(wait, b)
			}
		}
		w.batch = w.newBatchLocked()
	}
	w.mu.Unlock()
	if closed {
		return ErrWriterClosed
	}
	var first error
	for _, b := range wait {
		b.wg.Wait()
		b.done.Store(true)
		if e := b.err.Load(); e != nil && first == nil {
			first = *e
		}
	}
	w.mu.Lock()
	kept := w.live[:0]
	for _, b := range w.live {
		if !b.done.Load() {
			kept = append(kept, b)
		}
	}
	w.live = kept
	w.mu.Unlock()
	return first
}
