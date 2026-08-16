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
	"fmt"
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
	store    *storage.Store
	ts       []int64
	cols     map[string]*colBuf
	colOrder []string
	// newCols is scratch for the names one record introduces, so registering
	// them in a deterministic order costs no allocation on the per-row path.
	newCols  []string
	strmFlds []string // fields that identify a log stream; synthesize _stream from them
	// vecFlds is which fields are embeddings and at what dimension; vecs
	// holds their flat row-major data for the batch being built. Separate
	// from cols because a vector is not a string and round-tripping it
	// through the dictionary would store the TEXT of 768 floats per row and
	// make every one of them a distinct dictionary entry -- the worst case
	// for a structure whose whole value is repetition.
	vecFlds VectorFields
	// hasVec mirrors len(vecFlds) > 0 without the mutex, so the per-record path
	// can ask "is any field an embedding here" for the price of one atomic load
	// rather than a lock. Almost every deployment configures none, and that
	// case has to cost nothing.
	hasVec    atomic.Bool
	vecs      map[string][]float32
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
	// outcomes is what batches retired from hist left behind, oldest first,
	// so a FlushMark arriving after its batch aged out still gets an answer.
	// oldestAnswerable is the lowest mark that answer can cover; below it the
	// writer says it does not know rather than saying nothing failed.
	outcomes         []batchOutcome
	oldestAnswerable uint64

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

	// jobs and failed count the groups this batch handed to the pool and the
	// ones that did not reach the store. Counted per JOB, not per batch: one
	// batch routinely holds several groups (a caller whose rows crossed the
	// row or byte trigger while it was adding them), so a batch-level flag
	// cannot tell "every group failed" from "one of three failed". That is
	// exactly the distinction a client needs, because the first means a
	// retry is clean and the second means a retry duplicates.
	jobs   atomic.Int32
	failed atomic.Int32

	// outstanding is jobs this batch handed to the pool that have not come
	// back. It is what makes the counters above safe to READ.
	//
	// The outcome log froze a retired batch's counters, and a batch can leave
	// the ring while its job is still running: FlushMark waits only on
	// batches at or after its own mark, so a later caller never blocks on an
	// older batch, and 64 later flushes retire it. The snapshot then said
	// "one job, none failed" for a job that went on to fail with ENOSPC, and
	// FlushMark returned nil for rows that are not in the store -- the same
	// 200-for-lost-rows failure the outcome log was added to prevent, moved
	// from the ring into the log.
	//
	// A batch is only retired at zero. The worker decrements this AFTER
	// recording its error and its failure, so observing zero is observing
	// counters that can no longer change.
	outstanding atomic.Int32
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
	vecs     map[string][]float32
	vecFlds  VectorFields
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
		vecs:     map[string][]float32{},
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
		// Vector columns after the dictionaries, in configured-name order via
		// the job's own map -- their content is fixed-width and needs no
		// dictionary build.
		for name, dim := range j.vecFlds {
			data := j.vecs[name]
			if len(data) == 0 {
				continue
			}
			// A row that carried no vector was zero-filled at add time, so the
			// flat buffer is exactly Rows*Dim. A short buffer here would be a
			// bug in that backfill, and appending a column whose length does
			// not match the row count would build a group whose vector rows
			// are offset from its other columns -- every score attached to the
			// wrong line.
			if len(data) != len(j.ts)*dim {
				e := fmt.Errorf("ingest: vector column %q has %d floats for %d rows of %d dimensions",
					name, len(data), len(j.ts), dim)
				j.batch.err.CompareAndSwap(nil, &e)
				j.batch.failed.Add(1)
				continue
			}
			cols = append(cols, storage.Column{
				Name: name, Type: storage.ColVector, Vec: data, Dim: dim,
			})
		}
		g := &storage.Group{Rows: len(j.ts), Columns: cols, Compact: j.compact}
		if _, err := w.store.AppendGroup(g); err != nil {
			e := err
			j.batch.err.CompareAndSwap(nil, &e)
			// Counted whether or not this was the first error. The first one
			// is what the caller is shown; the count is what says whether
			// ANY group in the window landed, which is the fact that decides
			// whether a retry duplicates.
			j.batch.failed.Add(1)
		}
		// After the counters, before the Wait is released. A reader that sees
		// zero here has seen every write above it.
		j.batch.outstanding.Add(-1)
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

// SetVectorFields declares which record fields are embeddings, and at what
// dimension. Safe to call before the writer takes any rows; changing it with
// rows buffered would put two dimensions in one column.
//
// The dimension is configuration rather than something learned from the first
// record, because a learned one is re-learned after a restart: a deployment
// whose first post-restart record carried 768 floats would silently define the
// column afresh and split its corpus in two, each half invisible to the
// other's queries.
func (w *Writer) SetVectorFields(v VectorFields) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.vecFlds = v
	w.hasVec.Store(len(v) > 0)
}

// VectorFields reports the configured embedding fields.
func (w *Writer) VectorFields() VectorFields {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.vecFlds
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

// AddVectors is Add for a record carrying embeddings.
//
// The floats arrive already parsed, from the ingest path that read them out of
// the payload -- not as text this re-parses. A vector round-tripped through a
// string is 768 floats formatted and re-parsed per record on the hot path, for
// a value that was already in the right form when it was read.
//
// vecs is borrowed for the call: the writer copies into its own column buffer,
// so an ingest loop reuses one scratch slice per record rather than allocating
// 3 KiB of garbage per log line.
func (w *Writer) AddVectors(ts int64, fields map[string]string, vecs map[string][]float32) {
	w.addVec(ts, fields, false, vecs)
}

// AddStreamOverriddenVectors is AddVectors for a request that names its own
// _stream_fields.
//
// addVec has taken both a stream-override flag and a vector map since it was
// written; only three of the four pairings had a caller, and the missing one
// was "this request labels its own streams AND carries an embedding". A record
// hitting both had to lose one of them.
func (w *Writer) AddStreamOverriddenVectors(ts int64, fields map[string]string, vecs map[string][]float32) {
	w.addVec(ts, fields, true, vecs)
}

func (w *Writer) add(ts int64, fields map[string]string, streamOverridden bool) {
	w.addVec(ts, fields, streamOverridden, nil)
}

func (w *Writer) addVec(ts int64, fields map[string]string, streamOverridden bool,
	vecs map[string][]float32,
) {
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
	// New columns are registered in SORTED order, not in the order this map
	// iterates.
	//
	// A group's column order is the order its columns were first seen, and
	// `range` over a map is randomised per process. So the same records written
	// by two processes produced groups whose columns were in different orders,
	// and a row read back came out with its fields permuted -- differently on
	// every node. In a cluster that is visible: one shard answers
	// {_msg, level, user} and its neighbour answers {user, _msg, level} for the
	// same row, and a client reading NDJSON sees the shape change by shard.
	//
	// Sorting only the names that are NEW keeps this off the per-row path: a
	// row that introduces no column sorts nothing, which is every row after the
	// first in a uniform stream. The scratch slice is reused, so the common
	// case allocates nothing.
	w.newCols = w.newCols[:0]
	for k, v := range fields {
		if k == "_stream" && dropPayloadStream {
			continue
		}
		if _, isVec := w.vecFlds.Dim(k); isVec {
			// A configured vector field arriving as an ordinary string is
			// dropped, not stored as text. Storing the TEXT of 768 floats
			// would make every row a distinct dictionary entry -- the worst
			// case for a structure whose whole value is repetition -- and the
			// search reads float32s, so it would be invisible to the one
			// query the field exists for. The float path is AddVectors.
			continue
		}
		cb := w.cols[k]
		if cb == nil {
			w.newCols = append(w.newCols, k) // registered below, in order
			continue
		}
		// backfill any gap (rows added before this column existed)
		for len(cb.vals) < row {
			cb.vals = append(cb.vals, "")
		}
		cb.vals = append(cb.vals, v)
		w.bytes += len(k) + len(v)
	}
	if len(w.newCols) > 0 {
		sort.Strings(w.newCols)
		for _, k := range w.newCols {
			cb := &colBuf{name: k, vals: make([]string, row)} // backfill prior rows
			w.cols[k] = cb
			w.colOrder = append(w.colOrder, k)
			v := fields[k]
			cb.vals = append(cb.vals, v)
			w.bytes += len(k) + len(v)
		}
	}
	// The embeddings, into the flat per-column buffer.
	//
	// Backfilled to this row's offset first: a record that carried no vector
	// for a configured field still occupies its slot, because the search reads
	// row i of the flat buffer as row i of the group. A gap would move every
	// later row's score onto the wrong line -- a wrong answer rather than a
	// missing one.
	for name, dim := range w.vecFlds {
		v := vecs[name]
		if len(v) != dim {
			if _, seen := w.vecs[name]; !seen && len(v) == 0 {
				// Nothing has ever supplied this field: no column, no
				// backfill. A column of zeros for a field nobody sends is
				// Rows*Dim*4 bytes of nothing.
				continue
			}
			v = nil
		}
		buf := w.vecs[name]
		for len(buf) < row*dim {
			buf = append(buf, 0)
		}
		if v == nil {
			for i := 0; i < dim; i++ {
				buf = append(buf, 0)
			}
		} else {
			buf = append(buf, v...)
		}
		w.vecs[name] = buf
		w.bytes += len(name) + dim*4
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

// FlushWithReceipt flushes and records a write id in the same commit.
//
// The id is recorded only after the rows are DURABLE. Recording it when the
// rows were merely accepted into the buffer would be worse than not recording
// it at all: a crash before the flush loses the rows while the receipt says
// committed, so the retry that would have saved them is refused as a
// duplicate. The whole point of the receipt is to make a retry safe, and that
// version would make it unsafe in the one case where it matters.
//
// A flush per replicated write is a real cost, and it is the cost of the
// guarantee: the writer batches rows from many requests, so "this request's
// rows are stored" is not a question the batch can answer without one.
// Ordinary client writes do not pay it -- they carry no write id.
func (w *Writer) FlushWithReceipt(id storage.WriteID) error {
	if err := w.Flush(); err != nil {
		return err
	}
	if id == "" {
		return nil
	}
	// After the flush: the rows this request contributed are in a committed
	// group, so the receipt is now true.
	return w.store.CommitReceipt(id)
}

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
		// Typed like every other flush failure, so a caller that reads the
		// retry metadata does not have to special-case shutdown.
		//
		// Partial, and that is not a hedge. Close FLUSHES the shared buffer
		// before it sets closed, so the rows an in-flight handler added are
		// routinely on disk by the time that handler gets here -- and
		// http.Server.Shutdown letting one request finish is exactly the case
		// ErrWriterClosed exists for. Reporting Partial: false rendered as
		// `"duplicateOnRetry": false` for rows that were durable, which is the
		// claim that costs a client a duplicate.
		//
		// Zero groups because this CALL handed none to the pool; the counts
		// and the partial flag answer different questions.
		return &WriteError{Err: ErrWriterClosed, Class: RetrySoon, Partial: true}
	}

	return w.awaitBatches(wait, nil)
}

// awaitBatches waits for every batch in wait, retires what has finished, and
// turns any failure into the typed *WriteError a client can act on.
//
// The counters are read AFTER each Wait, and that ordering is what makes them
// a consistent pair: both are written only by the jobs the batch is waiting
// on, and no job can join a batch that has already been swapped out of
// w.batch, so nothing can add to either counter once the Wait returns.
func (w *Writer) awaitBatches(wait []*flushBatch, retired []batchOutcome) error {
	var first error
	var total, failed int
	// Retired batches first, so their seqs -- which are older -- contribute
	// their counts before the live ones and the first error is the earliest.
	for _, o := range retired {
		total += int(o.jobs)
		failed += int(o.failed)
		if o.err != nil && first == nil {
			first = o.err
		}
	}
	for _, b := range wait {
		b.wg.Wait()
		b.done.Store(true)
		total += int(b.jobs.Load())
		failed += int(b.failed.Load())
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
	if first == nil {
		return nil
	}
	return newWriteError(first, failed, total)
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
	w.batch.jobs.Add(1)
	w.batch.outstanding.Add(1)
	// Every vector column is padded to the full row count before handoff. A
	// row that carried no embedding still occupies its slot: a vector column
	// shorter than the group's rows would put every score on the wrong line,
	// because the search reads row i of the flat buffer as row i of the group.
	for name, dim := range w.vecFlds {
		if len(w.vecs[name]) == 0 {
			continue
		}
		for len(w.vecs[name]) < len(w.ts)*dim {
			w.vecs[name] = append(w.vecs[name], 0)
		}
	}
	w.jobs <- flushJob{ts: w.ts, colOrder: w.colOrder, vals: vals, vecs: w.vecs,
		vecFlds: w.vecFlds, compact: w.compact, batch: w.batch}

	// Fresh buffers; the job owns the handed-off ones.
	w.ts = make([]int64, 0, FlushRows)
	w.cols = map[string]*colBuf{}
	w.vecs = map[string][]float32{}
	w.colOrder = nil
	w.bytes = 0
	w.lastFlsh = nowFn()
}

// batchHistory bounds the live batch ring, and outcomeHistory bounds what a
// retired batch leaves behind.
//
// The ring alone was not enough, and the way it failed is the reason for the
// second bound. Every Flush and FlushMark installs a new batch whether or not
// the old one carried anything, so 64 flushes from ANY caller on the tenant --
// other requests, a syslog connection flushing per line, the FlushEvery timer,
// Close at shutdown -- evicted the batch a marked caller's rows were in. That
// batch was then never waited on by FlushMark and its error never seen: the
// caller was told success for rows that are not in the store. Measured at
// MaxConcurrentWrite 32, 64 slots is about two completed request cycles.
//
// Two changes close it. An outgoing batch that carried NO jobs is dropped from
// the ring instead of aging a real one out of it, so an idle or rejected flush
// costs nothing. And a batch that did carry jobs leaves a small outcome record
// behind when it is retired, so a FlushMark arriving late still gets the right
// answer rather than a nil.
const (
	batchHistory   = 64
	outcomeHistory = 1024
	// maxHistory is the hard ceiling on the ring, including batches held past
	// batchHistory because a job of theirs is still running. Past it a stalled
	// batch is dropped as unanswerable rather than held forever: a stalled
	// fsync must not let client traffic grow a per-tenant slice without bound,
	// and FlushMark walks that slice under the writer's lock.
	//
	// 4096 is two orders past batchHistory, so it is only reached by a stall
	// that outlasts thousands of requests -- and at that point the writer
	// genuinely does not know, which is what it then says.
	maxHistory = 4096
)

// batchOutcome is what a retired batch leaves behind: enough to answer a
// FlushMark that arrives after the batch itself is gone.
//
// Four words rather than a whole flushBatch with its WaitGroup, so a thousand
// of them per writer is tens of kilobytes.
type batchOutcome struct {
	seq    uint64
	jobs   int32
	failed int32
	err    error
}

// newBatchLocked installs the next batch and remembers it. w.mu must be held.
func (w *Writer) newBatchLocked() *flushBatch {
	// The outgoing batch carried nothing, so it can hold no rows and no error
	// and there is nothing for any caller to learn from it. Dropping it here
	// is what keeps an empty flush from aging a real batch out of the ring.
	if n := len(w.hist); n > 0 && w.hist[n-1] == w.batch && w.batch.jobs.Load() == 0 {
		w.hist = w.hist[:n-1]
	}
	w.nextSeq++
	b := &flushBatch{seq: w.nextSeq}
	w.hist = append(w.hist, b)
	if len(w.hist) > batchHistory {
		// Retire from the front, and STOP at the first batch still holding a
		// running job. Retiring one would freeze counters that are not final
		// yet, and the frozen answer is "nothing failed" -- the wrong
		// direction to be wrong in.
		//
		// That stop is not itself a bound. The pool bounds outstanding JOBS;
		// it bounds no number of batches, and every later job-carrying flush
		// appends one more. One worker stalled on a slow fsync while the
		// others drain is all it takes: measured, 5000 client requests with
		// one job pinned left hist at 5002 entries, and FlushMark walks the
		// whole slice under w.mu on every request. Growth is request rate
		// times stall duration, per tenant -- driven by exactly what a client
		// sends, which an earlier version of this comment denied.
		//
		// So there is a hard ceiling. Past it the stalled batch is dropped
		// WITHOUT an outcome and oldestAnswerable moves past it, which makes
		// every mark at or below it answer ErrDurabilityUnknown. Refusing to
		// answer is the only safe thing to do with counters that are not
		// final; recording them would be the frozen-zero defect again.
		drop := 0
		for drop < len(w.hist)-batchHistory {
			old := w.hist[drop]
			if old.outstanding.Load() != 0 {
				if len(w.hist) <= maxHistory {
					break
				}
				// Over the ceiling and still running: drop it unanswerable,
				// and drop it from `live` with it.
				//
				// Leaving it in `live` kept two problems. Its counters --
				// final by the time anyone read them -- were folded into the
				// next unrelated plain Flush, so a caller whose own rows all
				// landed was handed someone else's "1 of 2 groups failed,
				// partial". And Flush waits on all of `live`, so every Flush
				// blocked on the stalled job.
				//
				// FLUSH ONLY. An earlier version of this comment said the same
				// of Writer.Close and of tenant eviction, and measurement says
				// otherwise: Close runs Flush and then workers.Wait(), and a
				// worker parked inside AppendGroup has not returned, so Close
				// blocks on a stalled writer whether or not this branch ever
				// fires. Eviction blocks for the same reason, one level up.
				// The drop bounds the ring and unblocks Flush; it does not
				// unblock a join. See docs/wrong.md.
				//
				// The job keeps its pointer and will still call wg.Done() and
				// write its atomics. Nothing reads them, and nothing frees the
				// batch until the job lets go of it.
				if old.seq >= w.oldestAnswerable {
					w.oldestAnswerable = old.seq + 1
				}
				w.dropFromLiveLocked(old)
				drop++
				continue
			}
			w.retireLocked(old)
			drop++
		}
		if drop > 0 {
			w.hist = append(w.hist[:0], w.hist[drop:]...)
		}
	}
	return b
}

// dropFromLiveLocked removes one batch from w.live. w.mu must be held.
//
// Used only by the ceiling drop: everywhere else a batch leaves `live` by
// being waited on and marked done, which is what makes its counters final.
// This one is being abandoned precisely because they are not.
func (w *Writer) dropFromLiveLocked(b *flushBatch) {
	for i, live := range w.live {
		if live == b {
			w.live = append(w.live[:i], w.live[i+1:]...)
			return
		}
	}
}

// retireLocked folds a batch leaving the ring into the outcome log. w.mu must
// be held.
func (w *Writer) retireLocked(b *flushBatch) {
	jobs := b.jobs.Load()
	if jobs == 0 {
		return // held nothing; there is no outcome to remember
	}
	o := batchOutcome{seq: b.seq, jobs: jobs, failed: b.failed.Load()}
	if e := b.err.Load(); e != nil {
		o.err = *e
	}
	w.outcomes = append(w.outcomes, o)
	if len(w.outcomes) > outcomeHistory {
		drop := len(w.outcomes) - outcomeHistory
		// oldestAnswerable rises with the log, so a mark older than anything
		// retained is answered "unknown" rather than "fine". A wrong nil is
		// the failure this whole mechanism exists to prevent; refusing to
		// answer is not.
		//
		// It only ever RISES. This assignment was unconditional, and the
		// ceiling path in newBatchLocked also moves it -- so a normal retire
		// following an unanswerable drop lowered it again and un-hid the
		// dropped batch, which is in neither the ring nor the log. Measured
		// end to end with two stalled workers: FlushMark answered nil for a
		// group that had failed with ENOSPC. The whole mechanism is a
		// watermark; a watermark that can go backwards is not one.
		if seq := w.outcomes[drop-1].seq + 1; seq > w.oldestAnswerable {
			w.oldestAnswerable = seq
		}
		w.outcomes = append(w.outcomes[:0], w.outcomes[drop:]...)
	}
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
	var retired []batchOutcome
	unanswerable := false
	if !closed {
		w.flushLocked()
		for _, b := range w.hist {
			if b.seq >= mark {
				wait = append(wait, b)
			}
		}
		// Batches that have already left the ring but could still hold this
		// caller's rows. Without this a caller whose batch aged out was told
		// success for rows that never reached the store.
		for _, o := range w.outcomes {
			if o.seq >= mark {
				retired = append(retired, o)
			}
		}
		unanswerable = mark < w.oldestAnswerable
		w.batch = w.newBatchLocked()
	}
	w.mu.Unlock()
	if unanswerable {
		// Older than anything retained. The truthful answer is that this
		// writer no longer knows, and a caller must treat that as a possible
		// failure -- Partial, because some of the payload may well be stored.
		return &WriteError{
			Err:     ErrDurabilityUnknown,
			Class:   RetrySoon,
			Partial: true,
		}
	}
	if closed {
		// Typed like every other flush failure, so a caller that reads the
		// retry metadata does not have to special-case shutdown.
		//
		// Partial, and that is not a hedge. Close FLUSHES the shared buffer
		// before it sets closed, so the rows an in-flight handler added are
		// routinely on disk by the time that handler gets here -- and
		// http.Server.Shutdown letting one request finish is exactly the case
		// ErrWriterClosed exists for. Reporting Partial: false rendered as
		// `"duplicateOnRetry": false` for rows that were durable, which is the
		// claim that costs a client a duplicate.
		//
		// Zero groups because this CALL handed none to the pool; the counts
		// and the partial flag answer different questions.
		return &WriteError{Err: ErrWriterClosed, Class: RetrySoon, Partial: true}
	}
	return w.awaitBatches(wait, retired)
}
