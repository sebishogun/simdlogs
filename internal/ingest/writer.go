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
	strmFlds []string // fields that identify a log stream; synthesize _stream from them
	bytes    int
	lastFlsh time.Time
	mu       sync.Mutex

	jobs      chan flushJob
	pending   sync.WaitGroup // in-flight flush jobs
	workers   sync.WaitGroup // worker goroutines, joined by Close
	flushErr  atomic.Pointer[error]
	closeOnce sync.Once
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
	}
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
		g := &storage.Group{Rows: len(j.ts), Columns: cols}
		if _, err := w.store.AppendGroup(g); err != nil {
			e := err
			w.flushErr.CompareAndSwap(nil, &e)
		}
		w.pending.Done()
	}
}

// Add appends one record: a timestamp and a set of string fields. Unknown
// fields create a column; a row missing a known field gets an empty value
// in it, which the dict encodes once (schema-free, like the reference).
func (w *Writer) Add(ts int64, fields map[string]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	row := len(w.ts)
	w.ts = append(w.ts, ts)
	for k, v := range fields {
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
	if len(w.strmFlds) > 0 {
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
func (w *Writer) Flush() error {
	w.mu.Lock()
	w.flushLocked()
	w.mu.Unlock()
	w.pending.Wait()
	if e := w.flushErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Close flushes, stops the pool, and joins the workers. After Close the
// writer is done; the store remains usable.
func (w *Writer) Close() error {
	err := w.Flush()
	w.closeOnce.Do(func() {
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
	w.pending.Add(1)
	w.jobs <- flushJob{ts: w.ts, colOrder: w.colOrder, vals: vals}

	// Fresh buffers; the job owns the handed-off ones.
	w.ts = make([]int64, 0, FlushRows)
	w.cols = map[string]*colBuf{}
	w.colOrder = nil
	w.bytes = 0
	w.lastFlsh = nowFn()
}
