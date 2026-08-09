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
func NewWriter(s *storage.Store) *Writer {
	w := &Writer{
		store:    s,
		cols:     map[string]*colBuf{},
		lastFlsh: nowFn(),
		jobs:     make(chan flushJob, flushWorkers),
	}
	for i := 0; i < flushWorkers; i++ {
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
