// Package ingest turns log lines into stored groups. The writer buffers
// rows column-first and flushes a group at the size/byte/time trigger;
// the jsonline pipeline parses on simdjson with the values interned into
// the group's per-column dictionaries. Buffers are reused across flushes.
package ingest

import (
	"sync"
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

// Writer accumulates rows and flushes groups to a store. It is safe for
// one goroutine; shard by stream id for concurrent ingest (see Sharded).
type Writer struct {
	store    *storage.Store
	ts       []int64
	cols     map[string]*colBuf
	colOrder []string
	bytes    int
	lastFlsh time.Time
	mu       sync.Mutex
}

// colBuf is one column's row values awaiting a flush.
type colBuf struct {
	name string
	vals []string
}

// NewWriter makes a writer over the store.
func NewWriter(s *storage.Store) *Writer {
	return &Writer{store: s, cols: map[string]*colBuf{}, lastFlsh: nowFn()}
}

var nowFn = time.Now // overridable in tests

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

// Flush writes the buffered rows as a group now (end of a batch).
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *Writer) flushLocked() error {
	if len(w.ts) == 0 {
		return nil
	}
	cols := make([]storage.Column, 0, len(w.colOrder)+1)
	cols = append(cols, storage.Column{Name: "_time", Type: storage.ColTimestamp, Ts: w.ts})
	for _, k := range w.colOrder {
		d := storage.BuildDict(w.cols[k].vals)
		cols = append(cols, storage.Column{Name: k, Type: storage.ColDict, Dict: &d})
	}
	g := &storage.Group{Rows: len(w.ts), Columns: cols}
	if _, err := w.store.AppendGroup(g); err != nil {
		return err
	}
	// reset buffers, keep capacity
	w.ts = w.ts[:0]
	for _, k := range w.colOrder {
		w.cols[k].vals = w.cols[k].vals[:0]
	}
	w.colOrder = w.colOrder[:0]
	w.cols = map[string]*colBuf{}
	w.bytes = 0
	w.lastFlsh = nowFn()
	return nil
}
