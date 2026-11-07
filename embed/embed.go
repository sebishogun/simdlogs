// Package embed runs simdlogs inside another Go program: no listener, no second
// process, and whatever authorization the host program has already done.
//
// A DB is one store. A host that keeps tenants apart opens one DB for each, which
// is what the HTTP server does, because a query can name no data outside its store:
// subqueries, joins and unions included. The host owns what a query may see by
// choosing which DB to run it on, and never needs to rewrite the query.
//
// As with the rest of simdlogs, there is no stable format or API yet. Pin a commit,
// and treat what is in Dir as a copy you can build again.
package embed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Row is one matching row. A row's strings belong to the caller, but the slice
// handed to a search's callback is only valid during that call.
type Row = query.Row

// Field is one field of a row.
type Field = query.Field

// Config says where a DB lives and what it may keep. The zero value of each
// limit means no limit.
type Config struct {
	// Dir holds the store, and nothing else may write there.
	Dir string
	// StreamFields are the fields that identify a log stream. Set once and
	// keep: rows are grouped by them on disk.
	StreamFields []string
	// MaxBytes is how much disk the store may use. Past it the oldest groups
	// go, rather than new rows being refused: for logs the newest matter most.
	MaxBytes int64
	// Retention is how long rows are kept.
	Retention time.Duration
	// MaintainEvery is how often limits are enforced and small groups merged.
	// Zero is once a minute.
	MaintainEvery time.Duration
}

// ErrRowLimit says a search matched more than MaxRows, and ErrBadQuery that its
// query could not be read.
var (
	ErrRowLimit = query.ErrRowLimit
	ErrBadQuery = errors.New("embed: the query cannot be read")
)

// DB is an open store. It is safe for concurrent use.
type DB struct {
	config Config
	store  *storage.Store
	writer *ingest.Writer
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	closed error
}

// maintenance merges the small groups that frequent flushes leave. A host
// flushing once a second would otherwise have 86,400 of them a day.
const (
	compactMinGroups = 8
	compactMaxRows   = 64 << 10
	compactOutputs   = 16
	compactAfter     = time.Minute
)

// Open opens Dir, creating it if need be. An unreadable group fails the open:
// the host decides whether to set the directory aside and build it again.
func Open(c Config) (*DB, error) {
	if c.Dir == "" {
		return nil, errors.New("embed: no directory")
	}
	if c.MaxBytes < 0 || c.Retention < 0 || c.MaintainEvery < 0 {
		return nil, errors.New("embed: a negative limit")
	}
	if c.MaintainEvery == 0 {
		c.MaintainEvery = time.Minute
	}
	store, err := storage.OpenStore(c.Dir)
	if err != nil {
		return nil, err
	}
	writer := ingest.NewWriter(store)
	if len(c.StreamFields) != 0 {
		writer.SetStreamFields(c.StreamFields)
	}
	db := &DB{config: c, store: store, writer: writer, stop: make(chan struct{}), done: make(chan struct{})}
	go db.maintain()
	return db, nil
}

// Add buffers one row. It keeps the strings, not the map, so a caller may reuse
// the map and should not build new strings if it can share them: a line can be a
// substring of one string per batch, with no allocation per row.
func (db *DB) Add(at int64, fields map[string]string) { db.writer.Add(at, fields) }

// Columns adds rows by position, with the field names declared once. A host that
// writes the same fields on every line uses it in place of Add: it takes no map, it
// finds each column by name once per group rather than per row, and it builds the
// _stream label once per distinct set of stream values rather than per row. Once a
// store has flushed a batch about the size of the next, adding rows allocates
// nothing.
//
// What it stores is what Add stores for a map holding the same non-empty values.
// It is safe for concurrent use, and rows from it and from Add may be mixed.
type Columns struct{ c *ingest.Columns }

// Columns declares the fields rows will be added with, in order. A name may not be
// empty, repeated, _time, or start with a dot.
func (db *DB) Columns(names ...string) (*Columns, error) {
	c, err := db.writer.Columns(names)
	if err != nil {
		return nil, err
	}
	return &Columns{c: c}, nil
}

// Add buffers len(at) rows, column by column: values[i][r] is field i of row r, and
// each of values holds len(at) of them. An empty value is a field the row does not
// have, as if Add's map had left the key out. The strings are kept and the slices
// are not, so a caller may reuse every slice as soon as Add returns.
func (c *Columns) Add(at []int64, values ...[]string) error { return c.c.Add(at, values) }

// AddRow buffers one row: values[i] is field i. It costs a lock per row that Add
// pays once per call.
func (c *Columns) AddRow(at int64, values ...string) error { return c.c.AddRow(at, values...) }

// Flush makes everything added so far durable, and returns once it is.
func (db *DB) Flush() error { return db.writer.Flush() }

// SearchOptions bounds one search. Zero values mean all of time and no limit
// beyond what the host asks for in Limits.
type SearchOptions struct {
	From, To int64 // Unix nanoseconds. Zero To means now and later.
	// Newest keeps only this many rows, the newest ones. Zero keeps them all.
	Newest int
	// Timeout, MaxBytes and MaxRows stop a search that costs too much, with an
	// error rather than a short answer.
	Timeout  time.Duration
	MaxBytes int64
	MaxRows  int
}

// Search runs a LogsQL query. each may be called several times, in time order,
// and an error from it stops the search and is returned as it is. A search that
// stops early for any other reason returns an error, never a short answer.
func (db *DB) Search(ctx context.Context, logsql string, o SearchOptions, each func(rows []Row) error) error {
	q, err := query.ParseLogsQL(logsql)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadQuery, err)
	}
	now := time.Now().UnixNano()
	q.SetNow(now)
	q.From, q.To = o.From, o.To
	if q.To == 0 {
		q.To = 1 << 62
	}
	q.ToSet = true
	q.LastN = o.Newest
	q.MatAll = true // Whole rows, and a pipe chain that needs less clears it.
	e := query.Executor{Store: db.store, Limits: query.Limits{MaxRows: o.MaxRows, MaxBytes: o.MaxBytes, Timeout: o.Timeout}}
	err = e.Execute(ctx, q, each)
	if errors.Is(err, query.ErrCanceled) && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// DiskBytes is how much disk the store uses, up to the moment it is asked.
func (db *DB) DiskBytes() int64 { return db.store.FreshDiskBytes() }

// Rows is how many rows the store holds.
func (db *DB) Rows() int { return db.store.TotalRows() }

// Close flushes what was added, stops maintenance and closes the store.
func (db *DB) Close() error {
	db.once.Do(func() {
		close(db.stop)
		<-db.done
		db.closed = errors.Join(db.writer.Close(), db.store.Close())
	})
	return db.closed
}

func (db *DB) maintain() {
	defer close(db.done)
	db.enforce() // A host restarted after a long time should not wait a pass.
	t := time.NewTicker(db.config.MaintainEvery)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			db.enforce()
		}
	}
}

// enforce does one pass. It merges before it trims, so a trim sees true sizes.
func (db *DB) enforce() {
	now := time.Now()
	// A failed merge leaves the groups as they were, and the next pass tries again.
	db.store.CompactGroups(storage.CompactOptions{
		MinGroups: compactMinGroups, MaxRowsPerOutput: compactMaxRows, MaxOutputs: compactOutputs,
		OlderThan: now.Add(-compactAfter).UnixNano(),
	})
	if db.config.Retention > 0 {
		db.store.DropGroupsBefore(now.Add(-db.config.Retention).UnixNano())
	}
	if db.config.MaxBytes > 0 {
		db.trim(db.config.MaxBytes)
	}
}

// trim drops the oldest groups until the store is within its budget. Groups differ
// in size, so this is an estimate from the average, and the next pass corrects it.
func (db *DB) trim(budget int64) {
	total := db.DiskBytes()
	if total <= budget {
		return
	}
	var times []int64
	db.store.DropGroupsWhere(func(timeMax int64, _ []string) bool {
		times = append(times, timeMax)
		return false
	})
	if len(times) == 0 {
		return
	}
	slices.Sort(times)
	over := total - budget
	drop := min(int(over*int64(len(times))/total)+1, len(times))
	db.store.DropGroupsBefore(times[drop-1] + 1)
}
