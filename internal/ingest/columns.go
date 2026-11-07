package ingest

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
)

// Columns adds rows whose field names were declared once, as values in the
// declared order. It stores exactly what Add stores for a map holding the same
// non-empty values, and it is for a host that writes the same fields on every
// line, where Add's per-row cost is mostly the map.
//
// Add pays per row for: two iterations of the record map, a lookup of every
// field's column by name, a lookup of every column again to pad it, and a
// _stream label built from scratch -- a slice, a sort and a strings.Builder,
// five or six allocations for a string that is the same on every line of a
// job. Here a column is found by name once per group, a label is built once
// per distinct set of stream values, and rows go in column by column: a
// declared column's values for a whole call are one copy into the group's
// buffer.
//
// Steady state allocates nothing per row. The column buffers of a group are
// sized from the row count of the group before it, so a host whose batches are
// of a similar size stops growing them after its first flush; the timestamp
// buffer is already sized to FlushRows by every handoff.
//
// A Columns is safe for concurrent use: everything it keeps per group is read
// and written under the writer's mutex.
type Columns struct {
	w     *Writer
	names []string

	// Everything below is guarded by w.mu.

	// gen is the Writer.gen that slots and stream belong to. A handoff gives
	// the writer fresh buffers, and a pointer into the group before it must
	// never be written through: the flush job owns those.
	gen    uint64
	slots  []*colBuf // each name's column in the current group, nil until found or registered
	stream *colBuf   // the current group's synthesized _stream column, likewise

	// cfg is the Writer.cfgGen that skip, keys and labels were built for.
	cfg uint64
	// skip marks a declared _stream when the writer labels streams itself:
	// Add drops a record's own _stream then, so this drops it too.
	skip []bool
	// keys are the writer's stream fields that are among names, sorted by
	// name, which is the order a label lists them in.
	keys []streamKey
	// labelFixed bounds what one row's label adds to the group's byte count
	// beyond its values: len("_stream"), the braces, and per key its name,
	// =, two quotes and a comma.
	labelFixed int
	labels     labelCache

	// Scratch for one chunk, sized when declared, so the per-row path never
	// grows it.
	sums    []int      // bytes each column adds to the group, as Add counts them
	raw     []int      // bytes of each column's values alone
	pend    []pending  // columns this chunk creates
	offs    []int      // where each key's value starts in the label being built
	buf     []byte     // the label being built
	row     [][]string // AddRow's columns of one value each, over rowVals
	rowVals []string
}

// streamKey is one stream field and the declared column that carries it.
type streamKey struct {
	key string
	col int
}

// pending is a column a chunk creates: the chunk row its first value is on,
// and its declared index, or -1 for _stream.
type pending struct {
	row, col int
}

// labelSlots is how many distinct stream labels a Columns remembers. A host
// writing one job's lines at a time needs one; past this many interleaved
// label sets a row may build its label again, which costs what Add costs.
const labelSlots = 8

// labelCache holds recent labels and the values each was built from. The
// values are kept as substrings of the label itself rather than as the
// caller's strings: a caller's value is often a substring of a whole batch,
// and keeping it would keep the batch alive after its group is gone.
type labelCache struct {
	label [labelSlots]string
	vals  []string // slot s's values at [s*len(keys), (s+1)*len(keys))
	used  int      // slots filled
	last  int      // the slot the previous row used
	next  int      // the slot the next new label replaces
}

// ErrColumnsUnsupported says a writer applies record limits or takes
// embeddings, which Columns does not do. Add does both.
var ErrColumnsUnsupported = errors.New("ingest: declared columns apply no record limits and take no embeddings")

// Columns declares the field names rows will be added with, in order. A name
// may not be empty, repeated, _time (the timestamp column's name), or start
// with a dot (reserved for the server's own signalling).
func (w *Writer) Columns(names []string) (*Columns, error) {
	for i, n := range names {
		switch {
		case n == "":
			return nil, errors.New("ingest: an empty column name")
		case n == "_time":
			return nil, errors.New("ingest: _time is the timestamp, not a column")
		case controlField(n):
			return nil, fmt.Errorf("ingest: column %q: a leading dot is reserved", n)
		case slices.Contains(names[:i], n):
			return nil, fmt.Errorf("ingest: column %q declared twice", n)
		}
	}
	k := len(names)
	c := &Columns{
		w:       w,
		names:   slices.Clone(names),
		slots:   make([]*colBuf, k),
		skip:    make([]bool, k),
		sums:    make([]int, k),
		raw:     make([]int, k),
		pend:    make([]pending, 0, k+1),
		row:     make([][]string, k),
		rowVals: make([]string, k),
	}
	for i := range c.row {
		c.row[i] = c.rowVals[i : i+1 : i+1]
	}
	w.mu.Lock()
	c.configure()
	w.mu.Unlock()
	return c, nil
}

// configure derives what depends on the writer's stream settings. w.mu held.
func (c *Columns) configure() {
	w := c.w
	c.cfg = w.cfgGen
	labelled := len(w.strmFlds) > 0
	for i, n := range c.names {
		c.skip[i] = labelled && n == "_stream"
	}
	c.keys = c.keys[:0]
	c.labelFixed = len("_stream") + 2
	// Every configured field that is declared, repeats included: Add's label
	// lists a field configured twice twice, and so does this one.
	for _, f := range w.strmFlds {
		if i := slices.Index(c.names, f); i >= 0 {
			c.keys = append(c.keys, streamKey{key: f, col: i})
			c.labelFixed += len(f) + 4
		}
	}
	slices.SortStableFunc(c.keys, func(a, b streamKey) int { return strings.Compare(a.key, b.key) })
	c.offs = make([]int, len(c.keys))
	c.labels = labelCache{vals: make([]string, labelSlots*len(c.keys))}
	// Found again on the next chunk: a declared _stream that is now skipped
	// must not keep a slot, or its values and the labels would both go into
	// the one _stream column.
	clear(c.slots)
	c.stream = nil
}

// Add adds len(ts) rows column by column: cols[i][r] is field i of row r.
// An empty value is a field the row does not have, as if Add's map had left
// the key out. The strings are kept and the slices are not, so a caller may
// reuse cols as soon as Add returns.
func (c *Columns) Add(ts []int64, cols [][]string) error {
	if len(cols) != len(c.names) {
		return fmt.Errorf("ingest: %d columns for %d declared names", len(cols), len(c.names))
	}
	for i, col := range cols {
		if len(col) != len(ts) {
			return fmt.Errorf("ingest: column %q has %d values for %d rows", c.names[i], len(col), len(ts))
		}
	}
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	return c.addLocked(ts, cols)
}

// AddRow adds one row: values[i] is field i.
func (c *Columns) AddRow(ts int64, values ...string) error {
	if len(values) != len(c.names) {
		return fmt.Errorf("ingest: %d values for %d declared names", len(values), len(c.names))
	}
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	copy(c.rowVals, values)
	at := [1]int64{ts}
	err := c.addLocked(at[:], c.row)
	// The group's buffers keep the strings; this scratch must not, or the
	// last row's batch stays alive after its group is flushed.
	clear(c.rowVals)
	return err
}

func (c *Columns) addLocked(ts []int64, cols [][]string) error {
	w := c.w
	if w.closed.Load() {
		return nil // as Add: the rows go nowhere, and Flush reports ErrWriterClosed
	}
	if w.limits != (RecordLimits{}) || len(w.vecFlds) > 0 {
		return ErrColumnsUnsupported
	}
	if c.cfg != w.cfgGen {
		c.configure()
	}
	for r := 0; r < len(ts); {
		room := FlushRows - len(w.ts)
		if room <= 0 {
			w.flushLocked()
			continue
		}
		if c.gen != w.gen {
			clear(c.slots)
			c.stream = nil
			c.gen = w.gen
		}
		m := c.fit(cols, r, min(len(ts)-r, room))
		c.appendChunk(ts, cols, r, m)
		r += m
		// The row and byte triggers, checked where Add would have reached
		// them: fit ends a chunk no later than the row that crosses one.
		if len(w.ts) >= FlushRows || w.bytes >= FlushBytes {
			w.flushLocked()
		}
	}
	// The time trigger once per call rather than per row. It moves where a
	// group ends, never what is stored, and asking the clock per row cost Add
	// a vDSO call per line.
	if len(ts) > 0 && nowFn().Sub(w.lastFlsh) >= FlushEvery {
		w.flushLocked()
	}
	return nil
}

// fit returns how many of the m rows from r0 go in as one chunk, and leaves
// each column's byte count for them in sums and raw.
//
// All m, unless the group could reach FlushBytes before the last of them: Add
// hands a group off at the first row that reaches it, and a chunk past that
// row would put rows in a different group. The label's bytes are bounded
// rather than counted, since counting them means building them; near the
// boundary the chunk halves until the bound fits, down to one row, which is
// exactly Add's check.
func (c *Columns) fit(cols [][]string, r0, m int) int {
	w := c.w
	for {
		total := 0
		for i, col := range cols {
			raw, empty := valueBytes(col[r0 : r0+m])
			c.raw[i] = raw
			c.sums[i] = 0
			if !c.skip[i] {
				// Add counts a field's name and value for each row that has it.
				c.sums[i] = raw + (m-empty)*len(c.names[i])
				total += c.sums[i]
			}
		}
		if len(c.keys) > 0 {
			total += m * c.labelFixed
			for _, k := range c.keys {
				total += c.raw[k.col]
			}
		}
		if w.bytes+total < FlushBytes || m == 1 {
			return m
		}
		m /= 2
	}
}

// valueBytes is the bytes in vals and how many of them are empty. It reads
// only the lengths, and has no branch.
func valueBytes(vals []string) (n, empty int) {
	for _, v := range vals {
		l := len(v)
		n += l
		empty -= (l - 1) >> 63 // -1 when l is 0, else 0
	}
	return n, empty
}

// firstValue is the index of the first non-empty value, or -1.
func firstValue(vals []string) int {
	for i, v := range vals {
		if v != "" {
			return i
		}
	}
	return -1
}

// appendChunk adds rows r0 to r0+m: the timestamps, each declared column as
// one copy, the labels, and an empty value in every column of the group the
// chunk gave none.
func (c *Columns) appendChunk(ts []int64, cols [][]string, r0, m int) {
	w := c.w
	base := len(w.ts)
	end := base + m
	w.ts = append(w.ts, ts[r0:r0+m]...)
	c.register(cols, r0, m, base)
	for i, cb := range c.slots {
		if cb == nil || c.skip[i] {
			continue // no value in this chunk; padded below like any other column
		}
		// A row with no value stores "", which is what Add's padding stores
		// for a field the record left out, so the values copy as they are.
		cb.vals = append(padTo(cb.vals, base), cols[i][r0:r0+m]...)
		w.bytes += c.sums[i]
	}
	if sc := c.stream; sc != nil && len(c.keys) > 0 {
		vals := slices.Grow(padTo(sc.vals, base), m)[:end]
		n := 0
		for j := range m {
			l := c.label(cols, r0+j)
			vals[base+j] = l
			if l != "" {
				n += len("_stream") + len(l)
			}
		}
		sc.vals = vals
		w.bytes += n
	}
	for _, cb := range w.colList {
		if len(cb.vals) < end {
			cb.vals = padTo(cb.vals, end)
		}
	}
}

// register finds or creates each column this chunk gives its group's first
// value for, in the order Add creates them: by the row of that first value,
// a row's fields by name, and _stream after the fields of its row.
func (c *Columns) register(cols [][]string, r0, m, base int) {
	w := c.w
	c.pend = c.pend[:0]
	for i, cb := range c.slots {
		// sums is 0 exactly when no row of the chunk has the field, since a
		// declared name is never empty. Such a column is left alone: if
		// another path created it, the padding reaches it.
		if cb != nil || c.skip[i] || c.sums[i] == 0 {
			continue
		}
		if cb = w.cols[c.names[i]]; cb != nil {
			c.slots[i] = cb // created by Add, or by another Columns
			continue
		}
		c.pend = append(c.pend, pending{row: firstValue(cols[i][r0 : r0+m]), col: i})
	}
	if len(c.keys) > 0 && c.stream == nil {
		if cb := w.cols["_stream"]; cb != nil {
			c.stream = cb
		} else if f := c.firstLabel(cols, r0, m); f >= 0 {
			c.pend = append(c.pend, pending{row: f, col: -1})
		}
	}
	if len(c.pend) == 0 {
		return
	}
	for i := 1; i < len(c.pend); i++ { // a handful at most, and only once a group
		for j := i; j > 0 && c.before(c.pend[j], c.pend[j-1]); j-- {
			c.pend[j], c.pend[j-1] = c.pend[j-1], c.pend[j]
		}
	}
	// Sized for the whole group when the last one was as big, so the rows
	// that follow are copies into room that is already there.
	capacity := max(base+m, w.rowHint())
	for _, p := range c.pend {
		name := "_stream"
		if p.col >= 0 {
			name = c.names[p.col]
		}
		cb := &colBuf{name: name, vals: make([]string, base, capacity)}
		w.cols[name] = cb
		w.colOrder = append(w.colOrder, name)
		w.colList = append(w.colList, cb)
		if p.col >= 0 {
			c.slots[p.col] = cb
		} else {
			c.stream = cb
		}
	}
}

// before orders the columns one chunk creates.
func (c *Columns) before(a, b pending) bool {
	if a.row != b.row {
		return a.row < b.row
	}
	if (a.col < 0) != (b.col < 0) {
		return b.col < 0
	}
	return c.names[a.col] < c.names[b.col]
}

// firstLabel is the first chunk row with a non-empty label, or -1: the first
// row any stream field has a value on.
func (c *Columns) firstLabel(cols [][]string, r0, m int) int {
	first := -1
	for _, k := range c.keys {
		if f := firstValue(cols[k.col][r0 : r0+m]); f >= 0 && (first < 0 || f < first) {
			first = f
		}
	}
	return first
}

// label is row r's _stream label, the string Add's buildStreamLabel would
// build for it. A label seen recently is returned rather than built.
func (c *Columns) label(cols [][]string, r int) string {
	lc := &c.labels
	if lc.used > 0 && c.hit(lc.last, cols, r) {
		return lc.label[lc.last]
	}
	for s := range lc.used {
		if s != lc.last && c.hit(s, cols, r) {
			lc.last = s
			return lc.label[s]
		}
	}
	return c.build(cols, r)
}

// hit reports whether slot s was built from row r's stream values.
func (c *Columns) hit(s int, cols [][]string, r int) bool {
	k := len(c.keys)
	vals := c.labels.vals[s*k : s*k+k]
	for j, key := range c.keys {
		if cols[key.col][r] != vals[j] {
			return false
		}
	}
	return true
}

// build makes row r's label and remembers it, replacing the oldest. The one
// allocation is the label itself, once per distinct set of values.
func (c *Columns) build(cols [][]string, r int) string {
	b := append(c.buf[:0], '{')
	for j, key := range c.keys {
		c.offs[j] = -1
		v := cols[key.col][r]
		if v == "" {
			continue // Add's label leaves out a field with no value
		}
		if len(b) > 1 {
			b = append(b, ',')
		}
		b = append(b, key.key...)
		b = append(b, `="`...)
		c.offs[j] = len(b)
		b = append(b, v...)
		b = append(b, '"')
	}
	label := ""
	if len(b) > 1 {
		b = append(b, '}')
		label = string(b)
	}
	c.buf = b
	lc := &c.labels
	s := lc.next
	lc.next = (s + 1) % labelSlots
	lc.used = min(lc.used+1, labelSlots)
	lc.last = s
	lc.label[s] = label
	k := len(c.keys)
	vals := lc.vals[s*k : s*k+k]
	for j, key := range c.keys {
		vals[j] = ""
		if o := c.offs[j]; o >= 0 {
			vals[j] = label[o : o+len(cols[key.col][r])]
		}
	}
	return label
}

// padTo lengthens vals to n with empty values: what a row stores for a field
// it does not have.
func padTo(vals []string, n int) []string {
	old := len(vals)
	if old >= n {
		return vals
	}
	vals = slices.Grow(vals, n-old)[:n]
	clear(vals[old:])
	return vals
}

// rowHint is the capacity a new column buffer gets: the last group's row
// count, rounded up to a power of two and at most FlushRows. w.mu held.
func (w *Writer) rowHint() int {
	if w.lastRows <= 0 {
		return 0
	}
	return min(1<<bits.Len(uint(w.lastRows-1)), FlushRows)
}
