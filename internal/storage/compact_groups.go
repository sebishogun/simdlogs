package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Merging many small groups into fewer large ones.
//
// # Why small groups happen and why they cost
//
// A group is written per ingest request, so a client sending one row per call
// produces one group per row. Every group carries a header, a footer, a
// per-column skip record, a dictionary and a postings section -- fixed cost
// that a one-row group pays in full. Worse, every query walks the group list:
// the time-window skip that makes a selective query cheap is per GROUP, so a
// million one-row groups is a million skip decisions before a single column is
// touched.
//
// Recompact re-encodes one group in place under a different codec. This is the
// other axis: it takes several groups and produces fewer, with the same rows.
//
// # The commit is one manifest record
//
// The manifest already commits an add-set and a remove-set in a single record
// (`manifest.commit`), which is exactly the transaction this needs and is why
// nothing in that file changed: the outputs become visible and the inputs stop
// being visible at the same fsync. A crash before it leaves the inputs visible
// and the outputs as files the next open leaves alone -- no record named them,
// so nothing licenses deleting them. A crash after it leaves the inputs as
// files the manifest recorded as REMOVED, which the next open reclaims; the
// tombstone list that would have done it in-process does not survive a
// restart.
// There is no ordering in which a row is visible twice or not at all, and the
// crash matrix asserts that rather than this comment.
//
// # What it will not touch
//
// A group holding a column this package cannot decode back to its in-memory
// form -- a vector column today -- is skipped rather than rebuilt, the same
// rule Recompact follows. A group at or near the row ceiling is skipped
// because merging it saves nothing. And the whole pass is bounded by
// thresholds the caller sets, because compaction is I/O that competes with
// ingest and a store that compacts continuously is slower than one that never
// does.

// CompactOptions bounds one compaction pass.
//
// Every field is a refusal rather than a target: the zero value compacts
// nothing, so a caller that has not decided what it wants does not get a
// rewrite of its whole store by default.
type CompactOptions struct {
	// MinGroups is the fewest inputs worth merging, and the switch: a value
	// below 1 disables the pass entirely.
	//
	// That is what makes the zero value safe. An earlier version floored this
	// at 2 and documented the zero value as "compacts nothing"; measured, it
	// merged a 500-group store into one group. A field whose zero value
	// rewrites the caller's whole store is not a threshold, and three
	// documents said otherwise because the floor was read as a refusal.
	MinGroups int

	// MaxRowsPerOutput caps the rows an output group holds. 0 means MaxRows,
	// which is the format's own ceiling; a smaller value is how a caller keeps
	// the time-window skip fine-grained, since the skip is per group and a
	// bigger group is a coarser skip.
	MaxRowsPerOutput int

	// MaxInputBytes bounds the total input a single pass reads and rewrites,
	// so one call cannot turn into an unbounded rewrite of the whole store.
	// 0 means no bound, which is a choice rather than a default.
	MaxInputBytes int64

	// MaxGroupBytes is the largest input group the pass will merge. A group
	// already big enough is left alone: merging it copies its bytes for no
	// change in the group count that matters. 0 means no bound.
	MaxGroupBytes int64

	// OlderThan restricts the pass to groups whose newest row is older than
	// this unix-nanos cutoff, so compaction never rewrites the range an
	// ingest is still appending to. 0 means no age bound.
	OlderThan int64

	// MaxOutputs bounds how many output groups one pass writes, which bounds
	// its I/O without needing a rate limiter: a caller running the pass on a
	// timer sets this from the interval and the disk it is willing to spend.
	// 0 means no bound.
	MaxOutputs int
}

func (o CompactOptions) maxRowsPerOutput() int {
	if o.MaxRowsPerOutput <= 0 || o.MaxRowsPerOutput > MaxRows {
		return MaxRows
	}
	return o.MaxRowsPerOutput
}

// enabled reports whether the caller asked for anything at all.
func (o CompactOptions) enabled() bool { return o.MinGroups >= 1 }

func (o CompactOptions) minGroups() int {
	if o.MinGroups < 2 {
		// One group is not a run: merging a group into itself is Recompact's
		// job. A caller that asked for 1 gets the smallest run that is one.
		return 2
	}
	return o.MinGroups
}

// CompactStats reports what a pass did.
type CompactStats struct {
	Inputs      int   // groups merged away
	Outputs     int   // groups written
	Rows        int64 // rows moved
	BytesBefore int64
	BytesAfter  int64
}

// CompactGroups merges runs of small adjacent groups into fewer larger ones.
//
// Adjacent in the store's own order, which is by timeMin: merging a run that
// is contiguous there keeps every output's time span inside the span the
// inputs already covered, so no output overlaps a group it did not come from
// and the per-group time skip stays as selective as it was. Merging arbitrary
// groups would widen spans and make every query read more.
//
// Returns what it did; a pass with nothing to do is not an error.
func (s *Store) CompactGroups(opt CompactOptions) (CompactStats, error) {
	var st CompactStats
	if !opt.enabled() {
		return st, nil
	}
	// There is no pre-check on len(s.groups) before the lock. The version with
	// one raced Close, AppendGroup and a second pass -- four reports under
	// -race -- for an early return that saves a mutex acquisition on a path
	// that is about to do file I/O.

	// structMu for the same reason Recompact takes it: this writes group files
	// and retires entries, and running alongside a promotion or a recompaction
	// is two writers deciding what a path holds.
	s.structMu.Lock()
	defer s.structMu.Unlock()

	runs, release := s.compactionRuns(opt)
	defer release()
	if len(runs) == 0 {
		return st, nil
	}

	for _, run := range runs {
		if spent(&st, opt) {
			break
		}
		if err := s.compactRun(run, opt, &st); err != nil {
			return st, err
		}
	}
	s.retryTombstones()
	return st, nil
}

// compactionRuns picks maximal runs of adjacent eligible groups, taking a
// reference on each so retention cannot unmap one mid-pass.
//
// The returned release drops every reference, including those of runs the
// caller never got to.
func (s *Store) compactionRuns(opt CompactOptions) (runs [][]*groupEntry, release func()) {
	s.mu.RLock()
	// s.groups is kept sorted by timeMin, so index order IS store order and a
	// run is a slice of it.
	var held []*groupEntry
	var cur []*groupEntry
	flush := func() {
		if len(cur) >= opt.minGroups() {
			runs = append(runs, cur)
		} else {
			// Not a run: drop the references now rather than holding them for
			// the whole pass.
			for _, g := range cur {
				g.release()
			}
			held = held[:len(held)-len(cur)]
		}
		cur = nil
	}
	for _, g := range s.groups {
		if !compactCandidate(g, opt) {
			flush()
			continue
		}
		if !g.acquire() {
			// Being retired underneath us. It breaks the run rather than
			// joining it: a run has to be contiguous to keep spans tight.
			flush()
			continue
		}
		held = append(held, g)
		cur = append(cur, g)
	}
	flush()
	s.mu.RUnlock()
	return runs, func() {
		for _, g := range held {
			g.release()
		}
	}
}

// compactCandidate reports whether one group may join a run.
func compactCandidate(g *groupEntry, opt CompactOptions) bool {
	r := g.reader
	if r == nil {
		return false
	}
	if opt.OlderThan != 0 && g.timeMax >= opt.OlderThan {
		return false
	}
	if opt.MaxGroupBytes > 0 && int64(len(r.blob)) > opt.MaxGroupBytes {
		return false
	}
	// A group holding a column this package cannot decode back is not a
	// candidate at all, so it BREAKS the run rather than joining a batch and
	// refusing it. The version that discovered this in mergeGroups threw away
	// every other group in the batch: one vector column among sixty left all
	// sixty unmerged, permanently, and at the default row cap that is up to
	// 8192 rows' worth of groups blocked by one.
	if !mergeableColumns(r) {
		return false
	}
	// A group with no timestamp column has a span of [MaxInt64, MinInt64] and
	// is invisible to every window query. Merging it into one that has a span
	// makes its rows visible where they were not, and takes the output's span
	// outside the union of its inputs' -- measured, a merged output spanning
	// [0, 1000003] from inputs one of which spanned nothing. Making rows
	// appear is a change no caller asked a compactor for. Latent today,
	// because internal/ingest always writes _time; refused rather than argued
	// about.
	if !hasTimestampColumn(r) {
		return false
	}
	// A group already at the row ceiling cannot absorb anything and merging it
	// only copies its bytes.
	return r.Rows < opt.maxRowsPerOutput()
}

// hasTimestampColumn reports whether the group carries any timestamp column,
// which is what gives it a span.
func hasTimestampColumn(r *Reader) bool {
	for i := range r.cols {
		if r.cols[i].Type == ColTimestamp {
			return true
		}
	}
	return false
}

// mergeableColumns reports whether every column decodes back to its in-memory
// form. A vector column is the case today.
func mergeableColumns(r *Reader) bool {
	for i := range r.cols {
		if t := r.cols[i].Type; t != ColTimestamp && t != ColDict {
			return false
		}
	}
	return true
}

// spent reports whether the pass has used up a budget.
//
// Checked before every BATCH and not only between runs. The version that
// checked only between runs let a single run of forty groups produce ten
// outputs against a one-byte ceiling, because a run is where the work
// actually happens and the ceiling was being tested at the wrong granularity.
func spent(st *CompactStats, opt CompactOptions) bool {
	if opt.MaxOutputs > 0 && st.Outputs >= opt.MaxOutputs {
		return true
	}
	if opt.MaxInputBytes > 0 && st.BytesBefore >= opt.MaxInputBytes {
		return true
	}
	return false
}

// compactRun merges one run, writing as many outputs as the row cap needs and
// committing each output against the inputs it consumed.
//
// One manifest record per OUTPUT, not one for the whole run: a run of two
// hundred groups at the row cap is several outputs, and holding every input
// visible until the last one landed would mean a crash in the middle threw
// away all the work instead of the tail. Each record is still atomic in the
// only sense that matters -- its outputs and its inputs change visibility
// together.
func (s *Store) compactRun(run []*groupEntry, opt CompactOptions, st *CompactStats) error {
	rowCap := opt.maxRowsPerOutput()
	// The floor is a property of the RUN, checked once, and not of each batch.
	// Applied per batch it made a misconfiguration permanent and silent: with
	// MinGroups 10 and a row cap of 8 over one-row groups, every batch the cap
	// produced was below the floor and nothing ever merged -- no error, no
	// stat, forever. compactionRuns has already applied the floor here.
	if len(run) < opt.minGroups() {
		return nil
	}

	batch := make([]*groupEntry, 0, len(run))
	rows := 0
	var batchBytes int64
	flush := func() error {
		// Two groups is the smallest thing that IS a merge; below it there is
		// nothing to do.
		if len(batch) < 2 {
			batch, rows, batchBytes = batch[:0], 0, 0
			return nil
		}
		bst, err := s.mergeAndCommit(batch, opt)
		st.Inputs += bst.Inputs
		st.Outputs += bst.Outputs
		st.Rows += bst.Rows
		st.BytesBefore += bst.BytesBefore
		st.BytesAfter += bst.BytesAfter
		batch, rows, batchBytes = batch[:0], 0, 0
		return err
	}
	for _, g := range run {
		r := g.reader
		if r == nil {
			continue
		}
		size := int64(len(r.blob))
		// The byte budget bounds the BATCH as it grows, not only the totals
		// between batches. A batch runs to MaxRows rows, which is up to
		// 131,072 one-row groups, so a store that fits in one batch was read
		// in full against a one-byte ceiling: 184,845 bytes read against a
		// limit of 1, measured.
		overBudget := opt.MaxInputBytes > 0 && len(batch) > 0 &&
			st.BytesBefore+batchBytes+size > opt.MaxInputBytes
		if (rows+r.Rows > rowCap || overBudget) && len(batch) > 0 {
			if err := flush(); err != nil {
				return err
			}
			if spent(st, opt) {
				return nil
			}
		}
		batch = append(batch, g)
		rows += r.Rows
		batchBytes += size
	}
	if spent(st, opt) {
		return nil
	}
	return flush()
}

// mergeAndCommit builds one output group from the batch and swaps it for them.
func (s *Store) mergeAndCommit(batch []*groupEntry, opt CompactOptions) (CompactStats, error) {
	var st CompactStats
	g, before := mergeGroups(batch)
	if g == nil {
		// Something in the batch does not round-trip. Left alone, as Recompact
		// leaves a group it cannot rebuild.
		return st, nil
	}

	blob := g.Marshal()
	if int64(len(blob)) >= before {
		// Recompact has this rule and this did not: two groups at the row
		// ceiling merged from 1,961,807 bytes to 1,991,372, and the operator
		// log reported it as "saved -0.0 MB". A merge that grows the store
		// still cuts the group count, but not by enough to pay for rewriting
		// two full groups -- and a pass on a timer would do it every time.
		return st, nil
	}

	// The id is taken AFTER the decision, not before. Taking it first and
	// giving it back on the refusal is not safe: AppendGroup may have taken
	// the next one in between, and decrementing would hand two groups one id.
	// A skipped id costs nothing -- they need to be unique and increasing, not
	// contiguous.
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.mu.Unlock()
	final := filepath.Join(s.dir, fmt.Sprintf("group-%d.bin", id))
	if ferr := fault(faultCompactWrite); ferr != nil {
		return st, ferr
	}
	if err := writeFileAtomic(final, blob, DataFileMode); err != nil {
		s.discardUncommitted(final, id, err)
		return st, err
	}
	if ferr := fault(faultCompactWritten); ferr != nil {
		// The output is durable and uncommitted: the same state a killed
		// AppendGroup leaves, which the next open ignores.
		s.discardUncommitted(final, id, ferr)
		return st, ferr
	}
	mb, unmap, err := mmapFile(final)
	if err != nil {
		s.discardUncommitted(final, id, err)
		return st, err
	}
	nr, err := ReadGroup(mb)
	if err != nil {
		unmap()
		s.discardUncommitted(final, id, err)
		return st, err
	}

	remove := make([]uint64, 0, len(batch))
	for _, ge := range batch {
		remove = append(remove, ge.id)
	}

	s.mu.Lock()
	// Every input must still be the store's own entry. If retention or another
	// structural pass replaced one while this output was being written, the
	// remove-set names an id that is no longer what it was, and committing it
	// would retire a group this pass never read.
	if !s.stillPresent(batch) {
		s.mu.Unlock()
		unmap()
		s.discardUncommitted(final, id, nil)
		return st, nil
	}
	if ferr := fault(faultCompactCommit); ferr != nil {
		s.mu.Unlock()
		unmap()
		s.discardUncommitted(final, id, ferr)
		return st, ferr
	}
	// The transaction: the output becomes visible and the inputs stop being
	// visible in one record and one fsync.
	if err := s.man.commit([]uint64{id}, remove, nil); err != nil {
		s.mu.Unlock()
		unmap()
		s.discardUncommitted(final, id, err)
		return st, err
	}
	victims := make([]*groupEntry, 0, len(batch))
	gone := make(map[*groupEntry]bool, len(batch))
	for _, ge := range batch {
		gone[ge] = true
	}
	kept := s.groups[:0:0]
	for _, cur := range s.groups {
		if gone[cur] {
			victims = append(victims, cur)
			continue
		}
		kept = append(kept, cur)
	}
	kept = append(kept, &groupEntry{
		id: id, path: final, reader: nr,
		timeMin: nr.TimeMin, timeMax: nr.TimeMax, unmap: unmap,
	})
	s.groups = kept
	s.sortGroups()
	paths := make([]string, 0, len(victims))
	for _, ge := range victims {
		paths = append(paths, ge.path)
		ge.retire()
	}
	s.mu.Unlock()

	if ferr := fault(faultCompactUnlink); ferr != nil {
		// Committed. The inputs are files nothing names; leaving them is disk,
		// not data, and the tombstone path reclaims them.
		for _, p := range paths {
			s.addTombstone(p)
			pendingTombstones.Add(1)
		}
		return st, ferr
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			pendingTombstones.Add(1)
			s.addTombstone(p)
		}
	}

	st.Inputs = len(batch)
	st.Outputs = 1
	st.Rows = int64(g.Rows)
	st.BytesBefore = before
	st.BytesAfter = int64(len(blob))
	return st, nil
}

// stillPresent reports whether every entry is still in the store's group set.
// Caller holds s.mu.
func (s *Store) stillPresent(batch []*groupEntry) bool {
	have := make(map[*groupEntry]bool, len(s.groups))
	for _, g := range s.groups {
		have[g] = true
	}
	for _, ge := range batch {
		if !have[ge] || ge.retired.Load() {
			return false
		}
	}
	return true
}

// mergeGroups concatenates the batch's rows into one Group, in batch order,
// and reports the input bytes.
//
// Concatenation, not a sort. The batch is contiguous in the store's own
// timeMin order and each input's rows keep their order, so the output presents
// rows in exactly the sequence a query walking those groups would have seen.
// Sorting by timestamp instead would change the answer for rows sharing a
// timestamp, which is a reordering no caller asked for.
//
// Returns nil if any input holds a column that cannot be decoded back to its
// in-memory form.
func mergeGroups(batch []*groupEntry) (*Group, int64) {
	var before int64
	total := 0
	for _, ge := range batch {
		if ge.reader == nil {
			return nil, 0
		}
		total += ge.reader.Rows
		before += int64(len(ge.reader.blob))
	}
	if total == 0 {
		return nil, before
	}

	// The column set is the UNION, not the first group's. Groups written by
	// different requests carry different fields, and taking one group's set
	// would drop every column the others had -- a query that used to find a
	// field would stop finding it, which is data loss with a successful exit
	// code. A row from a group without a column gets that column's zero value,
	// which is what the store already returns for an absent field.
	type colInfo struct {
		typ   ColumnType
		order int
	}
	cols := map[string]colInfo{}
	var names []string
	for _, ge := range batch {
		r := ge.reader
		for i := range r.cols {
			m := &r.cols[i]
			if _, ok := cols[m.Name]; ok {
				continue
			}
			if m.Type != ColTimestamp && m.Type != ColDict {
				return nil, before // vector or a future type: not merged
			}
			cols[m.Name] = colInfo{typ: m.Type, order: len(names)}
			names = append(names, m.Name)
		}
	}
	// A name carrying two types across the batch cannot be merged into one
	// column. Refusing the batch keeps both readable rather than picking one.
	for _, ge := range batch {
		r := ge.reader
		for i := range r.cols {
			if got := cols[r.cols[i].Name]; got.typ != r.cols[i].Type {
				return nil, before
			}
		}
	}
	// And the batch has to agree on WHICH column is the timestamp, not merely
	// that each input has one. Two groups, one with `_time` and one with `ts`,
	// merge to the union and each side gets 0 in the other's column -- so the
	// output spans from zero, outside the span its inputs covered, and a
	// window that matched nothing now returns every row. Measured on six
	// mixed groups: an output spanning [0, 6000000000] from inputs whose
	// minimum was 1000000000. This is the case compactCandidate refuses a
	// timestamp-less group for, reached through a second door.
	tsName := ""
	for _, name := range names {
		if cols[name].typ != ColTimestamp {
			continue
		}
		if tsName != "" {
			return nil, before // more than one timestamp column across the batch
		}
		tsName = name
	}
	for _, ge := range batch {
		if !ge.reader.hasColumn(tsName) {
			return nil, before
		}
	}
	sort.SliceStable(names, func(i, j int) bool { return cols[names[i]].order < cols[names[j]].order })

	out := &Group{Rows: total}
	for _, name := range names {
		switch cols[name].typ {
		case ColTimestamp:
			ts := make([]int64, 0, total)
			for _, ge := range batch {
				r := ge.reader
				if !r.hasColumn(name) {
					ts = append(ts, make([]int64, r.Rows)...)
					continue
				}
				got := r.TimestampsRange(name, 0, r.Rows)
				if len(got) != r.Rows {
					return nil, before
				}
				ts = append(ts, got...)
			}
			out.Columns = append(out.Columns, Column{Name: name, Type: ColTimestamp, Ts: ts})
		case ColDict:
			vals := make([]string, 0, total)
			for _, ge := range batch {
				r := ge.reader
				if !r.hasColumn(name) {
					vals = append(vals, make([]string, r.Rows)...)
					continue
				}
				idx, dict := r.dictIndicesForMerge(name)
				if idx == nil {
					return nil, before
				}
				for row := 0; row < r.Rows; row++ {
					if row < len(idx) && int(idx[row]) < len(dict) {
						vals = append(vals, dict[idx[row]])
						continue
					}
					vals = append(vals, "")
				}
			}
			d := BuildDict(vals)
			out.Columns = append(out.Columns, Column{Name: name, Type: ColDict, Dict: &d})
		}
	}
	return out, before
}

// dictIndicesForMerge decodes a dict column WITHOUT caching the result.
//
// DictIndices caches, which is right for a query -- the blob is immutable and
// the next query wants the same decode. It is wrong here: compaction reads
// every dict column of every group it is about to unlink, so the cache fills
// with the indices of groups that no longer exist. Measured: one pass over
// 80,000 rows added 640,000 bytes to a 256 MB budget that has no eviction and
// is decremented nowhere, so ~32M merged rows retire the query cache for the
// life of the process.
func (r *Reader) dictIndicesForMerge(name string) ([]uint32, []string) {
	ci := r.colIndex(name)
	if ci < 0 || r.cols[ci].Type != ColDict {
		return nil, nil
	}
	m := &r.cols[ci]
	if v := r.cachedIdx(ci); v != nil {
		// Already decoded by a query: use it rather than decoding twice.
		return v, dictSectionAll(r.dictSec(m), m.DictLen)
	}
	data := r.blob[m.DataOff : m.DataOff+r.idxBytes(m)]
	return decodeIndices(data, r.Rows, m.Width), dictSectionAll(r.dictSec(m), m.DictLen)
}

// hasColumn reports whether the group carries a column by that name.
func (r *Reader) hasColumn(name string) bool {
	for i := range r.cols {
		if r.cols[i].Name == name {
			return true
		}
	}
	return false
}
