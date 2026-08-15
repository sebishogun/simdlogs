package ingest

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/sebishogun/simdjson"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// MinParallelBytes is the body size below which parallel ingest is not worth
// the goroutine and per-shard writer setup; a small POST stays serial.
const MinParallelBytes = 1 << 20

// ParallelConfig carries the deployment-wide writer settings that every
// temporary shard writer must inherit. It exists because the shard writers
// are built here rather than handed in: anything the server configured on
// its own writer has to be repeated onto them, and a setting that is
// forgotten changes the stored schema for large requests only. Compact was
// copied and the stream fields were not, so the same records produced a
// _stream column under the small-body path and none under this one.
type ParallelConfig struct {
	Compact      bool
	StreamFields []string
	// MaxLineBytes rejects a single line longer than this. Zero means no
	// bound. It is per line rather than per body because the body limit is
	// enforced at the HTTP layer; this is what stops one line from being the
	// whole body.
	MaxLineBytes int

	// Limits are the per-record caps. They were absent, so every shard
	// writer ran with a zero RecordLimits and add() skipped
	// truncateForLimits entirely: a body over MinParallelBytes on
	// /insert/jsonline or either bulk route got no field cap, no name or
	// value cap, and no control-name drop. Measured on a 1 MiB body with
	// MaxFieldNameBytes=8 and MaxFieldValueBytes=16: a 64-byte name and a
	// 512-byte value stored, and a forged ".error" control name with them.
	Limits RecordLimits

	// Shards overrides the shard count. Zero means derive it from the CPU
	// count. It exists because the derived value is runtime.NumCPU()/3, which
	// is below the 2-shard minimum on any machine with fewer than six cores --
	// including every stock CI runner. Without an override the concurrent
	// branch is dead code exactly where it is meant to be gated, and the
	// tests that cover it pass by running the serial fallback instead.
	Shards int
}

// shards resolves the shard count for a body of n bytes: 0 means run serial.
func (c ParallelConfig) shards(n int) int {
	sh := c.Shards
	if sh == 0 {
		sh = runtime.NumCPU() / 3
	}
	if sh < 2 || n < MinParallelBytes {
		return 0
	}
	return sh
}

func (c ParallelConfig) apply(w *Writer) {
	w.SetCompact(c.Compact)
	w.SetRecordLimits(c.Limits)
	if c.MaxLineBytes > 0 {
		w.SetMaxLineBytes(c.MaxLineBytes)
	}
	if len(c.StreamFields) > 0 {
		w.SetStreamFields(c.StreamFields)
	}
}

// IngestJSONLinesParallel splits an NDJSON body at line boundaries and
// ingests the chunks concurrently, each through its own writer over the
// shared store (AppendGroup is concurrency-safe). The parser was the ingest
// bottleneck once flushing went async and single-threaded on a many-core
// box; sharding the parse is the lever. Shard count and per-shard flush
// pool are sized so the total goroutines stay near the core count rather
// than oversubscribing. Falls back to serial for a small body.
//
// The returned error is the caller's proof that the rows reached the store.
// It must be checked: a non-nil error means some or all of the counted rows
// were parsed but not persisted, and the request has to fail.
func IngestJSONLinesParallel(store *storage.Store, data []byte, fallback func() int64, compact bool) (ingested, skipped int, err error) {
	return IngestJSONLinesParallelCfg(store, data, fallback, ParallelConfig{Compact: compact}, nil)
}

// IngestJSONLinesParallelCfg is IngestJSONLinesParallel with the deployment
// writer settings and the request's field mappings applied.
//
// Every shard writer's Close is checked. Close flushes, waits for the flush
// pool, and reports the first AppendGroup failure, so discarding it -- which
// this function used to do -- turned a store that could not write a single
// group into a 200 with a row count. The first error is preserved and the
// number of failed shards is reported with it, because "3 of 8 shards failed"
// and "1 of 8 shards failed" are different operational events.
func IngestJSONLinesParallelCfg(store *storage.Store, data []byte, fallback func() int64, cfg ParallelConfig, opts *Options) (ingested, skipped int, err error) {
	shards := cfg.shards(len(data))
	if shards == 0 {
		w := NewWriter(store)
		cfg.apply(w)
		r, perr := IngestJSONLinesOpts(w, data, fallback, opts)
		if cerr := w.Close(); cerr != nil {
			// Partial comes off the shard's own error here too. This branch
			// is taken whenever cfg.shards() is below 2 -- runtime.NumCPU()/3,
			// so EVERY host with fewer than six cores -- and dropping it made
			// As synthesize an answer strictly worse than the one it replaced:
			// the inner *WriteError said "1 of 3 groups, partial", the
			// synthesized one said "1 of 1 shard writers, not partial", and
			// the client was told a retry was clean with a group on disk.
			partial := false
			var we *WriteError
			if errors.As(cerr, &we) {
				partial = we.Partial
			}
			return r.Accepted, r.Rejected, &ParallelWriteError{
				Shards: 1, Failed: 1, Err: cerr, Partial: partial,
			}
		}
		return r.Accepted, r.Rejected, perr
	}
	chunks := splitLines(data, shards)
	var ing, skp int64
	var (
		mu       sync.Mutex
		firstErr error
		// anyPartial is set by any shard whose own failure was partial.
		anyPartial bool
		failed     int
		started    int
	)
	var wg sync.WaitGroup
	for _, c := range chunks {
		if len(c) == 0 {
			continue
		}
		started++
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			w := NewWriterWorkers(store, 2)
			cfg.apply(w)
			pr, _ := IngestJSONLinesOpts(w, chunk, fallback, opts)
			i, s := pr.Accepted, pr.Rejected
			// Close before counting: a shard whose rows never landed must
			// not contribute to the accepted total.
			cerr := w.Close()
			// Skipped lines are a parse fact: they were malformed whether or
			// not the group landed, so they are counted either way. Ingested
			// is only counted when the rows are durable.
			atomic.AddInt64(&skp, int64(s))
			if cerr != nil {
				mu.Lock()
				failed++
				if firstErr == nil {
					firstErr = cerr
				}
				// Partial-ness is collected from EVERY shard, not read back
				// off the first-recorded error. Only firstErr is kept, so a
				// shard that landed some groups before failing was invisible
				// unless it happened to win the mutex -- and with every shard
				// failed, the aggregate then reported "nothing landed, a retry
				// is clean" while rows were on disk.
				var we *WriteError
				if errors.As(cerr, &we) && we.Partial {
					anyPartial = true
				}
				mu.Unlock()
				return
			}
			atomic.AddInt64(&ing, int64(i))
		}(c)
	}
	wg.Wait()
	if firstErr != nil {
		return int(ing), int(skp), &ParallelWriteError{
			Shards: started, Failed: failed, Err: firstErr, Partial: anyPartial,
		}
	}
	return int(ing), int(skp), nil
}

// ParallelWriteError reports that at least one shard writer failed to
// persist its rows. Ingested counts only the shards that succeeded, so the
// caller can report how much of the batch is durable while still failing the
// request.
type ParallelWriteError struct {
	Shards int // shard writers started
	Failed int // shard writers whose Close reported a failure
	Err    error

	// Partial reports that at least one FAILED shard had groups reach the
	// store before it failed. It is collected across every shard, because
	// only one shard's error is kept in Err and reading partial-ness off that
	// one made the answer depend on which shard won a mutex.
	Partial bool
}

func (e *ParallelWriteError) Error() string {
	return "ingest: " + strconv.Itoa(e.Failed) + " of " + strconv.Itoa(e.Shards) +
		" shard writers failed to persist: " + e.Err.Error()
}

func (e *ParallelWriteError) Unwrap() error { return e.Err }

// As makes errors.As see the WHOLE parallel write rather than one shard's.
//
// Without it, As walks Unwrap and lands on the first failing shard's
// *WriteError -- one shard's counts and one shard's Partial. A request split
// across ten shards where shard 1 landed and shard 2 failed then reported
// "1 of 1 failed, duplicateOnRetry=false", which is the opposite of the truth:
// shard 1's rows are durable and resending the body stores them twice.
func (e *ParallelWriteError) As(target any) bool {
	p, ok := target.(**WriteError)
	if !ok {
		return false
	}
	*p = e.writeError()
	return true
}

// writeError aggregates the shards into one answer.
func (e *ParallelWriteError) writeError() *WriteError {
	under := e.Err
	var we *WriteError
	if errors.As(e.Err, &we) {
		under = we.Err
	}
	// Partial when some shard survived -- its rows are durable and a retry of
	// the body stores them again -- or when ANY failing shard was itself
	// partial, which e.Partial carries from the shard loop. With every shard
	// failed and none of them partial, nothing landed and a retry is clean.
	partial := (e.Failed > 0 && e.Failed < e.Shards) || e.Partial
	return &WriteError{
		Err:          under,
		Class:        classify(under),
		Partial:      partial,
		FailedGroups: e.Failed,
		TotalGroups:  e.Shards,
		Unit:         "shard writers",
	}
}

// splitLines cuts data into at most n chunks, each ending on a newline so no
// line is split across chunks.
func splitLines(data []byte, n int) [][]byte {
	if n <= 1 {
		return [][]byte{data}
	}
	out := make([][]byte, 0, n)
	target := len(data) / n
	start := 0
	for len(out) < n-1 && start < len(data) {
		end := start + target
		if end >= len(data) {
			break
		}
		nl := indexByte(data[end:], '\n')
		if nl < 0 {
			break
		}
		end += nl + 1
		out = append(out, data[start:end])
		start = end
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}

// IngestJSONLines parses NDJSON log lines through simdjson and appends
// each object's fields to the writer. A record's timestamp comes from the
// _time field (RFC3339 or unix nanos) or, absent one, a monotonic
// fallback so ingest never rejects a line for a missing clock. Malformed
// lines are counted and skipped -- ingest is never fatal on bad input,
// matching the reference's leniency.
//
// The field values are taken as strings (StringNoCopy where the bytes are
// unescaped) and interned by the writer's per-column dictionaries; a
// number keeps its source text, which is what a log store round-trips.
func IngestJSONLines(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestJSONLinesOpts(w, data, fallback, nil)
}

// IngestJSONLinesOpts is IngestJSONLines with the request's field mappings
// applied: a shipper configured against the reference sends them as query args
// and expects its message, timestamp and stream read from the fields it names.
func IngestJSONLinesOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	fields := map[string]string{}
	// Hoisted out of the loop: the configured set is asked for once per body
	// rather than once per record (it takes the writer's mutex), and the
	// scratch buffer is reused so a 768-float embedding is not 3 KiB of
	// garbage per log line.
	vecFlds := w.VectorFields()
	var vecScratch []float32
	for len(data) > 0 {
		// Split one line; lenient ingest parses each independently so a
		// malformed line is counted and skipped, never aborting the batch
		// (ForEachLine stops at the first bad line, which ingest must not).
		nl := indexByte(data, '\n')
		var line []byte
		if nl < 0 {
			line, data = data, nil
		} else {
			line, data = data[:nl], data[nl+1:]
		}
		if len(trimSpace(line)) == 0 {
			continue
		}
		// One line must not be the whole body. The HTTP layer bounds the
		// request; this bounds a single record inside it, so a 64 MiB body
		// cannot be one 64 MiB line that the parser holds entire.
		// ordinal counts every NON-BLANK line, accepted or not, so it is the
		// record's position within the batch as the caller numbered it.
		ordinal := res.Accepted + res.Rejected
		if w.LineTooLong(len(line)) {
			res.Reject(ordinal)
			res.Warn(int64(ordinal), "line of %d bytes exceeds the configured maximum", len(line))
			continue
		}
		doc, err := simdjson.Parse(line)
		if err != nil {
			res.Reject(ordinal)
			continue
		}
		v := doc.Root()
		if v.Kind() != simdjson.Object {
			res.Reject(ordinal)
			continue
		}
		for k := range fields {
			delete(fields, k)
		}
		var ts int64
		haveTS := false
		var rowVecs map[string][]float32
		var vecErr error
		v.ForEachKey(func(key string, val simdjson.Value) bool {
			switch val.Kind() {
			case simdjson.String:
				s := val.String()
				if opts.isTime(key) {
					if t, ok := parseTime(s); ok {
						// The timestamp column holds it now, so keeping the field
						// as well stored every record's time twice -- and as a
						// nearly unique string, the worst thing a dictionary can
						// hold. Nothing read it back: output prints the row's
						// timestamp, and a pipe asking for _time is served from
						// the timestamp column. A time we could NOT parse is kept
						// as an ordinary field, since then it is just data.
						ts, haveTS = t, true
						return true
					}
				}
				fields[key] = s
			case simdjson.Number:
				if opts.isTime(key) {
					ts, haveTS = val.Int(), true
					return true
				}
				fields[key] = strconv.FormatFloat(val.Float(), 'f', -1, 64)
			case simdjson.Bool:
				if val.Int() != 0 {
					fields[key] = "true"
				} else {
					fields[key] = "false"
				}
			case simdjson.Array:
				// An array is only read when the field is CONFIGURED as an
				// embedding. `[1,2,3]` is not self-evidently a vector -- it
				// might be a retry schedule or a status sequence -- and a
				// store that guessed would decide the column type from
				// whichever record arrived first, so the same payload would
				// land as a vector on an empty store and as text on a
				// populated one.
				dim, isVec := vecFlds.Dim(key)
				if !isVec {
					return true
				}
				buf := vecScratch[:0]
				n := 0
				bad := false
				val.ForEach(func(e simdjson.Value) bool {
					if e.Kind() != simdjson.Number || n == dim {
						bad = true
						return false
					}
					f := e.Float()
					// NaN and Inf are refused, not clamped: a NaN component
					// makes every score computed against the vector NaN, and
					// NaN compares false against everything, so one bad record
					// does not fail -- it quietly makes a whole result set
					// meaningless.
					if math.IsNaN(f) || math.IsInf(f, 0) {
						bad = true
						return false
					}
					buf = append(buf, float32(f))
					n++
					return true
				})
				vecScratch = buf[:0]
				if bad || n != dim {
					vecErr = fmt.Errorf("%w: %s has an unusable value for a %d-dimension field",
						ErrVector, key, dim)
					return false
				}
				if rowVecs == nil {
					rowVecs = map[string][]float32{}
				}
				rowVecs[key] = buf
			}
			return true
		})
		if !haveTS {
			ts = fallback()
		}
		if vecErr != nil {
			// Rejected as a RECORD, at its ordinal, rather than stored with
			// the embedding dropped. A log line stored without its vector is
			// a line invisible to the one search it was ingested for, which
			// is worse than a rejection the client can see and fix.
			res.Reject(ordinal)
			res.Warn(int64(ordinal), "%v", vecErr)
			continue
		}
		if mapped {
			opts.apply(fields)
		}
		addWithStreamVec(w, ts, fields, opts, rowVecs)
		res.Accepted++
	}
	return res, nil
}

// isTimeKey reports whether a field carries the record timestamp: _time
// (LogsQL) or @timestamp (Elasticsearch / OpenTelemetry / ECS).
func isTimeKey(k string) bool { return k == "_time" || k == "@timestamp" }

func indexByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// allDigits reports whether s is a non-empty run of ASCII digits (optionally
// signed), i.e. whether ParseInt can possibly succeed.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		if len(s) == 1 {
			return false
		}
		i = 1
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseTime accepts unix-nanos-as-digits or RFC3339; returns nanos.
func parseTime(s string) (int64, bool) {
	// Only call ParseInt when the value really can be an integer. An RFC3339
	// _time -- the common case for logs -- made it fail on EVERY row, and a failed
	// ParseInt allocates a syntaxError plus a copy of the string: ~36% of all
	// ingest allocations were errors that were built and immediately discarded.
	if allDigits(s) {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, true
		}
	}
	for _, layout := range timeLayouts {
		if t, err := parseLayout(layout, s); err == nil {
			return t, true
		}
	}
	return 0, false
}
