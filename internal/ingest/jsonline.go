package ingest

import (
	"errors"
	"math"
	"runtime"
	"strconv"
	"sync"

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
// forgotten changes the stored schema for large requests only.
//
// THE SAME OMISSION HAPPENED THREE TIMES, WHICH IS A DESIGN SIGNAL AND NOT
// THREE ACCIDENTS. Compact was copied and StreamFields was not, so the same
// records grew a _stream column under the small-body path and none under this
// one. Then Limits, so a body over MinParallelBytes got no field cap, no name
// or value cap and a forgeable ".error" control name. Then VectorFields, so a
// body over MinParallelBytes lost every embedding. Each time the cause was the
// same: a second enumeration of the settings, written in internal/api beside
// the code that builds the tenant writer, kept by hand.
//
// The second enumeration is gone. Build this with Writer.ShardSettings from
// the writer that is already configured, and set Shards; a field added below
// and forgotten in ShardSettings or apply fails
// TestShardSettingsRoundTripEveryField, which walks the struct by reflection
// rather than naming the fields.
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

	// VectorFields is which record fields are embeddings, and their
	// dimensions. It was absent, and IngestJSONLinesOpts reads
	// `w.VectorFields()` once per body: on a shard writer that set was
	// empty, so `Dim(key)` said no for every configured field and the
	// `simdjson.Array` arm returned early. The row was stored with its
	// embedding silently dropped -- text-queryable and invisible to
	// /select/vector, which the reject arm two hundred lines below calls
	// worse than a rejection the client can see. Measured on the built
	// binary with -vector-fields=emb:4: a 105-byte body of 3 rows answered
	// all 3 ranked; a 1,052,693-byte body of 27,277 rows answered 0, at HTTP
	// 200 with {"ingested":27277,"skipped":0}.
	//
	// It is not on the wire-limit axis the three fields above are on: this
	// one decides the COLUMN TYPE, so forgetting it does not tighten or
	// loosen a bound, it changes what was stored.
	VectorFields VectorFields

	// Shards overrides the shard count. Zero means derive it from the CPU
	// count. It exists because the derived value is runtime.NumCPU()/3, which
	// is below the 2-shard minimum on any machine with fewer than six cores --
	// including every stock CI runner. Without an override the concurrent
	// branch is dead code exactly where it is meant to be gated, and the
	// tests that cover it pass by running the serial fallback instead.
	Shards int
}

// ShardsFor resolves the shard count for a body of n bytes: 0 means run the
// serial fallback.
//
// EXPORTED SO A TEST IN ANOTHER PACKAGE CAN ASSERT WHICH BRANCH IT IS ON.
// The condition is TWO halves -- `n >= MinParallelBytes` AND `Shards >= 2`,
// derived as runtime.NumCPU()/3 -- and a gate in internal/api guarded on the
// first alone while its failure message asserted the whole ("under
// MinParallelBytes the parallel path is not exercised"). It was not: at four
// cores the derived count is 1, the body ran serial, and the mutation the
// gate exists to kill (`base += 0` in mergeShardResults) was GREEN under
// `taskset -c 0-3` and RED at 32 CPUs. Asking this function is the only way
// to know.
func (c ParallelConfig) ShardsFor(n int) int {
	sh := c.Shards
	if sh == 0 {
		sh = runtime.NumCPU() / 3
	}
	if sh < 2 || n < MinParallelBytes {
		return 0
	}
	return sh
}

// apply stamps the deployment settings onto one shard writer.
//
// EVERY FIELD EXCEPT Shards MUST BE SET HERE, and ShardSettings must read
// every one of them back. The pair is what makes a shard writer equal to the
// tenant writer, and TestShardSettingsRoundTripEveryField walks the struct by
// reflection so a field added to it and forgotten in either half is a
// compile-and-run failure rather than a schema change on large bodies only.
func (c ParallelConfig) apply(w *Writer) {
	w.SetCompact(c.Compact)
	w.SetRecordLimits(c.Limits)
	w.SetVectorFields(c.VectorFields)
	if c.MaxLineBytes > 0 {
		w.SetMaxLineBytes(c.MaxLineBytes)
	}
	if len(c.StreamFields) > 0 {
		w.SetStreamFields(c.StreamFields)
	}
}

// ShardSettings reads a writer's deployment settings back as the config the
// shard writers of a large body must inherit. Shards is left zero for the
// caller to fill in.
//
// THE CALLER IS THE REASON THIS EXISTS. internal/api used to rebuild the
// config from the SERVER's own fields, in parallel with the code that builds
// the tenant writer from the same fields, and the two lists drifted three
// times: Compact copied and StreamFields forgotten, then RecordLimits, then
// VectorFields -- each one a body over MinParallelBytes stored under different
// rules from the same body one byte smaller. Reading the settings off the
// writer that is already configured removes that list entirely: there is no
// second enumeration to keep in step, because the tenant writer IS the
// specification.
//
// MaxDecompressedBytes is deliberately not here. It bounds what one COMPRESSED
// body may expand to and is read in exactly one place, lokipb.go, before any
// shard writer exists; the Loki protobuf route has no sharded branch. If a
// sharded caller ever reads it, it belongs in ParallelConfig.
//
// THE REFLECTION GATE WILL NOT SAY SO. It walks `ParallelConfig`, so it catches
// a field that is in the struct and dropped by one half -- which is what
// `TestShardSettingsRoundTripEveryField` is for. A Writer setting that never
// enters the struct is invisible to it, and that is the shape all three
// historical drifts took: measured, deleting `VectorFields` from the struct,
// from `apply` and from here at once leaves the reflection gate GREEN and is
// caught only by `TestALargeBodyKeepsItsEmbeddings`. docs/lld/ingest.md:469
// states this scope correctly.
func (w *Writer) ShardSettings() ParallelConfig {
	w.mu.Lock()
	defer w.mu.Unlock()
	return ParallelConfig{
		Compact:      w.compact,
		StreamFields: append([]string(nil), w.strmFlds...),
		MaxLineBytes: w.maxLine,
		Limits:       w.limits,
		VectorFields: w.vecFlds,
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
	r, perr := IngestJSONLinesParallelResult(store, data, fallback, cfg, opts)
	return r.Accepted, r.Rejected, perr
}

// IngestJSONLinesParallelResult is IngestJSONLinesParallelCfg with the
// per-record POSITIONS kept, rebased onto the whole batch.
//
// THE POSITIONS ARE NOT LOST BY SHARDING; THEY WERE THROWN AWAY.
// `IngestJSONLinesParallelCfg` returned three integers, so `/_bulk` -- the one
// caller that has to say something per document, because ES clients match
// items to their requests by position -- declared the positions UNKNOWN for
// any body over MinParallelBytes and `markBulkRejects` then marked EVERY
// candidate item. After round 18 made a rejection a 400 that is a permanent
// failure stamped on documents that are on disk: measured, a 6 MiB body of
// 20,871 `index` actions with one unstorable `_time` answered 20,871 items at
// 400 with 20,870 rows stored, and no shipper ever re-sends a 4xx.
//
// `splitLines` cuts on line boundaries and the chunks are contiguous and in
// body order, so shard k's first record is at the sum of the earlier shards'
// record counts -- and `IngestJSONLinesOpts` counts an ordinal for every
// non-blank line it sees, accepted or not, so `Accepted+Rejected` IS that
// count. The rebase is exact and costs one addition per recorded position.
//
// This is the merge `Result.Add` refuses to do, and the reason it refuses does
// not apply here: Add cannot know that two results came from adjacent slices
// of one body in order, and this function does.
func IngestJSONLinesParallelResult(store *storage.Store, data []byte, fallback func() int64, cfg ParallelConfig, opts *Options) (Result, error) {
	shards := cfg.ShardsFor(len(data))
	if shards == 0 {
		w := NewWriter(store)
		cfg.apply(w)
		r, perr := IngestJSONLinesOpts(w, data, fallback, opts)
		if cerr := w.Close(); cerr != nil {
			// Partial comes off the shard's own error here too. This branch
			// is taken whenever cfg.ShardsFor() is below 2 -- runtime.NumCPU()/3,
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
			// THE ONE SHARD IS LOST, AND A LOST SHARD'S Accepted IS NOT
			// COUNTED. mergeShardResults zeroes it below for exactly this
			// reason, and this branch returned `r` with its parse-time
			// Accepted intact -- so the SAME request against the SAME
			// unwritable store answered `"durable":0` on a machine that
			// sharded and `"durable":9698` on one that did not. The split is
			// runtime.NumCPU()/3 < 2, i.e. every host with fewer than six
			// cores, so the wrong answer is the one a small host gives: an
			// operator is told 9,698 rows landed when the store refused every
			// group. Rejected and its positions stay -- a malformed line was
			// malformed whether or not the group landed.
			r.Accepted = 0
			return r, &ParallelWriteError{
				Shards: 1, Failed: 1, Err: cerr, Partial: partial,
			}
		}
		return r, perr
	}
	chunks := splitLines(data, shards)
	// One slot per chunk, written by that chunk's goroutine alone: the merge
	// below needs the shards IN ORDER, which a shared counter cannot give.
	per := make([]Result, len(chunks))
	lost := make([]bool, len(chunks))
	var (
		mu       sync.Mutex
		firstErr error
		// anyPartial is set by any shard whose own failure was partial.
		anyPartial bool
		failed     int
		started    int
	)
	var wg sync.WaitGroup
	for ci, c := range chunks {
		if len(c) == 0 {
			continue
		}
		started++
		wg.Add(1)
		go func(ci int, chunk []byte) {
			defer wg.Done()
			w := NewWriterWorkers(store, 2)
			cfg.apply(w)
			pr, _ := IngestJSONLinesOpts(w, chunk, fallback, opts)
			// Close before counting: a shard whose rows never landed must
			// not contribute to the accepted total.
			cerr := w.Close()
			// Skipped lines are a parse fact: they were malformed whether or
			// not the group landed, so they are counted either way. Ingested
			// is only counted when the rows are durable, which `lost` carries
			// to the merge -- the shard's own record count is still needed
			// there, because it is the BASE every later shard's ordinals are
			// measured from.
			per[ci] = pr
			if cerr != nil {
				lost[ci] = true
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
			}
		}(ci, c)
	}
	wg.Wait()

	out := mergeShardResults(per, lost)
	if firstErr != nil {
		return out, &ParallelWriteError{
			Shards: started, Failed: failed, Err: firstErr, Partial: anyPartial,
		}
	}
	return out, nil
}

// mergeShardResults folds the shards' results into one, rebasing each shard's
// record ordinals onto the batch. `lost[i]` marks a shard whose rows never
// reached the store: its Accepted is not counted, and its record count still
// is, because that count is the base for every shard after it.
func mergeShardResults(per []Result, lost []bool) Result {
	var out Result
	total := 0
	for _, pr := range per {
		total += pr.Rejected
	}
	if total > 0 {
		n := total
		if n > MaxRejectedAt {
			n = MaxRejectedAt
		}
		out.RejectedAt = make([]int32, 0, n)
	}
	base := 0
	for i, pr := range per {
		if !lost[i] {
			out.Accepted += pr.Accepted
		}
		out.Rejected += pr.Rejected
		if pr.RejectedTruncated {
			out.RejectedTruncated = true
		}
		for _, ord := range pr.RejectedAt {
			abs := int64(base) + int64(ord)
			// The bound is the same one Reject applies, and it is checked on
			// the MERGED list: eight shards each holding MaxRejectedAt
			// positions is eight times the array the bound exists to prevent.
			// int32 is the field's width, so a batch with more records than
			// that cannot be attributed either.
			if len(out.RejectedAt) >= MaxRejectedAt || abs > math.MaxInt32 {
				out.RejectedTruncated = true
				break
			}
			out.RejectedAt = append(out.RejectedAt, int32(abs))
		}
		// Warning.Ordinal rebases; Warning.Offset CANNOT, and this function
		// is where that stops being a choice. It receives per-shard RECORD
		// COUNTS and nothing else -- the chunk byte starts stay inside
		// IngestJSONLinesParallelResult -- so there is no value here to add
		// to a byte offset. `splitLines` hands each shard `data[start:end]`,
		// so any Offset a shard records is CHUNK-relative, and leaving it
		// alone publishes it as if it were body-relative. That is not a rule
		// worth pinning as correct; it is the best this signature can do.
		//
		// It costs nothing today because no producer exists on this path:
		// Result.Warn's callers are lokipb.go (which passes UnknownPos) and
		// three sites in journald.go, and journald does not shard. The first
		// shard-path caller of Warn has to pass UnknownPos or hand this
		// function the chunk starts.
		for _, wn := range pr.Warnings {
			if wn.Ordinal != UnknownPos {
				wn.Ordinal += int64(base)
			}
			out.warn(wn)
		}
		base += pr.Accepted + pr.Rejected
	}
	return out
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
			res.WarnAt(ordinal, "line of %d bytes exceeds the configured maximum", len(line))
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
		var tsErr error
		v.ForEachKey(func(key string, val simdjson.Value) bool {
			switch val.Kind() {
			case simdjson.String:
				s := val.String()
				if opts.isTime(key) {
					t, ok, terr := parseTime(s)
					if terr != nil {
						tsErr = terr
						return false
					}
					if ok {
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
					// THE SAME RANGE CHECK THE STRING SPELLING GETS.
					//
					// This was `ts, haveTS = val.Int(), true` -- no parseTime,
					// no nanosOf, no range check at all -- so the whole
					// ErrTimeOutOfRange contract was reachable only by quoting
					// the value. Measured on /insert/jsonline, every one
					// answering 200 {"ingested":4,"skipped":0}:
					//
					//	{"_time":9.3e18}        stored at 1677-09-21T00:12:43.145224192Z
					//	{"_time":2534023008e11} stored at 1677-09-21T00:12:43.145224192Z
					//	{"_time":-9.3e18}       stored at 1677-09-21T00:12:43.145224192Z
					//
					// Three different instants, two of them in opposite
					// directions, all filed at MinInt64 -- the collapse this
					// round's own `satFloatNanos` doc comment describes on the
					// query side, happening here to a row's FACT rather than
					// to a bound. `/_bulk` inherits it, and so does every
					// `--field-time` mapping onto a numeric field.
					t, ok, terr := numberTime(val.Raw())
					if terr != nil {
						tsErr = terr
						return false
					}
					if ok {
						ts, haveTS = t, true
						return true
					}
					// Unreadable as an instant: ordinary data, kept as a field
					// under the raw digits, exactly as the String arm does.
					fields[key] = string(val.Raw())
					return true
				}
				// An INTEGER keeps its digits.
				//
				// Every number went through Float(), so 9007199254740993 --
				// one past the last integer a float64 represents exactly --
				// was stored as 9007199254740992. A snowflake id, a trace id
				// and an epoch-nanosecond timestamp are all in that range, and
				// the row comes back off by one with nothing to say so.
				if raw := val.Raw(); isIntegerLiteral(raw) {
					fields[key] = string(raw) // the wire's own digits
				} else {
					fields[key] = strconv.FormatFloat(val.Float(), 'f', -1, 64)
				}
			case simdjson.Bool:
				// Bool(), not Int().
				//
				// simdjson's Value.Int() returns 0 for every kind that is not
				// a Number, so on a Bool this branch was dead and EVERY JSON
				// boolean -- true and false alike -- was stored as the string
				// "false". `v:=true` matched no row ever ingested and
				// `v:=false` matched all of them, at HTTP 200 with
				// {"ingested":1,"skipped":0}. Value.Bool() is four lines below
				// Value.Int() in the same file and was used nowhere in this
				// repository.
				if val.Bool() {
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
					// The MESSAGE IS DEFERRED, the same shape tsRangeError
					// takes: `fmt.Errorf` here is two allocations per rejected
					// record on a path `/_bulk` reaches, and past the 32nd
					// warning the string it builds is discarded unread.
					vecErr = &vecShapeError{field: key, dim: dim}
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
		if tsErr != nil {
			// Rejected as a RECORD, at its ordinal, for the same reason the
			// vector case below is: a row stored under a timestamp the client
			// did not send is worse than a rejection the client can see and
			// fix. See ErrTimeOutOfRange.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "%v", tsErr)
			continue
		}
		if vecErr != nil {
			// Rejected as a RECORD, at its ordinal, rather than stored with
			// the embedding dropped. A log line stored without its vector is
			// a line invisible to the one search it was ingested for, which
			// is worse than a rejection the client can see and fix.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "%v", vecErr)
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
//
// THREE OUTCOMES, not two, and the third is the one that used to be silent:
//
//	ok, nil err    a timestamp, storable
//	!ok, nil err   not a timestamp at all -- ordinary data. The caller keeps it
//	               as a field and stamps the row with the fallback.
//	!ok, err       a timestamp that PARSES and cannot be stored. The caller
//	               REFUSES the record and counts it; see ErrTimeOutOfRange.
//
// The out-of-range case cannot be folded into the second: a row whose `_time`
// says 9999 is not a row with no timestamp, and stamping it `now` files it
// under an instant nobody sent just as surely as wrapping did.
func parseTime(s string) (int64, bool, error) {
	// Only call ParseInt when the value really can be an integer. An RFC3339
	// _time -- the common case for logs -- made it fail on EVERY row, and a failed
	// ParseInt allocates a syntaxError plus a copy of the string: ~36% of all
	// ingest allocations were errors that were built and immediately discarded.
	//
	// ParseInt bounds its RESULT to int64, so every value it accepts is a
	// storable nanosecond count -- and a value it REFUSES for range is
	// outcome three, not outcome two. See below.
	if allDigits(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n, true, nil
		}
		// AN ALL-DIGITS VALUE TOO BIG FOR int64 IS OUTCOME THREE, NOT TWO.
		//
		// This fell through to the layouts -- none of which matches a bare run
		// of digits -- and returned "not a timestamp at all", so the caller
		// stamped the row with the RECEIVER'S CLOCK. That is the exact thing
		// the paragraph above forbids: "a row whose `_time` says 9999 is not a
		// row with no timestamp, and stamping it `now` files it under an
		// instant nobody sent just as surely as wrapping did."
		//
		// It is also the ONLY spelling Loki and OTLP emit. A Loki push
		// timestamp and an OTLP `timeUnixNano` are always a decimal nanosecond
		// string, so entry 130's ingest gate -- six out-of-range rows, every
		// one a date-LAYOUT spelling -- tested a spelling neither protocol
		// sends. Measured with `"253402300800000000000"` (year 9999 in
		// nanoseconds), before this branch existed:
		//
		//	/insert/jsonline    200 {"ingested":2,"skipped":1}  (the 1 is a layout row)
		//	/insert/logfmt      200 {"ingested":2,"skipped":0}
		//	/loki/api/v1/push   204, no X-Simdlogs-Rejected header
		//	/v1/logs            200 {}   -- full success
		//	/api/v2/logs        202, empty body
		//	control, the same year spelled as a layout, via OTLP:
		//	                    200 {"partialSuccess":{"rejectedLogRecords":"1"}}
		//
		// ParseInt's only possible failure here is ErrRange -- allDigits has
		// already ruled out a syntax error -- so there is no value this
		// refuses that was ever ordinary data.
		if errors.Is(err, strconv.ErrRange) {
			return 0, false, &tsRangeError{value: s, unit: "ns"}
		}
	}
	for _, layout := range timeLayouts {
		t, err := parseLayout(layout, s)
		if err == nil {
			return t, true, nil
		}
		// A LAYOUT THAT MATCHED and produced an unstorable instant stops the
		// loop; one that did not match just moves on. Falling through would let
		// a later layout fail to parse and report the value as ordinary data,
		// which is how a year-9999 row would become a row stamped `now`.
		if errors.Is(err, ErrTimeOutOfRange) {
			return 0, false, err
		}
	}
	return 0, false, nil
}

// isIntegerLiteral reports whether a JSON number's raw bytes are a plain
// integer -- optional sign, then digits, and nothing else.
//
// The raw text is used verbatim for those, because routing them through
// float64 loses digits above 2^53 and a snowflake id, a trace id and an
// epoch-nanosecond timestamp are all in that range. Costs one string
// conversion, which is what FormatFloat cost on the same path.
func isIntegerLiteral(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	i := 0
	if b[0] == '-' {
		i = 1
	}
	if i == len(b) {
		return false
	}
	for ; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return false
		}
	}
	return true
}

// vecShapeError is ErrVector carrying the field and dimension that were
// refused, with the message built in Error() rather than at construction.
//
// IT IS A STRUCT FOR THE REASON tsRangeError IS. This arm is per record and
// `/_bulk` reaches it: a bulk body of documents whose embedding is the wrong
// length rejected each one through `fmt.Errorf("%w: %s has an unusable value
// for a %d-dimension field", ...)`, two allocations built before WarnAt is
// reached and so before warnFull can drop them. Measured with
// testing.AllocsPerRun over 200,000 calls: the fmt.Errorf form is 2.000
// allocations every time; this one is 1.000, and the message is built only
// for the at most 32 warnings a client is actually shown.
//
// The wording is unchanged -- it is what a client reads on a partial ingest --
// and Unwrap keeps `errors.Is(err, ErrVector)` true, which is what the reject
// arm and the status mapping both ask.
type vecShapeError struct {
	field string
	dim   int
}

func (e *vecShapeError) Error() string {
	return ErrVector.Error() + ": " + e.field +
		" has an unusable value for a " + strconv.Itoa(e.dim) + "-dimension field"
}

func (e *vecShapeError) Unwrap() error { return ErrVector }
