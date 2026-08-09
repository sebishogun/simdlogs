package ingest

import (
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

// IngestJSONLinesParallel splits an NDJSON body at line boundaries and
// ingests the chunks concurrently, each through its own writer over the
// shared store (AppendGroup is concurrency-safe). The parser was the ingest
// bottleneck once flushing went async and single-threaded on a many-core
// box; sharding the parse is the lever. Shard count and per-shard flush
// pool are sized so the total goroutines stay near the core count rather
// than oversubscribing. Falls back to serial for a small body.
func IngestJSONLinesParallel(store *storage.Store, data []byte, fallback func() int64) (ingested, skipped int) {
	shards := runtime.NumCPU() / 3
	if shards < 2 || len(data) < MinParallelBytes {
		w := NewWriter(store)
		i, s := IngestJSONLines(w, data, fallback)
		w.Close()
		return i, s
	}
	chunks := splitLines(data, shards)
	var ing, skp int64
	var wg sync.WaitGroup
	for _, c := range chunks {
		if len(c) == 0 {
			continue
		}
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			w := NewWriterWorkers(store, 2)
			i, s := IngestJSONLines(w, chunk, fallback)
			w.Close()
			atomic.AddInt64(&ing, int64(i))
			atomic.AddInt64(&skp, int64(s))
		}(c)
	}
	wg.Wait()
	return int(ing), int(skp)
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
func IngestJSONLines(w *Writer, data []byte, fallback func() int64) (ingested, skipped int) {
	fields := map[string]string{}
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
		doc, err := simdjson.Parse(line)
		if err != nil {
			skipped++
			continue
		}
		v := doc.Root()
		if v.Kind() != simdjson.Object {
			skipped++
			continue
		}
		for k := range fields {
			delete(fields, k)
		}
		var ts int64
		haveTS := false
		v.ForEachKey(func(key string, val simdjson.Value) bool {
			switch val.Kind() {
			case simdjson.String:
				s := val.String()
				if key == "_time" {
					if t, ok := parseTime(s); ok {
						ts, haveTS = t, true
					}
				}
				fields[key] = s
			case simdjson.Number:
				if key == "_time" {
					ts, haveTS = val.Int(), true
				}
				fields[key] = strconv.FormatFloat(val.Float(), 'f', -1, 64)
			case simdjson.Bool:
				if val.Int() != 0 {
					fields[key] = "true"
				} else {
					fields[key] = "false"
				}
			}
			return true
		})
		if !haveTS {
			ts = fallback()
		}
		w.Add(ts, fields)
		ingested++
	}
	return ingested, skipped
}

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

// parseTime accepts unix-nanos-as-digits or RFC3339; returns nanos.
func parseTime(s string) (int64, bool) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, true
	}
	for _, layout := range timeLayouts {
		if t, err := parseLayout(layout, s); err == nil {
			return t, true
		}
	}
	return 0, false
}
