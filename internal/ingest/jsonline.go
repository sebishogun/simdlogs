package ingest

import (
	"strconv"

	"github.com/sebishogun/simdjson"
)

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
