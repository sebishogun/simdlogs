package query

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// A stream is a distinct value of the synthesized _stream column, a canonical
// label set {k="v",...} (keys sorted). Its id is a stable hash of that string.
// These back the /select/logsql/stream* endpoints and the _stream_id filter.

// StreamID is the stable id of a _stream label-set value.
func StreamID(streamValue string) string {
	h := fnv.New64a()
	h.Write([]byte(streamValue))
	return strconv.FormatUint(h.Sum64(), 16)
}

// Streams lists the distinct _stream label sets in the window, with hit counts.
func Streams(s Store, from, to int64) []ValueCount {
	out := FieldValues(s, "_stream", from, to)
	filtered := out[:0]
	for _, vc := range out {
		if vc.Value != "" {
			filtered = append(filtered, vc)
		}
	}
	return filtered
}

// StreamIDs lists the distinct stream ids in the window, with hit counts.
func StreamIDs(s Store, from, to int64) []ValueCount {
	streams := Streams(s, from, to)
	out := make([]ValueCount, 0, len(streams))
	for _, vc := range streams {
		out = append(out, ValueCount{Value: StreamID(vc.Value), Count: vc.Count})
	}
	return out
}

// StreamFieldNames lists the distinct label names across all streams.
func StreamFieldNames(s Store, from, to int64) []string {
	seen := map[string]struct{}{}
	for _, vc := range Streams(s, from, to) {
		for k := range parseStreamLabels(vc.Value) {
			seen[k] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// StreamFieldValues lists the distinct values of one stream label, with hits.
func StreamFieldValues(s Store, field string, from, to int64) []ValueCount {
	counts := map[string]int{}
	for _, vc := range Streams(s, from, to) {
		if v, ok := parseStreamLabels(vc.Value)[field]; ok {
			counts[v] += vc.Count
		}
	}
	out := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		out = append(out, ValueCount{Value: v, Count: c})
	}
	sortValueCounts(out)
	return out
}

// parseStreamLabels parses a `{k="v",k2="v2"}` label set into a map. Values are
// unescaped as written by buildStreamLabel (no comma/quote escaping there).
func parseStreamLabels(s string) map[string]string {
	m := map[string]string{}
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return m
	}
	for _, part := range strings.Split(s[1:len(s)-1], ",") {
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(part[:eq])
		v := strings.Trim(strings.TrimSpace(part[eq+1:]), `"`)
		if k != "" {
			m[k] = v
		}
	}
	return m
}
