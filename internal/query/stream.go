package query

import (
	"encoding/hex"
	"hash/fnv"
	"strings"
)

// A stream is a distinct value of the synthesized _stream column, a canonical
// label set {k="v",...} (keys sorted). Its id is a stable hash of that string.
// These back the /select/logsql/stream* endpoints and the _stream_id filter.

// StreamID is the stable id of a _stream label-set value: a 128-bit hash in the
// 48-hex-character form the reference uses, so a client that validates the id's
// shape (or round-trips it into a _stream_id: filter) sees what it expects. The
// leading 16 zeros are the tenant half of that form; ids are computed inside one
// tenant's store, which is the scope they are compared in.
func StreamID(streamValue string) string {
	h := fnv.New128a()
	h.Write([]byte(streamValue))
	var buf [16]byte
	return streamIDTenantPrefix + hex.EncodeToString(h.Sum(buf[:0]))
}

const streamIDTenantPrefix = "0000000000000000"

// EmptyStream is the label set of rows that belong to no configured stream.
// With no stream fields set, every row is in this one stream -- which is what
// the reference reports, rather than reporting no streams at all.
const EmptyStream = "{}"

// Streams lists the distinct _stream label sets among the rows matching q, with
// hit counts.
func Streams(s Store, q *Query) []ValueCount {
	out, named := streamValues(s, q)
	if !named {
		// No _stream column: the rows are not streamless, they are all in the
		// empty stream. Reporting nothing here made /streams and /stream_ids
		// answer [] against a store a client could plainly query.
		if n := Count(s, q); n > 0 {
			return []ValueCount{{Value: EmptyStream, Count: n}}
		}
		return nil
	}
	sortValueCounts(out)
	return out
}

// streamValues returns the named streams, and whether there were any. The two
// answers are separate because only the endpoints that report a stream's HITS
// need the row count behind the empty-stream fallback -- the ones that report
// its LABELS do not, and the empty stream has none.
func streamValues(s Store, q *Query) (out []ValueCount, named bool) {
	all := StatsByField(s, q, "_stream")
	out = all[:0]
	for _, vc := range all {
		if vc.Value != "" {
			out = append(out, vc)
		}
	}
	return out, len(out) > 0
}

// StreamIDs lists the distinct stream ids among the rows matching q.
func StreamIDs(s Store, q *Query) []ValueCount {
	streams := Streams(s, q)
	out := make([]ValueCount, 0, len(streams))
	for _, vc := range streams {
		out = append(out, ValueCount{Value: StreamID(vc.Value), Count: vc.Count})
	}
	return out
}

// StreamFieldNames lists the distinct label names across matching streams, with
// the number of rows each label covers.
func StreamFieldNames(s Store, q *Query) []ValueCount {
	streams, named := streamValues(s, q)
	if !named {
		// Every row is in the empty stream, which has no labels. Falling through
		// to Streams here counted every matching row to learn a total that the
		// answer -- an empty list -- never uses.
		return nil
	}
	counts := map[string]int{}
	for _, vc := range streams {
		for k := range parseStreamLabels(vc.Value) {
			counts[k] += vc.Count
		}
	}
	out := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		out = append(out, ValueCount{Value: v, Count: c})
	}
	sortValueCounts(out)
	return out
}

// StreamFieldValues lists the distinct values of one stream label, with hits.
func StreamFieldValues(s Store, q *Query, field string) []ValueCount {
	streams, named := streamValues(s, q)
	if !named {
		return nil // the empty stream carries no labels, so no label has values
	}
	counts := map[string]int{}
	for _, vc := range streams {
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
