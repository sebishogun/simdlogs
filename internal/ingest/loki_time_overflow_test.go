package ingest

import (
	"math"
	"testing"
)

// A LOKI PROTOBUF TIMESTAMP IS TWO ARBITRARY INTEGERS OFF THE WIRE.
//
// `logproto.EntryAdapter.Timestamp` carries int64 seconds and int32 nanos, and
// the parser combined them as `seconds*1e9 + nanos`. Neither operation is safe
// on its own: the multiply overflows for any seconds value past the year 2262,
// and the addition can carry a sum over the edge from a seconds value that was
// inside it. A wrapped product is a row filed in the distant past, accepted at
// 200 and invisible to every query -- the same silent loss the JSON and NDJSON
// paths had, arriving through the encoding Promtail and Grafana Alloy send BY
// DEFAULT.
//
// Refused and COUNTED, not stored: see ErrTimeOutOfRange for why an unstorable
// row timestamp is not clamped the way a query BOUND is.
func TestALokiProtoTimestampOutsideTheStorableRangeIsRefused(t *testing.T) {
	body := lokiPushProto(lokiStream(`{a="b"}`,
		lokiEntry(1714521600, 250000000, "storable"),
		lokiEntry(13000000000, 0, "year-2381"), // seconds*1e9 = 1.3e19
		lokiEntry(math.MaxInt64, 0, "max-seconds"),
	))
	const fallbackTS = 999999999
	rows, res := rowsOf(t, func(w *Writer) (Result, error) {
		return IngestLokiProto(w, body, func() int64 { return fallbackTS }, nil)
	})
	if res.Accepted != 1 || res.Rejected != 2 {
		t.Fatalf("accepted=%d rejected=%d, want 1/2. A timestamp that cannot "+
			"be stored must be counted, not written at an unrelated instant",
			res.Accepted, res.Rejected)
	}
	if len(rows) != 1 {
		t.Fatalf("%d rows stored, want 1: %v", len(rows), rows)
	}
	if got := fieldsOfRow(rows[0])["_msg"]; got != "storable" {
		t.Fatalf("the stored row is %q, want the storable one", got)
	}
}

// lokiNanos' own two edges, which the end-to-end test above cannot separate.
func TestLokiNanosSaysWhenTheCombinationDoesNotFit(t *testing.T) {
	for _, tc := range []struct {
		name           string
		seconds, nanos int64
		want           int64
		ok             bool
	}{
		{"an ordinary instant", 1714521600, 250000000, 1714521600250000000, true},
		{"the last storable second", math.MaxInt64 / 1_000_000_000, 0, (math.MaxInt64 / 1_000_000_000) * 1_000_000_000, true},
		{"one second past it", math.MaxInt64/1_000_000_000 + 1, 0, 0, false},
		{"the first storable second", math.MinInt64 / 1_000_000_000, 0, (math.MinInt64 / 1_000_000_000) * 1_000_000_000, true},
		{"one second before it", math.MinInt64/1_000_000_000 - 1, 0, 0, false},
		// THE ADDITION, not the multiply: seconds is inside the range and the
		// nanos field pushes the sum out of it. A guard on the multiply alone
		// passes this and still wraps.
		{"in range until the nanos are added", math.MaxInt64 / 1_000_000_000, 1 << 30, 0, false},
		{"in range until negative nanos are added", math.MinInt64 / 1_000_000_000, -(1 << 30), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := lokiNanos(tc.seconds, tc.nanos)
			if ok != tc.ok {
				t.Fatalf("lokiNanos(%d, %d) ok=%v, want %v", tc.seconds, tc.nanos, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("lokiNanos(%d, %d) = %d, want %d", tc.seconds, tc.nanos, got, tc.want)
			}
		})
	}
}
