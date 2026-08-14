package query

import (
	"fmt"
	"testing"
)

func TestHLLAccuracy(t *testing.T) {
	t.Parallel()
	// Small cardinality: linear counting is effectively exact.
	h := newHLL()
	for _, s := range []string{"a", "b", "c", "a", "b"} {
		h.add(s)
	}
	if got := h.count(); got != 3 {
		t.Fatalf("hll small count = %d, want 3", got)
	}

	// Large cardinality: bounded 16KB, within a few percent of truth.
	h = newHLL()
	const n = 200000
	for i := 0; i < n; i++ {
		h.add(fmt.Sprintf("value-%d", i))
	}
	got := h.count()
	err := float64(got-n) / float64(n)
	if err < -0.03 || err > 0.03 {
		t.Fatalf("hll count = %d for %d distinct (%.1f%% error), want within 3%%", got, n, err*100)
	}
}

func TestCountUniqExactSmall(t *testing.T) {
	s := statsStore(t) // service a,a,b,a,b -> 2 distinct services; latency 5 distinct
	pq, err := ParseLogsQL(`* | stats count_uniq(service) as u, count_uniq(latency) as l`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if u := rowField(rows[0], "u"); u != "2" {
		t.Fatalf("count_uniq(service) = %q, want 2", u)
	}
	if l := rowField(rows[0], "l"); l != "5" {
		t.Fatalf("count_uniq(latency) = %q, want 5", l)
	}
}
