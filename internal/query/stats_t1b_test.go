package query

import (
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestT1bStats(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := storage.BuildDict([]string{"a", "b", "c", "d"})
	lat := storage.BuildDict([]string{"100", "50", "200", "5"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "service", Type: storage.ColDict, Dict: &svc},
		{Name: "latency", Type: storage.ColDict, Dict: &lat},
	}}); err != nil {
		t.Fatal(err)
	}
	one := func(q string) Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		r := RunPipeline(s, pq)
		if len(r) != 1 {
			t.Fatalf("%q: %d rows", q, len(r))
		}
		return r[0]
	}

	if got := rowField(one(`* | stats row_max(latency, service) as slow`), "slow"); got != "c" {
		t.Errorf("row_max = %q want c", got)
	}
	if got := rowField(one(`* | stats row_min(latency, service) as fast`), "fast"); got != "d" {
		t.Errorf("row_min = %q want d", got) // latency 5 is min -> service d
	}
	// histogram: 4 values across buckets, hits sum to 4, JSON array of vmrange.
	h := rowField(one(`* | stats histogram(latency) as h`), "h")
	if !strings.HasPrefix(h, "[") || !strings.Contains(h, "vmrange") {
		t.Errorf("histogram not a bucket array: %q", h)
	}
	if n := strings.Count(h, `"hits"`); n == 0 {
		t.Errorf("histogram has no buckets: %q", h)
	}
}
