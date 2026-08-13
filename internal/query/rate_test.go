package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestRateStats(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	x := storage.BuildDict([]string{"10", "20", "30", "40"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{0, 1e9, 2e9, 3e9}},
		{Name: "x", Type: storage.ColDict, Dict: &x},
	}}); err != nil {
		t.Fatal(err)
	}
	one := func(q string) Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, 4e9 // 4-second window
		r := RunPipeline(s, pq)
		if len(r) != 1 {
			t.Fatalf("%q: %d rows", q, len(r))
		}
		return r[0]
	}
	if got := rowField(one(`* | stats rate() as r`), "r"); got != "1" { // 4 rows / 4s
		t.Errorf("rate = %q want 1", got)
	}
	if got := rowField(one(`* | stats rate_sum(x) as r`), "r"); got != "25" { // 100 / 4s
		t.Errorf("rate_sum = %q want 25", got)
	}
}
