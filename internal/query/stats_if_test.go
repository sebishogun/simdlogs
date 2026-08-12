package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestConditionalAggregates covers `agg(...) if (<filter>)`.
func TestConditionalAggregates(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	level := storage.BuildDict([]string{"error", "info", "error", "warn"})
	status := storage.BuildDict([]string{"500", "200", "503", "200"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "level", Type: storage.ColDict, Dict: &level},
		{Name: "status", Type: storage.ColDict, Dict: &status},
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

	r := one(`* | stats count() if (level:error) as errors, count() as total`)
	if rowField(r, "errors") != "2" || rowField(r, "total") != "4" {
		t.Errorf("conditional count: errors=%q total=%q want 2/4", rowField(r, "errors"), rowField(r, "total"))
	}
	if got := rowField(one(`* | stats sum(status) if (status:>=500) as s`), "s"); got != "1003" {
		t.Errorf("sum if = %q want 1003", got) // 500+503
	}
	if got := rowField(one(`* | stats count() if (status:>=500) as bad`), "bad"); got != "2" {
		t.Errorf("count if = %q want 2", got)
	}
}
