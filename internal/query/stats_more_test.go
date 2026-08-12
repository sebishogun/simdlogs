package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestNewStatsFuncs covers the VL stats functions added for parity:
// values, uniq_values, sum_len, count_empty.
func TestNewStatsFuncs(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// 5 rows, one empty level -- so count_empty and the empty-skip in values show.
	lvl := storage.BuildDict([]string{"error", "info", "error", "", "warn"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 5, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4, 5}},
		{Name: "level", Type: storage.ColDict, Dict: &lvl},
	}}); err != nil {
		t.Fatal(err)
	}
	run := func(q string) Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		rows := RunPipeline(s, pq)
		if len(rows) != 1 {
			t.Fatalf("%q: got %d rows, want 1", q, len(rows))
		}
		return rows[0]
	}

	if got := rowField(run(`* | stats values(level) as v`), "v"); got != `["error","info","error","warn"]` {
		t.Errorf("values = %q", got)
	}
	if got := rowField(run(`* | stats uniq_values(level) as v`), "v"); got != `["error","info","warn"]` {
		t.Errorf("uniq_values = %q", got)
	}
	if got := rowField(run(`* | stats sum_len(level) as n`), "n"); got != "18" { // 5+4+5+0+4
		t.Errorf("sum_len = %q want 18", got)
	}
	if got := rowField(run(`* | stats count_empty(level) as n`), "n"); got != "1" {
		t.Errorf("count_empty = %q want 1", got)
	}
}
