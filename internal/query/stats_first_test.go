package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestRowAnyFirstLast covers row_any, count_uniq_hash stats and the first/last
// pipes (sort+limit sugar).
func TestRowAnyFirstLast(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := storage.BuildDict([]string{"a", "b", "c", "d"})
	num := storage.BuildDict([]string{"3", "1", "2", "1"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "id", Type: storage.ColDict, Dict: &id},
		{Name: "n", Type: storage.ColDict, Dict: &num},
	}}); err != nil {
		t.Fatal(err)
	}
	rows := func(q string) []Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return RunPipeline(s, pq)
	}
	one := func(q string) Row {
		r := rows(q)
		if len(r) != 1 {
			t.Fatalf("%q: got %d rows, want 1", q, len(r))
		}
		return r[0]
	}

	if got := rowField(one(`* | stats row_any(id) as a`), "a"); got != "a" {
		t.Errorf("row_any(id) = %q want a", got)
	}
	if got := rowField(one(`* | stats count_uniq_hash(id) as c`), "c"); got != "4" {
		t.Errorf("count_uniq_hash(id) = %q want 4", got)
	}
	if got := rowField(one(`* | stats count_uniq_hash(n) as c`), "c"); got != "3" { // {1,2,3}
		t.Errorf("count_uniq_hash(n) = %q want 3", got)
	}
	// first 2 by (n): ascending 1,1,2,3 -> two rows.
	if r := rows(`* | first 2 by (n)`); len(r) != 2 || rowField(r[0], "n") != "1" {
		t.Errorf("first 2 by (n): got %d rows, r0.n=%q", len(r), rowField(r[0], "n"))
	}
	// last 2 by (n): descending 3,2,1,1 -> first is n=3.
	if r := rows(`* | last 2 by (n)`); len(r) != 2 || rowField(r[0], "n") != "3" {
		t.Errorf("last 2 by (n): got %d rows, r0.n=%q", len(r), rowField(r[0], "n"))
	}
}
