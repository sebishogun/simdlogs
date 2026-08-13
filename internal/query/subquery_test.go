package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestSubqueryPipes(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// users: id -> name; events: id + action.
	id := storage.BuildDict([]string{"1", "2", "3", "1"})
	act := storage.BuildDict([]string{"login", "buy", "login", "logout"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "id", Type: storage.ColDict, Dict: &id},
		{Name: "action", Type: storage.ColDict, Dict: &act},
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
	// in(subquery): ids that ever bought -> {2}. So action:login id:in(buyers) -> none (id 2 never login).
	// Simpler: id:in(action:buy | fields id) -> id==2 rows -> 1 row (the buy row).
	if n := len(rows(`id:in(action:buy | fields id)`)); n != 1 {
		t.Errorf("in(subquery) = %d rows want 1", n)
	}
	// union: login rows + logout rows.
	if n := len(rows(`action:login | union (action:logout)`)); n != 3 { // 2 login + 1 logout
		t.Errorf("union = %d rows want 3", n)
	}
	// join by (id): attach action of the buy row to matching id.
	joined := rows(`action:login | join by (id) (action:buy)`)
	// login rows have id 1 and 3; buy row has id 2 -> no match -> passthrough 2 rows.
	if len(joined) != 2 {
		t.Errorf("join = %d rows want 2", len(joined))
	}
}

func TestStreamContext(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// 5 rows in time order; only row 2 (idx) matches level:error.
	lvl := storage.BuildDict([]string{"info", "info", "error", "info", "info"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 5, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4, 5}},
		{Name: "level", Type: storage.ColDict, Dict: &lvl},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`level:error | stream_context before 1 after 1`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 3 { // the error row plus one before and one after
		t.Errorf("stream_context = %d rows want 3", len(rows))
	}
}
