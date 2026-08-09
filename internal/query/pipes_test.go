package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestParsePipes(t *testing.T) {
	q, err := ParseLogsQL(`service:=auth | stats by (level) count() as c, avg(latency) | sort by (c) desc limit 5 | fields level, c`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Pipes) != 3 {
		t.Fatalf("got %d pipes, want 3", len(q.Pipes))
	}
	sp, ok := q.Pipes[0].(*StatsPipe)
	if !ok || len(sp.By) != 1 || sp.By[0] != "level" || len(sp.Aggs) != 2 {
		t.Fatalf("stats pipe wrong: %+v", q.Pipes[0])
	}
	if sp.Aggs[0].Kind != AggCount || sp.Aggs[0].Alias != "c" {
		t.Fatalf("agg0 wrong: %+v", sp.Aggs[0])
	}
	if sp.Aggs[1].Kind != AggAvg || sp.Aggs[1].Field != "latency" {
		t.Fatalf("agg1 wrong: %+v", sp.Aggs[1])
	}
	so, ok := q.Pipes[1].(*SortPipe)
	if !ok || !so.Desc || so.Limit != 5 {
		t.Fatalf("sort pipe wrong: %+v", q.Pipes[1])
	}
	if _, ok := q.Pipes[2].(*FieldsPipe); !ok {
		t.Fatalf("fields pipe wrong: %+v", q.Pipes[2])
	}
}

func statsStore(t *testing.T) *storage.Store {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := []string{"a", "a", "b", "a", "b"} // a:3, b:2
	lat := []string{"10", "20", "30", "60", "40"}
	ts := []int64{1, 2, 3, 4, 5}
	sd := storage.BuildDict(svc)
	ld := storage.BuildDict(lat)
	if _, err := s.AppendGroup(&storage.Group{Rows: 5, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "service", Type: storage.ColDict, Dict: &sd},
		{Name: "latency", Type: storage.ColDict, Dict: &ld},
	}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRunPipelineStats(t *testing.T) {
	s := statsStore(t)
	q, err := ParseLogsQL(`* | stats by (service) count() as c, sum(latency) as s, max(latency) as m`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	rows := RunPipeline(s, q)
	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2", len(rows))
	}
	byS := map[string]Row{}
	for _, r := range rows {
		byS[rowField(r, "service")] = r
	}
	// a: count 3, sum 10+20+60=90, max 60 ; b: count 2, sum 70, max 40
	if c := rowField(byS["a"], "c"); c != "3" {
		t.Fatalf("a count = %q want 3", c)
	}
	if s := rowField(byS["a"], "s"); s != "90" {
		t.Fatalf("a sum = %q want 90", s)
	}
	if m := rowField(byS["a"], "m"); m != "60" {
		t.Fatalf("a max = %q want 60", m)
	}
	if c := rowField(byS["b"], "c"); c != "2" {
		t.Fatalf("b count = %q want 2", c)
	}
}

func TestRunPipelineSortLimit(t *testing.T) {
	s := statsStore(t)
	q, err := ParseLogsQL(`* | stats by (service) count() as c | sort by (c) desc | limit 1`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	rows := RunPipeline(s, q)
	if len(rows) != 1 || rowField(rows[0], "service") != "a" {
		t.Fatalf("sort/limit got %v, want [service=a]", rows)
	}
}

func TestMorePipes(t *testing.T) {
	s := statsStore(t) // service a:3, b:2
	run := func(q string) []Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return RunPipeline(s, pq)
	}
	// top over * needs field materialization; a=3 leads.
	top := run(`* | top 5 (service)`)
	if len(top) != 2 || rowField(top[0], "service") != "a" || rowField(top[0], "count") != "3" {
		t.Fatalf("top = %v", top)
	}
	// uniq by service -> 2 distinct.
	if u := run(`* | uniq by (service)`); len(u) != 2 {
		t.Fatalf("uniq rows = %d want 2", len(u))
	}
	// rename + delete on stats output.
	rd := run(`* | stats by (service) count() as c | rename c as total | delete service`)
	for _, r := range rd {
		if rowField(r, "total") == "" || rowField(r, "service") != "" {
			t.Fatalf("rename/delete wrong: %v", r.Fields)
		}
	}
}
