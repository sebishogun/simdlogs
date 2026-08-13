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
	if len(top) != 2 || rowField(top[0], "service") != "a" || rowField(top[0], "hits") != "3" { // VL names the column hits
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

func TestTransformPipes(t *testing.T) {
	// A store whose _msg carries JSON and a formattable pattern.
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.BuildDict([]string{
		`{"level":"error","code":500}`,
		`{"level":"info","code":200}`,
	})
	lvl := storage.BuildDict([]string{"error", "info"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 2, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
		{Name: "level", Type: storage.ColDict, Dict: &lvl},
	}}); err != nil {
		t.Fatal(err)
	}
	run := func(q string) []Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return RunPipeline(s, pq)
	}

	// unpack_json: _msg's JSON keys become fields.
	uj := run(`* | unpack_json`)
	byLevel := map[string]Row{}
	for _, r := range uj {
		byLevel[rowField(r, "level")] = r
	}
	if rowField(byLevel["error"], "code") != "500" || rowField(byLevel["info"], "code") != "200" {
		t.Fatalf("unpack_json code = %v", uj)
	}

	// format: build a field from a template.
	fm := run(`* | format "<level>=<_msg>" as line`)
	found := false
	for _, r := range fm {
		if rowField(r, "line") == `error={"level":"error","code":500}` {
			found = true
		}
	}
	if !found {
		t.Fatalf("format line not found in %v", fm)
	}

	// extract: pull code out of the JSON text via a literal/capture pattern.
	ex := run(`* | extract "\"code\":<code>}" from _msg`)
	codes := map[string]bool{}
	for _, r := range ex {
		codes[rowField(r, "code")] = true
	}
	if !codes["500"] || !codes["200"] {
		t.Fatalf("extract codes = %v want 500 and 200", codes)
	}

	// math: arithmetic over an unpacked numeric field.
	mt := run(`* | unpack_json | math "(code + 1) / 2" as half`)
	halves := map[string]bool{}
	for _, r := range mt {
		halves[rowField(r, "half")] = true
	}
	if !halves["250.5"] || !halves["100.5"] { // (500+1)/2, (200+1)/2
		t.Fatalf("math half = %v want 250.5 and 100.5", halves)
	}
}

func TestStoreTailCursor(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mk := func(v string) *storage.Group {
		d := storage.BuildDict([]string{v})
		return &storage.Group{Rows: 1, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1}},
			{Name: "service", Type: storage.ColDict, Dict: &d},
		}}
	}
	c := s.TailCursor() // 0 on the empty store
	if _, err := s.AppendGroup(mk("a")); err != nil {
		t.Fatal(err)
	}
	r1, c1 := s.GroupsAfterID(c) // the first-ever group (id 0) must be tailable
	if len(r1) != 1 {
		t.Fatalf("first group not tailed: %d readers", len(r1))
	}
	if _, err := s.AppendGroup(mk("b")); err != nil {
		t.Fatal(err)
	}
	r2, _ := s.GroupsAfterID(c1) // next poll sees only the newly-appended group
	if len(r2) != 1 {
		t.Fatalf("second poll saw %d readers, want 1 (only the new group)", len(r2))
	}
}

func TestCollapseNums(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Three lines, same template once the request id / count are collapsed.
	msg := storage.BuildDict([]string{"req 123 done in 5ms", "req 456 done in 12ms", "req 7 done in 300ms"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 3, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`* | collapse_nums | stats by (_msg) count() as c`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 1 || rowField(rows[0], "c") != "3" {
		t.Fatalf("collapse_nums mining = %v want one template count 3", rows)
	}
	if got := rowField(rows[0], "_msg"); got != "req <N> done in <N>ms" {
		t.Fatalf("collapsed template = %q", got)
	}
}

func TestRankPipe(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.BuildDict([]string{
		"error error timeout in handler", // score 3 (2 error + 1 timeout)
		"ok",                             // score 0
		"error handling request",         // score 1
	})
	if _, err := s.AppendGroup(&storage.Group{Rows: 3, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`* | rank "error timeout"`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 3 {
		t.Fatalf("rank got %d rows", len(rows))
	}
	if rowField(rows[0], "_score") != "3" || rowField(rows[2], "_score") != "0" {
		t.Fatalf("rank order = %s..%s, want 3 first, 0 last", rowField(rows[0], "_score"), rowField(rows[2], "_score"))
	}
}

func TestPatternTemplating(t *testing.T) {
	// pattern also collapses hex/uuid identifiers to <ID>.
	if got := templatize("req deadbeef12345678 done in 5ms", true); got != "req <ID> done in <N>ms" {
		t.Fatalf("pattern full = %q want 'req <ID> done in <N>ms'", got)
	}
	// collapse_nums (Full=false) leaves the hex token, only digits collapse.
	if got := templatize("req deadbeef12345678 done in 5ms", false); got != "req deadbeef<N> done in <N>ms" {
		t.Fatalf("collapse_nums = %q", got)
	}
}

func TestQuantileAgg(t *testing.T) {
	s := statsStore(t) // a: latency 10,20,60 ; b: 30,40
	run := func(q string) map[string]string {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		m := map[string]string{}
		for _, r := range RunPipeline(s, pq) {
			m[rowField(r, "service")] = rowField(r, "p")
		}
		return m
	}
	// Nearest-rank, matching VictoriaLogs: quantile returns a value that is
	// actually in the data. Verified against the real VL binary by the compat
	// differential (internal/bench/compat_test.go), where linear interpolation
	// gave p50=290.5 and VL -- like this -- gives 291. For b's {30,40} that is 40.
	if m := run(`* | stats by (service) quantile(0.5, latency) as p`); m["a"] != "20" || m["b"] != "40" {
		t.Fatalf("median latency = %v want a:20 b:40", m)
	}
	if m := run(`* | stats by (service) quantile(1, latency) as p`); m["a"] != "60" {
		t.Fatalf("p100 latency a = %q want 60", m["a"])
	}
	if m := run(`* | stats by (service) median(latency) as p`); m["a"] != "20" {
		t.Fatalf("median() a = %q want 20", m["a"])
	}
}

func TestFilterPipe(t *testing.T) {
	s := statsStore(t) // service a:3, b:2
	q, err := ParseLogsQL(`* | stats by (service) count() as c | filter c:>2`)
	if err != nil {
		t.Fatal(err)
	}
	q.From, q.To = 0, int64(1)<<62
	rows := RunPipeline(s, q)
	if len(rows) != 1 || rowField(rows[0], "service") != "a" {
		t.Fatalf("filter c:>2 = %v want [service=a]", rows)
	}
}
