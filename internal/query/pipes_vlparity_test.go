package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestVLParityPipes(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.BuildDict([]string{"ip=10.0.0.1 status=500 \x1b[31mFAIL\x1b[0m"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 1, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
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
			t.Fatalf("%q: got %d rows", q, len(rows))
		}
		return rows[0]
	}

	// extract_regexp: named groups become fields.
	r := run(`* | extract_regexp "ip=(?P<ip>[0-9.]+) status=(?P<code>[0-9]+)" from _msg`)
	if rowField(r, "ip") != "10.0.0.1" || rowField(r, "code") != "500" {
		t.Errorf("extract_regexp: ip=%q code=%q", rowField(r, "ip"), rowField(r, "code"))
	}
	// decolorize: strip the ANSI codes from _msg.
	if got := rowField(run(`* | decolorize`), "_msg"); got != "ip=10.0.0.1 status=500 FAIL" {
		t.Errorf("decolorize = %q", got)
	}
	// pack_json of selected fields (after extract).
	if got := rowField(run(`* | extract_regexp "status=(?P<code>[0-9]+)" | pack_json fields (code) as j`), "j"); got != `{"code":"500"}` {
		t.Errorf("pack_json = %q", got)
	}
	// pack_logfmt of selected fields.
	if got := rowField(run(`* | extract_regexp "status=(?P<code>[0-9]+)" | pack_logfmt fields (code) as j`), "j"); got != `code=500` {
		t.Errorf("pack_logfmt = %q", got)
	}
	// bad regexp is a parse error, not a panic.
	if _, err := ParseLogsQL(`* | extract_regexp "(?P<x>"`); err == nil {
		t.Error("extract_regexp: expected parse error on bad regex")
	}
}

func TestSamplePipe(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]string, 10)
	ts := make([]int64, 10)
	for i := range vals {
		vals[i] = "x"
		ts[i] = int64(i + 1)
	}
	d := storage.BuildDict(vals)
	if _, err := s.AppendGroup(&storage.Group{Rows: 10, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
		{Name: "f", Type: storage.ColDict, Dict: &d},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`* | sample 3`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	if rows := RunPipeline(s, pq); len(rows) != 4 { // indices 0,3,6,9
		t.Errorf("sample 3 of 10: got %d rows, want 4", len(rows))
	}
}
