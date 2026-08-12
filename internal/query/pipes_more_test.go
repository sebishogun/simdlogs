package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestMorePipesCoverage(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.BuildDict([]string{`level=error code=500 msg="boom"`})
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

	// unpack_logfmt splits key=value into fields.
	r := run(`* | unpack_logfmt`)
	if rowField(r, "level") != "error" || rowField(r, "code") != "500" || rowField(r, "msg") != "boom" {
		t.Fatalf("unpack_logfmt = %v", r.Fields)
	}
	// replace edits _msg in place.
	if got := rowField(run(`* | replace ("error", "ERR")`), "_msg"); got != `level=ERR code=500 msg="boom"` {
		t.Fatalf("replace = %q", got)
	}
	// replace_regexp with a template.
	if got := rowField(run(`* | replace_regexp ("code=[0-9]+", "code=NNN")`), "_msg"); got != `level=error code=NNN msg="boom"` {
		t.Fatalf("replace_regexp = %q", got)
	}
	// copy duplicates a field (after unpack).
	if got := rowField(run(`* | unpack_logfmt | copy level as lvl`), "lvl"); got != "error" {
		t.Fatalf("copy lvl = %q", got)
	}
	// len of _msg.
	if got := rowField(run(`* | len(_msg) as n`), "n"); got != "31" {
		t.Fatalf("len(_msg) = %q want 31", got)
	}
}
