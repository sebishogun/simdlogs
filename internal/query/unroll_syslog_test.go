package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestUnrollPipe(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tags := storage.BuildDict([]string{`["x","y","z"]`})
	if _, err := s.AppendGroup(&storage.Group{Rows: 1, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1}},
		{Name: "tags", Type: storage.ColDict, Dict: &tags},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`* | unroll (tags)`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 3 {
		t.Fatalf("unroll: got %d rows, want 3", len(rows))
	}
	got := []string{rowField(rows[0], "tags"), rowField(rows[1], "tags"), rowField(rows[2], "tags")}
	for i, want := range []string{"x", "y", "z"} {
		if got[i] != want {
			t.Errorf("unroll row %d tags = %q want %q", i, got[i], want)
		}
	}
}

func TestUnpackSyslog(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// row 0: RFC5424, row 1: RFC3164.
	msg := storage.BuildDict([]string{
		`<165>1 2003-10-11T22:14:15.003Z myhost evntslog 1234 ID47 - hello world`,
		`<34>Oct 11 22:14:15 mymachine su: auth failure`,
	})
	if _, err := s.AppendGroup(&storage.Group{Rows: 2, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
	}}); err != nil {
		t.Fatal(err)
	}
	pq, err := ParseLogsQL(`* | unpack_syslog | sort by (_time)`)
	if err != nil {
		t.Fatal(err)
	}
	pq.From, pq.To = 0, int64(1)<<62
	rows := RunPipeline(s, pq)
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	// RFC5424 row
	r := rows[0]
	for k, want := range map[string]string{
		"priority": "165", "facility": "20", "severity": "5",
		"hostname": "myhost", "app_name": "evntslog", "proc_id": "1234",
		"msg_id": "ID47", "message": "hello world",
	} {
		if got := rowField(r, k); got != want {
			t.Errorf("rfc5424 %s = %q want %q", k, got, want)
		}
	}
	// RFC3164 row
	r = rows[1]
	for k, want := range map[string]string{
		"priority": "34", "facility": "4", "severity": "2",
		"hostname": "mymachine", "app_name": "su", "message": "auth failure",
	} {
		if got := rowField(r, k); got != want {
			t.Errorf("rfc3164 %s = %q want %q", k, got, want)
		}
	}
}
