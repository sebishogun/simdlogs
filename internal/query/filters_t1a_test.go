package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestT1aFilters covers seq, ipv4_range, and the *_field comparisons.
func TestT1aFilters(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.BuildDict([]string{"start x end", "no match here", "end then start"})
	ip := storage.BuildDict([]string{"10.0.0.5", "192.168.1.1", "10.0.0.200"})
	a := storage.BuildDict([]string{"5", "3", "10"})
	b := storage.BuildDict([]string{"5", "7", "2"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 3, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3}},
		{Name: "_msg", Type: storage.ColDict, Dict: &msg},
		{Name: "ip", Type: storage.ColDict, Dict: &ip},
		{Name: "a", Type: storage.ColDict, Dict: &a},
		{Name: "b", Type: storage.ColDict, Dict: &b},
	}}); err != nil {
		t.Fatal(err)
	}
	count := func(q string) int {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return len(RunPipeline(s, pq))
	}
	cases := []struct {
		q    string
		want int
	}{
		{`_msg:seq("start", "end")`, 1},
		{`ip:ipv4_range("10.0.0.1", "10.0.0.255")`, 2},
		{`a:eq_field(b)`, 1},
		{`a:lt_field(b)`, 1},
		{`a:gt_field(b)`, 1},
		{`a:ne_field(b)`, 2},
		{`a:ge_field(b)`, 2}, // 5>=5, 10>=2
	}
	for _, c := range cases {
		if n := count(c.q); n != c.want {
			t.Errorf("%s = %d rows, want %d", c.q, n, c.want)
		}
	}
}
