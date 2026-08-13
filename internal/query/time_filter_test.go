package query

import (
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestTimeFilters(t *testing.T) {
	mk := func(y, mo, d, h int) int64 {
		return time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC).UnixNano()
	}
	t0 := mk(2024, 1, 15, 10) // Mon
	t1 := mk(2024, 1, 15, 14)
	t2 := mk(2024, 1, 16, 10) // Tue
	t3 := mk(2024, 1, 20, 10) // Sat
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tag := storage.BuildDict([]string{"a", "b", "c", "d"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{t0, t1, t2, t3}},
		{Name: "tag", Type: storage.ColDict, Dict: &tag},
	}}); err != nil {
		t.Fatal(err)
	}
	count := func(q string, now int64) int {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To, pq.Now = 0, int64(1)<<62, now
		return len(RunPipeline(s, pq))
	}
	cases := []struct {
		q    string
		now  int64
		want int
	}{
		{`_time:2024-01-15`, 0, 2},               // that day: t0,t1
		{`_time:[2024-01-15, 2024-01-16]`, 0, 3}, // inclusive Jan16 day: +t2
		{`_time:>=2024-01-16`, 0, 2},             // t2,t3
		{`_time:<2024-01-16`, 0, 2},              // t0,t1
		{`_time:day_range[09:00, 11:00]`, 0, 3},  // 10:00 rows: t0,t2,t3
		{`_time:week_range[Mon, Fri]`, 0, 3},     // t0,t1,t2 weekdays; t3 Sat out
		{`_time:5m`, t3 + int64(time.Minute), 1}, // last 5m ending now: t3
		{`_time:2024-01-15 tag:a`, 0, 1},         // combines with a field filter
	}
	for _, c := range cases {
		if n := count(c.q, c.now); n != c.want {
			t.Errorf("%s = %d rows, want %d", c.q, n, c.want)
		}
	}
}
