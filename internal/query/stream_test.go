package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestStreamSelector(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	app := storage.BuildDict([]string{"nginx", "nginx", "redis", "nginx"})
	env := storage.BuildDict([]string{"prod", "dev", "prod", "prod"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "app", Type: storage.ColDict, Dict: &app},
		{Name: "env", Type: storage.ColDict, Dict: &env},
	}}); err != nil {
		t.Fatal(err)
	}
	run := func(q string) int {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return len(Run(s, pq))
	}
	cases := []struct {
		q    string
		want int
	}{
		{`_stream:{app="nginx"}`, 3},             // equality (lowers to a flat pred)
		{`_stream:{app="nginx",env!="prod"}`, 1}, // AND with a != (NOT) -> the dev row
		{`_stream:{app=~"ng.*"}`, 3},             // regexp label match
		{`_stream:{env!~"pr.*"}`, 1},             // regexp not-match -> only dev
		{`_stream:{}`, 4},                        // empty selector matches all
		{`!app:redis`, 3},                        // bare ! NOT prefix
	}
	for _, c := range cases {
		if got := run(c.q); got != c.want {
			t.Errorf("%s = %d rows, want %d", c.q, got, c.want)
		}
	}
}
