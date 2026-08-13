package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestStatsQueryRange(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lvl := storage.BuildDict([]string{"error", "error", "error", "info"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{0, 1e9, 2e9, 3e9}},
		{Name: "level", Type: storage.ColDict, Dict: &lvl},
	}}); err != nil {
		t.Fatal(err)
	}
	series, err := StatsQueryRange(s, `* | stats by (level) count() c`, 0, 4e9, 2e9, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][][2]string{}
	for _, se := range series {
		got[se.Metric["level"]] = se.Values
	}
	if v := got["error"]; len(v) != 2 || v[0] != [2]string{"0", "2"} || v[1] != [2]string{"2", "1"} {
		t.Errorf("error series = %v want [[0 2] [2 1]]", v)
	}
	if v := got["info"]; len(v) != 1 || v[0] != [2]string{"2", "1"} {
		t.Errorf("info series = %v want [[2 1]]", v)
	}
}
