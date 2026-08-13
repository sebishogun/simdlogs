package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestBlockPipes(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 3; b++ { // 3 groups
		d := storage.BuildDict([]string{"x", "y"})
		if _, err := s.AppendGroup(&storage.Group{Rows: 2, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{int64(b*10 + 1), int64(b*10 + 2)}},
			{Name: "f", Type: storage.ColDict, Dict: &d},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	rows := func(q string) []Row {
		pq, _ := ParseLogsQL(q)
		pq.From, pq.To = 0, int64(1)<<62
		return RunPipeline(s, pq)
	}
	if r := rows(`* | blocks_count`); len(r) != 1 || rowField(r[0], "blocks_count") != "3" {
		t.Errorf("blocks_count = %v want 3", r)
	}
	bs := rows(`* | block_stats`)
	if len(bs) != 3 || rowField(bs[0], "rows") != "2" || rowField(bs[0], "columns") != "2" {
		t.Errorf("block_stats = %v", bs)
	}
}
