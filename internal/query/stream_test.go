package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestStreamModel(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// two streams: {app="api",env="prod"} x3, {app="db",env="prod"} x1
	sa := `{app="api",env="prod"}`
	sb := `{app="db",env="prod"}`
	str := storage.BuildDict([]string{sa, sa, sa, sb})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "_stream", Type: storage.ColDict, Dict: &str},
	}}); err != nil {
		t.Fatal(err)
	}
	streams := Streams(s, 0, int64(1)<<62)
	if len(streams) != 2 {
		t.Fatalf("streams = %d want 2", len(streams))
	}
	ids := StreamIDs(s, 0, int64(1)<<62)
	if len(ids) != 2 || ids[0].Value == "" {
		t.Errorf("stream_ids = %v", ids)
	}
	names := StreamFieldNames(s, 0, int64(1)<<62)
	if len(names) != 2 || names[0] != "app" || names[1] != "env" {
		t.Errorf("stream_field_names = %v want [app env]", names)
	}
	if apps := StreamFieldValues(s, "app", 0, int64(1)<<62); len(apps) != 2 {
		t.Errorf("stream_field_values app = %v want 2", apps)
	}
	// _stream_id filter: match rows of stream sa by its id.
	pq, _ := ParseLogsQL(`_stream_id:` + StreamID(sa))
	pq.From, pq.To = 0, int64(1)<<62
	if n := len(RunPipeline(s, pq)); n != 3 {
		t.Errorf("_stream_id filter = %d rows want 3", n)
	}
}
