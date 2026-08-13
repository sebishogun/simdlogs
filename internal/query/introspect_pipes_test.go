package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestIntrospectPipes(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lvl := storage.BuildDict([]string{"error", "info", "error", "error"})
	svc := storage.BuildDict([]string{"a", "a", "b", "b"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "level", Type: storage.ColDict, Dict: &lvl},
		{Name: "service", Type: storage.ColDict, Dict: &svc},
	}}); err != nil {
		t.Fatal(err)
	}
	rows := func(q string) []Row {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return RunPipeline(s, pq)
	}
	// field_values level -> error:3, info:1
	fv := rows(`* | field_values level`)
	if len(fv) != 2 || rowField(fv[0], "value") != "error" || rowField(fv[0], "hits") != "3" {
		t.Errorf("field_values level = %v", fv)
	}
	// field_names -> level, service (and hits)
	names := map[string]bool{}
	for _, r := range rows(`* | field_names`) {
		names[rowField(r, "name")] = true
	}
	if !names["level"] || !names["service"] {
		t.Errorf("field_names = %v", names)
	}
	// facets -> rows with field/value/hits
	fac := rows(`* | facets`)
	if len(fac) == 0 || rowField(fac[0], "field") == "" {
		t.Errorf("facets empty or malformed: %v", fac)
	}
}
