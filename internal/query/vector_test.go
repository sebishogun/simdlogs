package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestVectorSearch(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Three 2-D embeddings; query is closest to the second row.
	svc := storage.BuildDict([]string{"a", "b", "c"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 3, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3}},
		{Name: "service", Type: storage.ColDict, Dict: &svc},
		{Name: "emb", Type: storage.ColVector, Dim: 2, Vec: []float32{
			1, 0, // a
			0, 1, // b
			-1, 0, // c
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	rows := VectorSearch(s, 0, int64(1)<<62, "emb", []float32{0, 1}, 2, nil)
	if len(rows) != 2 {
		t.Fatalf("knn returned %d rows, want 2", len(rows))
	}
	// Nearest is b (identical direction), score ~1.
	if rowField(rows[0], "service") != "b" {
		t.Fatalf("nearest = %s, want b", rowField(rows[0], "service"))
	}
	if rowField(rows[0], "_score") != "1.0000" {
		t.Fatalf("nearest score = %s, want 1.0000", rowField(rows[0], "_score"))
	}
}
