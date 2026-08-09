package query

import (
	"sort"
	"strconv"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// VectorSearch returns the k rows whose `field` embedding is most cosine-similar
// to q across the window, each with a _score field -- semantic / vector log
// search, which VictoriaLogs does not offer. Brute-force k-NN (exact; an ANN
// index is a future optimization). Embeddings are bring-your-own: logs carry a
// vector column and this ranks them.
func VectorSearch(s Store, from, to int64, field string, q []float32, k int) []Row {
	if k <= 0 {
		k = 10
	}
	qn := storage.Norm(q)
	type cand struct {
		score float64
		g     *storage.Reader
		row   int
	}
	var cands []cand
	for _, g := range s.Groups(from, to) {
		dim, data := g.Vectors(field)
		if dim == 0 || dim != len(q) {
			continue
		}
		n := len(data) / dim
		for i := 0; i < n; i++ {
			cands = append(cands, cand{storage.Cosine(q, data[i*dim:(i+1)*dim], qn), g, i})
		}
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].score > cands[b].score })
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]Row, 0, len(cands))
	for _, c := range cands {
		t, _ := c.g.TimestampAt("_time", c.row)
		row := Row{Time: t, Fields: []Field{{"_score", strconv.FormatFloat(c.score, 'f', 4, 64)}}}
		for _, f := range c.g.ColumnNames() {
			if f == "_time" || f == field {
				continue
			}
			if v, ok := c.g.DictValueAt(f, c.row); ok {
				row.Fields = append(row.Fields, Field{f, v})
			}
		}
		out = append(out, row)
	}
	return out
}
