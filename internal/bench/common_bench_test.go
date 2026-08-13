package bench

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// BenchmarkCommonSelect mirrors the realistic `common` query: an equality on a
// low-cardinality field (level:=error) with MatAll -- a big, whole-record
// result set. This is the materialize path, which is where the 1.0x-vs-VL time
// goes, not the filter.
func BenchmarkCommonSelect(b *testing.B) {
	const n = 100_000
	ts := make([]int64, 0, n)
	cols := map[string][]string{}
	var order []string
	corpus.GenRealistic(7, n, func(r corpus.RealisticRecord) {
		ts = append(ts, r.Time.UnixNano())
		for _, f := range r.Fields {
			if f.Key == "_time" {
				continue
			}
			if _, ok := cols[f.Key]; !ok {
				order = append(order, f.Key)
			}
			cols[f.Key] = append(cols[f.Key], f.Value)
		}
	})
	gcols := []storage.Column{{Name: "_time", Type: storage.ColTimestamp, Ts: ts}}
	for _, k := range order {
		d := storage.BuildDict(cols[k])
		gcols = append(gcols, storage.Column{Name: k, Type: storage.ColDict, Dict: &d})
	}
	s, _ := storage.OpenStore(b.TempDir())
	s.AppendGroup(&storage.Group{Rows: n, Columns: gcols})

	q := &query.Query{From: 0, To: int64(1) << 62, MatAll: true,
		Preds: []query.Pred{{Field: "level", Value: "error", Kind: query.Eq}}}
	b.ResetTimer()
	var sink int
	for b.Loop() {
		sink = len(query.RunPipeline(s, q))
	}
	_ = sink
}
