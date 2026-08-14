package bench

import (
	"sort"
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestFootprintBreakdown reports where a realistic group's bytes go, per
// column and section (index / postings / dict / bloom / timestamps), so the
// footprint work targets the biggest chunk rather than guessing.
func TestFootprintBreakdown(t *testing.T) {
	t.Parallel()
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
	g := &storage.Group{Rows: n, Columns: gcols}
	hot := g.Marshal()
	// What the tiering pass (-recompact-after) achieves on this corpus: the same
	// group re-encoded with flate dictionaries. Hex-coded columns (trace_id,
	// span_id) already use the hex codec and do not change.
	g.Compact = true
	cold := g.Marshal()
	g.NoPostings = true
	coldNP := g.Marshal()
	g.Compact, g.NoPostings = false, false
	t.Logf("TIERING hot %dKB -> cold(flate) %dKB (%.1f%%) -> cold(flate,no-postings) %dKB (%.1f%%)",
		len(hot)/1024, len(cold)/1024, 100*(1-float64(len(cold))/float64(len(hot))),
		len(coldNP)/1024, 100*(1-float64(len(coldNP))/float64(len(hot))))
	r, err := storage.ReadGroup(hot)
	if err != nil {
		t.Fatal(err)
	}
	cbs := r.ColumnBytes()
	sort.Slice(cbs, func(i, j int) bool {
		return total(cbs[i]) > total(cbs[j])
	})
	var idx, post, dict, bloom, tc int
	for _, c := range cbs {
		idx += c.Index
		post += c.Postings
		dict += c.Dict
		bloom += c.Bloom
		tc += c.TimeCol
		t.Logf("%-11s total %6dKB | index %5dKB post %5dKB dict %5dKB bloom %4dKB",
			c.Name, total(c)/1024, c.Index/1024, c.Postings/1024, c.Dict/1024, c.Bloom/1024)
	}
	blob := g.Marshal()
	t.Logf("TOTALS %dKB | index %dKB postings %dKB dict %dKB bloom %dKB time %dKB | %d rows, %d bytes/row",
		len(blob)/1024, idx/1024, post/1024, dict/1024, bloom/1024, tc/1024, n, len(blob)/n)
}

func total(c storage.ColumnFootprint) int {
	return c.Index + c.Postings + c.Dict + c.Bloom + c.TimeCol
}
