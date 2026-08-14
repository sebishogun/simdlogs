package query

import (
	"strconv"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The A/B for sizing runParallel's merge up front. Both arms are in this one
// binary and run interleaved in one session, because a two-build comparison
// would put the 8.3% code-layout noise floor between them.
//
// The store is built here rather than borrowed from the other benchmark files
// so this file stands alone.

func mergeStore(t testing.TB, groups, gsize int) (*storage.Store, int64, int64) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svcs := []string{"auth", "api", "db", "cache", "web"}
	ts := make([]int64, 0, gsize)
	sv := make([]string, 0, gsize)
	msg := make([]string, 0, gsize)
	var lo, hi int64
	now := int64(1_700_000_000_000_000_000)
	for g := 0; g < groups; g++ {
		ts, sv, msg = ts[:0], sv[:0], msg[:0]
		for i := 0; i < gsize; i++ {
			now += 1000
			if lo == 0 {
				lo = now
			}
			hi = now
			ts = append(ts, now)
			sv = append(sv, svcs[(g*gsize+i)%len(svcs)])
			msg = append(msg, "request handled id="+strconv.Itoa((g*gsize+i)%9973))
		}
		sd := storage.BuildDict(sv)
		md := storage.BuildDict(msg)
		if _, err := s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
			{Name: "service", Type: storage.ColDict, Dict: &sd},
			{Name: "_msg", Type: storage.ColDict, Dict: &md},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s, lo, hi
}

// TestMergePresizeMatchesAppend asserts the presized merge returns exactly what
// growing by append returned, row for row -- including the empty case, where
// the serial path answers nil and an empty non-nil slice would encode as [] on
// the wire instead of null.
func TestMergePresizeMatchesAppend(t *testing.T) {
	s, lo, hi := mergeStore(t, 6, 20_000)
	defer func() { mergePresize = true }()
	for _, q := range []*Query{
		{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}},
		{From: lo, To: hi + 1},
		{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "nosuchservice"}}},
		{From: hi + 1000, To: hi + 2000},
	} {
		mergePresize = false
		want := Run(s, q)
		mergePresize = true
		got := Run(s, q)
		if (got == nil) != (want == nil) {
			t.Fatalf("nil-ness differs: presized nil=%v, append nil=%v", got == nil, want == nil)
		}
		if len(got) != len(want) {
			t.Fatalf("presized %d rows, append %d rows", len(got), len(want))
		}
		for i := range want {
			if got[i].Time != want[i].Time || got[i].NoTime != want[i].NoTime || len(got[i].Fields) != len(want[i].Fields) {
				t.Fatalf("row %d: presized %+v, append %+v", i, got[i], want[i])
			}
			for j := range want[i].Fields {
				if got[i].Fields[j] != want[i].Fields[j] {
					t.Fatalf("row %d field %d: presized %+v, append %+v", i, j, got[i].Fields[j], want[i].Fields[j])
				}
			}
		}
	}
}

func BenchmarkMergePresize(b *testing.B) {
	s, lo, hi := mergeStore(b, 10, 100_000)
	shapes := []struct {
		name string
		q    *Query
	}{
		{"full-scan", &Query{From: lo, To: hi + 1, Preds: []Pred{{Field: "service", Kind: Eq, Value: "auth"}}}},
		{"all-rows", &Query{From: lo, To: hi + 1}},
	}
	// Arms interleaved per shape, so any drift over the run hits both.
	for _, sh := range shapes {
		for _, arm := range []struct {
			name string
			pre  bool
		}{{"presize", true}, {"append", false}} {
			b.Run(sh.name+"/"+arm.name, func(b *testing.B) {
				mergePresize = arm.pre
				defer func() { mergePresize = true }()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					sinkRows = Run(s, sh.q)
				}
			})
		}
	}
}
