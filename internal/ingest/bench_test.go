package ingest

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

func ndjson(n int) []byte {
	var b bytes.Buffer
	corpus.Gen(42, n, func(r corpus.Record) {
		fmt.Fprintf(&b, `{"_time":%q,"level":%q,"service":%q,"trace":%q,"_msg":%q}`+"\n",
			r.Time.UTC().Format(time.RFC3339Nano), r.Level, r.Service, r.TraceID, r.Message)
	})
	return b.Bytes()
}

// BenchmarkIngest measures the full parse+intern+flush path (no HTTP): the
// number the head-to-head's ingest row reflects.
func BenchmarkIngest(b *testing.B) {
	nd := ndjson(500_000)
	var mono int64
	fallback := func() int64 { mono++; return mono }
	b.SetBytes(int64(len(nd)))
	b.ResetTimer()
	for b.Loop() {
		s, _ := storage.OpenStore(b.TempDir())
		w := NewWriter(s)
		IngestJSONLines(w, nd, fallback)
		w.Close()
	}
}

// BenchmarkIngestParallel is the sharded parse path the server takes for a
// large body -- the parser was the bottleneck once flushing went async.
func BenchmarkIngestParallel(b *testing.B) {
	nd := ndjson(500_000)
	var mono int64
	fallback := func() int64 { mono++; return mono }
	b.SetBytes(int64(len(nd)))
	b.ResetTimer()
	for b.Loop() {
		s, _ := storage.OpenStore(b.TempDir())
		IngestJSONLinesParallel(s, nd, fallback)
	}
}
