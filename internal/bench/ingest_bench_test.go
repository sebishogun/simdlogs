package bench

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// BenchmarkIngestParallel measures the NDJSON ingest path on a realistic body,
// through the same parallel entry point the HTTP handler uses.
func BenchmarkIngestParallel(b *testing.B) {
	body := clusterCorpusB(200_000)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := storage.OpenStore(b.TempDir())
		ingest.IngestJSONLinesParallel(s, body, func() int64 { return 0 }, false)
		s.Close()
	}
}
