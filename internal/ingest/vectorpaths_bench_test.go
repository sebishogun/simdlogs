package ingest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The no-vector-configured case, which is almost every deployment.
//
// addWithStream asks "does this record carry an embedding" once per record, and
// the answer is almost always no. It is one atomic load rather than the
// writer's mutex, because VectorFields() takes that mutex and the JSON-lines
// parser already hoists the call out of its loop for exactly that reason.
//
// Measured, instructions retired and cycles, perf stat, three interleaved
// rounds, 500-record bodies, minimum:
//
//	without the guard   11,770,399,608 instr   3,028,649,554 cycles
//	with the guard      11,764,759,125         3,005,235,288
//
// The guarded build is 0.05% BELOW on the minimum and the ranges overlap
// completely in both directions -- one atomic load is not visible against a
// logfmt record parse. Reported as no measurable cost rather than as a win.
func BenchmarkLogfmtNoVectorFields(b *testing.B) {
	var body bytes.Buffer
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&body, "_msg=request%d level=info service=api latency=%d\n", i, i)
	}
	data := body.Bytes()
	st, _ := storage.OpenStore(b.TempDir())
	w := NewWriter(st)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IngestLogfmt(w, data, ts1)
	}
}
