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
		IngestJSONLinesParallel(s, nd, fallback, false)
	}
}

// BenchmarkEachFieldVarint measures the wire-type-0 walk's allocation count.
// The old shape re-encoded every varint field's value into an escaping [8]byte
// buffer -- one allocation per varint field on the default OTLP and Loki
// encodings (severity_number and the dropped counts; Timestamp seconds and
// nanos). The regression test TestEachFieldVarintZeroAlloc gates the count;
// this reports the same number as allocs/op for -benchmem runs.
func BenchmarkEachFieldVarint(b *testing.B) {
	var msg []byte
	for i := 0; i < 64; i++ {
		msg = pvarint(msg, 1, uint64(i+1))
	}
	fn := func(num, wire int, payload []byte) {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eachField(msg, fn)
	}
}

// BenchmarkJournalMicros is the number behind the byte scan's justification.
//
// The doc comment on journalMicros first claimed the scan was there because
// `strconv.ParseInt(string(val), ...)` allocates a string per entry; it does
// not, and the comment was corrected to say what the scan actually buys. The
// correction then claimed "both forms benchmark at 0 B/op, 0 allocs/op", which
// is true on the SUCCESS path ONLY -- and the out-of-range path is the case
// the function was rewritten for. Interleaved, one session, minimum of three:
//
//	input          ParseInt          scan + fmt.Errorf   scan + tsRangeErr
//	in-range       16.77 ns  0/0     12.51 ns  0/0       13.07 ns  0/0
//	out of range   53.09 ns 72B/2   146.0  ns 200B/3      38.44 ns 48B/2
//	non-numeric    37.15 ns 64B/2     1.565 ns  0/0        1.736 ns  0/0
//
// 3 allocations and 200 bytes against ParseInt's 2 and 72, all of it in
// `fmt.Errorf` with `%w` and `%s` over the bytes. tsRangeErr formats in
// Error() instead, so the message is built where it is read.
func BenchmarkJournalMicros(b *testing.B) {
	for _, tc := range []struct{ name, in string }{
		{"in-range", "1714521600000000"},
		{"out-of-range", "9223372036854775808"},
		{"non-numeric", "not-a-number"},
		{"signed", "-1714521600000000"},
	} {
		in := []byte(tc.in)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ns, ok, err := journalMicros(in)
				sinkNs, sinkOK, sinkErr = ns, ok, err
			}
		})
	}
}

var (
	sinkNs  int64
	sinkOK  bool
	sinkErr error
)
