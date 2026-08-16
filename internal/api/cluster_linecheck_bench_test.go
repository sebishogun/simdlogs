package api

import (
	"strings"
	"testing"
)

// looksLikeJSONObject runs on every line of every shard body on the
// bare-select path, which is the path that exists to avoid parsing. It is the
// one structural walk a clustered read cannot skip, so it gets a benchmark.
//
// Measured 2026-08-16, load average 3-5, minimum of five runs of 500,
// interleaved in one session against a byte-at-a-time copy of the same walk:
//
//	                       narrow (~90 B)   wide (~980 B)
//	byte at a time              51,744 ns      425,872 ns
//	shipped                     53,184 ns      434,764 ns
//
// 2.8% and 2.1%, both inside this repository's 8.3% noise floor. Two earlier
// shapes were measured and rejected; see docs/wrong.md.
func benchLineCheck(b *testing.B, width int) {
	pad := strings.Repeat("v", width)
	lines := make([][]byte, 1000)
	for i := range lines {
		lines[i] = []byte(`{"_time":"2026-06-01T12:00:00Z","_msg":"` + pad +
			`","level":"info","ctx":{"a":1,"b":"` + pad + `"}}`)
	}
	b.SetBytes(int64(len(lines) * len(lines[0])))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, l := range lines {
			if !looksLikeJSONObject(l) {
				b.Fatal("rejected a valid line")
			}
		}
	}
}

func BenchmarkLineCheckNarrow(b *testing.B) { benchLineCheck(b, 20) }
func BenchmarkLineCheckWide(b *testing.B)   { benchLineCheck(b, 450) }
