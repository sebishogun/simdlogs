package storage

import (
	"fmt"
	"testing"
)

// dictInput builds n values with the given number of distinct strings, in a
// shape like a real column: high card (card==n) is a trace id, low card is a
// level/service. 16-hex-char values match the trace corpus.
func dictInput(n, card int) []string {
	distinct := make([]string, card)
	for i := range distinct {
		distinct[i] = fmt.Sprintf("%016x", i*0x9e3779b1)
	}
	out := make([]string, n)
	for i := range out {
		out[i] = distinct[i%card]
	}
	return out
}

func BenchmarkBuildDict(b *testing.B) {
	for _, c := range []struct {
		name string
		card int
	}{
		{"highcard", 128 * 1024}, // trace: every value distinct
		{"lowcard", 8},           // service
	} {
		vals := dictInput(128*1024, c.card)
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				sinkDict = BuildDict(vals)
			}
		})
	}
}

var sinkDict DictColumn
