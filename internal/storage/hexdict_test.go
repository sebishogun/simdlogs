package storage

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
)

// TestHexDictOpportunity measures the disk headroom of a ClickHouse-style
// specialized codec: high-cardinality hex columns (trace_id, span_id) carry 4
// bits of entropy per char in an 8-bit byte, so LZ4 (no entropy coding) barely
// dents them. Nibble-packing halves them losslessly and decodes fast. This is a
// measurement to size the win, not a format change.
func TestHexDictOpportunity(t *testing.T) {
	t.Parallel()
	const n = 100_000
	cols := map[string][]string{}
	corpus.GenRealistic(7, n, func(r corpus.RealisticRecord) {
		for _, f := range r.Fields {
			cols[f.Key] = append(cols[f.Key], f.Value)
		}
	})
	for _, name := range []string{"trace_id", "span_id", "_msg", "path"} {
		vals := cols[name]
		d := BuildDict(vals)
		distinct := d.Dict
		// current on-disk dict bytes (LZ4 of the concatenated dict blocks).
		raw := marshalDictBlob(distinct)
		lz4 := len(lz4Compress(raw))
		// nibble-packed size for a pure-hex column: 4 bits/char + a length.
		hexBytes, allHex := 0, true
		for _, s := range distinct {
			if !isHex(s) {
				allHex = false
				break
			}
			hexBytes += (len(s) + 1) / 2
		}
		flate := len(flateCompress(raw))
		if allHex {
			t.Logf("%-9s distinct=%-7d rawDict=%6dKB lz4=%6dKB flate=%6dKB nibble=%6dKB  (nibble %.2fx vs lz4)",
				name, len(distinct), len(raw)/1024, lz4/1024, flate/1024, hexBytes/1024, float64(lz4)/float64(hexBytes))
		} else {
			t.Logf("%-9s distinct=%-7d rawDict=%6dKB lz4=%6dKB flate=%6dKB  (not hex; entropy=flate %.2fx vs lz4)",
				name, len(distinct), len(raw)/1024, lz4/1024, flate/1024, float64(lz4)/float64(flate))
		}
	}
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
