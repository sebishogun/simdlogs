package bench

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// frontCode encodes sorted values as shared-prefix-len + suffix-len + suffix
// (uvarints) -- the prefix each value shares with its predecessor is not
// repeated. Lossless, and decodes by memcpy (no match parsing), so it is faster
// than LZ4 while exploiting the sorted prefix sharing LZ4 misses.
func frontCode(vals []string) []byte {
	var out []byte
	prev := ""
	for _, v := range vals {
		p := 0
		for p < len(prev) && p < len(v) && prev[p] == v[p] {
			p++
		}
		out = binary.AppendUvarint(out, uint64(p))
		out = binary.AppendUvarint(out, uint64(len(v)-p))
		out = append(out, v[p:]...)
		prev = v
	}
	return out
}

// frontThenLZ4 front-codes a block then LZ4s the result -- front-coding exposes
// the residual structure for LZ4 to compress further.
func frontThenLZ4(vals []string) int {
	return len(storage.LZ4CompressExported(frontCode(vals)))
}

func blockFC(vals []string, bs int, combo bool) int {
	total := 0
	for i := 0; i < len(vals); i += bs {
		hi := i + bs
		if hi > len(vals) {
			hi = len(vals)
		}
		if combo {
			total += frontThenLZ4(vals[i:hi])
		} else {
			total += len(frontCode(vals[i:hi]))
		}
	}
	return total
}

// TestCodecCeiling measures how much smaller the dictionary sections could get
// under a stronger codec than the hand-rolled 64-value-block LZ4 in use. It
// collects the distinct values of each high-cardinality column from the
// realistic corpus and compresses the concatenation with our LZ4 (whole and
// per-64-block, as stored) vs stdlib flate/gzip -- the achievable footprint
// ceiling with no new dependency.
func TestCodecCeiling(t *testing.T) {
	const n = 200_000
	cols := map[string]map[string]struct{}{}
	order := []string{}
	corpus.GenRealistic(7, n, func(r corpus.RealisticRecord) {
		for _, f := range r.Fields {
			if f.Key == "_time" {
				continue
			}
			if cols[f.Key] == nil {
				cols[f.Key] = map[string]struct{}{}
				order = append(order, f.Key)
			}
			cols[f.Key][f.Value] = struct{}{}
		}
	})

	flateLen := func(b []byte) int {
		var buf bytes.Buffer
		w, _ := flate.NewWriter(&buf, flate.BestCompression)
		w.Write(b)
		w.Close()
		return buf.Len()
	}
	gzipLen := func(b []byte) int {
		var buf bytes.Buffer
		w, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		w.Write(b)
		w.Close()
		return buf.Len()
	}
	// our LZ4 in blocks of `bs` values (the dict section stores 64). Bigger
	// blocks give the matcher a larger window -> better ratio, still random-
	// accessible one block at a time, and decoded by the fast SIMD kernel.
	blockLZ4 := func(vals []string, bs int) int {
		total := 0
		for i := 0; i < len(vals); i += bs {
			hi := i + bs
			if hi > len(vals) {
				hi = len(vals)
			}
			var raw []byte
			for _, v := range vals[i:hi] {
				raw = append(raw, v...)
			}
			total += len(storage.LZ4CompressExported(raw))
		}
		return total
	}

	var traw, t64, tfc, tfclz4, tflate int
	for _, k := range order {
		set := cols[k]
		vals := make([]string, 0, len(set))
		for v := range set {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		var raw []byte
		for _, v := range vals {
			raw = append(raw, v...)
		}
		if len(raw) < 50_000 {
			continue // only the columns that matter for footprint
		}
		b64 := blockLZ4(vals, 64)
		fc := blockFC(vals, 64, false)   // front-coding, lossless + fast decode
		fclz4 := blockFC(vals, 64, true) // front-coding then LZ4
		fl := flateLen(raw)
		_ = gzipLen
		traw += len(raw)
		t64 += b64
		tfc += fc
		tfclz4 += fclz4
		tflate += fl
		t.Logf("%-10s raw %6dKB | lz4 %6dKB (%.2fx) | front %6dKB (%.2fx) | front+lz4 %6dKB (%.2fx) | flate %6dKB (%.2fx)",
			k, len(raw)/1024, b64/1024, cratio(len(raw), b64), fc/1024, cratio(len(raw), fc), fclz4/1024, cratio(len(raw), fclz4), fl/1024, cratio(len(raw), fl))
	}
	t.Logf("TOTAL raw %dKB | lz4 %dKB (%.2fx) | front %dKB (%.2fx) | front+lz4 %dKB (%.2fx) | flate %dKB (%.2fx)",
		traw/1024, t64/1024, cratio(traw, t64), tfc/1024, cratio(traw, tfc), tfclz4/1024, cratio(traw, tfclz4), tflate/1024, cratio(traw, tflate))
	t.Logf("front vs lz4: %.2fx smaller | front+lz4 vs lz4: %.2fx smaller", cratio(t64, tfc), cratio(t64, tfclz4))
}

func cratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
