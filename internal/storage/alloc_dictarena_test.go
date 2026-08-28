package storage

import (
	"encoding/binary"
	"strconv"
	"testing"
)

// The A/B for materializing a dict block through one shared string instead of
// one string per value. Both arms are in this one binary and run interleaved
// in one session: a two-build comparison would put the 8.3% code-layout noise
// floor between them. allocs/op and B/op are exact and load-independent; the
// ns/op is the weaker half.

func arenaBenchDicts() []struct {
	name string
	sec  []byte
	n    int
} {
	lowcard := make([]string, 1024) // the `top N by (host)` shape
	for i := range lowcard {
		lowcard[i] = "node-" + strconv.Itoa(i)
	}
	highcard := make([]string, 65536) // a trace-id column
	seed := uint32(7)
	for i := range highcard {
		b := make([]byte, 16)
		for j := range b {
			seed = seed*1664525 + 1013904223
			// The HIGH nibble: an LCG's low four bits have period 16, so
			// seed&0xf generated two distinct values for the whole column
			// and this shape measured a two-entry dictionary.
			b[j] = hexChar(byte(seed >> 28))
		}
		highcard[i] = string(b)
	}
	long := make([]string, 512) // long values: the arena copy is the same bytes
	for i := range long {
		long[i] = "a-considerably-longer-dictionary-value-" + strconv.Itoa(i) + "-with-tail-padding-to-make-it-wide"
	}
	mk := func(v []string) ([]byte, int) {
		d := BuildDict(v).Dict
		return marshalDictSection(d, false), len(d)
	}
	lsec, ln := mk(lowcard)
	hsec, hn := mk(highcard)
	gsec, gn := mk(long)
	return []struct {
		name string
		sec  []byte
		n    int
	}{
		{"lowcard-1k", lsec, ln},
		{"highcard-64k", hsec, hn},
		{"long-512", gsec, gn},
	}
}

// TestDictArenaMatchesPerValue is the correctness gate on the shared-arena
// path: every value it yields must equal, byte for byte, what the per-value
// blockValAt path yields -- for well-formed sections and for corrupt ones,
// where "" is the contract. A shared arena that mis-slices returns a
// NEIGHBOURING value rather than a wrong-looking one, which is why the
// assertion is exact equality against the reference arm and not a spot check.
func TestDictArenaMatchesPerValue(t *testing.T) {
	shapes := arenaBenchDicts()
	// Values that stress the offset table: empty, single byte, spanning a
	// block boundary at 64, and a non-hex value forcing the LZ4 arm.
	var mixed []string
	for i := 0; i < 200; i++ {
		mixed = append(mixed, strconv.Itoa(i))
	}
	mixed = append(mixed, "", "z", "not-hex-value!")
	md := BuildDict(mixed).Dict
	shapes = append(shapes, struct {
		name string
		sec  []byte
		n    int
	}{"mixed", marshalDictSection(md, false), len(md)})
	shapes = append(shapes, struct {
		name string
		sec  []byte
		n    int
	}{"mixed-compact", marshalDictSection(md, true), len(md)})

	defer func() { dictArena = true }()
	for _, s := range shapes {
		for _, n := range []int{s.n, s.n - 1, s.n + 7, 0} {
			if n < 0 {
				continue
			}
			dictArena = false
			want := dictSectionAll(s.sec, n)
			dictArena = true
			got := dictSectionAll(s.sec, n)
			if len(got) != len(want) {
				t.Fatalf("%s n=%d: arena len %d, per-value len %d", s.name, n, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("%s n=%d: value %d: arena %q, per-value %q", s.name, n, i, got[i], want[i])
				}
			}
		}
	}

	// Corrupt sections: a truncated block must answer identically on both
	// arms rather than panic or return a neighbour's bytes.
	for _, cut := range []int{1, 3, 8, 17, 64, 129} {
		sec := marshalDictSection(md, false)
		if cut >= len(sec) {
			continue
		}
		trunc := sec[:len(sec)-cut]
		dictArena = false
		want := dictSectionAll(trunc, len(md))
		dictArena = true
		got := dictSectionAll(trunc, len(md))
		if len(got) != len(want) {
			t.Fatalf("truncated -%d: arena len %d, per-value len %d", cut, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("truncated -%d: value %d: arena %q, per-value %q", cut, i, got[i], want[i])
			}
		}
	}
}

// TestAppendBlockValsCorruptBlock drives appendBlockVals directly against
// blockValAt on raw blocks whose offset table is inconsistent -- the paths the
// section-level test cannot reach because marshalDictSection never writes them.
func TestAppendBlockValsCorruptBlock(t *testing.T) {
	blocks := [][]byte{
		nil,
		{},
		{1, 2, 3},
		make([]byte, 4),
		make([]byte, 12),
		append(make([]byte, 12), 'a', 'b', 'c'),
	}
	// A block claiming an offset past its own end.
	bad := make([]byte, 12)
	binary.LittleEndian.PutUint32(bad[8:], 1<<20)
	blocks = append(blocks, bad)
	// A block whose offsets run backwards.
	rev := make([]byte, 16)
	binary.LittleEndian.PutUint32(rev[0:], 9)
	binary.LittleEndian.PutUint32(rev[4:], 2)
	blocks = append(blocks, rev)

	for bi, raw := range blocks {
		for _, cnt := range []int{-1, 0, 1, 2, 3, 64} {
			var want []string
			for i := 0; i < cnt; i++ {
				want = append(want, blockValAt(raw, cnt, i))
			}
			got := appendBlockVals(nil, raw, cnt)
			if len(got) != len(want) {
				t.Fatalf("block %d cnt=%d: arena len %d want %d", bi, cnt, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("block %d cnt=%d: value %d: arena %q want %q", bi, cnt, i, got[i], want[i])
				}
			}
		}
	}
}

var sinkStrs []string

func BenchmarkDictSectionAllArena(b *testing.B) {
	shapes := arenaBenchDicts()
	// Arms interleaved per shape, so any drift over the run hits both.
	for _, s := range shapes {
		for _, arm := range []struct {
			name  string
			arena bool
		}{{"arena", true}, {"pervalue", false}} {
			b.Run(s.name+"/"+arm.name, func(b *testing.B) {
				dictArena = arm.arena
				defer func() { dictArena = true }()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					sinkStrs = dictSectionAll(s.sec, s.n)
				}
			})
		}
	}
}
