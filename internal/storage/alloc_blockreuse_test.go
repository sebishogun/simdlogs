package storage

import (
	"bytes"
	"compress/flate"
	"strconv"
	"testing"
)

// The A/B for reusing one decompressed-block buffer across a whole section
// walk. Both arms are in this one binary and run interleaved in one session,
// because a two-build comparison would put the 8.3% code-layout noise floor
// between them.

// poisonBuf is a buffer whose every byte is a value no decoder may leave
// behind. A decoder that does not write all of its output returns some of
// these, which is what the tests below look for.
func poisonBuf(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 0xDE
	}
	return b
}

// TestDecodeIntoWritesEveryByte is the poisoning test the reused buffer rests
// on: each block decoder, handed a dirty buffer, must return exactly what it
// returns when it allocates a fresh one -- including for inputs it cannot
// fully decode, where the unwritten tail was zero and must stay zero.
func TestDecodeIntoWritesEveryByte(t *testing.T) {
	vals := make([]string, 64)
	for i := range vals {
		vals[i] = "value-" + strconv.Itoa(i) + "-payload"
	}
	raw := marshalRawBlock(vals)

	// LZ4, whole and truncated.
	lz4ed := lz4Compress(raw)
	for _, src := range [][]byte{lz4ed, lz4ed[:len(lz4ed)/2], lz4ed[:1], {}} {
		want := lz4Decompress(src, len(raw))
		got := lz4DecompressInto(poisonBuf(len(raw)*2), src, len(raw))
		if !bytes.Equal(got, want) {
			t.Fatalf("lz4 (%d bytes in): reused %q, fresh %q", len(src), got, want)
		}
	}

	// Hex, whole and truncated.
	hexVals := make([]string, 64)
	seed := uint32(3)
	for i := range hexVals {
		b := make([]byte, 16)
		for j := range b {
			seed = seed*1664525 + 1013904223
			b[j] = hexChar(byte(seed >> 28))
		}
		hexVals[i] = string(b)
	}
	hraw := marshalRawBlock(hexVals)
	hexed := hexPackBlock(hraw, 64)
	for _, src := range [][]byte{hexed, hexed[:len(hexed)/2], hexed[:3], {}} {
		want := hexUnpackBlock(src)
		got := hexUnpackBlockInto(poisonBuf(len(hraw)*2), src)
		if !bytes.Equal(got, want) {
			t.Fatalf("hex (%d bytes in): reused %q, fresh %q", len(src), got, want)
		}
	}

	// Flate, whole and truncated. The truncated stream is the case that made
	// reuse unsafe: ReadFull's error is ignored, so the tail of the output is
	// never written and a dirty buffer would return the previous block's
	// characters as dictionary values.
	var fbuf bytes.Buffer
	fw, _ := flate.NewWriter(&fbuf, flate.BestCompression)
	fw.Write(raw)
	fw.Close()
	flated := fbuf.Bytes()
	for _, src := range [][]byte{flated, flated[:len(flated)/2], flated[:2], {}} {
		want := flateDecompress(src, len(raw))
		got := flateDecompressInto(poisonBuf(len(raw)*2), src, len(raw))
		if !bytes.Equal(got, want) {
			t.Fatalf("flate (%d bytes in): reused %q, fresh %q", len(src), got, want)
		}
		if bytes.Contains(got, []byte{0xDE, 0xDE, 0xDE, 0xDE}) {
			t.Fatalf("flate (%d bytes in): poison survived into the output", len(src))
		}
	}
}

// blockReuseSections covers all three block codecs plus a section whose later
// blocks decompress SHORTER than its earlier ones, which is when a reused
// buffer carries the most stale bytes past the end of the live region.
func blockReuseSections(t testing.TB) []struct {
	name string
	sec  []byte
	n    int
} {
	mk := func(v []string, compact bool) ([]byte, int) {
		d := BuildDict(v).Dict
		return marshalDictSection(d, compact), len(d)
	}
	var plain, hexy, shrink []string
	for i := 0; i < 500; i++ {
		plain = append(plain, "service-"+strconv.Itoa(i)+"-instance")
	}
	seed := uint32(5)
	for i := 0; i < 500; i++ {
		b := make([]byte, 24)
		for j := range b {
			seed = seed*1664525 + 1013904223
			b[j] = hexChar(byte(seed >> 28))
		}
		hexy = append(hexy, string(b))
	}
	// Sorted order puts the long "0" values in the first blocks and the short
	// "z" values in the last ones.
	for i := 0; i < 200; i++ {
		shrink = append(shrink, "0"+strconv.Itoa(i)+"-a-very-long-dictionary-value-with-plenty-of-bytes-in-it")
	}
	for i := 0; i < 200; i++ {
		shrink = append(shrink, "z"+strconv.Itoa(i))
	}
	psec, pn := mk(plain, false)
	hsec, hn := mk(hexy, false)
	csec, cn := mk(plain, true)
	ssec, sn := mk(shrink, false)
	return []struct {
		name string
		sec  []byte
		n    int
	}{
		{"lz4", psec, pn},
		{"hex", hsec, hn},
		{"flate", csec, cn},
		{"shrinking", ssec, sn},
	}
}

// TestBlockReuseMatchesFresh compares a whole section walked with the buffer
// reused against the same walk allocating a block at a time.
func TestBlockReuseMatchesFresh(t *testing.T) {
	defer func() { blockReuse = true }()
	for _, s := range blockReuseSections(t) {
		blockReuse = false
		want := dictSectionAll(s.sec, s.n)
		blockReuse = true
		got := dictSectionAll(s.sec, s.n)
		if len(got) != len(want) {
			t.Fatalf("%s: reused len %d, fresh len %d", s.name, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: value %d: reused %q, fresh %q", s.name, i, got[i], want[i])
			}
		}
	}
}

// TestValueCountsBlockReuse is the same comparison through the streamed walk,
// which hands its buffer back one block later than dictSectionAll does.
func TestValueCountsBlockReuse(t *testing.T) {
	defer func() { blockReuse = true }()
	for _, card := range []int{64, 65, 1000, 4096} {
		r := vcTestReader(t, 8192, card)
		blockReuse = false
		want := r.ValueCounts("c")
		blockReuse = true
		got := r.ValueCounts("c")
		if len(got) != len(want) {
			t.Fatalf("card=%d: reused len %d, fresh len %d", card, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("card=%d: entry %d: reused %+v, fresh %+v", card, i, got[i], want[i])
			}
		}
	}
}

func BenchmarkBlockReuse(b *testing.B) {
	for _, s := range blockReuseSections(b) {
		// Arms interleaved per shape, so any drift over the run hits both.
		for _, arm := range []struct {
			name  string
			reuse bool
		}{{"reuse", true}, {"fresh", false}} {
			b.Run(s.name+"/"+arm.name, func(b *testing.B) {
				blockReuse = arm.reuse
				defer func() { blockReuse = true }()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					sinkStrs = dictSectionAll(s.sec, s.n)
				}
			})
		}
	}
}
