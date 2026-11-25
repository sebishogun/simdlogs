package storage

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// marshalDictSectionReference is the section writer as it was before sections
// were appended into the group's buffer, kept verbatim with the block helpers
// it called, so the bytes can be compared.
func marshalDictSectionReference(dict []string, compact bool) []byte {
	n := len(dict)
	numBlocks := 0
	if n > 0 {
		numBlocks = (n + dictBlock - 1) / dictBlock
	}
	var fvStr []byte
	fvOff := make([]uint32, numBlocks+1)
	type bi struct{ compOff, compLen, rawLen uint32 }
	idx := make([]bi, numBlocks)
	var comp []byte
	for k := 0; k < numBlocks; k++ {
		lo := k * dictBlock
		hi := lo + dictBlock
		if hi > n {
			hi = n
		}
		fvOff[k] = uint32(len(fvStr))
		fvStr = append(fvStr, dict[lo]...)
		raw := marshalRawBlockReference(dict[lo:hi])
		var c []byte
		rawLen := uint32(len(raw))
		switch {
		case allLowerHex(dict[lo:hi]):
			c = hexPackBlockReference(raw, hi-lo)
			rawLen |= dictCodecHex
		case compact:
			c = flateCompress(raw)
			rawLen |= dictCodecFlate
		default:
			c = lz4Compress(raw)
		}
		idx[k] = bi{uint32(len(comp)), uint32(len(c)), rawLen}
		comp = append(comp, c...)
	}
	fvOff[numBlocks] = uint32(len(fvStr))

	out := make([]byte, 0, 8+4*(numBlocks+1)+4+len(fvStr)+numBlocks*12+4+len(comp))
	out = appU32(out, uint32(numBlocks))
	out = appU32(out, dictBlock)
	for _, o := range fvOff {
		out = appU32(out, o)
	}
	out = appU32(out, uint32(len(fvStr)))
	out = append(out, fvStr...)
	for _, e := range idx {
		out = appU32(out, e.compOff)
		out = appU32(out, e.compLen)
		out = appU32(out, e.rawLen)
	}
	out = appU32(out, uint32(len(comp)))
	out = append(out, comp...)
	return out
}

func marshalRawBlockReference(vals []string) []byte {
	strsLen := 0
	for _, s := range vals {
		strsLen += len(s)
	}
	out := make([]byte, 0, 4*(len(vals)+1)+strsLen)
	var off uint32
	for _, s := range vals {
		out = appU32(out, off)
		off += uint32(len(s))
	}
	out = appU32(out, off)
	for _, s := range vals {
		out = append(out, s...)
	}
	return out
}

func hexPackBlockReference(raw []byte, count int) []byte {
	strBase := 4 * (count + 1)
	strs := raw[strBase:]
	total := len(strs)
	nb := (total + 1) / 2
	out := make([]byte, 0, 4+strBase+nb)
	out = appU32(out, uint32(count))
	out = append(out, raw[:strBase]...)
	base := len(out)
	out = append(out, make([]byte, nb)...)
	packed := out[base:]
	for i := 0; i < total; i += 2 {
		hi := hexNibble(strs[i]) << 4
		var lo byte
		if i+1 < total {
			lo = hexNibble(strs[i+1])
		}
		packed[i>>1] = hi | lo
	}
	return out
}

// sectionDicts are sorted, deduplicated dictionaries of the shapes a section
// meets: empty, one value, block edges, hex blocks beside text blocks, values
// from empty to several KiB, and bytes that are not UTF-8.
func sectionDicts() [][]string {
	r := rand.New(rand.NewPCG(1, 2))
	value := func(kind, n int) string {
		b := make([]byte, n)
		for i := range b {
			switch kind {
			case 0: // lowercase hex: the hex codec
				b[i] = hexChar(byte(r.IntN(16)))
			case 1: // log text
				b[i] = "abcdefghij klmnop=:/0123456789"[r.IntN(30)]
			default: // anything
				b[i] = byte(r.IntN(256))
			}
		}
		return string(b)
	}
	var dicts [][]string
	for _, n := range []int{0, 1, 2, 63, 64, 65, 127, 128, 129, 700} {
		for kind := range 4 {
			d := make([]string, 0, n)
			for i := range n {
				k := kind
				if kind == 3 {
					k = i / dictBlock % 3 // hex, text and binary blocks in one section
				}
				size := r.IntN(48)
				if i%17 == 0 {
					size = r.IntN(6000)
				}
				d = append(d, value(k, size)+strconv.Itoa(i))
			}
			if kind == 1 && n > 0 {
				d[0] = "" // an empty value sorts first
			}
			slices.Sort(d)
			dicts = append(dicts, slices.Compact(d))
		}
	}
	return dicts
}

// TestDictSectionMatchesReference: a section appended into a group's buffer has
// exactly the bytes the separate section had, in both codecs, after whatever the
// buffer already held.
func TestDictSectionMatchesReference(t *testing.T) {
	prefix := []byte("group bytes before the section")
	for i, d := range sectionDicts() {
		for _, compact := range []bool{false, true} {
			want := marshalDictSectionReference(d, compact)
			if got := marshalDictSection(d, compact); !bytes.Equal(got, want) {
				t.Fatalf("dict %d (%d values, compact %t): section differs from the reference", i, len(d), compact)
			}
			got := appendDictSection(slices.Clip(slices.Clone(prefix)), d, compact)
			if !bytes.Equal(got[:len(prefix)], prefix) || !bytes.Equal(got[len(prefix):], want) {
				t.Fatalf("dict %d (%d values, compact %t): appended section differs from the reference", i, len(d), compact)
			}
		}
	}
	for i, d := range sectionDicts() {
		for k := 0; k*dictBlock < len(d); k++ {
			vals := d[k*dictBlock : min((k+1)*dictBlock, len(d))]
			raw := marshalRawBlock(vals)
			if !bytes.Equal(raw, marshalRawBlockReference(vals)) {
				t.Fatalf("dict %d block %d: raw block differs", i, k)
			}
			if allLowerHex(vals) && !bytes.Equal(hexPackBlock(raw, len(vals)), hexPackBlockReference(raw, len(vals))) {
				t.Fatalf("dict %d block %d: hex block differs", i, k)
			}
		}
	}
}

// marshalBenchGroup is a group shaped like Cawl's daemon's: nine dictionary
// columns, most of them a few distinct values, and a message column of either
// short log lines or 4 KiB rows of binary output made valid UTF-8.
func marshalBenchGroup(rows, msgBytes int, binary bool) *Group {
	r := rand.New(rand.NewPCG(3, 4))
	msg := make([]string, rows)
	for i := range msg {
		b := make([]byte, 0, msgBytes)
		for len(b) < msgBytes {
			if binary && r.IntN(2) == 0 {
				b = append(b, "�"...)
			} else {
				b = append(b, "abcdefghij klmnop=:/0123456789"[r.IntN(30)])
			}
		}
		msg[i] = string(b)
	}
	cols := []Column{{Name: "_time", Type: ColTimestamp, Ts: make([]int64, rows)}}
	for i := range rows {
		cols[0].Ts[i] = int64(1e18) + int64(i)*1000
	}
	add := func(name string, vals []string) {
		d := BuildDict(vals)
		cols = append(cols, Column{Name: name, Type: ColDict, Dict: &d})
	}
	add("_msg", msg)
	constant := func(v string) []string {
		s := make([]string, rows)
		for i := range s {
			s[i] = v
		}
		return s
	}
	add("kind", constant("output"))
	add("owner", constant("seb"))
	add("job", constant("0123456789abcdef0123456789abcdef"))
	add("attempt", constant("1"))
	add("stream", constant("stdout"))
	offsets := make([]string, rows)
	for i := range offsets {
		offsets[i] = strconv.Itoa(i * msgBytes)
	}
	add("offset", offsets)
	add("_stream", constant(`{kind="output",owner="seb"}`))
	return &Group{Rows: rows, Columns: cols}
}

// BenchmarkGroupMarshal: the bytes and allocations one flush of the embedded
// writer makes to write its group.
func BenchmarkGroupMarshal(b *testing.B) {
	for _, c := range []struct {
		name           string
		rows, msgBytes int
		binary         bool
	}{
		{"text-4096x60", 4096, 60, false},
		{"binary-256x4K", 256, 4096, true},
	} {
		g := marshalBenchGroup(c.rows, c.msgBytes, c.binary)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			size := 0
			for b.Loop() {
				size = len(g.Marshal())
			}
			b.ReportMetric(float64(size), "group-bytes")
		})
	}
}
