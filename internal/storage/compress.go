package storage

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"io"

	"github.com/sebishogun/simd"
)

// flateCompress/flateDecompress are the compact-mode dict codec: stdlib DEFLATE
// (Huffman + LZ77) roughly halves the dictionary vs the in-tree LZ4, because
// LZ4 has no entropy coding. It is opt-in only -- flate decode is slower than
// the SIMD LZ4 kernel, so it trades scan speed for size and never touches the
// default path. Measured on the realistic dict: LZ4 2.05x vs flate 3.99x.
func flateCompress(src []byte) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestCompression)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}

func flateDecompress(src []byte, rawLen int) []byte {
	r := flate.NewReader(bytes.NewReader(src))
	out := make([]byte, rawLen)
	io.ReadFull(r, out)
	r.Close()
	return out
}

// LZ4 for the dictionary tables. The design's decision #1: LZ4 is the
// default codec because decode speed is the scan bottleneck, and decode
// runs on the simd kernel (simd.LZ4BlockDecode). Encode is not on the
// hot path -- it happens once at group flush -- so a straightforward
// greedy block compressor suffices, producing the raw block format the
// kernel reads. High-cardinality dict tables (messages, trace ids) are
// most of a group's bytes; compressing them is the footprint lever at
// scale.

// LZ4CompressExported exposes the in-tree LZ4 compressor for benchmarks that
// measure codec ratios against it.
func LZ4CompressExported(src []byte) []byte { return lz4Compress(src) }

// lz4Compress produces a raw LZ4 block from src via a greedy hash-table
// match finder. Format: LZ4 sequences (token, literal-length ext,
// literals, offset, match-length ext), matching simd.LZ4BlockDecode.
func lz4Compress(src []byte) []byte {
	var out []byte
	var table [1 << 12]int32
	for i := range table {
		table[i] = -1
	}
	i, lit := 0, 0
	emit := func(litEnd, matchLen, offset int) {
		ll := litEnd - lit
		mt := 0
		if matchLen > 0 {
			mt = matchLen - 4
		}
		tok := byte(0)
		if ll >= 15 {
			tok = 15 << 4
		} else {
			tok = byte(ll) << 4
		}
		if matchLen > 0 {
			if mt >= 15 {
				tok |= 15
			} else {
				tok |= byte(mt)
			}
		}
		out = append(out, tok)
		if ll >= 15 {
			r := ll - 15
			for ; r >= 255; r -= 255 {
				out = append(out, 255)
			}
			out = append(out, byte(r))
		}
		out = append(out, src[lit:litEnd]...)
		if matchLen > 0 {
			out = binary.LittleEndian.AppendUint16(out, uint16(offset))
			if mt >= 15 {
				r := mt - 15
				for ; r >= 255; r -= 255 {
					out = append(out, 255)
				}
				out = append(out, byte(r))
			}
		}
	}
	for i+4 <= len(src) {
		h := (binary.LittleEndian.Uint32(src[i:]) * 2654435761) >> 20
		cand := int(table[h])
		table[h] = int32(i)
		if cand >= 0 && i-cand <= 65535 &&
			binary.LittleEndian.Uint32(src[cand:]) == binary.LittleEndian.Uint32(src[i:]) {
			ml := 4
			for i+ml < len(src) && src[cand+ml] == src[i+ml] {
				ml++
			}
			emit(i, ml, i-cand)
			i += ml
			lit = i
		} else {
			i++
		}
	}
	emit(len(src), 0, 0) // final literals
	return out
}

// lz4Decompress inflates a block into a buffer of the known original
// length, on the simd kernel.
func lz4Decompress(block []byte, origLen int) []byte {
	dst := make([]byte, origLen)
	n := simd.LZ4BlockDecode(dst, block)
	if n != origLen {
		return nil
	}
	return dst
}

// marshalDictBlob concatenates a dict's strings as [len u32][bytes]... and
// returns the raw blob (for compression). Split by unmarshalDictBlob.
func marshalDictBlob(dict []string) []byte {
	var b []byte
	for _, s := range dict {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

func unmarshalDictBlob(b []byte, n int) []string {
	out := make([]string, 0, n)
	p := 0
	for i := 0; i < n && p+4 <= len(b); i++ {
		l := int(binary.LittleEndian.Uint32(b[p:]))
		p += 4
		out = append(out, string(b[p:p+l]))
		p += l
	}
	return out
}
