package storage

// The dictionary is stored block-compressed for random access from the
// mmap'd file: values are grouped into small sorted blocks, each block LZ4'd,
// with an uncompressed sparse index of every block's first value. A lookup
// binary-searches the first-value index (no decompression) to find the one
// block that could hold the value, then decompresses just that block and
// searches within it. So a store of billions of rows answers a membership
// probe by touching one small compressed block -- compression back (the
// uncompressed section was ~4-5x larger on disk) without giving up the mmap
// random access that keeps decoded dictionaries off the heap.
//
// This is the standard columnar-DB shape (Parquet/ORC/ClickHouse string
// columns): compress in blocks, index the block boundaries.
//
// Layout:
//
//	numBlocks u32, blockSize u32
//	fvOff[numBlocks+1] u32                 -- offsets into the first-value blob
//	fvLen u32, first-value blob            -- each block's first value, uncompressed
//	per block: compOff u32, compLen u32, rawLen u32
//	compLenTotal u32, compressed blocks    -- each block raw = [k+1 offsets u32][strings], LZ4'd

const dictBlock = 64

// marshalRawBlock lays out one block's values as [k+1 offsets][strings], the
// uncompressed form that a decompressed block decodes to.
func marshalRawBlock(vals []string) []byte {
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

// dictCodecFlate is the high bit of a block's rawLen field, marking the block
// as flate- rather than LZ4-compressed. Blocks are tiny so the real rawLen
// never approaches 2^31; this keeps compact-mode data self-describing per
// block with no format-version change (old data has the bit clear = LZ4).
const dictCodecFlate = uint32(1) << 31

// marshalDictSection block-compresses a sorted dict. compact selects the flate
// codec (smaller, slower decode) over the default LZ4 (fast SIMD decode).
func marshalDictSection(dict []string, compact bool) []byte {
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
		raw := marshalRawBlock(dict[lo:hi])
		var c []byte
		rawLen := uint32(len(raw))
		if compact {
			c = flateCompress(raw)
			rawLen |= dictCodecFlate
		} else {
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

// dictSec navigates a marshaled dict section without decompressing anything.
type dictSec struct {
	numBlocks int
	fvOff     []byte // fvOff table, numBlocks+1 u32
	fvStr     []byte // first-value strings
	idx       []byte // block index, numBlocks*12
	comp      []byte // compressed blocks
}

func parseDictSec(sec []byte) dictSec {
	if len(sec) < 8 {
		return dictSec{}
	}
	nb := int(get32(sec, 0))
	p := 8
	fvOff := sec[p : p+4*(nb+1)]
	p += 4 * (nb + 1)
	fvLen := int(get32(sec, p))
	p += 4
	fvStr := sec[p : p+fvLen]
	p += fvLen
	idx := sec[p : p+nb*12]
	p += nb * 12
	compLen := int(get32(sec, p))
	p += 4
	comp := sec[p : p+compLen]
	return dictSec{numBlocks: nb, fvOff: fvOff, fvStr: fvStr, idx: idx, comp: comp}
}

func (d dictSec) firstVal(k int) string {
	o0 := get32(d.fvOff, 4*k)
	o1 := get32(d.fvOff, 4*(k+1))
	return string(d.fvStr[o0:o1])
}

// block decompresses block k into [k'+1 offsets][strings], dispatching on the
// per-block codec flagged in rawLen's high bit (default LZ4, compact flate).
func (d dictSec) block(k int) []byte {
	compOff := int(get32(d.idx, k*12))
	compLen := int(get32(d.idx, k*12+4))
	rawField := get32(d.idx, k*12+8)
	rawLen := int(rawField &^ dictCodecFlate)
	comp := d.comp[compOff : compOff+compLen]
	if rawField&dictCodecFlate != 0 {
		return flateDecompress(comp, rawLen)
	}
	return lz4Decompress(comp, rawLen)
}

// blockValAt reads value i within a decompressed block of count vals.
func blockValAt(raw []byte, count, i int) string {
	base := 4 * (count + 1)
	o0 := get32(raw, 4*i)
	o1 := get32(raw, 4*(i+1))
	return string(raw[base+int(o0) : base+int(o1)])
}

// blockCount is how many values block k holds.
func (d dictSec) blockCount(k, n int) int {
	c := n - k*dictBlock
	if c > dictBlock {
		c = dictBlock
	}
	return c
}

// dictSectionSearch returns value's id or -1, decompressing at most one block.
func dictSectionSearch(sec []byte, n int, value string) int {
	d := parseDictSec(sec)
	if d.numBlocks == 0 {
		return -1
	}
	// Largest block k with firstVal(k) <= value.
	lo, hi := 0, d.numBlocks
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if d.firstVal(mid) <= value {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	k := lo - 1
	if k < 0 {
		return -1 // value precedes the first value
	}
	raw := d.block(k)
	cnt := d.blockCount(k, n)
	blo, bhi := 0, cnt
	for blo < bhi {
		mid := int(uint(blo+bhi) >> 1)
		if blockValAt(raw, cnt, mid) < value {
			blo = mid + 1
		} else {
			bhi = mid
		}
	}
	if blo < cnt && blockValAt(raw, cnt, blo) == value {
		return k*dictBlock + blo
	}
	return -1
}

// dictSectionAt returns the i-th value, decompressing its block.
func dictSectionAt(sec []byte, n, i int) string {
	d := parseDictSec(sec)
	k := i / dictBlock
	if k >= d.numBlocks {
		return ""
	}
	raw := d.block(k)
	return blockValAt(raw, d.blockCount(k, n), i%dictBlock)
}

// dictSectionSome materializes only the values whose id is marked in want,
// decompressing a block only if it holds a wanted value. out[id] is set for
// wanted ids, "" elsewhere -- the materialize path for a subset of a dict.
func dictSectionSome(sec []byte, n int, want []bool) []string {
	d := parseDictSec(sec)
	out := make([]string, n)
	for k := 0; k < d.numBlocks; k++ {
		base := k * dictBlock
		cnt := d.blockCount(k, n)
		any := false
		for i := 0; i < cnt; i++ {
			if want[base+i] {
				any = true
				break
			}
		}
		if !any {
			continue // no wanted value in this block: skip the decompress
		}
		raw := d.block(k)
		for i := 0; i < cnt; i++ {
			if want[base+i] {
				out[base+i] = blockValAt(raw, cnt, i)
			}
		}
	}
	return out
}

// dictSectionAll materializes the whole dict -- the scan path, decompressing
// every block once.
func dictSectionAll(sec []byte, n int) []string {
	d := parseDictSec(sec)
	out := make([]string, 0, n)
	for k := 0; k < d.numBlocks; k++ {
		raw := d.block(k)
		cnt := d.blockCount(k, n)
		for i := 0; i < cnt; i++ {
			out = append(out, blockValAt(raw, cnt, i))
		}
	}
	return out
}
