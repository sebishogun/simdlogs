package storage

import "github.com/sebishogun/simd"

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

// dictCodecHex marks a block whose values are all lowercase hex, stored
// nibble-packed (4 bits/char) instead of a byte per char. Random hex (trace/span
// ids) carries 4 bits of entropy per char, so LZ4 barely dents it; nibble
// packing halves it losslessly and decodes with a fast unpack (no entropy
// decoder), beating even flate on hex. Self-describing per block; old blocks
// have the bit clear. Uses bit 30 so it composes with neither codec set = LZ4.
const dictCodecHex = uint32(1) << 30

func isLowerHexByte(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f'
}

func allLowerHex(vals []string) bool {
	total := 0
	for _, s := range vals {
		if s == "" {
			return false
		}
		total += len(s)
		for i := 0; i < len(s); i++ {
			if !isLowerHexByte(s[i]) {
				return false
			}
		}
	}
	return total > 0
}

func hexNibble(c byte) byte {
	if c <= '9' {
		return c - '0'
	}
	return c - 'a' + 10
}

func hexChar(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'a' + v - 10
}

// hexPackBlock nibble-packs a raw dict block ([k+1 char-offsets][hex strings])
// into [k u32][k+1 offsets][nibbles], two hex chars per byte, high nibble = the
// first char -- the simd.HexEncode/HexDecode convention, so the hot unpack is
// the SIMD kernel. Pack runs once at flush, so it stays a scalar loop.
func hexPackBlock(raw []byte, count int) []byte {
	strBase := 4 * (count + 1)
	strs := raw[strBase:]
	total := len(strs)
	nb := (total + 1) / 2
	out := make([]byte, 0, 4+strBase+nb)
	out = appU32(out, uint32(count))
	out = append(out, raw[:strBase]...) // char offsets verbatim
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

// hexUnpackBlock reverses hexPackBlock, reconstructing the [k+1 offsets][strings]
// raw block so blockValAt and the searches read it unchanged. The nibble->char
// expansion is simd.HexEncode (SIMD): faster than the LZ4 kernel it replaces.
func hexUnpackBlock(packed []byte) []byte {
	return hexUnpackBlockInto(nil, packed)
}

// hexUnpackBlockInto is hexUnpackBlock reusing the caller's buffer. See
// dictSec.blockInto for who may pass one. Every byte of the returned slice is
// written: the offset table by the copy, the characters by HexEncode.
func hexUnpackBlockInto(buf, packed []byte) []byte {
	if len(packed) < 4 {
		return nil
	}
	count := int(get32(packed, 0))
	// count and total are file bytes: a corrupt count slices the offset
	// table past the block, and a corrupt total sizes the output.
	if count < 0 || count > (len(packed)-4)/4 {
		return nil
	}
	strBase := 4 * (count + 1)
	if 4+strBase > len(packed) {
		return nil
	}
	total := int(get32(packed[4:], 4*count)) // last offset = total chars
	nb := (total + 1) / 2
	nibBase := 4 + strBase
	if total < 0 || nb > len(packed)-nibBase {
		return nil
	}
	// HexEncode writes 2*nb chars; for an odd total that is one pad char past
	// the strings, unread (offsets bound reads to total). Size raw for it.
	raw := fitBuf(buf, strBase+2*nb)
	copy(raw, packed[4:4+strBase]) // offsets
	simd.HexEncode(raw[strBase:], packed[nibBase:nibBase+nb])
	return raw
}

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
		switch {
		case allLowerHex(dict[lo:hi]): // hex codec wins on size and speed, both modes
			c = hexPackBlock(raw, hi-lo)
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

// dictSec navigates a marshaled dict section without decompressing anything.
type dictSec struct {
	numBlocks int
	fvOff     []byte // fvOff table, numBlocks+1 u32
	fvStr     []byte // first-value strings
	idx       []byte // block index, numBlocks*12
	comp      []byte // compressed blocks
}

// parseDictSec reads a dict section, validating every length against what is
// left of the slice.
//
// It had no bounds checks at all: nb came straight from the file, and
// sec[p : p+4*(nb+1)] on a corrupt count panicked. That mattered more than a
// bad request, because needsRecompact calls this from the tiering goroutine,
// which has no recover -- a single corrupt group killed the process. A zero
// dictSec is the failure signal; every caller already treats it as an empty
// section.
func parseDictSec(sec []byte) dictSec {
	if len(sec) < 8 {
		return dictSec{}
	}
	nb := int(get32(sec, 0))
	// Each block costs 4 bytes of first-value offset plus 12 of index, so a
	// count beyond that is corrupt however large the section is.
	if nb < 0 || nb > (len(sec)-8)/16 {
		return dictSec{}
	}
	p := 8
	need := func(n int) bool { return n >= 0 && n <= len(sec)-p }

	if !need(4 * (nb + 1)) {
		return dictSec{}
	}
	fvOff := sec[p : p+4*(nb+1)]
	p += 4 * (nb + 1)

	if !need(4) {
		return dictSec{}
	}
	fvLen := int(get32(sec, p))
	p += 4
	if !need(fvLen) {
		return dictSec{}
	}
	fvStr := sec[p : p+fvLen]
	p += fvLen

	if !need(nb * 12) {
		return dictSec{}
	}
	idx := sec[p : p+nb*12]
	p += nb * 12

	if !need(4) {
		return dictSec{}
	}
	compLen := int(get32(sec, p))
	p += 4
	if !need(compLen) {
		return dictSec{}
	}
	comp := sec[p : p+compLen]
	return dictSec{numBlocks: nb, fvOff: fvOff, fvStr: fvStr, idx: idx, comp: comp}
}

func (d dictSec) firstVal(k int) string {
	// Both offsets are four bytes of file, and so is every length below in
	// block/blockValAt. parseDictSec validates the section's own geometry;
	// nothing validated the per-block table inside it, and Recompact reaches
	// here from the tiering goroutine, which has no recover.
	if k < 0 || 4*(k+1)+4 > len(d.fvOff) {
		return ""
	}
	o0 := int(get32(d.fvOff, 4*k))
	o1 := int(get32(d.fvOff, 4*(k+1)))
	if o0 < 0 || o1 < o0 || o1 > len(d.fvStr) {
		return ""
	}
	return string(d.fvStr[o0:o1])
}

// block decompresses block k into [k'+1 offsets][strings], dispatching on the
// per-block codec flagged in rawLen's high bit (default LZ4, compact flate).
func (d dictSec) block(k int) []byte {
	return d.blockInto(k, nil)
}

// blockInto is block reusing the caller's buffer for the decompressed bytes.
//
// It is for a caller that walks EVERY block of a section and is finished with
// one before it asks for the next: dictSectionAll and dictWalk convert each
// block's string region to its own string, so the decompressed bytes are dead
// as soon as that conversion returns. Passing the same buffer back turns one
// allocation per block into one allocation per walk -- at 64 values per block
// and a 131K-value dictionary that is 2048 allocations of a kilobyte-plus
// each.
//
// The buffer comes back holding a PREVIOUS block's bytes, so every decoder
// reached from here must write every byte of it. blockReuse selects the arm;
// TestBlockReuseMatchesFresh poisons the buffer and compares.
func (d dictSec) blockInto(k int, buf []byte) []byte {
	if k < 0 || k*12+12 > len(d.idx) {
		return nil
	}
	compOff := int(get32(d.idx, k*12))
	compLen := int(get32(d.idx, k*12+4))
	rawField := get32(d.idx, k*12+8)
	rawLen := int(rawField &^ (dictCodecFlate | dictCodecHex))
	if compOff < 0 || compLen < 0 || compOff > len(d.comp) || compLen > len(d.comp)-compOff {
		return nil
	}
	if !blockReuse {
		buf = nil
	}
	comp := d.comp[compOff : compOff+compLen]
	switch {
	case rawField&dictCodecHex != 0:
		return hexUnpackBlockInto(buf, comp)
	case rawField&dictCodecFlate != 0:
		return flateDecompressInto(buf, comp, rawLen)
	default:
		return lz4DecompressInto(buf, comp, rawLen)
	}
}

// blockReuse selects whether the whole-section walkers hand their block buffer
// back to be refilled. Both arms compile into ONE binary so they can be
// benchmarked interleaved in a single session -- a two-build comparison would
// put the 8.3% code-layout noise floor between them. Production reuses.
var blockReuse = true

// fitBuf returns a buffer of exactly n bytes, reusing buf's array when it is
// large enough. The contents are whatever the previous user left: the caller
// must write all n bytes before any is read.
func fitBuf(buf []byte, n int) []byte {
	if cap(buf) < n {
		return make([]byte, n)
	}
	return buf[:n]
}

// blockValAt reads value i within a decompressed block of count vals.
func blockValAt(raw []byte, count, i int) string {
	if i < 0 || count < 0 || 4*(i+1)+4 > len(raw) || i >= count {
		return ""
	}
	base := 4 * (count + 1)
	o0 := int(get32(raw, 4*i))
	o1 := int(get32(raw, 4*(i+1)))
	if o0 < 0 || o1 < o0 || base < 0 || base+o1 > len(raw) {
		return ""
	}
	return string(raw[base+o0 : base+o1])
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

// dictArena selects how dictSectionAll materializes a block's values: one
// shared string per block (true) or one string per value (false). Both arms
// compile into ONE binary so they can be benchmarked interleaved in a single
// session -- a two-build comparison would put the 8.3% code-layout noise floor
// between them. Production always takes the arena path.
var dictArena = true

// dictSectionAll materializes the whole dict -- the scan path, decompressing
// every block once.
func dictSectionAll(sec []byte, n int) []string {
	d := parseDictSec(sec)
	out := make([]string, 0, n)
	// One buffer for every block: each block's values are copied into their own
	// string before the next block overwrites it.
	var buf []byte
	for k := 0; k < d.numBlocks; k++ {
		cnt := d.blockCount(k, n)
		if dictArena {
			raw := d.blockInto(k, buf)
			out = appendBlockVals(out, raw, cnt)
			buf = raw
			continue
		}
		raw := d.block(k)
		for i := 0; i < cnt; i++ {
			out = append(out, blockValAt(raw, cnt, i))
		}
	}
	return out
}

// appendBlockVals appends every value of a decompressed block, converting the
// block's string region ONCE and slicing each value out of it.
//
// blockValAt does `string(raw[a:b])` per value, which is one heap allocation
// and one copy per dict value -- 94% of all objects allocated by a
// `top N by (host)`, at dictBlock=64 values per block. A substring of a Go
// string shares its backing array and allocates nothing, so converting the
// whole region once gives the same bytes in one allocation per block instead
// of 64, and copies exactly the same number of bytes doing it.
//
// The values returned therefore share one backing array per block: a caller
// that keeps ONE value keeps its block's bytes alive. dictSectionAll's callers
// materialize the whole dictionary and were holding all of those bytes anyway
// (in more allocations, each rounded up to a size class), so the retained
// footprint does not grow.
//
// Out-of-range reads answer "" exactly as blockValAt does, including the
// corrupt-header case where the offset table itself runs past the block --
// that one falls back to the per-value path, which decides per value.
func appendBlockVals(out []string, raw []byte, cnt int) []string {
	a := newBlockArena(raw, cnt)
	for i := 0; i < cnt; i++ {
		out = append(out, a.at(i))
	}
	return out
}

// blockArena is a decompressed dict block plus its string region converted
// once. It holds the value-slicing rules in one place for the two callers that
// read a whole block: appendBlockVals and the dictWalk ValueCounts drives.
type blockArena struct {
	raw   []byte
	vals  string
	cnt   int
	split bool // false: offset table runs past the block, so read per value
}

func newBlockArena(raw []byte, cnt int) blockArena {
	base := 4 * (cnt + 1)
	if cnt < 0 || base > len(raw) {
		return blockArena{raw: raw, cnt: cnt}
	}
	return blockArena{raw: raw, vals: string(raw[base:]), cnt: cnt, split: true}
}

// at is blockValAt's answer for value i, sliced out of the shared arena.
func (a *blockArena) at(i int) string {
	if !a.split {
		return blockValAt(a.raw, a.cnt, i)
	}
	if i < 0 || i >= a.cnt {
		return ""
	}
	o0 := int(get32(a.raw, 4*i))
	o1 := int(get32(a.raw, 4*(i+1)))
	if o0 < 0 || o1 < o0 || o1 > len(a.vals) {
		return ""
	}
	return a.vals[o0:o1]
}

// dictWalk yields a section's values in id order, decompressing each block
// once, WITHOUT materializing the []string dictSectionAll returns. A caller
// that consumes the dictionary in order -- ValueCounts pairing each value with
// its posting count -- then allocates nothing for the dictionary at all.
//
// It yields exactly the values dictSectionAll would append, in that order, so
// a caller that stopped at len(vals) stops when next reports false.
type dictWalk struct {
	d dictSec
	n int
	k int // next block
	i int // next value in the current block
	a blockArena
}

func newDictWalk(sec []byte, n int) dictWalk {
	return dictWalk{d: parseDictSec(sec), n: n}
}

func (w *dictWalk) next() (string, bool) {
	for w.i >= w.a.cnt {
		if w.k >= w.d.numBlocks {
			return "", false
		}
		// The previous block's buffer, refilled: every value it held was
		// copied into the arena string (or, on the corrupt-header path, into
		// its own string) before the walk moved off that block, so nothing
		// still points into these bytes.
		raw := w.d.blockInto(w.k, w.a.raw)
		w.a = newBlockArena(raw, w.d.blockCount(w.k, w.n))
		w.i = 0
		w.k++
	}
	v := w.a.at(w.i)
	w.i++
	return v, true
}
