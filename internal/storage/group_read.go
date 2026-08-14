package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync/atomic"
)

// Format versions this package can read. v7 is the historical layout; v8 is
// v7 plus a CRC32C over every preceding byte, appended after the footer
// length. v7 stores stay readable forever -- an operator upgrading a binary
// must not have to rewrite a disk of groups.
const (
	versionV7 = 7
	versionV8 = 8
)

// crc32c is the Castagnoli table, which has a hardware instruction on the
// architectures this library targets, so the checksum costs a fraction of the
// parse it protects.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// Structural ceilings applied before any allocation. They exist because the
// counts they bound come from the file: a corrupt four-byte column count of
// 4e9 otherwise turns into a 4-billion-element make() and an OOM kill long
// before any of the values are validated.
//
// The values are far above anything the writer produces (a group is capped by
// FlushRows/FlushBytes in the ingest writer) and far below anything that
// exhausts memory.
const (
	maxGroupRows    = 1 << 30 // ~1e9 rows in one group
	maxGroupColumns = 1 << 16 // 65536 columns
	maxBloomWords   = 1 << 20 // 8 MB of bloom per column
	maxNameBytes    = 1 << 16 // 64 KB column name
)

// corruptf builds a corruption error that names what failed. The bare
// errCorrupt sentinel is kept for errors.Is; the text is for the operator who
// has to decide whether to quarantine a file.
func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errCorrupt, fmt.Sprintf(format, args...))
}

// A cursor is a bounds-checked reader over the footer. Every read goes
// through it, so no parse step can index past the slice: the previous parser
// advanced a raw int and indexed directly, which turned any corrupt length
// into a panic that killed the process rather than an error that skipped one
// file.
type cursor struct {
	b   []byte
	at  int
	err error
}

func (c *cursor) fail(format string, args ...any) {
	if c.err == nil {
		c.err = corruptf(format, args...)
	}
}

func (c *cursor) need(n int) bool {
	if c.err != nil {
		return false
	}
	if n < 0 || c.at < 0 || n > len(c.b)-c.at {
		c.fail("need %d bytes at offset %d of %d", n, c.at, len(c.b))
		return false
	}
	return true
}

func (c *cursor) u32() uint32 {
	if !c.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(c.b[c.at:])
	c.at += 4
	return v
}

func (c *cursor) u64() uint64 {
	if !c.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(c.b[c.at:])
	c.at += 8
	return v
}

func (c *cursor) byte() byte {
	if !c.need(1) {
		return 0
	}
	v := c.b[c.at]
	c.at++
	return v
}

func (c *cursor) str() string {
	n := int(c.u32())
	if c.err != nil {
		return ""
	}
	if n < 0 || n > maxNameBytes {
		c.fail("name length %d exceeds the %d-byte ceiling", n, maxNameBytes)
		return ""
	}
	if !c.need(n) {
		return ""
	}
	s := string(c.b[c.at : c.at+n])
	c.at += n
	return s
}

// ReadGroup parses a marshaled group, validating every length before it is
// used. The footer is read first (its length is the last four bytes) so a
// query can consult skip metadata without decoding any column.
//
// v8 blobs are checksum-verified before anything is parsed. v7 blobs have no
// checksum -- that is why v8 exists -- so they are parsed with the same
// bounds checking and no integrity guarantee, which is what a store written
// by an older binary can offer.
func ReadGroup(b []byte) (*Reader, error) {
	if len(b) < 20 {
		return nil, corruptf("blob is %d bytes, shorter than a header plus footer length", len(b))
	}
	if got := get32(b, 0); got != magic {
		return nil, corruptf("magic %#x, want %#x", got, magic)
	}
	switch v := get32(b, 4); v {
	case versionV7:
		return readGroup(b, b[len(b)-4:], false)
	case versionV8:
		return readGroupV8(b)
	default:
		return nil, corruptf("version %d is not readable by this build (supports %d and %d)", v, versionV7, versionV8)
	}
}

// readGroupV8 verifies the trailing CRC32C, then parses the body exactly as
// v7 is parsed. The checksum covers every byte before it, so a single flipped
// bit anywhere -- header, column data, footer -- is caught here rather than
// becoming a wrong answer or a panic deep in a column decode.
func readGroupV8(b []byte) (*Reader, error) {
	if len(b) < 24 {
		return nil, corruptf("v8 blob is %d bytes, shorter than a header plus footer length plus checksum", len(b))
	}
	body := b[:len(b)-4]
	want := binary.LittleEndian.Uint32(b[len(b)-4:])
	if got := crc32.Checksum(body, crc32c); got != want {
		return nil, corruptf("checksum %#08x, want %#08x", got, want)
	}
	// The footer-length word sits immediately before the checksum.
	return readGroup(b, body[len(body)-4:], true)
}

// readGroup parses the common body. flenAt is the four bytes holding the
// footer length, which is the last word of the blob in v7 and the word before
// the checksum in v8. hasCRC records which, so the footer's end offset is
// computed from the right place.
func readGroup(b, flenWord []byte, hasCRC bool) (*Reader, error) {
	trailer := 4
	if hasCRC {
		trailer = 8
	}
	rows := int(get32(b, 8))
	ncol := int(get32(b, 12))
	if rows < 0 || rows > maxGroupRows {
		return nil, corruptf("row count %d outside [0, %d]", rows, maxGroupRows)
	}
	if ncol < 0 || ncol > maxGroupColumns {
		return nil, corruptf("column count %d outside [0, %d]", ncol, maxGroupColumns)
	}

	flen := int(binary.LittleEndian.Uint32(flenWord))
	// The footer must fit between the header and the trailer.
	if flen < 16 || flen > len(b)-trailer-16 {
		return nil, corruptf("footer length %d does not fit a %d-byte blob", flen, len(b))
	}
	fEnd := len(b) - trailer
	f := b[fEnd-flen : fEnd]

	r := &Reader{blob: b, Rows: rows}
	c := &cursor{b: f}
	r.TimeMin = int64(c.u64())
	r.TimeMax = int64(c.u64())

	r.cols = make([]colMeta, ncol)
	r.idxCache = make([]atomic.Pointer[[]uint32], ncol)
	r.emptyCache = make([]atomic.Int32, ncol)
	for i := range r.emptyCache {
		r.emptyCache[i].Store(-1) // -1: not yet asked
	}
	// dataEnd is the first byte the column data may not reach: everything
	// from the footer onward is metadata, so a column claiming to extend into
	// it is corrupt.
	dataEnd := fEnd - flen

	for i := 0; i < ncol; i++ {
		var m colMeta
		m.Name = c.str()
		m.Type = ColumnType(c.byte())
		m.Width = int(c.u32())
		m.DictLen = int(c.u32())
		m.MinIdx = c.u32()
		m.MaxIdx = c.u32()
		m.DataOff = int(c.u32())
		m.DataLen = int(c.u32())
		m.PostOff = int(c.u32())
		m.PostLen = int(c.u32())
		nb := int(c.u32())
		if c.err != nil {
			return nil, c.err
		}
		if nb < 0 || nb > maxBloomWords {
			return nil, corruptf("column %d bloom is %d words, above the %d ceiling", i, nb, maxBloomWords)
		}
		if !c.need(nb * 8) {
			return nil, c.err
		}
		m.Bloom = make([]uint64, nb)
		for j := 0; j < nb; j++ {
			m.Bloom[j] = c.u64()
		}
		m.DictOff = int(c.u32())
		m.DictLen2 = int(c.u32())
		if c.err != nil {
			return nil, c.err
		}
		if err := checkSpan(i, "data", m.DataOff, m.DataLen, dataEnd); err != nil {
			return nil, err
		}
		if err := checkSpan(i, "postings", m.PostOff, m.PostLen, dataEnd); err != nil {
			return nil, err
		}
		if err := checkSpan(i, "dict", m.DictOff, m.DictLen2, dataEnd); err != nil {
			return nil, err
		}
		if m.Width < 0 || m.Width > 32 {
			return nil, corruptf("column %d index width %d outside [0, 32]", i, m.Width)
		}
		if m.DictLen < 0 || m.DictLen > maxGroupRows {
			return nil, corruptf("column %d dictionary length %d outside [0, %d]", i, m.DictLen, maxGroupRows)
		}
		r.cols[i] = m
	}
	return r, nil
}

// checkSpan validates one offset/length pair against the region columns may
// occupy. Both the sum and the individual values are checked: off+len can
// wrap on a 32-bit int, and a wrapped sum passes a naive upper-bound test.
func checkSpan(col int, what string, off, length, limit int) error {
	if off < 0 || length < 0 {
		return corruptf("column %d %s span offset %d length %d is negative", col, what, off, length)
	}
	if off > limit || length > limit-off {
		return corruptf("column %d %s span [%d, %d) leaves the %d-byte data region", col, what, off, off+length, limit)
	}
	return nil
}
