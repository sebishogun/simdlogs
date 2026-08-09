package storage

import (
	"encoding/binary"
	"errors"
	"math"
)

// A Group is an immutable columnar row group. On disk it is header +
// column data + footer; the footer's per-column skip metadata is what
// lets a query touch 128K rows where the reference touches 8M.
//
//	header: magic u32, version u32, rows u32, columns u32
//	column: [name][type][width][rows][data]
//	footer: timeMin i64, timeMax i64, per-column meta, footer-len u32
//
// The design's granularity (64-128K rows) is the whole point: skip
// structures at this size vs the reference's 8M-row blocks.

const (
	magic   = 0x736C6F67 // "slog"
	version = 1
	// MaxRows is the group ceiling; ingest flushes at or before it.
	MaxRows = 128 * 1024
)

// Column is one built column ready to encode.
type Column struct {
	Name string
	Type ColumnType
	Dict *DictColumn // for ColDict
	Ts   []int64     // for ColTimestamp
}

// Group is the in-memory form: columns plus the row count.
type Group struct {
	Rows    int
	Columns []Column
}

// colMeta is the footer's per-column skip record.
type colMeta struct {
	Name     string
	Type     ColumnType
	Width    int
	DictLen  int
	MinIdx   uint32 // for dict: min/max index present -> min/max value via dict
	MaxIdx   uint32
	Bloom    []uint64 // dict-value bloom for equality skip
	DataOff  int
	DataLen  int
	PostOff  int
	PostLen  int
	DictData []string
}

// Marshal serializes the group to a self-describing blob.
func (g *Group) Marshal() []byte {
	var b []byte
	b = appU32(b, magic)
	b = appU32(b, version)
	b = appU32(b, uint32(g.Rows))
	b = appU32(b, uint32(len(g.Columns)))

	metas := make([]colMeta, len(g.Columns))
	var timeMin, timeMax int64 = math.MaxInt64, math.MinInt64
	for ci, c := range g.Columns {
		m := colMeta{Name: c.Name, Type: c.Type}
		m.DataOff = len(b)
		switch c.Type {
		case ColDict:
			m.Width = bitWidth(len(c.Dict.Dict))
			m.DictLen = len(c.Dict.Dict)
			m.DictData = c.Dict.Dict
			data := encodeIndices(c.Dict.Indices, m.Width)
			b = append(b, data...)
			mn, mx := uint32(0), uint32(0)
			for i, ix := range c.Dict.Indices {
				if i == 0 || ix < mn {
					mn = ix
				}
				if i == 0 || ix > mx {
					mx = ix
				}
			}
			m.MinIdx, m.MaxIdx = mn, mx
			m.Bloom = buildDictBloom(c.Dict.Dict)
			m.PostOff = len(b)
			b = buildPostings(c.Dict.Indices, len(c.Dict.Dict)).marshal(b)
			m.PostLen = len(b) - m.PostOff
		case ColTimestamp:
			data := encodeTimestamps(c.Ts)
			b = append(b, data...)
			for _, t := range c.Ts {
				if t < timeMin {
					timeMin = t
				}
				if t > timeMax {
					timeMax = t
				}
			}
		}
		m.DataLen = len(b) - m.DataOff
		metas[ci] = m
	}
	if g.Rows == 0 {
		timeMin, timeMax = 0, 0
	}

	footStart := len(b)
	b = appI64(b, timeMin)
	b = appI64(b, timeMax)
	for _, m := range metas {
		b = appStr(b, m.Name)
		b = append(b, byte(m.Type))
		b = appU32(b, uint32(m.Width))
		b = appU32(b, uint32(m.DictLen))
		b = appU32(b, m.MinIdx)
		b = appU32(b, m.MaxIdx)
		b = appU32(b, uint32(m.DataOff))
		b = appU32(b, uint32(m.DataLen))
		b = appU32(b, uint32(m.PostOff))
		b = appU32(b, uint32(m.PostLen))
		b = appU32(b, uint32(len(m.Bloom)))
		for _, w := range m.Bloom {
			b = appU64(b, w)
		}
		for _, s := range m.DictData {
			b = appStr(b, s)
		}
	}
	b = appU32(b, uint32(len(b)-footStart))
	return b
}

var errCorrupt = errors.New("storage: corrupt group")

// ReadGroup parses a marshaled group. The footer is read first (its
// length is the last four bytes) so a query can consult skip metadata
// without decoding any column.
func ReadGroup(b []byte) (*Reader, error) {
	if len(b) < 20 {
		return nil, errCorrupt
	}
	if get32(b, 0) != magic || get32(b, 4) != version {
		return nil, errCorrupt
	}
	r := &Reader{blob: b, Rows: int(get32(b, 8))}
	ncol := int(get32(b, 12))
	flen := int(get32(b, len(b)-4))
	if flen > len(b)-4 {
		return nil, errCorrupt
	}
	f := b[len(b)-4-flen : len(b)-4]
	r.TimeMin = int64(binary.LittleEndian.Uint64(f[0:]))
	r.TimeMax = int64(binary.LittleEndian.Uint64(f[8:]))
	p := 16
	r.cols = make([]colMeta, ncol)
	for i := 0; i < ncol; i++ {
		var m colMeta
		m.Name, p = getStr(f, p)
		m.Type = ColumnType(f[p])
		p++
		m.Width = int(get32(f, p))
		p += 4
		m.DictLen = int(get32(f, p))
		p += 4
		m.MinIdx = get32(f, p)
		p += 4
		m.MaxIdx = get32(f, p)
		p += 4
		m.DataOff = int(get32(f, p))
		p += 4
		m.DataLen = int(get32(f, p))
		p += 4
		m.PostOff = int(get32(f, p))
		p += 4
		m.PostLen = int(get32(f, p))
		p += 4
		nb := int(get32(f, p))
		p += 4
		m.Bloom = make([]uint64, nb)
		for j := 0; j < nb; j++ {
			m.Bloom[j] = binary.LittleEndian.Uint64(f[p:])
			p += 8
		}
		m.DictData = make([]string, m.DictLen)
		for j := 0; j < m.DictLen; j++ {
			m.DictData[j], p = getStr(f, p)
		}
		r.cols[i] = m
	}
	return r, nil
}

func appU32(b []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(b, v) }
func appU64(b []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(b, v) }
func appI64(b []byte, v int64) []byte  { return binary.LittleEndian.AppendUint64(b, uint64(v)) }
func appStr(b []byte, s string) []byte {
	b = appU32(b, uint32(len(s)))
	return append(b, s...)
}
func get32(b []byte, at int) uint32 { return binary.LittleEndian.Uint32(b[at:]) }
func getStr(b []byte, at int) (string, int) {
	n := int(get32(b, at))
	at += 4
	return string(b[at : at+n]), at + n
}
