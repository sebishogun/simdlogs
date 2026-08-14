package storage

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// The postings decoders read seventeen separate lengths straight out of the
// mapped file -- block counts, block size, compressed and raw extents, bit
// widths, intra-block offsets -- and validated none of them. The group
// checksum does not help: a repaired or partially rewritten file has a valid
// CRC and a wrong body, and v7 blobs carry no checksum at all. Sweeping
// twelve-byte corruptions across a real postings span at stride 1 with fills
// 0x00 and 0xff -- 7306 positions -- panicked at 96 of them before the
// guards: 12 "integer divide by zero" from an unguarded block size of zero,
// and the rest "slice bounds out of range" from four different unchecked
// lengths. Guarded, none do.

// postingsFixture builds a group whose dict column is wide enough to span
// several posting blocks, and returns the marshaled bytes plus the postings
// extent of the "level" column.
func postingsFixture(t *testing.T) (blob []byte, off, length int) {
	t.Helper()
	const rows = 4096
	vals := make([]string, rows)
	for i := range vals {
		// A handful of hot values plus a long tail, so both the dense and the
		// selective decode paths have something to read.
		switch {
		case i%3 == 0:
			vals[i] = "info"
		case i%3 == 1:
			vals[i] = "warn"
		default:
			vals[i] = "svc-" + string(rune('a'+i%23))
		}
	}
	ts := make([]int64, rows)
	for i := range ts {
		ts[i] = int64(i + 1)
	}
	d := BuildDict(vals)
	g := &Group{Rows: rows, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "level", Type: ColDict, Dict: &d},
	}}
	blob = g.Marshal()
	r, err := ReadGroup(blob)
	if err != nil {
		t.Fatalf("the fixture itself does not parse: %v", err)
	}
	for i := range r.cols {
		if r.cols[i].Name == "level" {
			if r.cols[i].PostLen == 0 {
				t.Fatal("the fixture has no postings to corrupt")
			}
			return blob, r.cols[i].PostOff, r.cols[i].PostLen
		}
	}
	t.Fatal("no level column")
	return nil, 0, 0
}

// reseal rewrites the trailing CRC so a corrupted body still parses, which is
// the state that matters: a checksum that agrees with a wrong body.
func reseal(b []byte) {
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crc32c))
}

// Every twelve-byte window of the postings span, zeroed and set to all-ones.
// Neither may panic, hang, or return rows outside the group.
func TestPostingsSurviveCorruptionSweep(t *testing.T) {
	t.Parallel()
	full, off, length := postingsFixture(t)
	const win = 12

	for _, fill := range []byte{0x00, 0xff} {
		for p := off; p+win <= off+length; p++ {
			b := make([]byte, len(full))
			copy(b, full)
			for i := 0; i < win; i++ {
				b[p+i] = fill
			}
			reseal(b)

			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("fill %#02x at offset %d panicked: %v", fill, p-off, r)
					}
				}()
				r, err := ReadGroup(b)
				if err != nil {
					return // rejected outright, which is also fine
				}
				for _, v := range []string{"info", "warn", "svc-a", "absent"} {
					rows, _ := r.EqualityRows("level", v)
					for _, row := range rows {
						if int(row) >= r.Rows {
							t.Fatalf("fill %#02x at offset %d: row %d outside a %d-row group",
								fill, p-off, row, r.Rows)
						}
					}
					r.EqualityCount("level", v)
				}
				// The whole-column sweep reads every id, not just four.
				r.ValueCounts("level")
			}()
		}
	}
}

// The decoders are also reachable with a blob that never came from Marshal:
// a v7 file, a truncated mapping, a header whose block size is zero. Feeding
// them directly pins the guards without depending on what the current writer
// happens to emit.
func TestPostingDecodersRejectHostileHeaders(t *testing.T) {
	u32 := func(vs ...uint32) []byte {
		b := make([]byte, 0, 4*len(vs))
		for _, v := range vs {
			b = binary.LittleEndian.AppendUint32(b, v)
		}
		return b
	}

	for _, c := range []struct {
		name string
		blob []byte
	}{
		{"empty", nil},
		{"one byte", []byte{1}},
		{"three bytes", []byte{1, 2, 3}},
		{"v7 header claiming a huge dict", u32(0x7fffffff)},
		{"v7 header, no offset table", u32(4)},
		{"v8 magic, nothing else", u32(postV8ForMagic)},
		{"v8 count length past the end", u32(postV8ForMagic, 4, 8, 0xffffffff)},
		{"v8 count width past the window", u32(postV8ForMagic, 4, 64, 0)},
		{"v8 FOR, block size zero", append(u32(postV8ForMagic, 4, 8, 4), u32(1, 0)...)},
		{"v8 FOR, block count huge", append(u32(postV8ForMagic, 4, 8, 4), u32(0x7fffffff, 4)...)},
		{"v8 LZ4, block size zero", append(u32(postV8Magic, 4, 8, 4), u32(1, 0)...)},
		{"v8 LZ4, comp length past the end", append(u32(postV8Magic, 4, 8, 4), u32(1, 4, 0, 0, 0, 0xffffffff)...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			for id := -1; id < 8; id++ {
				postCount(c.blob, id)
				for _, row := range postingRows(c.blob, id, 0) {
					_ = row
				}
			}
		})
	}
}
