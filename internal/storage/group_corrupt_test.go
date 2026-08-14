package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// goodGroup is a representative marshaled group: a timestamp column and two
// dictionary columns, enough to exercise the per-column footer walk.
func goodGroup(t *testing.T) []byte {
	t.Helper()
	d1 := BuildDict([]string{"info", "warn", "info", "error"})
	d2 := BuildDict([]string{"api", "api", "db", "api"})
	g := &Group{Rows: 4, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{10, 20, 30, 40}},
		{Name: "level", Type: ColDict, Dict: &d1},
		{Name: "service", Type: ColDict, Dict: &d2},
	}}
	b := g.Marshal()
	if _, err := ReadGroup(b); err != nil {
		t.Fatalf("the fixture itself does not parse: %v", err)
	}
	return b
}

// Truncation at every single byte offset. The parser must return an error at
// each one; a panic here is a corrupt file taking down the process, which is
// exactly what an unchecked slice index does.
func TestReadGroupTruncationAtEveryOffset(t *testing.T) {
	full := goodGroup(t)
	for n := 0; n < len(full); n++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("truncated to %d bytes panicked: %v", n, r)
				}
			}()
			if _, err := ReadGroup(full[:n]); err == nil {
				t.Fatalf("truncated to %d of %d bytes parsed without error", n, len(full))
			}
		}()
	}
}

// A single flipped bit anywhere must be caught by the v8 checksum. Sampling
// every byte position at one bit each keeps the test quick while covering
// header, column data and footer alike.
func TestReadGroupDetectsSingleBitFlips(t *testing.T) {
	full := goodGroup(t)
	for i := 0; i < len(full); i++ {
		b := make([]byte, len(full))
		copy(b, full)
		b[i] ^= 0x01
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("bit flip at byte %d panicked: %v", i, r)
				}
			}()
			if _, err := ReadGroup(b); err == nil {
				t.Fatalf("bit flip at byte %d parsed without error", i)
			}
		}()
	}
}

// Structural corruption with the checksum repaired afterwards, so the parser
// cannot pass by rejecting the CRC alone. These are the cases that used to
// index straight past the end of the slice.
func TestReadGroupRejectsStructuralCorruption(t *testing.T) {
	for _, c := range []struct {
		name   string
		mutate func(b []byte) []byte
	}{
		{"column count enormous", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[12:], 0xFFFFFFF0)
			return b
		}},
		{"column count large but under the ceiling", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[12:], 60000)
			return b
		}},
		{"row count enormous", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[8:], 0xFFFFFFFF)
			return b
		}},
		{"footer length enormous", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[len(b)-8:], 0xFFFFFFF0)
			return b
		}},
		{"footer length zero", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[len(b)-8:], 0)
			return b
		}},
		{"footer longer than the blob", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[len(b)-8:], uint32(len(b)))
			return b
		}},
		{"magic wrong", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[0:], 0xDEADBEEF)
			return b
		}},
		{"version unknown", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[4:], 99)
			return b
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := c.mutate(append([]byte(nil), goodGroup(t)...))
			// Repair the checksum so the structural check is what rejects it.
			binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crc32c))
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			if _, err := ReadGroup(b); err == nil {
				t.Fatal("parsed without error")
			} else if !errors.Is(err, errCorrupt) {
				t.Fatalf("err %v does not unwrap to errCorrupt", err)
			}
		})
	}
}

// A v8 blob whose checksum does not match its body is rejected before any
// structure is parsed.
func TestReadGroupChecksumMismatch(t *testing.T) {
	b := append([]byte(nil), goodGroup(t)...)
	binary.LittleEndian.PutUint32(b[len(b)-4:], 0)
	_, err := ReadGroup(b)
	if err == nil {
		t.Fatal("a wrong checksum parsed without error")
	}
	if !errors.Is(err, errCorrupt) {
		t.Fatalf("err %v does not unwrap to errCorrupt", err)
	}
}

// The writer emits v8 and the reader round-trips it, checksum included.
func TestWriterEmitsV8(t *testing.T) {
	b := goodGroup(t)
	if v := get32(b, 4); v != versionV8 {
		t.Fatalf("writer emitted version %d, want %d", v, versionV8)
	}
	want := crc32.Checksum(b[:len(b)-4], crc32c)
	if got := binary.LittleEndian.Uint32(b[len(b)-4:]); got != want {
		t.Fatalf("trailing checksum %#08x, want %#08x", got, want)
	}
	r, err := ReadGroup(b)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows != 4 || r.TimeMin != 10 || r.TimeMax != 40 {
		t.Fatalf("rows %d time [%d, %d]", r.Rows, r.TimeMin, r.TimeMax)
	}
}

// FuzzReadGroup: no input may panic, and no input may make the parser
// allocate without bound. The seeds are the real fixtures -- both the v7
// goldens and a fresh v8 -- so the fuzzer starts from structurally valid
// blobs and mutates outward.
func FuzzReadGroup(f *testing.F) {
	d := BuildDict([]string{"a", "b", "a"})
	g := &Group{Rows: 3, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{1, 2, 3}},
		{Name: "k", Type: ColDict, Dict: &d},
	}}
	f.Add(g.Marshal())
	f.Add([]byte{})
	f.Add(make([]byte, 20))

	if ents, err := os.ReadDir(filepath.Join("testdata", "v7")); err == nil {
		for _, e := range ents {
			if filepath.Ext(e.Name()) != ".bin" {
				continue
			}
			if b, err := os.ReadFile(filepath.Join("testdata", "v7", e.Name())); err == nil {
				f.Add(b)
			}
		}
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := ReadGroup(b)
		if err != nil {
			if r != nil {
				t.Fatal("a failed parse returned a non-nil reader")
			}
			return
		}
		// A parse that succeeded must have produced self-consistent metadata:
		// anything it reports is about to be used as a slice bound by the
		// column decoders.
		if r.Rows < 0 || r.Rows > maxGroupRows {
			t.Fatalf("accepted row count %d", r.Rows)
		}
		if len(r.cols) > maxGroupColumns {
			t.Fatalf("accepted %d columns", len(r.cols))
		}
		for i, m := range r.cols {
			if m.DataOff < 0 || m.DataLen < 0 || m.DataOff+m.DataLen > len(b) {
				t.Fatalf("column %d accepted data span [%d, %d) in a %d-byte blob", i, m.DataOff, m.DataOff+m.DataLen, len(b))
			}
			if m.PostOff < 0 || m.PostLen < 0 || m.PostOff+m.PostLen > len(b) {
				t.Fatalf("column %d accepted postings span [%d, %d) in a %d-byte blob", i, m.PostOff, m.PostOff+m.PostLen, len(b))
			}
			if m.DictOff < 0 || m.DictLen2 < 0 || m.DictOff+m.DictLen2 > len(b) {
				t.Fatalf("column %d accepted dict span [%d, %d) in a %d-byte blob", i, m.DictOff, m.DictOff+m.DictLen2, len(b))
			}
		}
	})
}
