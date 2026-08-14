package storage

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// A checksum-valid group whose row count does not match its column data must
// be rejected at parse, not panic in a decoder.
//
// The v8 work validated the footer and stopped there. Every column decode is
// driven by Rows -- idxBytes computes ((Rows*Width+31)/32+1)*4 and slices that
// out of the data span -- so a group claiming a million rows over a 512-byte
// span sliced past the blob. On an mmap that reads adjacent mapped pages or
// takes SIGBUS rather than panicking, which is worse: a wrong answer instead
// of a crash.
func TestReadGroupRejectsRowCountBeyondColumnData(t *testing.T) {
	d := BuildDict([]string{"a", "b", "a", "c"})
	g := &Group{Rows: 4, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "k", Type: ColDict, Dict: &d},
	}}
	blob := g.Marshal()
	if _, err := ReadGroup(blob); err != nil {
		t.Fatalf("the fixture does not parse: %v", err)
	}

	for _, rows := range []uint32{1_000_000, 1 << 20, 1 << 24} {
		b := append([]byte(nil), blob...)
		binary.LittleEndian.PutUint32(b[8:], rows)
		// Repair the checksum, so the row check is what rejects it.
		binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crc32c))

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("rows=%d panicked: %v", rows, r)
				}
			}()
			r, err := ReadGroup(b)
			if err == nil {
				// If it parses, the decoders must still not read past the
				// blob -- exercise the one that used to.
				_ = r.DictIndicesRaw("k")
				t.Fatalf("rows=%d was accepted over a %d-byte blob", rows, len(b))
			}
		}()
	}
}

// parseDictSec had no bounds checks at all, and it is reached from
// needsRecompact -- which runs in the tiering goroutine, where a panic has no
// recover and kills the process rather than failing one request.
func TestParseDictSecRejectsCorruptCounts(t *testing.T) {
	for _, c := range []struct {
		name string
		sec  []byte
	}{
		{"too short", []byte{1, 2, 3}},
		{"block count enormous", func() []byte {
			b := make([]byte, 64)
			binary.LittleEndian.PutUint32(b[0:], 0xFFFFFFF0)
			return b
		}()},
		{"block count past the section", func() []byte {
			b := make([]byte, 64)
			binary.LittleEndian.PutUint32(b[0:], 1000)
			return b
		}()},
		{"first-value length past the section", func() []byte {
			b := make([]byte, 64)
			binary.LittleEndian.PutUint32(b[0:], 1)
			binary.LittleEndian.PutUint32(b[16:], 0xFFFFFF)
			return b
		}()},
		{"all zeroes", make([]byte, 64)},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			got := parseDictSec(c.sec)
			// A rejected section reads as empty; the callers all treat it so.
			if got.numBlocks != 0 && len(got.idx) != got.numBlocks*12 {
				t.Fatalf("accepted an inconsistent section: numBlocks=%d idx=%d bytes",
					got.numBlocks, len(got.idx))
			}
		})
	}
}

// needsRecompact walks every column's dict section, so it is the path a
// corrupt file reaches from the background tiering loop. It must not panic on
// any of the corrupt shapes above.
func TestNeedsRecompactSurvivesCorruptDictSections(t *testing.T) {
	d := BuildDict([]string{"x", "y"})
	g := &Group{Rows: 2, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: []int64{1, 2}},
		{Name: "k", Type: ColDict, Dict: &d},
	}}
	blob := g.Marshal()
	r, err := ReadGroup(blob)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the dict section's block count in place, behind the reader's
	// back, which is what a bit flip on disk looks like to a live mapping.
	for _, m := range r.cols {
		if m.Type != ColDict || m.DictLen2 < 8 {
			continue
		}
		binary.LittleEndian.PutUint32(r.blob[m.DictOff:], 0xFFFFFF)
	}
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("needsRecompact panicked on a corrupt dict section: %v", rec)
		}
	}()
	_ = r.needsRecompact()
}
