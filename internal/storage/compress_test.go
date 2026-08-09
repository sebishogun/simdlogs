package storage

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/sebishogun/simdlogs/internal/bench/corpus"
)

func TestLZ4RoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	corpora := [][]byte{
		nil, []byte("a"), bytes.Repeat([]byte("log message repeated "), 500),
	}
	msgs := make([]byte, 0, 200000)
	corpus.Gen(1, 4000, func(r corpus.Record) { msgs = append(msgs, r.Message...) })
	corpora = append(corpora, msgs)
	rnd := make([]byte, 40000)
	rng.Read(rnd)
	corpora = append(corpora, rnd)
	for ci, src := range corpora {
		comp := lz4Compress(src)
		got := lz4Decompress(comp, len(src))
		if !bytes.Equal(got, src) {
			t.Fatalf("corpus %d: round trip failed (len %d -> comp %d)", ci, len(src), len(comp))
		}
	}
}

// Footprint and round-trip. The dictionary is stored uncompressed for
// random access from the mmap (a membership probe must not decompress the
// whole dict), so a group is no longer guaranteed smaller than its raw
// messages -- footprint was traded for the scale property. This checks the
// group stays within a sane bound and round-trips correctly. Block-
// compressing the dict string data (compress + a block-min index for
// random access) is the footprint follow-up; see the perf-max task.
func TestGroupFootprint(t *testing.T) {
	n := 100_000
	var ts []int64
	var msgs []string
	raw := 0
	corpus.Gen(2, n, func(r corpus.Record) {
		ts = append(ts, r.Time.UnixNano())
		msgs = append(msgs, r.Message)
		raw += len(r.Message)
	})
	md := BuildDict(msgs)
	g := &Group{Rows: n, Columns: []Column{
		{Name: "_time", Type: ColTimestamp, Ts: ts},
		{Name: "_msg", Type: ColDict, Dict: &md},
	}}
	blob := g.Marshal()
	t.Logf("100K rows: raw messages %d KB, group on disk %d KB (%.2fx of raw)",
		raw/1024, len(blob)/1024, float64(len(blob))/float64(raw))
	// Loose sanity: dedup + bitpacking keep it within ~2x raw even
	// uncompressed; a blowup past that is a real regression.
	if len(blob) > 2*raw {
		t.Fatalf("group %d bytes exceeds 2x raw messages %d", len(blob), raw)
	}
	// Round-trip: a materialized value matches the source through the mmap
	// dict path.
	r, err := ReadGroup(blob)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := r.DictValueAt("_msg", 12345); !ok || v != msgs[12345] {
		t.Fatalf("DictValueAt(12345)=%q,%v want %q", v, ok, msgs[12345])
	}
	if id := r.DictID("_msg", msgs[777]); id < 0 {
		t.Fatalf("DictID for a present message returned %d", id)
	}
}
