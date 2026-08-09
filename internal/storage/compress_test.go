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

// The footprint win: a group's messages compress well below their raw
// size, the whole reason LZ4 is the default codec.
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
	t.Logf("100K rows: raw messages %d KB, group on disk %d KB (%.2fx)",
		raw/1024, len(blob)/1024, float64(raw)/float64(len(blob)))
	// Sanity: the compressed group is smaller than the raw message bytes.
	if len(blob) >= raw {
		t.Fatalf("group %d not smaller than raw messages %d", len(blob), raw)
	}
}
