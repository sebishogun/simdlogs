package corpus

import (
	"hash/fnv"
	"testing"
)

func TestDeterministic(t *testing.T) {
	run := func() uint64 {
		h := fnv.New64a()
		Gen(42, 10000, func(r Record) {
			h.Write([]byte(r.Level))
			h.Write([]byte(r.Service))
			h.Write([]byte(r.Message))
			var b [8]byte
			for i, x := 0, r.Time.UnixNano(); i < 8; i++ {
				b[i] = byte(x >> (8 * i))
			}
			h.Write(b[:])
		})
		return h.Sum64()
	}
	if run() != run() {
		t.Fatal("corpus is not deterministic at a fixed seed")
	}
}

func TestMonotonicTime(t *testing.T) {
	var last int64
	Gen(1, 5000, func(r Record) {
		if n := r.Time.UnixNano(); n < last {
			t.Fatalf("time went backwards: %d < %d", n, last)
		} else {
			last = n
		}
	})
}
