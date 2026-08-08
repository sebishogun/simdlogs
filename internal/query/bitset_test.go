package query

import (
	"math/rand"
	"testing"
)

func TestBitsetOps(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{1, 63, 64, 65, 1000, 128 * 1024} {
		a := NewBitset(n)
		b := NewBitset(n)
		refA := make([]bool, n)
		refB := make([]bool, n)
		for i := 0; i < n; i++ {
			if rng.Intn(2) == 0 {
				a.Set(i)
				refA[i] = true
			}
			if rng.Intn(2) == 0 {
				b.Set(i)
				refB[i] = true
			}
		}
		and := NewBitset(n)
		and.SetAll()
		and.And(a)
		and.And(b)
		for i := 0; i < n; i++ {
			if and.Test(i) != (refA[i] && refB[i]) {
				t.Fatalf("n=%d AND[%d]", n, i)
			}
		}
		// ForEach visits exactly the set bits of a.
		seen := make([]bool, n)
		cnt := 0
		a.ForEach(func(i int) { seen[i] = true; cnt++ })
		if cnt != a.Count() {
			t.Fatalf("n=%d ForEach count %d != Count %d", n, cnt, a.Count())
		}
		for i := 0; i < n; i++ {
			if seen[i] != refA[i] {
				t.Fatalf("n=%d ForEach[%d]", n, i)
			}
		}
	}
}
