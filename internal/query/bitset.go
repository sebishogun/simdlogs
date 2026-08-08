// Package query is the vectorized execution engine: bitsets for filter
// composition, a planner that skips groups from footer stats, and scans
// that run on the simd kernels. The reference's anti-patterns are cited
// where this replaces them.
package query

import (
	"math/bits"
	"unsafe"

	"github.com/sebishogun/simd"
)

// Bitset is one bit per row of a group. Filters produce bitsets and
// compose them with word-wise vector ops; iteration is a tzcnt loop, not
// the reference's 64-step shift-and-branch (bitmap.go:128).
type Bitset struct {
	words []uint64
	n     int
}

// NewBitset makes a bitset for n rows, all clear.
func NewBitset(n int) *Bitset {
	return &Bitset{words: make([]uint64, (n+63)/64), n: n}
}

// SetAll sets every row (the starting point for AND-composition).
func (b *Bitset) SetAll() {
	for i := range b.words {
		b.words[i] = ^uint64(0)
	}
	if r := b.n & 63; r != 0 {
		b.words[len(b.words)-1] = 1<<uint(r) - 1
	}
}

func (b *Bitset) Set(i int)       { b.words[i>>6] |= 1 << (i & 63) }
func (b *Bitset) Test(i int) bool { return b.words[i>>6]&(1<<(i&63)) != 0 }

// And/Or/AndNot compose filters through simd's byte bit-ops over a byte
// view of the words -- bitwise AND is the same operation at any element
// width, so the u64 words alias as bytes without a copy.
func (b *Bitset) And(o *Bitset)    { simd.And(b.bytes(), o.bytes()) }
func (b *Bitset) Or(o *Bitset)     { simd.Or(b.bytes(), o.bytes()) }
func (b *Bitset) AndNot(o *Bitset) { simd.AndNot(b.bytes(), o.bytes()) }

func (b *Bitset) bytes() []byte {
	if len(b.words) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&b.words[0])), len(b.words)*8)
}

// Count is the popcount over the words.
func (b *Bitset) Count() int {
	total := 0
	for _, w := range b.words {
		total += bits.OnesCount64(w)
	}
	return total
}

// ForEach calls fn with each set row index, advancing by trailing-zeros
// -- one instruction per set bit, not one branch per bit position.
func (b *Bitset) ForEach(fn func(int)) {
	for wi, w := range b.words {
		for w != 0 {
			fn(wi<<6 + bits.TrailingZeros64(w))
			w &= w - 1
		}
	}
}
