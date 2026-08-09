package query

import "math"

// hyperLogLog is a fixed-precision HLL for count_uniq: it estimates distinct
// cardinality in a bounded 2^p bytes regardless of how many values it sees, so
// count_uniq over a high-cardinality field at billion-row scale can no longer
// OOM the way an exact set does. p=14 -> 16 KB per sketch, ~0.81% standard
// error. uniq (which must return the values) stays exact; only the count does.
type hyperLogLog struct {
	reg []uint8
}

const hllP = 14
const hllM = 1 << hllP

func newHLL() *hyperLogLog { return &hyperLogLog{reg: make([]uint8, hllM)} }

// add folds a value in. The top p bits pick the register; the rank is 1 + the
// number of leading zeros in the remaining bits.
func (h *hyperLogLog) add(v string) {
	x := hash64(v)
	idx := x >> (64 - hllP)
	rank := uint8(leadingZeros64(x<<hllP) + 1)
	if rank > h.reg[idx] {
		h.reg[idx] = rank
	}
}

// count is the bias-corrected harmonic-mean estimate, with the small- and
// large-range corrections from the HLL paper.
func (h *hyperLogLog) count() int {
	sum := 0.0
	zeros := 0
	for _, r := range h.reg {
		sum += 1.0 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	m := float64(hllM)
	est := hllAlpha * m * m / sum
	switch {
	case est <= 2.5*m && zeros > 0: // small range: linear counting
		est = m * math.Log(m/float64(zeros))
	case est > (1.0/30.0)*4294967296.0: // large range correction (2^32)
		est = -4294967296.0 * math.Log(1-est/4294967296.0)
	}
	return int(est + 0.5)
}

// hllAlpha is the bias constant for m = 2^14.
const hllAlpha = 0.7213 / (1.0 + 1.079/float64(hllM))

// hash64 is FNV-1a followed by a splitmix64 finalizer, so the high bits (which
// pick the HLL register) are well mixed -- plain FNV alone distributes poorly
// there and skews the estimate.
func hash64(s string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

func leadingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	for x&(uint64(1)<<63) == 0 {
		n++
		x <<= 1
	}
	return n
}
