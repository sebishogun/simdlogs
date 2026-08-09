package query

import (
	"math"
	"sort"
	"testing"
)

// TestQuantileBounded checks the thinning that bounds a quantile's sample
// buffer still yields an accurate quantile.
func TestQuantileBounded(t *testing.T) {
	const n = 500000
	full := make([]float64, n)
	for i := range full {
		full[i] = float64(i)
	}
	exact := quantileOf(append([]float64(nil), full...), 0.9)

	// Mimic accSample's halving thinning.
	v := append([]float64(nil), full...)
	for len(v) >= quantileCap*2 {
		sort.Float64s(v)
		j := 0
		for i := 0; i < len(v); i += 2 {
			v[j] = v[i]
			j++
		}
		v = v[:j]
	}
	if len(v) >= quantileCap*2 {
		t.Fatalf("sample not bounded: %d", len(v))
	}
	approx := quantileOf(v, 0.9)
	if math.Abs(approx-exact)/exact > 0.02 {
		t.Fatalf("thinned p90 = %v vs exact %v (>2%% off)", approx, exact)
	}
}
