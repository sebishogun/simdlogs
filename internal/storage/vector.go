package storage

import (
	"encoding/binary"
	"math"
)

// Vectors returns a ColVector column's dimension and its flat row-major
// float32 data (Rows*dim), read from the mmap'd blob. Zero dim if the column
// is absent or not a vector column.
func (r *Reader) Vectors(name string) (dim int, data []float32) {
	m := r.col(name)
	if m == nil || m.Type != ColVector || m.DataLen < 4 {
		return 0, nil
	}
	b := r.blob[m.DataOff : m.DataOff+m.DataLen]
	dim = int(binary.LittleEndian.Uint32(b))
	n := (len(b) - 4) / 4
	data = make([]float32, n)
	for i := 0; i < n; i++ {
		data[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4+i*4:]))
	}
	return dim, data
}

// Cosine is the cosine similarity of two equal-length vectors (0 if either is
// zero). qNorm is |q| precomputed by the caller so a k-NN scan does it once.
func Cosine(q, v []float32, qNorm float64) float64 {
	if len(q) != len(v) || qNorm == 0 {
		return 0
	}
	var dot, vn float64
	for i := range q {
		dot += float64(q[i]) * float64(v[i])
		vn += float64(v[i]) * float64(v[i])
	}
	if vn == 0 {
		return 0
	}
	return dot / (qNorm * math.Sqrt(vn))
}

// Norm is |v|.
func Norm(v []float32) float64 {
	var s float64
	for _, f := range v {
		s += float64(f) * float64(f)
	}
	return math.Sqrt(s)
}
