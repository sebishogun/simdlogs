package query

import (
	"container/heap"
	"fmt"
	"strconv"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Vector search: the k rows whose `field` embedding is most cosine-similar to
// a query vector.
//
// Semantic log search, which VictoriaLogs does not offer. Brute-force k-NN --
// exact; an ANN index is a future optimization. Embeddings are bring-your-own:
// logs carry a vector column and this ranks them.
//
// # Why a heap and not a sort
//
// It used to score every vector in the window into one slice, sort the slice,
// and take the first k. The sort is not the problem -- the SLICE is. A window
// holding ten million embeddings built a ten-million-entry candidate list to
// return ten rows, so the memory was proportional to the corpus and not to the
// answer, and the one thing a caller controls (k) had no effect on it at all.
//
// A bounded min-heap of exactly k entries makes the memory proportional to the
// answer. The heap's root is the WEAKEST kept candidate, so a new score is
// compared against it in one branch and discarded without touching the heap in
// the common case -- which on a large corpus is nearly every row. Rows are
// materialized for the k survivors only; materializing every candidate was the
// other half of the cost.
//
// # Why every ceiling here is separate
//
// k bounds the answer. MaxCandidates bounds the SCAN, which k does not: a
// query for the top 10 of a billion vectors still reads a billion of them, and
// "the answer is small" is not "the query is cheap". MaxDim bounds one
// comparison. MaxResultBytes bounds the materialized rows, which k does not
// either -- ten rows carrying megabyte payloads is a small answer by every
// other measure. Each is a different quantity, and a limit is only a limit on
// the quantity it is expressed in.

// VectorLimits bounds one vector search. The zero value is unbounded, which is
// what an internal caller with its own accounting wants; the HTTP layer fills
// it from configuration.
type VectorLimits struct {
	// MaxK bounds how many rows may be asked for.
	MaxK int

	// MaxDim bounds the query vector's dimension, and so the cost of one
	// comparison. The query vector is client-supplied and every stored vector
	// is compared against it.
	MaxDim int

	// MaxCandidates bounds how many stored vectors are scored. Distinct from
	// MaxK: the top 10 of a billion still reads a billion.
	MaxCandidates int

	// MaxResultBytes bounds the materialized result.
	MaxResultBytes int64
}

// ErrVectorSearch is a vector search refused by its own ceilings. It wraps
// ErrRowLimit, so the HTTP layer's existing 413 mapping covers it without a
// new branch.
var ErrVectorSearch = fmt.Errorf("%w: vector search", ErrRowLimit)

// candidate is one scored row. The reader pointer is held rather than the row,
// because the row is materialized only for the k that survive.
type candidate struct {
	score float64
	g     *storage.Reader
	row   int
}

// topK is a min-heap of the best candidates so far: the WEAKEST is at the
// root, so one comparison against it decides whether a new score is kept.
type topK []candidate

func (h topK) Len() int           { return len(h) }
func (h topK) Less(a, b int) bool { return h[a].score < h[b].score }
func (h topK) Swap(a, b int)      { h[a], h[b] = h[b], h[a] }
func (h *topK) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *topK) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// VectorSearch returns the k rows most similar to q, best first.
//
// budget carries the deadline, the context and the stop flag; nil means
// unbounded, which no HTTP caller should pass. A refusal is recorded on the
// budget as a typed error, so the caller reports it the way it reports every
// other limit rather than seeing a short answer.
func VectorSearch(s Store, from, to int64, field string, q []float32, k int,
	budget *Query, lim VectorLimits,
) []Row {
	if k <= 0 {
		k = 10
	}
	if lim.MaxK > 0 && k > lim.MaxK {
		stopVector(budget, fmt.Errorf("%w: k=%d exceeds the ceiling of %d",
			ErrVectorSearch, k, lim.MaxK))
		return nil
	}
	if lim.MaxDim > 0 && len(q) > lim.MaxDim {
		stopVector(budget, fmt.Errorf("%w: a %d-dimension query vector exceeds the ceiling of %d",
			ErrVectorSearch, len(q), lim.MaxDim))
		return nil
	}
	if len(q) == 0 {
		return nil
	}

	qn := storage.Norm(q)
	h := make(topK, 0, k)
	var scanned int64

	sn := snapshotOf(s, from, to)
	defer sn.Close()
	for _, g := range sn.Groups {
		// The deadline and cancellation, per group. This path reads every
		// vector in the window.
		if budget != nil && budget.exceeded(0) {
			break
		}
		dim, data := g.Vectors(field)
		// A dimension mismatch skips the group rather than failing: a store
		// can hold groups written before the field was reconfigured, and
		// comparing vectors of different lengths is not a worse answer, it is
		// undefined.
		if dim == 0 || dim != len(q) {
			continue
		}
		n := len(data) / dim
		if lim.MaxCandidates > 0 && scanned+int64(n) > int64(lim.MaxCandidates) {
			stopVector(budget, fmt.Errorf(
				"%w: more than %d stored vectors are in the window; narrow it or raise the ceiling",
				ErrVectorSearch, lim.MaxCandidates))
			return nil
		}
		scanned += int64(n)
		for i := 0; i < n; i++ {
			sc := storage.Cosine(q, data[i*dim:(i+1)*dim], qn)
			if len(h) < k {
				heap.Push(&h, candidate{sc, g, i})
				continue
			}
			// The common case on a large corpus: one comparison against the
			// weakest kept candidate, and nothing is allocated or moved.
			if sc <= h[0].score {
				continue
			}
			h[0] = candidate{sc, g, i}
			heap.Fix(&h, 0)
		}
	}
	if budget != nil && budget.stopErr() != nil {
		return nil
	}

	// Best first: pop the weakest repeatedly and fill the output backwards.
	out := make([]Row, len(h))
	var bytes int64
	for i := len(h) - 1; i >= 0; i-- {
		c := heap.Pop(&h).(candidate)
		row := materializeVectorRow(c, field)
		if lim.MaxResultBytes > 0 {
			bytes += rowBytes(row)
			if bytes > lim.MaxResultBytes {
				stopVector(budget, fmt.Errorf("%w: the result passed %d bytes",
					ErrVectorSearch, lim.MaxResultBytes))
				return nil
			}
		}
		out[i] = row
	}
	return out
}

// materializeVectorRow builds the answer row for one candidate. Only the k
// survivors reach here, which is the point of the heap.
func materializeVectorRow(c candidate, field string) Row {
	t, _ := c.g.TimestampAt("_time", c.row)
	row := Row{Time: t, Fields: []Field{{"_score", strconv.FormatFloat(c.score, 'f', 4, 64)}}}
	for _, f := range c.g.ColumnNames() {
		// The embedding is not echoed back: it is what the caller sent, it is
		// the largest thing on the row, and DictValueAt cannot read it anyway.
		if f == "_time" || f == field {
			continue
		}
		if v, ok := c.g.DictValueAt(f, c.row); ok {
			row.Fields = append(row.Fields, Field{f, v})
		}
	}
	return row
}

// stopVector records a refusal on the caller's budget. A nil budget is an
// internal caller with its own accounting, for which the empty result is the
// whole answer.
func stopVector(budget *Query, err error) {
	if budget != nil {
		budget.stop(err)
	}
}
