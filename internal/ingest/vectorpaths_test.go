package ingest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A configured embedding survives EVERY ingest protocol, or the record is
// refused with a reason.
//
// Only the JSON-lines parser ever built the float path. Writer.Add drops a
// configured vector field arriving as an ordinary string -- correctly, since
// storing 768 floats as dictionary text is the worst case for a dictionary --
// and every other protocol reached it. The line was stored, the client saw 2xx,
// and the row was invisible to the vector search it was ingested for. Measured
// before the fix: the same embedding through /insert/jsonline is
// `dim=4 data=[1 2 3 4]` in the store, and through logfmt the column is absent,
// both at accepted=1 rejected=0.
//
// Writer.ValidateVector's doc comment described exactly this failure -- "so the
// PARSE path refuses the record, with a reason, counted in Result.Rejected,
// rather than the writer silently zero-filling it" -- and it had no caller.
// Found by the unwired-mechanism gate, which is what that gate is for. The
// function is gone: splitVectors does its job and keeps the parsed vector too.

func vecWriter(t *testing.T) (*Writer, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(st)
	w.SetVectorFields(VectorFields{"embedding": 4})
	return w, st
}

// storedVector returns the embedding the store holds, or nil.
func storedVector(t *testing.T, w *Writer, st *storage.Store) []float32 {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	sn, err := st.Snapshot(0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Close()
	for _, g := range sn.Groups {
		if dim, data := g.Vectors("embedding"); dim > 0 {
			return data
		}
	}
	return nil
}

func ts1() int64 { return 1 }

func TestEveryProtocolStoresAConfiguredEmbedding(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(w *Writer) (Result, error)
	}{
		{"jsonline", func(w *Writer) (Result, error) {
			return IngestJSONLines(w, []byte(`{"_msg":"hi","embedding":[1,2,3,4]}`+"\n"), ts1)
		}},
		{"logfmt", func(w *Writer) (Result, error) {
			return IngestLogfmt(w, []byte(`_msg=hi embedding="[1,2,3,4]"`+"\n"), ts1)
		}},
		{"loki", func(w *Writer) (Result, error) {
			return IngestLoki(w, []byte(`{"streams":[{"stream":{"embedding":"[1,2,3,4]"},`+
				`"values":[["1","hi"]]}]}`), ts1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, st := vecWriter(t)
			res, err := tc.run(w)
			if err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if res.Accepted != 1 || res.Rejected != 0 {
				t.Fatalf("accepted=%d rejected=%d, want 1/0 (warnings %v)",
					res.Accepted, res.Rejected, res.Warnings)
			}
			got := storedVector(t, w, st)
			if got == nil {
				t.Fatalf("the record was accepted and its embedding is NOT in the store: " +
					"the row is invisible to the vector search it was ingested for")
			}
			want := []float32{1, 2, 3, 4}
			if len(got) != len(want) {
				t.Fatalf("stored %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("stored %v, want %v", got, want)
					break
				}
			}
		})
	}
}

// An UNUSABLE embedding is refused with a reason, not stored without it.
//
// This was ValidateVector's documented contract and is now enforced: a record
// the caller can fix beats a row nobody can find and nobody was told about.
func TestAnUnusableEmbeddingIsRefusedWithAReason(t *testing.T) {
	for _, tc := range []struct{ name, text string }{
		{"wrong dimension", `[1,2,3]`},
		{"too many", `[1,2,3,4,5]`},
		{"not an array", `hello`},
		{"not numbers", `["a","b","c","d"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, st := vecWriter(t)
			res, err := IngestLogfmt(w,
				[]byte(fmt.Sprintf(`_msg=hi embedding=%q`+"\n", tc.text)), ts1)
			if err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if res.Rejected != 1 || res.Accepted != 0 {
				t.Fatalf("accepted=%d rejected=%d, want 0/1 -- an unusable embedding was "+
					"stored without it", res.Accepted, res.Rejected)
			}
			if len(res.Warnings) == 0 {
				t.Error("the refusal carries no reason, so the client cannot fix it")
			}
			if v := storedVector(t, w, st); v != nil {
				t.Errorf("the refused record left %v in the store", v)
			}
		})
	}
}

// A deployment with NO vector fields configured is untouched -- the guard must
// not change any behaviour for the overwhelming majority that configure none.
func TestNoVectorFieldsConfiguredChangesNothing(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(st)
	// A field that WOULD be an embedding if one were configured.
	res, err := IngestLogfmt(w, []byte(`_msg=hi embedding="[1,2,3]"`+"\n"), ts1)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Accepted != 1 || res.Rejected != 0 {
		t.Errorf("accepted=%d rejected=%d, want 1/0: with no vector field configured "+
			"`embedding` is an ordinary string field", res.Accepted, res.Rejected)
	}
}

// The JSON-lines vector reject arm builds its message in Error(), not at
// construction, and says exactly what it used to say.
//
// `/_bulk` and `/insert/jsonline` both call IngestJSONLinesOpts, so this arm
// is per document on the route that carries the most of them, and past the
// 32nd warning the message it builds is dropped unread. The list on
// tsRangeError claimed of every member that "none is on the `_bulk` path,
// which is why none was converted"; this one was, and was not on the list.
//
// The old implementation is the specification: the message is compared against
// the `fmt.Errorf` it replaced rather than against a string typed here, so a
// change of wording is a failure and not a silent divergence from what a
// client used to read on a partial ingest.
func TestTheVectorRejectMessageIsDeferred(t *testing.T) {
	const field, dim = "emb", 4
	want := fmt.Errorf("%w: %s has an unusable value for a %d-dimension field",
		ErrVector, field, dim)
	got := error(&vecShapeError{field: field, dim: dim})

	if got.Error() != want.Error() {
		t.Errorf("message changed:\n  now:  %s\n  was:  %s", got, want)
	}
	if !errors.Is(got, ErrVector) {
		t.Error("errors.Is(err, ErrVector) is false; the reject arm and the status " +
			"mapping both ask it")
	}

	// The two constructions, measured both ways in ONE run so the comparison is
	// not against a number written down in another session.
	var sink error
	deferred := testing.AllocsPerRun(20000, func() {
		sink = &vecShapeError{field: field, dim: dim}
	})
	formatted := testing.AllocsPerRun(20000, func() {
		sink = fmt.Errorf("%w: %s has an unusable value for a %d-dimension field",
			ErrVector, field, dim)
	})
	_ = sink
	if deferred >= formatted {
		t.Errorf("the deferred form costs %.3f allocations and fmt.Errorf costs %.3f. "+
			"Deferring the message is the whole point: it must be cheaper on the "+
			"reject arm, where the string is discarded past the 32nd warning.",
			deferred, formatted)
	}
	t.Logf("construction: deferred %.3f allocs, fmt.Errorf %.3f allocs", deferred, formatted)

	// AND THE REAL PATH, because the construction being cheaper proves nothing
	// about which one the parser calls. 2,000 documents whose embedding is the
	// wrong length -- the shape a `/_bulk` of bad vectors takes -- through
	// IngestJSONLinesOpts, interleaved A/B in one session, three rounds each,
	// every figure identical across them:
	//
	//	allocations per rejected record   fmt.Errorf   this
	//	                                    20.027    18.043
	//
	// The delta is 1.984 rather than the 1.000 the constructions differ by,
	// because the real `key` is a dynamic string and boxing it into the
	// variadic allocates as well; the constant in the probe above does not.
	// The bound below sits between the two measurements with room for
	// unrelated drift on the parse path, and a revert of the call site alone
	// crosses it.
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	w.SetVectorFields(VectorFields{field: dim})
	const n = 2000
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"_time":1700000000000000000,"_msg":"m%d","emb":[1,2,3]}`+"\n", i)
	}
	body := []byte(sb.String())
	fallback := func() int64 { return 1 }
	perRecord := testing.AllocsPerRun(20, func() {
		res, err := IngestJSONLinesOpts(w, body, fallback, nil)
		if err != nil || res.Rejected != n || res.Accepted != 0 {
			t.Fatalf("the fixture did not reject every record: accepted=%d rejected=%d err=%v",
				res.Accepted, res.Rejected, err)
		}
	}) / float64(n)
	t.Logf("real path: %.4f allocations per rejected record", perRecord)
	if perRecord >= 19.5 {
		t.Errorf("%.4f allocations per rejected record. The deferred form measures "+
			"18.043 here and fmt.Errorf measures 20.027; anything at or above 19.5 "+
			"means the reject arm is building its message again, on a path /_bulk "+
			"reaches and where the message is discarded past the 32nd warning.",
			perRecord)
	}
}
