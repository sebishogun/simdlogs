package ingest

import (
	"fmt"
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
