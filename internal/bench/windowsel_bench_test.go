package bench

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/api"
)

// BenchmarkWindowedSelective is task #298's failing shape, isolated: a narrow
// window (2% of the range) plus an equality over the 3M harness corpus,
// returning ~15k full records over HTTP. The reference answers it in ~11ms and
// this server took ~20ms -- the one shape it still lost.
func BenchmarkWindowedSelective(b *testing.B) {
	nd, lo, hi := corpusNDJSON(3_000_000)
	srv, err := api.NewServer(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer srv.Close()
	sl := httptest.NewServer(srv.Handler())
	defer sl.Close()
	resp, err := http.Post(sl.URL+"/insert/jsonline", "application/x-ndjson", noCopyReader(nd))
	if err != nil {
		b.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	wlo := time.Unix(0, lo+(hi-lo)/2).UTC().Format(time.RFC3339Nano)
	whi := time.Unix(0, lo+(hi-lo)/2+(hi-lo)/50).UTC().Format(time.RFC3339Nano)
	qurl := sl.URL + "/select/logsql/query?" +
		url.Values{"query": {"service:=auth"}, "start": {wlo}, "end": {whi}}.Encode()

	// One warm call to report the response size, so a change here cannot
	// quietly shrink the work.
	r0, err := http.Get(qurl)
	if err != nil {
		b.Fatal(err)
	}
	n0, _ := io.Copy(io.Discard, r0.Body)
	r0.Body.Close()
	b.Logf("response: %d bytes", n0)
	if n0 == 0 {
		b.Fatal("the windowed selective query returned nothing")
	}

	b.ResetTimer()
	for b.Loop() {
		r, err := http.Get(qurl)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
}

func noCopyReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct{ b []byte }

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

var _ = fmt.Sprintf
