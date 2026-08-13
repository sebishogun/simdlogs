package bench

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/sebishogun/simdlogs/internal/api"
)

// BenchmarkHTTPCommonSelect is the full server path for a big-result query:
// scan + materialize + NDJSON out over HTTP, which is what the VL head-to-head
// actually times.
func BenchmarkHTTPCommonSelect(b *testing.B) {
	srv, err := api.NewServer(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	postNDJSONB(b, ts.URL+"/insert/jsonline", clusterCorpusB(200_000))
	u := ts.URL + "/select/logsql/query?query=" + url.QueryEscape("level:=error")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(u)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func postNDJSONB(b *testing.B, url string, body []byte) {
	b.Helper()
	resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(body))
	if err != nil {
		b.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
