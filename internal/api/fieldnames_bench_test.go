package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// benchServer ingests a realistic-width record set: many groups, several
// columns, one of them high cardinality.
func benchServer(b *testing.B, rows int) *httptest.Server {
	b.Helper()
	srv, err := NewServer(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	b.Cleanup(ts.Close)
	var body strings.Builder
	for i := 0; i < rows; i++ {
		// The width the gate measures: fifteen columns, several of them close to
		// unique per row, which is what makes a per-column footer lookup add up.
		fmt.Fprintf(&body,
			`{"_time":"2024-05-01T00:%02d:%02dZ","level":"%s","service":"s%d","host":"h%d","region":"r%d",`+
				`"pod":"p%d","container":"c%d","method":"m%d","path":"/api/v1/x/%d","status":"%d",`+
				`"user_id":"u%d","trace_id":"t%d","span_id":"s%d","latency_ms":"%d","bytes":"%d","_msg":"request handled"}`+"\n",
			(i/60)%60, i%60, []string{"error", "info"}[i%2], i%8, i%1024, i%6,
			i%8192, i%12, i%5, i%100000, 200+(i%5)*100,
			i%200000, i, i, i%400, 64+i%65536)
	}
	r, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body.String()))
	if err != nil {
		b.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	return ts
}

func benchGet(b *testing.B, ts *httptest.Server, path string) {
	b.ResetTimer()
	for b.Loop() {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func BenchmarkHTTPFieldNames(b *testing.B) {
	ts := benchServer(b, 200_000)
	benchGet(b, ts, "/select/logsql/field_names?query=*&start=1714521600&end=1714525200")
}

// BenchmarkHTTPHealth is the floor: the same round trip with no work behind it,
// so the endpoint's own cost is what the difference shows.
func BenchmarkHTTPHealth(b *testing.B) {
	ts := benchServer(b, 50_000)
	benchGet(b, ts, "/health")
}

func BenchmarkHTTPFieldValues(b *testing.B) {
	ts := benchServer(b, 50_000)
	benchGet(b, ts, "/select/logsql/field_values?query=*&field=level&start=1714521600&end=1714525200")
}
