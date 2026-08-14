package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/config"
)

func guardedServer(t *testing.T, tune func(*config.Config)) *httptest.Server {
	t.Helper()
	c := config.Default()
	c.Dir = t.TempDir()
	c.Limits = config.TestLimits()
	if tune != nil {
		tune(&c)
	}
	srv, err := NewServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return ts
}

// A wrong method is 405 with an Allow header naming what is allowed. The
// ingest handlers took any method, so a GET was processed as an empty POST
// and answered 200 with zero records.
func TestGuardRejectsWrongMethod(t *testing.T) {
	ts := guardedServer(t, nil)
	for _, path := range []string{"/insert/jsonline", "/insert/logfmt", "/_bulk", "/v1/logs"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("GET %s -> %d, want 405", path, resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow == "" || !strings.Contains(allow, "POST") {
			t.Errorf("GET %s Allow %q, want it to name POST", path, allow)
		}
	}
}

// An unsupported media type is 415 rather than a success with nothing
// ingested.
func TestGuardRejectsUnsupportedMediaType(t *testing.T) {
	ts := guardedServer(t, nil)
	resp, err := http.Post(ts.URL+"/insert/jsonline", "image/png", strings.NewReader("{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d, want 415", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("415 body is not JSON: %v", err)
	}
	if body["error"] == nil {
		t.Errorf("415 body has no error field: %v", body)
	}
}

// A body over the limit is 413. Unbounded, one request could hold an
// arbitrary amount of memory.
func TestGuardRejectsOversizedBody(t *testing.T) {
	ts := guardedServer(t, nil)
	lim := config.TestLimits().MaxBodyBytes
	big := strings.Repeat(`{"a":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`+"\n", int(lim/40)+64)
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413 for a %d-byte body over a %d-byte limit",
			resp.StatusCode, len(big), lim)
	}
}

// A body under the limit still works, so the guard is not simply refusing
// everything.
func TestGuardAcceptsNormalBody(t *testing.T) {
	ts := guardedServer(t, nil)
	body := `{"_time":1700000000000000000,"level":"info"}` + "\n"
	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
}

// Malformed gzip is 400, not a partial parse of whatever decompressed before
// the error.
func TestGuardRejectsMalformedGzip(t *testing.T) {
	ts := guardedServer(t, nil)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline",
		strings.NewReader("this is not gzip at all"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 for malformed gzip", resp.StatusCode)
	}
}

// A decompression bomb is 413 on the decompressed size. The wire limit alone
// does not catch it: a few hundred KB of gzip expands to gigabytes.
func TestGuardRejectsDecompressionBomb(t *testing.T) {
	t.Parallel()
	ts := guardedServer(t, nil)
	lim := config.TestLimits().MaxDecompressed

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	line := []byte(`{"_time":1700000000000000000,"msg":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` + "\n")
	for written := int64(0); written < lim*4; written += int64(len(line)) {
		if _, err := zw.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if int64(buf.Len()) > config.TestLimits().MaxBodyBytes {
		t.Skipf("compressed bomb is %d bytes, above the wire limit; the wire limit catches it first", buf.Len())
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline", bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413: %d compressed bytes expand past the %d-byte decompressed limit",
			resp.StatusCode, buf.Len(), lim)
	}
}

// A well-formed gzip body under both limits is accepted and ingested.
func TestGuardAcceptsGzip(t *testing.T) {
	ts := guardedServer(t, nil)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for i := 0; i < 20; i++ {
		fmt.Fprintf(zw, `{"_time":%d,"level":"info"}`+"\n", 1_700_000_000_000_000_000+int64(i))
	}
	zw.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 for valid gzip", resp.StatusCode)
	}
	var out map[string]int
	json.NewDecoder(resp.Body).Decode(&out)
	if out["ingested"] != 20 {
		t.Fatalf("ingested %d, want 20", out["ingested"])
	}
}

// An unknown Content-Encoding is refused rather than treated as identity,
// which would store compressed bytes as if they were log lines.
func TestGuardRejectsUnknownEncoding(t *testing.T) {
	ts := guardedServer(t, nil)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/insert/jsonline", strings.NewReader("{}\n"))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "br")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d, want 415 for an unknown Content-Encoding", resp.StatusCode)
	}
}

// The query surface still takes GET and POST.
func TestGuardLeavesQuerySurfaceUsable(t *testing.T) {
	ts := guardedServer(t, nil)
	resp, err := http.Get(ts.URL + "/select/logsql/query?query=*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET query -> %d, want 200", resp.StatusCode)
	}
}
