package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/ingest"
	"github.com/sebishogun/simdlogs/internal/query"
)

// bigNDJSON is a body over ingest.MinParallelBytes, so the handler takes the
// parallel branch rather than the persistent-writer one.
func bigNDJSON() string {
	var sb strings.Builder
	base := int64(1_700_000_000_000_000_000)
	for i := 0; sb.Len() < ingest.MinParallelBytes+(1<<16); i++ {
		fmt.Fprintf(&sb, `{"_time":%d,"level":"info","service":"api","host":"h%d","_msg":"filler filler filler filler %d"}`+"\n",
			base+int64(i)*1000, i%4, i)
	}
	return sb.String()
}

// blockTenantGroupIDs makes group creation fail inside the default tenant's
// store by occupying the temp-file names with directories.
func blockTenantGroupIDs(t *testing.T, dir string, n int) {
	t.Helper()
	tdir := filepath.Join(dir, "tenant-0-0")
	if err := os.MkdirAll(tdir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := os.MkdirAll(filepath.Join(tdir, fmt.Sprintf("group-%d.bin.tmp", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

// A large ingest whose rows cannot reach the store must not answer 200. This
// is the request-level contract; the ingest package's own tests cover the
// function, but only a handler test proves the error is not dropped between
// them -- which is exactly what used to happen.
func TestInsertJSONLineFailsWhenWritesFail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	blockTenantGroupIDs(t, dir, 64)
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(bigNDJSON()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 when no group could be written", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if body["error"] == nil {
		t.Fatalf("no error field in %v", body)
	}
	// Nothing landed, so nothing may be reported durable.
	if d, ok := body["durable"].(float64); !ok || d != 0 {
		t.Fatalf("durable %v, want 0", body["durable"])
	}
}

// Same contract on the Elasticsearch bulk path, which has its own copy of the
// branch.
func TestESBulkFailsWhenWritesFail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	blockTenantGroupIDs(t, dir, 64)
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Bulk format: an action line before every document.
	var sb strings.Builder
	for _, line := range strings.Split(strings.TrimRight(bigNDJSON(), "\n"), "\n") {
		sb.WriteString(`{"index":{}}` + "\n")
		sb.WriteString(line + "\n")
	}
	resp, err := http.Post(ts.URL+"/_bulk", "application/x-ndjson", strings.NewReader(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 when no group could be written", resp.StatusCode)
	}
}

// The parallel branch must inherit the deployment's stream fields, so a body
// above the threshold stores the same schema a small one does. This is the
// combination parallelCfg exists for, and nothing exercised it end to end.
func TestLargeAndSmallIngestAgreeOnSchema(t *testing.T) {
	t.Parallel()
	streamsFor := func(t *testing.T, body string) []string {
		t.Helper()
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		srv.SetStreamFields([]string{"service"})
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()

		resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if err := srv.def.w.Flush(); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range query.Run(srv.def.store, &query.Query{From: 0, To: int64(1) << 62, MatAll: true}) {
			for _, f := range r.Fields {
				if f.Key == "_stream" {
					out = append(out, f.Value)
				}
			}
		}
		return out
	}

	big := bigNDJSON()
	small := strings.Join(strings.Split(big, "\n")[:50], "\n") + "\n"

	smallStreams := streamsFor(t, small)
	bigStreams := streamsFor(t, big)

	if len(smallStreams) == 0 {
		t.Fatal("the small body produced no _stream values; the test proves nothing")
	}
	if len(bigStreams) == 0 {
		t.Fatal("the large body produced no _stream values -- the parallel path lost the configured stream fields")
	}
	want := smallStreams[0]
	for i, got := range bigStreams {
		if got != want {
			t.Fatalf("large-body _stream[%d] = %q, small body produced %q", i, got, want)
		}
	}
}
