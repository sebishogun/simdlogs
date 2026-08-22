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
// BOTH BRANCHES, because the machine picks one and it picked the passing one.
// `IngestJSONLinesParallelResult` runs the SERIAL fallback whenever
// runtime.NumCPU()/3 < 2 -- every host with fewer than six cores -- and that
// branch returned the parse-time Accepted alongside the write error while the
// sharded branch zeroes it. Measured on the unmutated tree before the fix:
//
//	taskset -c 0-2 / 0-3 / 0-4   FAIL  "durable 9698, want 0"
//	taskset -c 0-5 / 0-7         pass
//
// 9,698 rows reported durable to an operator when the store refused every
// group. Forcing the shard count is what makes the answer the machine's
// business rather than the code's.
func TestInsertJSONLineFailsWhenWritesFail(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the serial fallback", 1}, // below the 2-shard minimum
		{"the sharded path", 4},
		{"the derived shard count", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			blockTenantGroupIDs(t, dir, 64)
			srv, err := NewServer(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			srv.setIngestShardsForTest(tc.shards)
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()

			resp, err := http.Post(ts.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(bigNDJSON()))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			// 503, not 500, and not 200. A store that cannot be written to is
			// a retryable server failure like any other, and this path used to
			// answer a flat 500 with no Retry-After -- a different answer to
			// the same disk failure than a smaller body got, purely because it
			// crossed MinParallelBytes.
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status %d, want 503 when no group could be written", resp.StatusCode)
			}
			if got := resp.Header.Get("Retry-After"); got == "" {
				t.Fatal("no Retry-After on a retryable write failure")
			}
			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("response is not JSON: %v", err)
			}
			if body["error"] == nil {
				t.Fatalf("no error field in %v", body)
			}
			// Every shard failed, so nothing landed and a retry cannot duplicate.
			if dup, ok := body["duplicateOnRetry"].(bool); !ok || dup {
				t.Fatalf("duplicateOnRetry is %v with every shard failed", body["duplicateOnRetry"])
			}
			// Nothing landed, so nothing may be reported durable.
			if d, ok := body["durable"].(float64); !ok || d != 0 {
				t.Fatalf("durable %v, want 0", body["durable"])
			}
			if ing, ok := body["ingested"].(float64); !ok || ing != 0 {
				t.Fatalf("ingested %v, want 0: the same number under a different name",
					body["ingested"])
			}
		})
	}
}

// Same contract on the Elasticsearch bulk path, which has its own copy of the
// branch.
//
// BOTH BRANCHES HERE TOO. This sat four lines below the loop above and did not
// get the same treatment: with no override the shard count is
// runtime.NumCPU()/3, so on any host with fewer than six cores this ran the
// serial fallback and the sharded copy of the branch -- the one the comment
// above names -- was never reached. Lower impact than the loop above, because
// this asserts only 503 + Retry-After and both branches answer through
// failIngest, but the same omission.
func TestESBulkFailsWhenWritesFail(t *testing.T) {
	t.Parallel()

	// Bulk format: an action line before every document.
	var sb strings.Builder
	docBytes := 0
	for _, line := range strings.Split(strings.TrimRight(bigNDJSON(), "\n"), "\n") {
		sb.WriteString(`{"index":{}}` + "\n")
		sb.WriteString(line + "\n")
		docBytes += len(line) + 1
	}
	body := sb.String()

	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the serial fallback", 1}, // below the 2-shard minimum
		{"the sharded path", 4},
		{"the derived shard count", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The bulk parallel branch is keyed on len(DOCS), not len(body):
			// the action lines never reach the ingester.
			cfg := ingest.ParallelConfig{Shards: tc.shards}
			if tc.shards == 4 && cfg.ShardsFor(docBytes) < 2 {
				t.Fatalf("shards forced to 4 resolves to %d over %d document bytes: "+
					"this row runs the serial fallback", cfg.ShardsFor(docBytes), docBytes)
			}
			dir := t.TempDir()
			blockTenantGroupIDs(t, dir, 64)
			srv, err := NewServer(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			srv.setIngestShardsForTest(tc.shards)
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()

			resp, err := http.Post(ts.URL+"/_bulk", "application/x-ndjson", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status %d, want 503 when no group could be written", resp.StatusCode)
			}
			if got := resp.Header.Get("Retry-After"); got == "" {
				t.Fatal("no Retry-After on a retryable write failure")
			}
		})
	}
}

// The parallel branch must inherit the deployment's stream fields, so a body
// above the threshold stores the same schema a small one does. This is the
// combination parallelCfg exists for, and nothing exercised it end to end.
//
// AND THE SHARDED BRANCH IS ITS OWN CALL SITE. `cfg.apply(w)` appears twice in
// IngestJSONLinesParallelResult -- once on the serial fallback's single writer
// and once per shard goroutine -- so covering one is not covering the other,
// and with no override the machine picks. Measured with the SHARDED
// `cfg.apply(w)` deleted (the serial one left alone):
//
//	32 CPUs          FAIL
//	taskset -c 0-3   ok
//
// The large body took the serial fallback at four cores -- runtime.NumCPU()/3
// is 1 -- and compared it against the small body's persistent writer, which is
// two paths that are not this test's subject.
func TestLargeAndSmallIngestAgreeOnSchema(t *testing.T) {
	t.Parallel()
	streamsFor := func(t *testing.T, body string, shards int) []string {
		t.Helper()
		srv, err := NewServer(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		srv.SetStreamFields([]string{"service"})
		srv.setIngestShardsForTest(shards)
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

	smallStreams := streamsFor(t, small, 0)
	if len(smallStreams) == 0 {
		t.Fatal("the small body produced no _stream values; the test proves nothing")
	}
	want := smallStreams[0]

	for _, tc := range []struct {
		name   string
		shards int
	}{
		{"the derived shard count", 0},
		{"shards forced to 4", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ingest.ParallelConfig{Shards: tc.shards}
			if tc.shards != 0 && cfg.ShardsFor(len(big)) < 2 {
				t.Fatalf("shards forced to %d resolves to %d over %d bytes: this row "+
					"runs the serial fallback and the per-shard writers are not built",
					tc.shards, cfg.ShardsFor(len(big)), len(big))
			}
			bigStreams := streamsFor(t, big, tc.shards)
			if len(bigStreams) == 0 {
				t.Fatal("the large body produced no _stream values -- the parallel path lost the configured stream fields")
			}
			for i, got := range bigStreams {
				if got != want {
					t.Fatalf("large-body _stream[%d] = %q, small body produced %q", i, got, want)
				}
			}
		})
	}
}
