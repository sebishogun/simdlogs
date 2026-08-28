package bench

import (
	"bytes"
	"testing"
)

// TestDiskFootprint reports bytes on disk per stored row for the realistic
// corpus. Disk size is deterministic, so unlike a latency number it is worth
// reading on a busy machine.
//
//	go test -run TestDiskFootprint -v ./internal/bench/
//
// The body is POSTED IN CHUNKS. 200k rows of clusterCorpus is ~73 MB, over
// config.Default()'s 64 MiB MaxBodyBytes, so the single POST this used to make
// was 413'd on every run -- and postNDJSON discarded the status, so the test
// then measured an EMPTY store and logged "200000 rows, 0 bytes on disk, 0.00
// bytes/row" as a result. postNDJSON now fails on a non-2xx, which is what
// makes a repeat of this loud instead of silent.
func TestDiskFootprint(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("disk footprint measurement")
	}
	const n = 200_000
	body := clusterCorpus(n, "NEEDLEdisk")

	srv, url, stop := startNode(t)
	defer stop()
	for _, chunk := range splitNDJSON(body, 16<<20) {
		postNDJSON(t, url+"/insert/jsonline", chunk)
	}
	if err := srv.Close(); err != nil { // flush and unmap before measuring
		t.Fatal(err)
	}

	dir := srv.Dir()
	size := dirSize(dir)
	if size == 0 {
		t.Fatal("0 bytes on disk after ingesting the corpus: nothing was stored, " +
			"so there is no footprint to report")
	}
	t.Logf("simdlogs: %d rows, %d bytes on disk, %.2f bytes/row (ingest body %d bytes, %.2f bytes/row)",
		n, size, float64(size)/float64(n), len(body), float64(len(body))/float64(n))
}

// splitNDJSON cuts an NDJSON body into chunks of at most max bytes, always on a
// line boundary so no record is split across two requests.
func splitNDJSON(body []byte, max int) [][]byte {
	var out [][]byte
	for len(body) > 0 {
		if len(body) <= max {
			out = append(out, body)
			break
		}
		cut := bytes.LastIndexByte(body[:max], '\n')
		if cut < 0 {
			// One line longer than max: send it alone and let the server
			// decide, rather than splitting a record in half.
			cut = bytes.IndexByte(body, '\n')
			if cut < 0 {
				out = append(out, body)
				break
			}
		}
		out = append(out, body[:cut+1])
		body = body[cut+1:]
	}
	return out
}
