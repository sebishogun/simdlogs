package bench

import (
	"testing"
)

// TestDiskFootprint reports bytes on disk per stored row for the realistic
// corpus. Disk size is deterministic, so unlike a latency number it is worth
// reading on a busy machine.
//
//	SIMDLOGS_DISK=1 go test -run TestDiskFootprint -v ./internal/bench/
func TestDiskFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("disk footprint measurement")
	}
	const n = 200_000
	body := clusterCorpus(n, "NEEDLEdisk")

	srv, url, stop := startNode(t)
	defer stop()
	postNDJSON(t, url+"/insert/jsonline", body)
	if err := srv.Close(); err != nil { // flush and unmap before measuring
		t.Fatal(err)
	}

	dir := srv.Dir()
	size := dirSize(dir)
	t.Logf("simdlogs: %d rows, %d bytes on disk, %.2f bytes/row (ingest body %d bytes, %.2f bytes/row)",
		n, size, float64(size)/float64(n), len(body), float64(len(body))/float64(n))
}
